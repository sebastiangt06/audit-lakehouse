#!/usr/bin/env bash
# End-to-end demo: .NET core -> outbox -> Kafka -> Glue streaming -> Iceberg bronze -> dbt silver/gold,
# with reconciliation at every layer. Works from scratch (make clean && make up && make demo).
# Usage: bash scripts/e2e_demo.sh   (or: make demo)   Env: N (1000), P (20), TIMEOUT seconds (180)
set -euo pipefail

# Git Bash on Windows rewrites /opt/... arguments into C:/... paths; disable that.
export MSYS_NO_PATHCONV=1

cd "$(dirname "$0")/.."

N="${N:-1000}"
P="${P:-20}"
TIMEOUT="${TIMEOUT:-180}"

if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  . ./.env
  set +a
fi
TOPIC="${AUDIT_TOPIC:-audit.events}"

step() { printf '\n==> %s\n' "$*"; }
# Query errors stay visible on stderr; only the CLI's non-interactive terminal WARNING is dropped.
trino_tsv() { docker compose exec -T trino trino --output-format TSV --execute "$1" 2> >(grep -v '^WARNING' >&2); }
trino_table() { docker compose exec -T trino trino --output-format ALIGNED --execute "$1" 2> >(grep -v '^WARNING' >&2); }

# Same normalization as the dbt model stg_outbox_dispatched (uniqueidentifier -> lower varchar(36)).
OUTBOX_EVENT_ID="lower(cast(o.eventid as varchar(36)))"

step "1/5 Services up and healthy (first run pulls ~12 GB for the Glue image)"
docker compose up -d --wait

step "2/5 Load: $N transactions, parallel $P"
docker compose run --rm loadgen --count "$N" --parallel "$P"

step "3/5 Waiting until bronze has every event the outbox dispatched (timeout ${TIMEOUT}s, trigger ${INGEST_TRIGGER_SECONDS:-60}s)"
deadline=$((SECONDS + TIMEOUT))
while :; do
  read -r pending missing < <(trino_tsv "
    select
      (select count(*) from sqlserver.outbox.outboxmessages where processedat is null),
      (select count(*) from sqlserver.outbox.outboxmessages o
        where o.processedat is not null
          and not exists (select 1 from lake.audit.events_bronze b where b.event_id = ${OUTBOX_EVENT_ID}))") || true
  echo "    outbox pending=${pending:-?}  dispatched missing in bronze=${missing:-?}  (${SECONDS}s)"
  if [ "${pending:-}" = "0" ] && [ "${missing:-}" = "0" ]; then
    break
  fi
  if (( SECONDS >= deadline )); then
    echo "TIMEOUT: bronze did not catch up with the outbox in ${TIMEOUT}s (docker compose logs glue-ingest)" >&2
    exit 1
  fi
  sleep 10
done

step "4/5 dbt: source freshness + build (staging -> silver -> gold + tests)"
docker compose run --rm dbt source freshness
if ! docker compose run --rm dbt build; then
  echo "dbt build FAILED. Events dispatched by the outbox but missing in silver (if any):" >&2
  trino_table "select * from lake.dq_failures.assert_no_event_loss limit 20" >&2 || true
  exit 1
fi

step "5/5 Summary"
kafka_total=$(docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server kafka:9092 --topic "$TOPIC" --time -1 | awk -F: '{s += $3} END {print s}')
# Trino does not allow subqueries inside VALUES: one SELECT per layer, UNION ALL.
trino_table "
  select layer, events from (
              select 1 as ord, 'SQL Server core.transactions' as layer, count(*) as events from sqlserver.core.transactions
    union all select 2, 'Outbox dispatched',            count(*) from sqlserver.outbox.outboxmessages where processedat is not null
    union all select 3, 'Kafka ${TOPIC} (end offsets)', cast(${kafka_total} as bigint)
    union all select 4, 'Bronze rows',                  count(*) from lake.audit.events_bronze
    union all select 5, 'Bronze distinct event_id',     count(distinct event_id) from lake.audit.events_bronze
    union all select 6, 'Silver (deduplicated)',        count(*) from lake.silver.audit_events
  ) order by ord"

echo
echo "Gold: top 5 tenant-days by total amount"
trino_table "
  select activity_date, tenant_id, currency, transactions, total_amount, avg_amount
  from lake.gold.daily_activity_by_tenant order by total_amount desc limit 5"

echo
echo "Gold: top 5 high-value transactions (>= 10,000,000 COP)"
trino_table "
  select occurred_at, tenant_id, from_account, to_account, amount, currency
  from lake.gold.high_value_transactions order by amount desc limit 5"
