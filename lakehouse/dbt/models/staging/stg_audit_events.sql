-- Typed view over bronze. No dedupe here (silver does it); one row per Kafka record.
-- Money: the payload carries amounts as exact JSON strings ("amount":"12345678.1234", ADR-001),
-- cast straight from varchar to decimal(19,4): never through double, so no rounding.
select
    event_id,
    event_type,
    aggregate_type,
    aggregate_id,
    tenant_id,
    actor_id,
    occurred_at,
    schema_version,
    json_extract_scalar(payload, '$.transactionId')                       as transaction_id,
    json_extract_scalar(payload, '$.from')                                as from_account,
    json_extract_scalar(payload, '$.to')                                  as to_account,
    cast(json_extract_scalar(payload, '$.amount') as decimal(19, 4))      as amount,
    upper(json_extract_scalar(payload, '$.currency'))                     as currency,
    trace_id,
    payload,
    kafka_partition,
    kafka_offset,
    ingested_at
from {{ source('bronze', 'events_bronze') }}
