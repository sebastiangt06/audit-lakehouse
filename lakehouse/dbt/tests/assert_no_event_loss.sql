{{ config(store_failures=true, schema='dq_failures') }}
-- Zero-loss reconciliation across systems (Trino cross-catalog): every event the outbox reports
-- as dispatched (Kafka acked) must exist in silver. Returns the missing events; any row fails.
-- With store_failures they are kept in lake.dq_failures.assert_no_event_loss for inspection.
-- Run it after ingestion caught up (make demo waits for bronze before dbt build).
select
    o.event_id,
    o.event_type,
    o.tenant_id,
    o.occurred_at,
    o.processed_at
from {{ ref('stg_outbox_dispatched') }} as o
where not exists (
    select 1
    from {{ ref('audit_events') }} as s
    where s.event_id = o.event_id
)
