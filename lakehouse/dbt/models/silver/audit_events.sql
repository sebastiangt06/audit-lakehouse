{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='event_id',
        on_schema_change='append_new_columns',
        properties={
            "format": "'PARQUET'",
            "partitioning": "ARRAY['day(occurred_at)']",
        },
    )
}}

{#-
  Silver = deduplicated truth (ADR-005): exactly one row per event_id, the FIRST arrival
  (lowest kafka_offset) of an at-least-once stream.

  Incremental batch, three decisions:

  1. Watermark on ingested_at (arrival), not occurred_at (business time). A late event, e.g. after
     an ingestion outage, a replay (step 10) or the backfill of legacy months (step 09), carries an
     OLD occurred_at; filtering on occurred_at would skip it forever. The 1h lookback covers
     micro-batches committed out of order around the watermark.

  2. Event_ids already in silver are excluded (anti-join). A redelivery has a higher kafka_offset;
     letting MERGE update the existing row would replace the first arrival with a later copy. With
     the anti-join the merge only inserts, and re-running is idempotent.

  3. The anti-join only reads the silver partitions of the batch's days. An event is immutable, so a
     redelivery keeps its occurred_at: if a batch event is already in silver, it is in a partition
     between the batch's min and max occurred_at. Those bounds are computed BEFORE building the SQL
     (run_query) and rendered as literals, so Iceberg prunes partitions at planning time instead of
     scanning all of silver on every run (a subquery bound would not be pruned statically).
-#}

{%- set watermark = none -%}
{%- set batch_min = none -%}
{%- set batch_max = none -%}
{%- if is_incremental() and execute -%}
    {%- set watermark = run_query("select cast(max(ingested_at) as varchar) from " ~ this).rows[0][0] -%}
    {%- if watermark is not none -%}
        {%- set bounds = run_query(
            "select cast(min(occurred_at) as varchar), cast(max(occurred_at) as varchar) from "
            ~ ref('stg_audit_events')
            ~ " where ingested_at >= timestamp '" ~ watermark ~ "' - interval '" ~ var('silver_lookback_hours') ~ "' hour"
        ).rows[0] -%}
        {%- set batch_min = bounds[0] -%}
        {%- set batch_max = bounds[1] -%}
    {%- endif -%}
{%- endif %}

with batch as (
    select *
    from {{ ref('stg_audit_events') }}
    {%- if watermark is not none %}
    where ingested_at >= timestamp '{{ watermark }}' - interval '{{ var("silver_lookback_hours") }}' hour
    {%- endif %}
),

first_arrival as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by event_id
                order by kafka_offset, kafka_partition, ingested_at
            ) as arrival_rank
        from batch
    )
    where arrival_rank = 1
)

select
    event_id,
    event_type,
    aggregate_type,
    aggregate_id,
    tenant_id,
    actor_id,
    occurred_at,
    schema_version,
    transaction_id,
    from_account,
    to_account,
    amount,
    currency,
    trace_id,
    payload,
    kafka_partition,
    kafka_offset,
    ingested_at
from first_arrival as f
{%- if batch_min is not none %}
where not exists (
    select 1
    from {{ this }} as s
    where s.occurred_at between timestamp '{{ batch_min }}' and timestamp '{{ batch_max }}'
      and s.event_id = f.event_id
)
{%- endif %}
