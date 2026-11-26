{{ config(materialized='ephemeral') }}
-- Events the outbox reports as delivered to Kafka (processedat set after the broker ack).
-- Trino maps uniqueidentifier to char(36) in UPPER case; Kafka/bronze carry lower case.
select
    lower(cast(eventid as varchar(36))) as event_id,
    eventtype                           as event_type,
    tenantid                            as tenant_id,
    occurredat                          as occurred_at,
    processedat                         as processed_at
from {{ source('core_outbox', 'outboxmessages') }}
where processedat is not null
