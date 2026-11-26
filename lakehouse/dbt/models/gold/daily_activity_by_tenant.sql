-- Daily activity per tenant. Currency is part of the grain so amounts of different currencies
-- are never added together (today every event is COP).
select
    cast(occurred_at as date)               as activity_date,
    tenant_id,
    currency,
    count(*)                                as transactions,
    sum(amount)                             as total_amount,
    cast(avg(amount) as decimal(19, 4))     as avg_amount
from {{ ref('audit_events') }}
group by 1, 2, 3
