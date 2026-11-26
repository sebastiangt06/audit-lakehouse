-- Regulatory example: transfers >= 10,000,000 COP (threshold in dbt_project.yml vars).
select
    event_id,
    transaction_id,
    occurred_at,
    tenant_id,
    actor_id,
    from_account,
    to_account,
    amount,
    currency
from {{ ref('audit_events') }}
where currency = 'COP'
  and amount >= {{ var('high_value_threshold_cop') }}
