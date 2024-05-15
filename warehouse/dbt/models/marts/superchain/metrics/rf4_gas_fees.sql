select
  project_id,
  'gas_fees' as metric,
  SUM(amount / 1e18) as amount
from {{ ref('rf4_events_daily_to_project') }}
where
  event_type = 'CONTRACT_INVOCATION_DAILY_L2_GAS_USED'
group by
  project_id
