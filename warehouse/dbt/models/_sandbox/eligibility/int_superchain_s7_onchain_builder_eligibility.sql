{{
  config(
    materialized='table'
  )
}}

{% set lookback_days = 180 %}
{% set single_chain_tx_threshold = 1000 %}
{% set multi_chain_tx_threshold = 1000 %}
{% set gas_fees_threshold = 0.1 %}
{% set user_threshold = 420 %}
{% set active_days_threshold = 10 %}


with base_events as (
  select
    events.project_id,
    events.chain,
    events.transaction_hash,
    events.from_artifact_id,
    events.event_type,
    events.gas_fee,
    date_trunc(events.block_timestamp, day) as bucket_day,
    coalesce(users.is_bot, false) as is_bot
  from {{ ref('int_superchain_trace_level_events_by_project') }} as events
  left outer join {{ ref('int_superchain_onchain_user_labels') }} as users
    on events.from_artifact_id = users.artifact_id
  where
    date(events.block_timestamp)
    >= date_sub(current_date(), interval {{ lookback_days }} day)
),

builder_metrics as (
  select
    project_id,
    count(distinct chain) as chain_count,
    approx_count_distinct(transaction_hash) as transaction_count_all_levels,
    approx_count_distinct(
      case
        when event_type = 'TRANSACTION_EVENT'
          then transaction_hash
      end
    ) as transaction_count_txn_level_only,
    approx_count_distinct(
      case
        when event_type = 'TRACE_EVENT'
          then transaction_hash
      end
    ) as transaction_count_trace_level_only,
    approx_count_distinct(
      case
        when event_type = 'AA_EVENT'
          then transaction_hash
      end
    ) as transaction_count_aa_only,
    sum(gas_fee) as gas_fees_all_levels,
    sum(case when event_type = 'TRANSACTION_EVENT' then gas_fee else 0 end)
      as gas_fees_txn_level_only,
    approx_count_distinct(from_artifact_id) as user_count,
    count(distinct case when is_bot = false then from_artifact_id end)
      as bot_filtered_user_count,
    count(distinct bucket_day) as active_days
  from base_events
  group by project_id
),

project_eligibility as (
  select
    project_id,
    (
      (case
        when chain_count > 1
          then transaction_count_all_levels >= {{ multi_chain_tx_threshold }}
        else transaction_count_all_levels >= {{ single_chain_tx_threshold }}
      end)
      and gas_fees_all_levels >= {{ gas_fees_threshold }}
      and bot_filtered_user_count >= {{ user_threshold }}
      and active_days >= {{ active_days_threshold }}
    ) as is_eligible
  from builder_metrics
)

select
  builder_metrics.project_id,
  builder_metrics.chain_count,
  builder_metrics.transaction_count_all_levels,
  builder_metrics.transaction_count_txn_level_only,
  builder_metrics.transaction_count_trace_level_only,
  builder_metrics.transaction_count_aa_only,
  builder_metrics.gas_fees_all_levels,
  builder_metrics.gas_fees_txn_level_only,
  builder_metrics.user_count,
  builder_metrics.bot_filtered_user_count,
  builder_metrics.active_days,
  project_eligibility.is_eligible,
  current_timestamp() as sample_date
from builder_metrics
inner join project_eligibility
  on builder_metrics.project_id = project_eligibility.project_id
