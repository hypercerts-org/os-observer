{{
  config(
    materialized='table'
  )
}}

{% set project_weight_per_tx_event = 0.5 %}
{% set project_weight_per_trace_event = 0.5 %}

with base_events as (
  select
    events.project_id,
    events.chain,
    events.transaction_hash,
    events.from_artifact_id,
    events.event_type,
    events.gas_fee,
    timestamp_trunc(events.block_timestamp, month) as sample_date,
    coalesce(users.is_bot, false) as is_bot,
    coalesce(users.is_farcaster_user, false) as is_farcaster_user
  from {{ ref('int_superchain_trace_level_events_by_project') }} as events
  inner join
    {{ ref('int_superchain_s7_onchain_builder_eligibility') }} as eligibility
    on events.project_id = eligibility.project_id
  left outer join {{ ref('int_superchain_onchain_user_labels') }} as users
    on events.from_artifact_id = users.artifact_id
  where eligibility.is_eligible = true
),

-- Calculate projects per event type and transaction
events_per_project as (
  select
    transaction_hash,
    event_type,
    count(distinct project_id) as num_projects_per_event
  from base_events
  group by
    transaction_hash,
    event_type
),

enriched_events as (
  select
    base_events.*,
    (case
      when base_events.event_type = 'TRANSACTION_EVENT'
        then {{ project_weight_per_tx_event }}
      when base_events.event_type = 'TRACE_EVENT'
        then {{ project_weight_per_trace_event }}
    end) / events_per_project.num_projects_per_event as project_weight
  from base_events
  inner join events_per_project
    on
      base_events.transaction_hash = events_per_project.transaction_hash
      and base_events.event_type = events_per_project.event_type
),

-- Transaction counts
transaction_count as (
  select
    project_id,
    chain,
    sample_date,
    'transaction_count' as metric_name,
    approx_count_distinct(transaction_hash) as amount
  from enriched_events
  where event_type = 'TRANSACTION_EVENT'
  group by 1, 2, 3
),

transaction_count_bot_filtered as (
  select
    project_id,
    chain,
    sample_date,
    'transaction_count_bot_filtered' as metric_name,
    approx_count_distinct(transaction_hash) as amount
  from enriched_events
  where
    event_type = 'TRANSACTION_EVENT'
    and is_bot = false
  group by 1, 2, 3
),

-- Trace counts
trace_count as (
  select
    project_id,
    chain,
    sample_date,
    'trace_count' as metric_name,
    approx_count_distinct(transaction_hash) as amount
  from enriched_events
  where event_type = 'TRACE_EVENT'
  group by 1, 2, 3
),

trace_count_bot_filtered as (
  select
    project_id,
    chain,
    sample_date,
    'trace_count_bot_filtered' as metric_name,
    approx_count_distinct(transaction_hash) as amount
  from enriched_events
  where
    event_type = 'TRACE_EVENT'
    and is_bot = false
  group by 1, 2, 3
),

-- Amortized transaction counts
transaction_count_amortized_bot_filtered as (
  select
    project_id,
    chain,
    sample_date,
    'transaction_count_amortized_bot_filtered' as metric_name,
    sum(project_weight) as amount
  from enriched_events
  where is_bot = false
  group by 1, 2, 3
),

-- Transaction network usage
transaction_gas_fee as (
  select
    project_id,
    chain,
    sample_date,
    'transaction_gas_fee' as metric_name,
    sum(gas_fee) as amount
  from enriched_events
  where event_type = 'TRANSACTION_EVENT'
  group by 1, 2, 3
),

-- Amortized gas fees
amortized_gas as (
  select
    project_id,
    chain,
    sample_date,
    'amortized_gas_fee' as metric_name,
    sum(gas_fee * project_weight) as amount
  from enriched_events
  group by 1, 2, 3
),

-- Amortized gas fees (bot filtered)
amortized_gas_bot_filtered as (
  select
    project_id,
    chain,
    sample_date,
    'amortized_gas_fee_bot_filtered' as metric_name,
    sum(gas_fee * project_weight) as amount
  from enriched_events
  where is_bot = false
  group by 1, 2, 3
),

-- Active users
monthly_active_farcaster_users as (
  select
    project_id,
    chain,
    sample_date,
    'monthly_active_farcaster_users' as metric_name,
    approx_count_distinct(from_artifact_id) as amount
  from enriched_events
  where is_farcaster_user = true
  group by 1, 2, 3
),

-- Active addresses
monthly_active_addresses as (
  select
    project_id,
    chain,
    sample_date,
    'monthly_active_addresses' as metric_name,
    approx_count_distinct(from_artifact_id) as amount
  from enriched_events
  group by 1, 2, 3
),

-- Active addresses (bot filtered)
monthly_active_addresses_bot_filtered as (
  select
    project_id,
    chain,
    sample_date,
    'monthly_active_addresses_bot_filtered' as metric_name,
    approx_count_distinct(from_artifact_id) as amount
  from enriched_events
  where is_bot = false
  group by 1, 2, 3
),

-- Union all metrics together
metrics_combined as (
  select * from transaction_count
  union all
  select * from transaction_count_bot_filtered
  union all
  select * from trace_count
  union all
  select * from trace_count_bot_filtered
  union all
  select * from transaction_count_amortized_bot_filtered
  union all
  select * from transaction_gas_fee
  union all
  select * from amortized_gas
  union all
  select * from amortized_gas_bot_filtered
  union all
  select * from monthly_active_farcaster_users
  union all
  select * from monthly_active_addresses
  union all
  select * from monthly_active_addresses_bot_filtered
)

select * from metrics_combined
