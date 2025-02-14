-- TODO: turn this into a rolling model that includes a sample date (eg, every week)

MODEL (
  name metrics.int_superchain_s7_onchain_builder_eligibility,
  description "Determines if a project is eligible for measurement in the S7 onchain builder round",
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column sample_date,
    batch_size 90,
    batch_concurrency 1,
    lookback 7
  ),
  start '2015-01-01',
  cron '@daily',
  partitioned_by DAY("sample_date"),
  grain (sample_date, project_id)
);

@DEF(lookback_days, 180);
@DEF(single_chain_tx_threshold, 1000);
@DEF(multi_chain_tx_threshold, 1000);
@DEF(gas_fees_threshold, 0.1);
@DEF(user_threshold, 420);
@DEF(active_days_threshold, 10);

with base_events as (
  select
    events.project_id,
    events.chain,
    events.transaction_hash,
    events.from_artifact_id,
    events.event_type,
    events.gas_fee,
    date_trunc('day', events.block_timestamp) as bucket_day,
    coalesce(users.is_bot, false) as is_bot
  from metrics.int_superchain_trace_level_events_by_project as events
  left outer join metrics.int_superchain_onchain_user_labels as users
    on events.from_artifact_id = users.artifact_id
  where
    block_timestamp between @start_dt and @end_dt
    and date(block_timestamp) >= current_date - interval '@lookback_days' day
),

builder_metrics as (
  select
    project_id,
    count(distinct chain) as chain_count,
    count(distinct transaction_hash) as transaction_count_all_levels,
    count(distinct case when event_type = 'TRANSACTION_EVENT' then transaction_hash end)
      as transaction_count_txn_level_only,
    count(distinct case when event_type = 'TRACE_EVENT' then transaction_hash end)
      as transaction_count_trace_level_only,
    count(distinct case when event_type = 'AA_EVENT' then transaction_hash end)
      as transaction_count_aa_only,
    sum(gas_fee) as gas_fees_all_levels,
    sum(case when event_type = 'TRANSACTION_EVENT' then gas_fee else 0 end)
      as gas_fees_txn_level_only,
    count(distinct from_artifact_id) as user_count,
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
          then transaction_count_all_levels >= @multi_chain_tx_threshold
        else transaction_count_all_levels >= @single_chain_tx_threshold
      end)
      and gas_fees_all_levels >= @gas_fees_threshold
      and bot_filtered_user_count >= @user_threshold
      and active_days >= @active_days_threshold
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
  current_timestamp() as sample_date -- PLACEHOLDER: we should be checking eligibility on a daily or weekly basis
from builder_metrics
inner join project_eligibility
  on builder_metrics.project_id = project_eligibility.project_id
