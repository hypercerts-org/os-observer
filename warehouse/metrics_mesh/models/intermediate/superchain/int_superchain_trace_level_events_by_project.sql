MODEL (
  name metrics.int_superchain_trace_level_events_by_project,
  description 'Events (down to trace level) involving known contracts with project-level attribution',
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column block_timestamp,
    batch_size 90,
    batch_concurrency 1,
    lookback 7
  ),
  start '2015-01-01',
  cron '@daily',
  partitioned_by (DAY("block_timestamp"), "chain"),
  grain (
    block_timestamp,
    chain,
    transaction_hash,
    event_type,
    project_id,
    from_artifact_id
  )
);

-- Step 1: Get joined traces and transactions
with base_transactions as (
  select
    block_timestamp,
    chain,
    transaction_hash,
    from_address_tx,
    to_address_trace,
    to_address_tx,
    gas_used_tx * gas_price_tx / 1e18 as gas_fee,
    @oso_id(chain, from_address_tx) as from_address_tx_id,
    @oso_id(chain, to_address_trace) as to_address_trace_id,
    @oso_id(chain, to_address_tx) as to_address_tx_id,
    exists(
      select 1
      from unnest(['0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789',
                  '0x0000000071727de22e5e9d8baf0edac6f37da032']) as address
      where address in (from_address_tx, to_address_trace, to_address_tx)
    ) as is_entry_point_interaction
  from metrics.int_superchain_traces_txs_joined
  where block_timestamp between @start_dt and @end_dt
),

-- Step 2: Precompute address-to-project mapping
addresses_by_project as (
  select distinct
    artifact_id,
    project_id
  from metrics.int_artifacts_by_project
),

-- Step 3: Compute transaction and trace events
transaction_level_events as (
  select distinct
    base_transactions.block_timestamp,
    base_transactions.chain,
    base_transactions.transaction_hash,
    base_transactions.from_address_tx_id as from_artifact_id,
    base_transactions.gas_fee,
    abp.project_id,
    case
      when base_transactions.is_entry_point_interaction then 'AA_EVENT'
      else 'TRANSACTION_EVENT'
    end as event_type
  from base_transactions
  inner join addresses_by_project as abp
    on base_transactions.to_address_tx_id = abp.artifact_id
),

trace_level_events as (
  select distinct
    base_transactions.block_timestamp,
    base_transactions.chain,
    base_transactions.transaction_hash,
    base_transactions.from_address_tx_id as from_artifact_id,
    base_transactions.gas_fee,
    abp.project_id,
    case
      when base_transactions.is_entry_point_interaction then 'AA_EVENT'
      else 'TRACE_EVENT'
    end as event_type
  from base_transactions
  inner join addresses_by_project as abp
    on base_transactions.to_address_trace_id = abp.artifact_id
)

-- Combine all events
select * from transaction_level_events
union all
select * from trace_level_events