{{
  config(
    materialized='table',
    partition_by={
      "field": "block_timestamp",
      "data_type": "date",
      "granularity": "day",
    }
  )
}}

-- Step 1: Create entry points table
with entry_points as (
  select address
  from unnest([
    '0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789',
    '0x0000000071727de22e5e9d8baf0edac6f37da032'
  ]) as address
),

-- Step 2: Precompute address-to-project mapping
addresses_by_project as (
  select distinct
    artifact_id,
    project_id
  from {{ ref('int_artifacts_by_project') }}
),

-- Step 3: Join traces and transactions
base_transactions as (
  select
    block_timestamp,
    chain,
    transaction_hash,
    from_address_tx,
    to_address_trace,
    to_address_tx,
    gas_used_tx * gas_price_tx / 1e18 as gas_fee,
    {{ oso_id("chain", "from_address_tx") }} as from_address_tx_id,
    {{ oso_id("chain", "to_address_trace") }} as to_address_trace_id,
    {{ oso_id("chain", "to_address_tx") }} as to_address_tx_id,
    exists(
      select 1
      from entry_points
      where address in (from_address_tx, to_address_trace, to_address_tx)
    ) as is_entry_point_interaction
  from {{ ref('int_superchain_traces_txs_joined') }}
),

-- Step 4: Compute transaction and trace events
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
