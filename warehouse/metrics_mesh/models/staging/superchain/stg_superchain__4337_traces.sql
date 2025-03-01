MODEL (
  name metrics.stg_superchain__4337_traces,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column block_timestamp,
    batch_size 180,
    batch_concurrency 1,
    lookback 7
  ),
  start '2021-10-01',
  cron '@daily',
  partitioned_by (DAY("block_timestamp"), "chain"),
  grain (block_timestamp, chain, transaction_hash, userop_hash, from_address, to_address, sender_address, paymaster_address, method_id)
);

select
  @from_unix_timestamp(block_timestamp) as block_timestamp,
  transaction_hash,
  userop_hash,
  from_address,
  to_address,
  userop_sender as sender_address,
  userop_paymaster as paymaster_address,
  CAST(useropevent_actualgascost AS DECIMAL(38, 0)) as userop_gas_price,
  CAST(useropevent_actualgasused AS DECIMAL(38, 0)) as userop_gas_used,
  CAST(value AS DECIMAL(38, 0)) as value,
  method_id,
  @chain_name(chain) as chain
from @oso_source('bigquery.optimism_superchain_4337_account_abstraction_data.enriched_entrypoint_traces_v1')
where
  network = 'mainnet'
  and status = '1'
  and trace_type in ('call', 'create', 'create2')
  and call_type != 'staticcall'
  and useropevent_success = true
  -- Bigquery requires we specify partitions to filter for this data source
  and dt between @start_dt and @end_dt 