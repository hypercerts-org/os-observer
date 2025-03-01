MODEL (
  name metrics.stg_superchain__4337_userop_logs,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column block_timestamp,
    batch_size 180,
    batch_concurrency 1,
    lookback 7
  ),
  start '2021-10-01',
  cron '@daily',
  partitioned_by (DAY("block_timestamp"), "chain"),
  grain (block_timestamp, chain, transaction_hash, userop_hash, sender_address, paymaster_address, contract_address)
);

select
  @from_unix_timestamp(block_timestamp) as block_timestamp,
  transaction_hash,
  userophash as userop_hash,
  sender as sender_address,
  paymaster as paymaster_address,
  contract_address,
  CAST(actualGasCost AS DECIMAL(38, 0)) as userop_gas_price,
  CAST(actualGasUsed AS DECIMAL(38, 0)) as userop_gas_used,
  @chain_name(chain) as chain
from @oso_source('bigquery.optimism_superchain_4337_account_abstraction_data.useroperationevent_logs_v1')
where
  network = 'mainnet'
  and success = true
  -- Bigquery requires we specify partitions to filter for this data source
  and dt between @start_dt and @end_dt 