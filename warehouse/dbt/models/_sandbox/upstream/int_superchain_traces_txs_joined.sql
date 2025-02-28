{{
  config(
    materialized='incremental',
    partition_by={
      "field": "block_timestamp",
      "data_type": "timestamp",
      "granularity": "day",
    },
    on_schema_change="append_new_columns",
    incremental_strategy="insert_overwrite"
  ) 
}}

with traces as (
  select
    block_timestamp,
    chain,
    gas_used,
    transaction_hash,
    from_address,
    to_address
  from {{ ref('stg_superchain__traces') }}
),

transactions as (
  select
    block_timestamp,
    chain,
    gas_used,
    gas_price,
    transaction_hash,
    from_address,
    to_address
  from {{ ref('stg_superchain__transactions') }}
)

select
  transactions.block_timestamp,
  transactions.transaction_hash,
  transactions.chain,
  transactions.from_address as from_address_tx,
  traces.from_address as from_address_trace,
  traces.to_address as to_address_trace,
  transactions.to_address as to_address_tx,
  transactions.gas_used as gas_used_tx,
  traces.gas_used as gas_used_trace,
  transactions.gas_price as gas_price_tx
from transactions
left join traces
  on transactions.transaction_hash = traces.transaction_hash
