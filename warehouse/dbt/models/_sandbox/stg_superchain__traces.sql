select
  dt as block_timestamp,
  transaction_hash,
  from_address,
  to_address,
  gas_used,
  upper(
    case
      when chain = 'op' then 'optimism'
      when chain = 'fraxtal' then 'frax'
      else chain
    end
  ) as chain
from {{ source('optimism_superchain_raw_onchain_data', 'traces') }}
where
  dt >= '2024-11-01'
  and network = 'mainnet'
  and `status` = 1
  and call_type in ('delegatecall', 'call')
  and gas_used > 0
