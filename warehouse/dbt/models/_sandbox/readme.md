# **Optimism Retro Funding Season 7: Onchain Activity Metrics Pipeline**

## **Introduction**

This document details the data pipeline that measures onchain and TVL activity for **Optimism Retro Funding Season 7**. As with most pipelines, the ETL follows three steps:

1. **Staging**: Raw data is ingested from a public source and stored in the data lake.
2. **Intermediate Modeling**: The data is transformed into a usable format for final analysis.
3. **Metrics**: The transformed event data is used for defining metrics and for creating a final table of relevant, aggregate metrics by project.

The entire pipeline is public and fully open source.

## Data Sources

The pipeline currently combines data from the following primary raw sources:

1. **Optimism Superchain dataset**. Provided by Goldsky and maintained by OP Labs data team.
2. **Project : address registry**. Maintained by OSO (OSS Directory). This will soon include the new project registry maintained by Agora (OP Atlas) as an additional project namespace.
3. **Bot detection**. Algorithm developed by OP Labs data team, and implemented by OSO. Seems best at identifying MEV and algorithmic trading bots.
4. **Farcaster user : address registry**. Maintained by Farcaster. Identifies all Ethereum addresses linked to a given Farcaster profile.

The following datasets are not yet fully connected to the pipeline but will be soon:

1. **OP Labs project registry**. As mentioned above, this is the new project registry indexed by Agora (and available in raw form on Ethereum Attestation Service). It links project identities to their corresponding onchain addresses and other relevant artifacts.
2. **DefiLlama TVL**. Open source project that contains information about the TVL of DeFi and bridge protocols, split by chain and token. OSO ingests data directly from the DefiLlama REST API for all protocols of interest.
3. **Account Abstraction Datasets**. A working group comprised of members from the eth-infinitism team, AA operators and protocol engineers, BundleBear, TokenGuard, OP Labs, OSO, and others, is developing a set of public datasets that will also be used for Retro Funding Season 7. The dataset will include AA UserOps (from decoded traces) and a registry of labeled operators.

These datasets are ingested and connected by OSO to derive impact metrics for each project.

## Sandbox Pipeline

This metrics pipeline operates in a sandbox environment for testing and refinement.

At a high level, the pipeline:

- Processes raw blockchain data from 20+ chains in the Optimism Superchain ecosystem
- Uses a series of models to transform raw transaction and trace data and link events to projects
- Implements various filtering and weighting mechanisms to enrich the data and create opportunities to reduce the influence of bots and/or spam
- Computes a set of metrics that are used to evaluate the impact of projects on the Optimism Superchain

---

### Staging Models: Raw Blockchain Data

Two primary staging models process the Superchain dataset. Currently, the pipeline only considers transactions that produce stage changes on the network.

1. **Transactions** (`stg_superchain__transactions.sql`):

   - Includes only mainnet transactions
   - Requires successful transaction status (receipt_status = 1)
   - Requires positive gas usage (gas > 0)
   - Standardizes chain names (e.g., 'op' → 'OPTIMISM', 'fraxtal' → 'FRAX')

2. **Traces** (`stg_superchain__traces.sql`):
   - Includes only successful traces (status = 1)
   - Filters for specific call types: 'delegatecall' and 'call'
   - Requires positive gas usage (gas > 0)
   - Maintains chain name consistency with transaction model

#### Key Fields Retained

- Transaction metadata: `hash`, `block_timestamp`, `chain`
- Address information: `from_address`, `to_address`
- Economic data: `gas_used`, `gas_price`

---

### Intermediate Models: Connecting Events to Projects

#### Transaction-Trace Integration (`int_superchain_traces_txs_joined.sql`)

This model creates a unified view of blockchain activity by joining transactions with their associated traces:

- **Join Logic**: Join of transactions to traces is on `transaction_hash`
- **Address Preservation**:
  - Transaction addresses: `from_address_tx`, `to_address_tx`
  - Trace addresses: `from_address_trace`, `to_address_trace`
- **Gas Calculations**:
  - Preserves gas usage from both transaction and trace level
  - Computes ETH cost as: `gas_used_tx * gas_price_tx / 1e18`

#### Project Event Attribution (`int_superchain_s7_events_by_project.sql`)

This model connects blockchain events to specific projects and implements a weighted attribution system:

1. **Project Address Matching**:

   - Matches `to_address` fields (transaction and trace) against known project addresses
   - Removes any transactions that do not match a project address on the `to` side
   - Does not look at the `from` side of the transaction for project attribution
   - Uses distinct project IDs from the OSO directory
   - Will soon integrate with OP Atlas project registry

2. **Event Classification**:

   - `TRANSACTION_EVENT`: Direct contract interactions (to_address_tx matches)
   - `TRACE_EVENT`: Internal contract calls (to_address_trace matches)

3. **Attribution Weighting**:
   - Base split: 50% to transaction events, 50% to trace events
   - Weight per category = 0.5 / (number of distinct projects in category)
   - Example: For a transaction with 1 direct and 2 trace interactions:
     - Direct interaction receives 0.5 weight
     - Each trace interaction receives 0.25 weight (0.5/2)

#### User Classification (`int_superchain_s7_onchain_user_labels.sql`)

This model enriches transaction data with user context:

1. **Farcaster Integration**:

   - Boolean flag `is_farcaster_user`
   - Identifies addresses linked to Farcaster profiles

2. **Bot Detection**:
   - Boolean flag `is_bot`
   - Based on OP Labs' bot detection algorithm
   - Primarily catches MEV and trading bots

### Metrics Computation

#### Full Metrics by Project (`int_superchain_s7_onchain_metrics_by_project.sql`)

The final model computes various metrics for each project:

1. **Transaction Counts**:

   - `transaction_count`: Raw count of direct interactions (unique `transaction_hash`)
   - `trace_count`: Count of internal calls (unique `transaction_hash` associated with a trace)
   - `*_bot_filtered`: Versions excluding bot interactions
   - `transaction_count_amortized_bot_filtered`: Weighted by project involvement

2. **Gas Usage**:

   - `transaction_gas_fee`: Total ETH spent on gas
   - `amortized_gas_fee`: Gas costs weighted by project involvement
   - `amortized_gas_fee_bot_filtered`: Gas costs weighted by project involvement, excluding bot interactions

3. **User Activity**:
   - `monthly_active_addresses`: Unique interacting addresses
   - `monthly_active_addresses_bot_filtered`: Excludes bot addresses
   - `monthly_active_farcaster_users`: Farcaster-linked addresses only

All metrics are computed:

- Per project
- Per chain
- Aggregated monthly (using `timestamp_trunc(block_timestamp, month)`). This will be replaced with a SQLMesh-based 30-day rolling window.

#### Builder Eligibility (`int_superchain_s7_onchain_builder_eligibility.sql`)

This model determines project eligibility for builder rewards based on several criteria:

1. **Time Window**:
   - Looks back 180 days from current date
2. **Activity Thresholds**:
   - Multi-chain projects (active on >1 chain):
     - Minimum 1,000 transactions
   - Single-chain projects:
     - Minimum 10,000 transactions
   - All projects must meet:
     - Minimum 0.1 ETH in gas fees
     - At least 420 unique users
     - Active on 60 or more distinct days (out of 180)

### Current Limitations

1. **Project Recognition**: Limited to projects in OSO directory. Will expand with OP Atlas integration.

2. **Bot Detection**: Focuses on specific bot patterns. May miss sophisticated bots. Regular updates needed as patterns evolve. Submit a PR!

3. **Project Amortization**: The method for attribition has a bias towards protocols with simpler internal interactions. For an instance, an ERC20 in a liquidity pool with two tokens would receive a weight of 0.5/2 = 0.25, whereas an ERC20 in a vault with 100 tokens would receive a weight of 0.5/100 = 0.005. On the other hand, both the pair pool and the vault project do receive a transaction weight of 0.5.

### Coming Soon

- Integration with OP Atlas project registry
- DefiLlama TVL metrics
- Account Abstraction (UserOps) data

This documentation is maintained alongside the codebase and will be updated as the pipeline evolves.
