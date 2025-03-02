MODEL (
  name metrics.int_deployers_by_project,
  kind FULL,
  partitioned_by "artifact_namespace",
  description "Combines deployers from any EVM chain and chain-specific deployers"
);

with base_deployers as (
  select distinct
    project_id,
    artifact_source,
    artifact_name
  from metrics.int_artifacts_by_project_all_sources
  where artifact_type = 'DEPLOYER'
),

any_evm_matches as (
  select distinct
    base.project_id,
    deployers.chain as artifact_source,
    deployers.deployer_address as artifact_name
  from metrics.int_deployers as deployers
  inner join base_deployers as base
    on deployers.deployer_address = base.artifact_name
  where base.artifact_source = 'ANY_EVM'
),

chain_specific_matches as (
  select distinct
    base.project_id,
    deployers.chain as artifact_source,
    deployers.deployer_address as artifact_name
  from metrics.int_deployers as deployers
  inner join base_deployers as base
    on deployers.deployer_address = base.artifact_name
    and deployers.chain = base.artifact_source
  where base.artifact_source != 'ANY_EVM'
),

all_deployers as (
  select * from any_evm_matches
  union all
  select * from chain_specific_matches
)

select distinct
  project_id,
  @oso_id(artifact_source, artifact_name) as artifact_id,
  artifact_source,
  artifact_name as artifact_source_id,
  LOWER(artifact_source) as artifact_namespace,
  artifact_name
from all_deployers
