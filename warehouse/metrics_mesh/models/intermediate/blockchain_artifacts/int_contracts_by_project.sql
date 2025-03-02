MODEL (
  name metrics.int_contracts_by_project,
  kind FULL,
  partitioned_by "artifact_namespace",
  description "Combines directly associated contracts and derived contracts from factory deployments"
);

with base_contracts as (
  select distinct
    project_id,
    artifact_source,
    artifact_name
  from metrics.int_artifacts_by_project_all_sources
  where artifact_type = 'CONTRACT'
),

contracts_from_deployers as (
  select distinct
    deployers.project_id,
    derived.chain as artifact_source,
    derived.contract_address as artifact_name
  from metrics.int_derived_contracts as derived
  inner join metrics.int_deployers_by_project as deployers
    on derived.chain = deployers.artifact_source
    and derived.originating_address = deployers.artifact_name
),

direct_contracts as (
  select * from base_contracts
  union all
  select * from contracts_from_deployers
),

contracts_from_factories as (
  select distinct
    contracts.project_id,
    contracts.artifact_source,
    factories.contract_address as artifact_name
  from metrics.int_factories as factories
  inner join direct_contracts as contracts
    on factories.chain = contracts.artifact_source
    and factories.factory_address = contracts.artifact_name
),

all_contracts as (
  select * from direct_contracts
  union all
  select * from contracts_from_factories
)

select distinct
  project_id,
  @oso_id(artifact_source, artifact_name) as artifact_id,
  artifact_source,
  artifact_name as artifact_source_id,
  LOWER(artifact_source) as artifact_namespace,
  artifact_name
from all_contracts
