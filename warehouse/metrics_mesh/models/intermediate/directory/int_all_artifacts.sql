MODEL (
  name metrics.int_all_artifacts,
  description "a list of all artifacts associated with a project",
  kind FULL,
);

{#
  Notes:
  - This will create a separate row for each artifact_type, which is de-duplicated
    in int_artifacts_by_project
  - Currently, the source and namespace for blockchain artifacts are the same.
    This may change in the future.
#}

with onchain_artifacts as (
  select
    project_id,
    artifact_id,
    artifact_source_id,
    artifact_source,
    'DEPLOYER' as artifact_type,
    artifact_namespace,
    artifact_name,
    artifact_name as artifact_url
  from metrics.int_deployers_by_project
  union all
  select
    project_id,
    artifact_id,
    artifact_source_id,
    artifact_source,
    'CONTRACT' as artifact_type,
    artifact_namespace,
    artifact_name,
    artifact_name as artifact_url
  from metrics.int_contracts_by_project
),

all_normalized_artifacts as (
  select
    project_id,
    artifact_id,
    artifact_source_id,
    artifact_source,
    artifact_type,
    artifact_namespace,
    artifact_name,
    artifact_url
  from metrics.int_artifacts_by_project_all_sources
  union all
  select
    project_id,
    artifact_id,
    artifact_source_id,
    artifact_source,
    artifact_type,
    artifact_namespace,
    artifact_name,
    artifact_url
  from onchain_artifacts
)

select distinct
  project_id,
  artifact_id,
  artifact_source_id,
  artifact_source,
  artifact_namespace,
  artifact_name,
  artifact_url,
  artifact_type
from all_normalized_artifacts
