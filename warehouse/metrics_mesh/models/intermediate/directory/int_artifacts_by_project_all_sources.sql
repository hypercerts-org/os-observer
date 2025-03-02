MODEL (
  name metrics.int_artifacts_by_project_all_sources,
  kind FULL,
  dialect trino
);

select
  project_id,
  artifact_id,
  artifact_source_id,
  artifact_source,
  artifact_type,
  artifact_namespace,
  artifact_name,
  artifact_url
from metrics.int_artifacts_by_project_in_ossd
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
from metrics.int_artifacts_by_project_in_op_atlas