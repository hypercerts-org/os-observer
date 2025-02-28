{{
  config(
    materialized='table'
  )
}}


select
  ocbe.project_id as onchain_builder_project_id,
  dtre.project_id as devtooling_project_id,
  dependencies.dependent_artifact_id,
  dependencies.dependency_artifact_id,
  dependencies.dependency_name,
  dependencies.dependency_source
from {{ ref('int_code_dependencies') }} as dependencies
inner join {{ ref('int_repositories_enriched') }} as dependents
  on dependencies.dependent_artifact_id = dependents.artifact_id
inner join {{ ref('int_superchain_s7_onchain_builder_eligibility') }} as ocbe
  on dependents.project_id = ocbe.project_id
inner join {{ ref('int_superchain_s7_devtooling_repo_eligibility') }} as dtre
  on dependencies.dependency_artifact_id = dtre.repo_artifact_id
where
  ocbe.project_id != dtre.project_id
  and ocbe.is_eligible
  and dtre.is_eligible
