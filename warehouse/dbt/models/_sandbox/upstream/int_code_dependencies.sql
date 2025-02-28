{{
  config(
    materialized='table'
  )
}}

select distinct
  artifact_id as dependent_artifact_id,
  package_github_artifact_id as dependency_artifact_id,
  package_artifact_name as dependency_name,
  package_artifact_source as dependency_source
from {{ ref('int_sbom_artifacts') }}
