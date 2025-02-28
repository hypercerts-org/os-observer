{{
  config(
    materialized='table'
  )
}}

{% set lookback_days = 180 %}


select
  project_id,
  artifact_id as repo_artifact_id,
  last_release_published,
  num_packages_in_deps_dev,
  num_dependent_repos_in_oso,
  is_fork,
  created_at,
  updated_at,
  case when (
    date(last_release_published)
    >= date_sub(current_date(), interval {{ lookback_days }} day)
    or num_packages_in_deps_dev > 0
    or num_dependent_repos_in_oso > 0
  ) then true else false end as is_eligible,
  current_timestamp() as sample_date
from {{ ref('int_repositories_enriched') }}
