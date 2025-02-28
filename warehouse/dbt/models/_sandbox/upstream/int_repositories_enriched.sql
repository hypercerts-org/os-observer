{{
  config(
    materialized='table'
  )
}}

with releases as (
  select
    artifact_id as repo_artifact_id,
    last_release_published
  from {{ ref('int_latest_release_by_repo') }}
),

packages as (
  select distinct
    package_github_owner,
    package_github_repo,
    count(distinct package_artifact_name) as num_packages_in_deps_dev
  from {{ ref('int_packages') }}
  where is_current_owner = true
  group by
    package_github_owner,
    package_github_repo
),

deps as (
  select
    dependency_artifact_id,
    count(distinct dependent_artifact_id) as num_dependent_repos_in_oso
  from {{ ref('int_code_dependencies') }}
  group by dependency_artifact_id
)

select distinct
  repos.project_id,
  repos.artifact_id,
  repos.artifact_namespace,
  repos.artifact_name,
  repos.artifact_url,
  repos.is_fork,
  repos.star_count,
  repos.fork_count,
  repos.license_name,
  repos.license_spdx_id,
  repos.`language`,
  repos.created_at,
  repos.updated_at,
  releases.last_release_published,
  coalesce(packages.num_packages_in_deps_dev, 0) as num_packages_in_deps_dev,
  coalesce(deps.num_dependent_repos_in_oso, 0) as num_dependent_repos_in_oso
from {{ ref('int_repositories') }} as repos
left join releases
  on repos.artifact_id = releases.repo_artifact_id
left join packages
  on
    repos.artifact_namespace = packages.package_github_owner
    and repos.artifact_name = packages.package_github_repo
left join deps
  on repos.artifact_id = deps.dependency_artifact_id
