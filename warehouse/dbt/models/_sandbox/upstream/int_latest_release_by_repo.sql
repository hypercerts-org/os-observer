with repo_releases as (
  select
    to_artifact_id as artifact_id,
    max(`time`) as latest_repo_release
  from {{ ref('int_events__github') }}
  where event_type = 'RELEASE_PUBLISHED'
  group by to_artifact_id
),

package_releases as (
  select
    package_github_artifact_id as artifact_id,
    max(snapshot_at) as latest_package_release
  from {{ ref('int_sbom_artifacts') }}
  group by package_github_artifact_id
),

latest_releases as (
  select
    coalesce(
      repo_releases.artifact_id,
      package_releases.artifact_id
    ) as artifact_id,
    coalesce(
      repo_releases.latest_repo_release,
      package_releases.latest_package_release
    ) as last_release_published,
    case
      when repo_releases.latest_repo_release is not null then 'GITHUB_RELEASE'
      else 'PACKAGE_SNAPSHOT'
    end as last_release_source
  from repo_releases
  full outer join package_releases
    on repo_releases.artifact_id = package_releases.artifact_id
)

select distinct
  artifact_id,
  last_release_published,
  last_release_source
from latest_releases
where last_release_published is not null
