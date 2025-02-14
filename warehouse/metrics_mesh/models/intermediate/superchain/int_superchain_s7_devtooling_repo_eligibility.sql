MODEL (
  name metrics.int_superchain_s7_devtooling_repo_eligibility,
  description "Determines if a repository is eligible for measurement in the S7 devtooling round",
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column sample_date,
    batch_size 90,
    batch_concurrency 1,
    lookback 7
  ),
  start '2015-01-01',
  cron '@daily',
  partitioned_by DAY("sample_date"),
  grain (
    sample_date,
    project_id,
    repo_artifact_id
  )
);

@DEF(lookback_days, 180);

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
    current_timestamp() + interval '@lookback_days' day
      >= last_release_published
    or num_packages_in_deps_dev > 0
    or num_dependent_repos_in_oso > 0
  ) then true else false end as is_eligible,
  current_timestamp() as sample_date
from metrics.int_repositories_enriched