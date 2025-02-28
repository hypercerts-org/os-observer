{{
  config(
    materialized='table'
  )
}}

{% set min_repo_stars = 5 %}
{% set last_repo_update_date = '2024-07-01' %}
{% set active_months_threshold = 3 %}
{% set commits_threshold = 20 %}
{% set last_commit_threshold_months = 12 %}

with eligible_onchain_builder_repos as (
  select
    artifact_id as repo_artifact_id,
    project_id
  from {{ ref('int_repositories_enriched') }}
  where
    language in ('TypeScript', 'Solidity', 'Rust')
    and updated_at > '{{ last_repo_update_date }}'
    and star_count > {{ min_repo_stars }}
    and project_id in (
      select project_id
      from {{ ref('int_superchain_s7_onchain_builder_eligibility') }}
      where is_eligible
    )
),

developer_activity as (
  select
    eligible_onchain_builder_repos.project_id,
    events.developer_id,
    events.developer_name,
    sum(events.total_events) as total_commits_to_project,
    min(events.first_event) as first_commit,
    max(events.last_event) as last_commit
  from {{ ref('int_developer_activity_by_repo') }} as events
  inner join eligible_onchain_builder_repos
    on events.repo_artifact_id = eligible_onchain_builder_repos.repo_artifact_id
  where events.event_type = 'COMMIT_CODE'
  group by
    eligible_onchain_builder_repos.project_id,
    events.developer_id,
    events.developer_name
),

eligible_developers as (
  select distinct developer_id
  from developer_activity
  where
    total_commits_to_project >= {{ commits_threshold }}
    and date_diff(
      date(last_commit),
      date(first_commit),
      month
    ) >= {{ active_months_threshold }}
    and date(last_commit) >= date_sub(
      current_date(),
      interval {{ last_commit_threshold_months }} month
    )
)

select
  project_id,
  developer_id,
  developer_name,
  total_commits_to_project,
  first_commit,
  last_commit
from developer_activity
where developer_id in (
  select developer_id
  from eligible_developers
)
