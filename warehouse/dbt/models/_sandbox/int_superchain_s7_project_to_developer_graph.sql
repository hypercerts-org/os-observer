{{
  config(
    materialized='table'
  )
}}


with trusted_developers as (
  select
    developer_id,
    project_id as onchain_builder_project_id
  from {{ ref('int_superchain_s7_trusted_developers') }}
),

eligible_devtooling_repos as (
  select
    repo_artifact_id,
    project_id as devtooling_project_id
  from {{ ref('int_superchain_s7_devtooling_repo_eligibility') }}
  where is_eligible
),

developer_events as (
  select
    trusted_developers.developer_id,
    trusted_developers.onchain_builder_project_id,
    eligible_devtooling_repos.devtooling_project_id,
    events.event_type,
    sum(events.total_events) as total_events,
    min(events.first_event) as first_event,
    max(events.last_event) as last_event
  from {{ ref('int_developer_activity_by_repo') }} as events
  inner join trusted_developers
    on events.developer_id = trusted_developers.developer_id
  inner join eligible_devtooling_repos
    on events.repo_artifact_id = eligible_devtooling_repos.repo_artifact_id
  group by
    trusted_developers.developer_id,
    trusted_developers.onchain_builder_project_id,
    eligible_devtooling_repos.devtooling_project_id,
    events.event_type
),

graph as (
  select
    developer_id,
    onchain_builder_project_id,
    devtooling_project_id,
    max(coalesce(event_type = 'STARRED', false)) as has_starred,
    max(coalesce(event_type = 'FORKED', false)) as has_forked,
    max(coalesce(event_type in (
      'PULL_REQUEST_OPENED',
      'PULL_REQUEST_MERGED',
      'COMMIT_CODE'
    ), false)) as has_code_contribution,
    max(coalesce(event_type in (
      'ISSUE_OPENED',
      'ISSUE_COMMENTED'
    ), false)) as has_issue_contribution,
    sum(
      case
        when event_type != 'STARRED'
          then coalesce(total_events, 1)
        else 0
      end
    ) as total_non_star_events,
    min(
      case
        when event_type != 'STARRED'
          then date(first_event)
      end
    ) as first_event,
    max(
      case
        when event_type != 'STARRED'
          then date(last_event)
      end
    ) as last_event
  from developer_events
  where onchain_builder_project_id != devtooling_project_id
  group by
    developer_id,
    onchain_builder_project_id,
    devtooling_project_id
)

select
  developer_id,
  onchain_builder_project_id,
  devtooling_project_id,
  has_starred,
  has_forked,
  has_code_contribution,
  has_issue_contribution,
  total_non_star_events,
  first_event,
  last_event
from graph
