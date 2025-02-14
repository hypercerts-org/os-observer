MODEL (
  name metrics.int_superchain_s7_project_to_developer_graph,
  description "Maps relationships between trusted developers, onchain builder projects, and devtooling projects",
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
    developer_id,
    onchain_builder_project_id,
    devtooling_project_id
  )
);

with trusted_developers as (
  select
    developer_id,
    project_id as onchain_builder_project_id,
    sample_date
  from metrics.int_superchain_s7_trusted_developers
  where sample_date between @start_dt and @end_dt
),

eligible_devtooling_repos as (
  select
    repo_artifact_id,
    project_id as devtooling_project_id,
    sample_date
  from metrics.int_superchain_s7_devtooling_repo_eligibility
  where 
    is_eligible
    and sample_date between @start_dt and @end_dt
),

developer_events as (
  select
    trusted_developers.developer_id,
    trusted_developers.onchain_builder_project_id,
    eligible_devtooling_repos.devtooling_project_id,
    trusted_developers.sample_date,
    events.event_type,
    sum(events.total_events) as total_events,
    min(events.first_event) as first_event,
    max(events.last_event) as last_event
  from metrics.int_developer_activity_by_repo as events
  inner join trusted_developers
    on events.developer_id = trusted_developers.developer_id
  inner join eligible_devtooling_repos
    on events.repo_artifact_id = eligible_devtooling_repos.repo_artifact_id
  group by
    trusted_developers.developer_id,
    trusted_developers.onchain_builder_project_id,
    eligible_devtooling_repos.devtooling_project_id,
    trusted_developers.sample_date,
    events.event_type
),

graph as (
  select
    developer_id,
    onchain_builder_project_id,
    devtooling_project_id,
    sample_date,
    max(event_type = 'STARRED') as has_starred,
    max(event_type = 'FORKED') as has_forked,
    max(event_type in (
      'PULL_REQUEST_OPENED',
      'PULL_REQUEST_MERGED',
      'COMMIT_CODE'
    )) as has_code_contribution,
    max(event_type in (
      'ISSUE_OPENED',
      'ISSUE_COMMENTED'
    )) as has_issue_contribution,
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
          then first_event
        else null
      end
    ) as first_event,
    max(
      case
        when event_type != 'STARRED'
          then last_event
        else null
      end
    ) as last_event
  from developer_events
  where onchain_builder_project_id != devtooling_project_id
  group by
    developer_id,
    onchain_builder_project_id,
    devtooling_project_id,
    sample_date
)

select
  sample_date,
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