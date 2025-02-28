{{
  config(
    materialized='table'
  )
}}


with developers as (
  select distinct
    users.user_id as developer_id,
    users.display_name as developer_name,
    events.to_artifact_id as repo_artifact_id
  from {{ ref('int_events__github') }} as events
  inner join {{ ref('int_users') }} as users
    on events.from_artifact_id = users.user_id
  where
    events.event_type = 'COMMIT_CODE'
    and not regexp_contains(
      users.display_name,
      r'(^|[^a-zA-Z0-9_])bot([^a-zA-Z0-9_]|$)|bot$'
    )
)

select
  events.to_artifact_id as repo_artifact_id,
  developers.developer_id,
  developers.developer_name,
  events.event_type,
  min(events.time) as first_event,
  max(events.time) as last_event,
  count(distinct events.time) as total_events
from {{ ref('int_events__github') }} as events
inner join developers
  on events.from_artifact_id = developers.developer_id
group by
  events.to_artifact_id,
  developers.developer_id,
  developers.developer_name,
  events.event_type
