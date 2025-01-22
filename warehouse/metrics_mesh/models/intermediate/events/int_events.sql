MODEL (
  name metrics.int_events,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column time,
  ),
  start '2015-01-01',
  cron '@daily',
  grain (time, event_type, event_source, from_artifact_id, to_artifact_id)
);

select
  time,
  to_artifact_id,
  from_artifact_id,
  UPPER(event_type) as event_type,
  CAST(event_source_id as STRING) as event_source_id,
  UPPER(event_source) as event_source,
  LOWER(to_artifact_name) as to_artifact_name,
  LOWER(to_artifact_namespace) as to_artifact_namespace,
  UPPER(to_artifact_type) as to_artifact_type,
  LOWER(to_artifact_source_id) as to_artifact_source_id,
  LOWER(from_artifact_name) as from_artifact_name,
  LOWER(from_artifact_namespace) as from_artifact_namespace,
  UPPER(from_artifact_type) as from_artifact_type,
  LOWER(from_artifact_source_id) as from_artifact_source_id,
  CAST(amount as DOUBLE) as amount
from (
  select * from @oso_source('bigquery.oso.int_events__blockchain')
  union all
  select * from @oso_source('bigquery.oso.int_events__github')
  union all
  select * from @oso_source('bigquery.oso.int_events__dependencies')
  union all
  select * from @oso_source('bigquery.oso.int_events__open_collective')
)
