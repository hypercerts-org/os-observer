select distinct
  now() as metrics_sample_date,
  events.event_source,
  events.to_artifact_id,
  '' as from_artifact_id,
  @metric_name('release_count') as metric,
  count(*) as amount
from metrics.events_daily_to_artifact as events
where event_type = 'RELEASE_PUBLISHED'
group by 2, 3
