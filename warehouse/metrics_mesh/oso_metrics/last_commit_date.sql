select @metrics_sample_date(events.bucket_day) as metrics_sample_date,
  events.event_source,
  events.to_artifact_id as project_id,
  '' as from_artifact_id,
  @metric_name() as metric,
  MAX(events.bucket_day) as last_commit_date
from metrics.events_daily_to_artifact as events
where events.event_type = 'COMMIT_CODE'
  and events.bucket_day BETWEEN @metrics_start(DATE) AND @metrics_end(DATE)
group by 1,
  metric,
  from_artifact_id,
  project_id,
  event_source
