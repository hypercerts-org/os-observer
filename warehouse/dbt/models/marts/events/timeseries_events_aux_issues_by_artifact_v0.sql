{{ 
  config(meta = {
    'sync_to_db': True,
    'order_by': [ 'event_source', 'event_type', 'to_artifact_id', 'time' ]
  }) 
}}

select
  time,
  to_artifact_id,
  from_artifact_id,
  event_type,
  event_source_id,
  event_source,
  issue_number
from {{ ref('int_events_aux_issues') }}
