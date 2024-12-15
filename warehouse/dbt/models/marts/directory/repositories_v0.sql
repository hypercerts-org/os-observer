{{ 
  config(
    materialized='table',
    meta = {
      'sync_to_db': True,
    }
  )
}}

select
  project_id,
  artifact_source_id,
  artifact_source,
  artifact_namespace,
  artifact_name,
  artifact_url,
  is_fork,
  branch,
  star_count,
  watcher_count,
  fork_count,
  license_name,
  license_spdx_id,
  `language`,
  created_at,
  updated_at
from {{ ref('int_repositories') }}
