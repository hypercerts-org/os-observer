{#
  Collects all events into a single table

  SCHEMA
    
  time
  event_type

  source_id
  event_source

  to_artifact_name
  to_artifact_namespace
  to_artifact_type
  to_artifact_source_id

  from_artifact_name
  from_artifact_namespace
  from_artifact_type
  from_artifact_source_id

  amount
#}
{{
  config(
    materialized='table',
    partition_by={
      "field": "time",
      "data_type": "timestamp",
      "granularity": "day",
    }
  )
}}

with contract_invocation_daily_count as (
  select -- noqa: ST06
    time,
    "CONTRACT_INVOCATION_DAILY_COUNT" as event_type,
    CAST(source_id as STRING) as event_source_id,
    from_namespace as event_source,
    to_name,
    to_namespace,
    to_type,
    CAST(to_source_id as STRING) as to_source_id,
    from_name,
    from_namespace,
    from_type,
    CAST(from_source_id as STRING) as from_source_id,
    tx_count as amount
  from {{ ref('stg_dune__contract_invocation') }}
  {# a bit of a hack for now to keep this table small for dev and playground #}
  {% if target.name in ['dev', 'playground'] %}
    where time >= TIMESTAMP_SUB(
      CURRENT_TIMESTAMP(),
      interval {{ env_var("PLAYGROUND_DAYS", '14') }} day
    )
  {% endif %}
),

contract_invocation_daily_l2_gas_used as (
  select -- noqa: ST06
    time,
    "CONTRACT_INVOCATION_DAILY_L2_GAS_USED" as event_type,
    CAST(source_id as STRING) as event_source_id,
    from_namespace as event_source,
    to_name,
    to_namespace,
    to_type,
    CAST(to_source_id as STRING) as to_source_id,
    from_name,
    from_namespace,
    from_type,
    CAST(from_source_id as STRING) as from_source_id,
    l2_gas as amount
  from {{ ref('stg_dune__contract_invocation') }}
  {% if target.name in ['dev', 'playground'] %}
    where time >= TIMESTAMP_SUB(
      CURRENT_TIMESTAMP(),
      interval {{ env_var("PLAYGROUND_DAYS", '14') }} day
    )
  {% endif %}
),

contract_invocation_daily_l1_gas_used as (
  select -- noqa: ST06
    time,
    "CONTRACT_INVOCATION_DAILY_L1_GAS_USED" as event_type,
    CAST(source_id as STRING) as event_source_id,
    from_namespace as event_source,
    to_name,
    to_namespace,
    to_type,
    CAST(to_source_id as STRING) as to_source_id,
    from_name,
    from_namespace,
    from_type,
    CAST(from_source_id as STRING) as from_source_id,
    l1_gas as amount
  from {{ ref('stg_dune__contract_invocation') }}
  {% if target.name in ['dev', 'playground'] %}
    where time >= TIMESTAMP_SUB(
      CURRENT_TIMESTAMP(),
      interval {{ env_var("PLAYGROUND_DAYS", '14') }} day
    )
  {% endif %}
),

github_commits as (
  select -- noqa: ST06
    created_at as `time`,
    "COMMIT_CODE" as event_type,
    CAST(push_id as STRING) as event_source_id,
    "GITHUB" as event_source,
    repository_name as to_name,
    SPLIT(REPLACE(repository_name, "@", ""), "/")[SAFE_OFFSET(0)]
      as to_namespace,
    "REPOSITORY" as to_type,
    CAST(repository_id as STRING) as to_source_id,
    COALESCE(actor_login, author_email) as from_name,
    COALESCE(actor_login, author_email) as from_namespace,
    case
      when actor_login is not null then "GIT_USER"
      else "GIT_EMAIL"
    end as from_type,
    case
      when actor_login is not null then CAST(actor_id as STRING)
      else author_email
    end as from_source_id,
    CAST(1 as FLOAT64) as amount
  from {{ ref('stg_github__distinct_commits_resolved_mergebot') }}
),

github_issues as (
  select -- noqa: ST06
    created_at as `time`,
    type as event_type,
    CAST(id as STRING) as event_source_id,
    "GITHUB" as event_source,
    repository_name as to_name,
    SPLIT(REPLACE(repository_name, "@", ""), "/")[SAFE_OFFSET(0)]
      as to_namespace,
    "REPOSITORY" as to_type,
    CAST(repository_id as STRING) as to_source_id,
    actor_login as from_name,
    actor_login as from_namespace,
    "GIT_USER" as from_type,
    CAST(actor_id as STRING) as from_source_id,
    CAST(1 as FLOAT64) as amount
  from {{ ref('stg_github__issues') }}
),

github_pull_requests as (
  select -- noqa: ST06
    created_at as `time`,
    type as event_type,
    CAST(id as STRING) as event_source_id,
    "GITHUB" as event_source,
    repository_name as to_name,
    SPLIT(REPLACE(repository_name, "@", ""), "/")[SAFE_OFFSET(0)]
      as to_namespace,
    "REPOSITORY" as to_type,
    CAST(repository_id as STRING) as to_source_id,
    actor_login as from_name,
    actor_login as from_namespace,
    "GIT_USER" as from_type,
    CAST(actor_id as STRING) as from_source_id,
    CAST(1 as FLOAT64) as amount
  from {{ ref('stg_github__pull_requests') }}
),

github_pull_request_merge_events as (
  select -- noqa: ST06
    created_at as `time`,
    type as event_type,
    CAST(id as STRING) as event_source_id,
    "GITHUB" as event_source,
    repository_name as to_name,
    SPLIT(REPLACE(repository_name, "@", ""), "/")[SAFE_OFFSET(0)]
      as to_namespace,
    "REPOSITORY" as to_type,
    CAST(repository_id as STRING) as to_source_id,
    actor_login as from_name,
    actor_login as from_namespace,
    "GIT_USER" as from_type,
    CAST(actor_id as STRING) as from_source_id,
    CAST(1 as FLOAT64) as amount
  from {{ ref('stg_github__pull_request_merge_events') }}
),

github_stars_and_forks as (
  select -- noqa: ST06
    created_at as `time`,
    type as event_type,
    CAST(id as STRING) as event_source_id,
    "GITHUB" as event_source,
    repository_name as to_name,
    SPLIT(REPLACE(repository_name, "@", ""), "/")[SAFE_OFFSET(0)]
      as to_namespace,
    "REPOSITORY" as to_type,
    CAST(repository_id as STRING) as to_source_id,
    actor_login as from_name,
    actor_login as from_namespace,
    "GIT_USER" as from_type,
    CAST(actor_id as STRING) as from_source_id,
    CAST(1 as FLOAT64) as amount
  from {{ ref('stg_github__stars_and_forks') }}
),

all_events as (
  select * from contract_invocation_daily_count
  union all
  select * from contract_invocation_daily_l1_gas_used
  union all
  select * from contract_invocation_daily_l2_gas_used
  union all
  select * from github_commits
  union all
  select * from github_issues
  union all
  select * from github_pull_requests
  union all
  select * from github_pull_request_merge_events
  union all
  select * from github_stars_and_forks
)

select
  time,
  UPPER(event_type) as event_type,
  CAST(event_source_id as STRING) as event_source_id,
  UPPER(event_source) as event_source,
  LOWER(to_name) as to_artifact_name,
  UPPER(to_namespace) as to_artifact_namespace,
  UPPER(to_type) as to_artifact_type,
  CAST(to_source_id as STRING) as to_artifact_source_id,
  LOWER(from_name) as from_artifact_name,
  UPPER(from_namespace) as from_artifact_namespace,
  UPPER(from_type) as from_artifact_type,
  CAST(from_source_id as STRING) as from_artifact_source_id,
  CAST(amount as FLOAT64) as amount
from all_events
