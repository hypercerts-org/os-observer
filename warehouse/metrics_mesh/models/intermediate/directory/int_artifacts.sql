MODEL (
  name metrics.int_artifacts,
  description 'All artifacts',
  kind FULL,
);

with all_artifacts as (

  {#
    This grabs all the artifacts we know about from project sources
    and from the contract discovery process.
  #}

  select
    artifact_source_id,
    artifact_source,
    artifact_namespace,
    artifact_url,
    artifact_name
  from metrics.int_artifacts_by_project

  union all

  {#
    This grabs the universe of blockchain artifacts that have interacted with the contracts we care about from the events table.
    TODO: this should be refactored when we "index the universe"
  #}

  select distinct
    artifact_source_id,
    artifact_source,
    lower(artifact_source) as artifact_namespace,
    artifact_source_id as artifact_name,
    artifact_source_id as artifact_url
  from (
    select
      from_artifact_source_id as artifact_source_id,
      event_source as artifact_source
    from @oso_source('bigquery.oso.int_events__blockchain')
    where event_type = 'CONTRACT_INVOCATION_DAILY_COUNT'
    union all
    select
      to_artifact_source_id as artifact_source_id,
      event_source as artifact_source
    from @oso_source('bigquery.oso.int_events__blockchain')
    where event_type = 'CONTRACT_INVOCATION_DAILY_COUNT'
  )

  union all

  {# 
    This grabs the universe of GitHub users that have interacted with the repos we care about.
    The `last_used` value is later used in this query to determine what the most _current_ name is. However, oss-directory names are considered canonical so `last_used` is only relevent for `git_user` artifacts.
  #}

  select
    artifact_source_id,
    artifact_source,
    artifact_namespace,
    artifact_url,
    max_by(artifact_name, last_used) as artifact_name
  from metrics.int_artifacts_history
  group by
    artifact_source_id,
    artifact_source,
    artifact_namespace,
    artifact_url
)

select distinct
  @oso_id(artifact_source, artifact_source_id) as artifact_id,
  artifact_source_id,
  artifact_source,
  artifact_namespace,
  artifact_name,
  artifact_url
from all_artifacts
