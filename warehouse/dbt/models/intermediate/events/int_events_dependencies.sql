{% set event_source_name = '"DEPS_DEV"' %}

with snapshots as (
  select
    `SnapshotAt` as `time`,
    `System` as from_artifact_type,
    `Name` as from_artifact_name,
    `Version` as from_artifact_version,
    `Dependency`.`Name` as to_artifact_name,
    `Dependency`.`System` as to_artifact_type,
    `Dependency`.`Version` as to_artifact_version,
    LAG(`Dependency`.`Name`) over (
      partition by `System`, `Name`, `Dependency`.`Name`, `Version`, `Dependency`.`Version`
      order by `SnapshotAt`
    ) as previous_to_artifact_name
  from {{ ref('stg_deps_dev__dependencies') }}
  where `MinimumDepth` = 1
),

intermediate as (
  select
    `time`,
    case
      when previous_to_artifact_name is null then 'ADD_DEPENDENCY'
      when
        to_artifact_name is not null and to_artifact_name <> previous_to_artifact_name
        then 'REMOVE_DEPENDENCY'
      else 'NO_CHANGE'
    end as event_type,
    {{ event_source_name }} as event_source,
    {{ parse_name(
      'to_artifact_type',
      'to_artifact_name')
    }} as to_artifact_name,
    {{ parse_namespace(
      'to_artifact_type',
      'to_artifact_name')
    }} as to_artifact_namespace,
    to_artifact_type,
    {{ parse_name(
      'from_artifact_type',
      'from_artifact_name')
    }} as from_artifact_name,
    {{ parse_namespace(
      'from_artifact_type',
      'from_artifact_name')
    }} as from_artifact_namespace,
    from_artifact_type,
    1.0 as amount
  from snapshots
),

artifact_ids as (
  select
    `time`,
    event_type,
    event_source,
    {{ oso_id(
      'event_source',
      'to_artifact_namespace',
      'to_artifact_name')
    }} as to_artifact_id,
    to_artifact_name,
    to_artifact_namespace,
    to_artifact_type,
    {{ oso_id(
      'event_source',
      'to_artifact_type')
    }} as to_artifact_source_id,
    {{ oso_id(
      'event_source',
      'from_artifact_namespace',
      'from_artifact_name')  
    }} as from_artifact_id,
    from_artifact_name,
    from_artifact_namespace,
    from_artifact_type,
    {{ oso_id(
      'event_source',
      'from_artifact_type')
    }} as from_artifact_source_id,
    amount
  from intermediate
  where event_type <> 'NO_CHANGE'
),

changes as (
  select
    `time`,
    event_type,
    event_source,
    to_artifact_id,
    to_artifact_name,
    to_artifact_namespace,
    to_artifact_type,
    to_artifact_source_id,
    from_artifact_id,
    from_artifact_name,
    from_artifact_namespace,
    from_artifact_type,
    from_artifact_source_id,
    amount,
    {{ oso_id(
      'event_source',
      'time',
      'to_artifact_id',
      'to_artifact_type',
      'from_artifact_id',
      'from_artifact_type',
      'event_type')
    }} as event_source_id
  from artifact_ids
)

select * from changes
