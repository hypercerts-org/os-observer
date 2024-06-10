with projects as (
  select
    project_id,
    github,
    npm,
    blockchain
  from {{ ref('stg_ossd__current_projects') }}
),

github_repos as (
  select
    "GITHUB" as artifact_source,
    "REPOSITORY" as artifact_type,
    projects.project_id,
    repos.owner as artifact_namespace,
    repos.name as artifact_name,
    repos.url as artifact_url,
    CAST(repos.id as STRING) as artifact_source_id
  from projects
  cross join
    UNNEST(JSON_QUERY_ARRAY(projects.github)) as github
  inner join
    {{ ref('stg_ossd__current_repositories') }} as repos
    on
      LOWER(CONCAT("https://github.com/", repos.owner))
      = LOWER(JSON_VALUE(github.url))
      or LOWER(repos.url) = LOWER(JSON_VALUE(github.url))
),

all_npm_raw as (
  select
    "NPM" as artifact_source,
    "PACKAGE" as artifact_type,
    projects.project_id,
    JSON_VALUE(npm.url) as artifact_source_id,
    case
      when
        JSON_VALUE(npm.url) like "https://npmjs.com/package/%"
        then SUBSTR(JSON_VALUE(npm.url), 28)
      when
        JSON_VALUE(npm.url) like "https://www.npmjs.com/package/%"
        then SUBSTR(JSON_VALUE(npm.url), 31)
    end as artifact_name,
    JSON_VALUE(npm.url) as artifact_url
  from projects
  cross join
    UNNEST(JSON_QUERY_ARRAY(projects.npm)) as npm
),

all_npm as (
  select
    project_id,
    artifact_source_id,
    artifact_source,
    artifact_type,
    artifact_name,
    artifact_url,
    SPLIT(REPLACE(artifact_name, "@", ""), "/")[SAFE_OFFSET(0)]
      as artifact_namespace
  from all_npm_raw
),

ossd_blockchain as (
  select
    projects.project_id,
    tag as artifact_type,
    network as artifact_namespace,
    network as artifact_source,
    JSON_VALUE(blockchains.address) as artifact_source_id,
    JSON_VALUE(blockchains.address) as artifact_name,
    JSON_VALUE(blockchains.address) as artifact_url
  from projects
  cross join
    UNNEST(JSON_QUERY_ARRAY(projects.blockchain)) as blockchains
  cross join
    UNNEST(JSON_VALUE_ARRAY(blockchains.networks)) as network
  cross join
    UNNEST(JSON_VALUE_ARRAY(blockchains.tags)) as tag
),

all_artifacts as (
  select
    project_id,
    artifact_source_id,
    artifact_source,
    artifact_type,
    artifact_namespace,
    artifact_name,
    artifact_url
  from
    github_repos
  union all
  select
    project_id,
    artifact_source_id,
    artifact_source,
    artifact_type,
    artifact_namespace,
    artifact_name,
    artifact_url
  from
    ossd_blockchain
  union all
  select
    project_id,
    artifact_source_id,
    artifact_source,
    artifact_type,
    artifact_namespace,
    artifact_name,
    artifact_url
  from
    all_npm
),

all_normalized_artifacts as (
  select distinct
    project_id,
    LOWER(artifact_source_id) as artifact_source_id,
    {# 
      artifact_source and artifact_type are considered internal constants hence
      we apply an UPPER transform
    #}
    UPPER(artifact_source) as artifact_source,
    UPPER(artifact_type) as artifact_type,
    LOWER(artifact_namespace) as artifact_namespace,
    LOWER(artifact_name) as artifact_name,
    LOWER(artifact_url) as artifact_url
  from all_artifacts
)

select
  project_id,
  {{ oso_id("artifact_source", "artifact_source_id") }} as `artifact_id`,
  artifact_source_id,
  artifact_source,
  artifact_namespace,
  artifact_name,
  artifact_url,
  artifact_type
from all_normalized_artifacts
