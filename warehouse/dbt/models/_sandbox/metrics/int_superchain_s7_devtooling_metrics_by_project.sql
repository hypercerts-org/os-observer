{{
  config(
    materialized='table'
  )
}}

with trusted_developers as (
  select * from {{ ref('int_superchain_s7_trusted_developers') }}
),

dependency_graph as (
  select * from {{ ref('int_superchain_s7_project_to_dependency_graph') }}
),

developer_graph as (
  select * from {{ ref('int_superchain_s7_project_to_developer_graph') }}
),

repositories as (
  select * from {{ ref('int_repositories_enriched') }}
),

-- Package dependency counts
all_dependencies as (
  select
    devtooling_project_id as project_id,
    'package_dependent_count' as metric_name,
    count(distinct onchain_builder_project_id) as amount
  from dependency_graph
  group by 1
),

npm_dependencies as (
  select
    devtooling_project_id as project_id,
    'npm_package_dependent_count' as metric_name,
    count(distinct onchain_builder_project_id) as amount
  from dependency_graph
  where dependency_source = 'NPM'
  group by 1
),

rust_dependencies as (
  select
    devtooling_project_id as project_id,
    'cargo_package_dependent_count' as metric_name,
    count(distinct onchain_builder_project_id) as amount
  from dependency_graph
  where dependency_source = 'CARGO'
  group by 1
),

-- Developer connections
dev_connections as (
  select
    devtooling_project_id as project_id,
    'dev_connection_count' as metric_name,
    count(distinct developer_graph.developer_id) as amount
  from developer_graph
  group by 1
),

trusted_dev_connections as (
  select
    developer_graph.devtooling_project_id as project_id,
    'trusted_dev_connection_count' as metric_name,
    count(distinct developer_graph.developer_id) as amount
  from developer_graph
  inner join trusted_developers
    on developer_graph.developer_id = trusted_developers.developer_id
  group by 1
),

-- Repository metrics
repo_metrics as (
  select
    repositories.project_id,
    metric_name,
    sum(amount) as amount
  from repositories
  cross join
    unnest([
      struct('stars' as metric_name, star_count as amount),
      struct('forks' as metric_name, fork_count as amount)
    ])
  group by 1, 2
),

trusted_repo_metrics as (
  select
    developer_graph.devtooling_project_id as project_id,
    case
      when developer_graph.has_starred then 'trusted_stars'
      when developer_graph.has_forked then 'trusted_forks'
    end as metric_name,
    count(distinct developer_graph.developer_id) as amount
  from developer_graph
  inner join trusted_developers
    on developer_graph.developer_id = trusted_developers.developer_id
  where developer_graph.has_starred or developer_graph.has_forked
  group by 1, 2
),

-- Package counts and rankings
package_metrics as (
  select
    devtooling_project_id as project_id,
    'num_packages' as metric_name,
    count(distinct dependency_name) as amount
  from dependency_graph
  group by 1
),

-- Union all metrics together
metrics_combined as (
  select * from all_dependencies
  union all
  select * from npm_dependencies
  union all
  select * from rust_dependencies
  union all
  select * from dev_connections
  union all
  select * from trusted_dev_connections
  union all
  select * from repo_metrics
  union all
  select * from trusted_repo_metrics
  union all
  select * from package_metrics
)

select
  project_id,
  metric_name,
  amount,
  current_timestamp() as sample_date
from metrics_combined
