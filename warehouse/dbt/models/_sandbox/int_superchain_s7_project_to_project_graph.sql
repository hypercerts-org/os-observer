{{
  config(
    materialized='table'
  )
}}

with project_to_developer_graph as (
  select
    onchain_builder_project_id,
    devtooling_project_id,
    'DEVELOPER_OVERLAP' as edge_type,
    'SHARED_DEVELOPER_COUNT' as weighting_algorithm,
    count(distinct developer_id) as edge_weight
  from {{ ref('int_superchain_s7_project_to_developer_graph') }}
  group by 1, 2
),

project_to_trusted_developer_graph as (
  select
    onchain_builder_project_id,
    devtooling_project_id,
    'TRUSTED_DEVELOPER_OVERLAP' as edge_type,
    'SHARED_DEVELOPER_COUNT' as weighting_algorithm,
    count(distinct developer_id) as edge_weight
  from {{ ref('int_superchain_s7_project_to_developer_graph') }}
  where
    developer_id in (
      select developer_id
      from {{ ref('int_superchain_s7_trusted_developers') }}
      where
        project_id in (
          select distinct project_id
          from {{ ref('int_superchain_s7_onchain_builder_eligibility') }}
          where
            transaction_count_all_levels >= 100000
            or user_count >= 10000
        )
        and total_commits_to_project >= 100
    )
  group by 1, 2
),

project_to_dependency_graph as (
  select
    onchain_builder_project_id,
    devtooling_project_id,
    'DEPENDENCY' as edge_type,
    'PACKAGE_COUNT' as weighting_algorithm,
    count(distinct dependency_name) as edge_weight
  from {{ ref('int_superchain_s7_project_to_dependency_graph') }}
  group by 1, 2
),

project_to_npm_dependency_graph as (
  select
    onchain_builder_project_id,
    devtooling_project_id,
    'NPM_DEPENDENCY' as edge_type,
    'PACKAGE_COUNT' as weighting_algorithm,
    count(distinct dependency_name) as edge_weight
  from {{ ref('int_superchain_s7_project_to_dependency_graph') }}
  where dependency_source = 'NPM'
  group by 1, 2
),

project_to_cargo_dependency_graph as (
  select
    onchain_builder_project_id,
    devtooling_project_id,
    'CARGO_DEPENDENCY' as edge_type,
    'PACKAGE_COUNT' as weighting_algorithm,
    count(distinct dependency_name) as edge_weight
  from {{ ref('int_superchain_s7_project_to_dependency_graph') }}
  where dependency_source = 'CARGO'
  group by 1, 2
),

graph as (
  select * from project_to_developer_graph
  union all
  select * from project_to_trusted_developer_graph
  union all
  select * from project_to_dependency_graph
  union all
  select * from project_to_npm_dependency_graph
  union all
  select * from project_to_cargo_dependency_graph
)

select
  onchain_builder_project_id,
  devtooling_project_id,
  edge_type,
  weighting_algorithm,
  edge_weight,
  current_timestamp() as sample_date
from graph
where onchain_builder_project_id != devtooling_project_id
