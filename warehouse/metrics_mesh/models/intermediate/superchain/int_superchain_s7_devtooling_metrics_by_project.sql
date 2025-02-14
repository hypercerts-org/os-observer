MODEL (
  name metrics.int_superchain_s7_devtooling_metrics_by_project,
  description 'S7 devtooling metrics by project with various aggregations and filters',
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column sample_date,
    batch_size 90,
    batch_concurrency 1,
    lookback 7
  ),
  start '2015-01-01',
  cron '@daily',
  partitioned_by DAY("sample_date"),
  grain (
    sample_date,
    project_id,
    metric_name
  )
);

with trusted_developers as (
  select
    developer_id,
    sample_date
  from metrics.int_superchain_s7_trusted_developers
  where sample_date between @start_dt and @end_dt
),

-- Package dependency counts
all_dependencies as (
  select
    devtooling_project_id as project_id,
    'package_dependent_count' as metric_name,
    count(distinct onchain_builder_project_id) as amount,
    sample_date
  from metrics.int_superchain_s7_project_to_dependency_graph
  where sample_date between @start_dt and @end_dt
  group by devtooling_project_id, sample_date
),

npm_dependencies as (
  select
    devtooling_project_id as project_id,
    'npm_package_dependent_count' as metric_name,
    count(distinct onchain_builder_project_id) as amount,
    sample_date
  from metrics.int_superchain_s7_project_to_dependency_graph
  where 
    dependency_source = 'NPM'
    and sample_date between @start_dt and @end_dt
  group by devtooling_project_id, sample_date
),

rust_dependencies as (
  select
    devtooling_project_id as project_id,
    'cargo_package_dependent_count' as metric_name,
    count(distinct onchain_builder_project_id) as amount,
    sample_date
  from metrics.int_superchain_s7_project_to_dependency_graph
  where 
    dependency_source = 'CARGO'
    and sample_date between @start_dt and @end_dt
  group by devtooling_project_id, sample_date
),

-- Developer connections
dev_connections as (
  select
    devtooling_project_id as project_id,
    'dev_connection_count' as metric_name,
    count(distinct developer_id) as amount,
    sample_date
  from metrics.int_superchain_s7_project_to_developer_graph
  where sample_date between @start_dt and @end_dt
  group by devtooling_project_id, sample_date
),

trusted_dev_connections as (
  select
    dev_graph.devtooling_project_id as project_id,
    'trusted_dev_connection_count' as metric_name,
    count(distinct dev_graph.developer_id) as amount,
    dev_graph.sample_date
  from metrics.int_superchain_s7_project_to_developer_graph as dev_graph
  inner join trusted_developers
    on dev_graph.developer_id = trusted_developers.developer_id
    and dev_graph.sample_date = trusted_developers.sample_date
  group by dev_graph.devtooling_project_id, dev_graph.sample_date
),

-- Repository metrics
repo_metrics as (
  select
    repos.project_id,
    metrics.metric_name,
    metrics.amount,
    dev_graph.sample_date
  from metrics.int_repositories_enriched as repos
  cross join (
    select 'stars' as metric_name, star_count as amount from metrics.int_repositories_enriched
    union all
    select 'forks' as metric_name, fork_count as amount from metrics.int_repositories_enriched
  ) as metrics
  -- Join to get sample dates from developer graph
  inner join metrics.int_superchain_s7_project_to_developer_graph as dev_graph
    on repos.project_id = dev_graph.devtooling_project_id
    and dev_graph.sample_date between @start_dt and @end_dt
  group by repos.project_id, metrics.metric_name, metrics.amount, dev_graph.sample_date
),

trusted_repo_metrics as (
  select
    dev_graph.devtooling_project_id as project_id,
    case
      when dev_graph.has_starred then 'trusted_stars'
      when dev_graph.has_forked then 'trusted_forks'
    end as metric_name,
    count(distinct dev_graph.developer_id) as amount,
    dev_graph.sample_date
  from metrics.int_superchain_s7_project_to_developer_graph as dev_graph
  inner join trusted_developers
    on dev_graph.developer_id = trusted_developers.developer_id
    and dev_graph.sample_date = trusted_developers.sample_date
  where dev_graph.has_starred or dev_graph.has_forked
  group by 
    dev_graph.devtooling_project_id,
    case
      when dev_graph.has_starred then 'trusted_stars'
      when dev_graph.has_forked then 'trusted_forks'
    end,
    dev_graph.sample_date
),

-- Package counts
package_metrics as (
  select
    devtooling_project_id as project_id,
    'num_packages' as metric_name,
    count(distinct dependency_name) as amount,
    sample_date
  from metrics.int_superchain_s7_project_to_dependency_graph
  where sample_date between @start_dt and @end_dt
  group by devtooling_project_id, sample_date
)

-- Union all metrics together
select
  sample_date,
  project_id,
  metric_name,
  amount
from (
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
