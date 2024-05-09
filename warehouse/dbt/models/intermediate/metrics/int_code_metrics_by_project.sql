{# 
  Summary GitHub metrics for a project:
    - first_commit_date: The date of the first commit to the project
    - last_commit_date: The date of the last commit to the project
    - repos: The number of repositories in the project
    - stars: The number of stars the project has
    - forks: The number of forks the project has
    - contributors: The number of contributors to the project
    - contributors_6_months: The number of contributors to the project in the last 6 months
    - new_contributors_6_months: The number of new contributors to the project in the last 6 months    
    - avg_fulltime_devs_6_months: The number of full-time developers in the last 6 months
    - avg_active_devs_6_months: The average number of active developers in the last 6 months
    - commits_6_months: The number of commits to the project in the last 6 months
    - issues_opened_6_months: The number of issues opened in the project in the last 6 months
    - issues_closed_6_months: The number of issues closed in the project in the last 6 months
    - pull_requests_opened_6_months: The number of pull requests opened in the project in the last 6 months
    - pull_requests_merged_6_months: The number of pull requests merged in the project in the last 6 months
#}

with repos as (
  select
    project_id,
    MIN(first_commit_time) as first_commit_date,
    MAX(last_commit_time) as last_commit_date,
    COUNT(distinct artifact_id) as repositories,
    SUM(star_count) as stars,
    SUM(fork_count) as forks
  from {{ ref('int_repo_metrics_by_project') }}
  --WHERE r.is_fork = false
  group by project_id
),

project_repos_summary as (
  select
    repos.project_id,
    repos.first_commit_date,
    repos.last_commit_date,
    repos.repositories,
    repos.stars,
    repos.forks,
    int_projects.project_source,
    int_projects.project_namespace,
    int_projects.project_name,
    int_projects.display_name
  from repos
  left join {{ ref('int_projects') }}
    on repos.project_id = int_projects.project_id
),

n_cte as (
  select
    project_id,
    SUM(case when time_interval = 'ALL' then amount end) as contributors,
    SUM(case when time_interval = '6M' then amount end)
      as new_contributors_6_months
  from {{ ref('int_pm_new_contribs') }}
  group by project_id
),

c_cte as (
  select
    project_id,
    SUM(amount) as contributors_6_months
  from {{ ref('int_pm_contributors') }}
  where time_interval = '6M'
  group by project_id
),

d_cte as (
  select
    project_id,
    SUM(
      case
        when impact_metric = 'FULL_TIME_DEV_TOTAL' then amount / 6
        else 0
      end
    ) as avg_fts_6_months,
    SUM(
      case
        when impact_metric = 'PART_TIME_DEV_TOTAL' then amount / 6
        else 0
      end
    ) as avg_pts_6_months
  from {{ ref('int_pm_dev_months') }}
  where time_interval = '6M'
  group by project_id
),

contribs_cte as (
  select
    n.project_id,
    n.contributors,
    n.new_contributors_6_months,
    c.contributors_6_months,
    d.avg_fts_6_months as avg_fulltime_devs_6_months,
    d.avg_fts_6_months + d.avg_pts_6_months as avg_active_devs_6_months
  from n_cte as n
  left join c_cte as c
    on
      n.project_id = c.project_id
  left join d_cte as d
    on
      n.project_id = d.project_id
),

activity_cte as (
  select
    project_id,
    SUM(
      case
        when event_type = 'COMMIT_CODE' then amount
      end
    ) as commits_6_months,
    SUM(
      case
        when event_type = 'ISSUE_OPENED' then amount
      end
    ) as issues_opened_6_months,
    SUM(
      case
        when event_type = 'ISSUE_CLOSED' then amount
      end
    ) as issues_closed_6_months,
    SUM(
      case
        when event_type = 'PULL_REQUEST_OPENED' then amount
      end
    ) as pull_requests_opened_6_months,
    SUM(
      case
        when event_type = 'PULL_REQUEST_MERGED' then amount
      end
    ) as pull_requests_merged_6_months
  from {{ ref('int_events_daily_to_project') }}
  where
    event_source = 'GITHUB'
    and event_type in (
      'COMMIT_CODE',
      'ISSUE_OPENED',
      'ISSUE_CLOSED',
      'PULL_REQUEST_OPENED',
      'PULL_REQUEST_MERGED'
    )
    and DATE_DIFF(CURRENT_DATE(), DATE(bucket_day), month) <= 6
  group by project_id
)

select
  p.project_id,
  p.project_source,
  p.project_namespace,
  p.project_name,
  p.first_commit_date,
  p.last_commit_date,
  p.repositories,
  p.stars,
  p.forks,
  c.contributors,
  c.contributors_6_months,
  c.new_contributors_6_months,
  c.avg_fulltime_devs_6_months,
  c.avg_active_devs_6_months,
  a.commits_6_months,
  a.issues_opened_6_months,
  a.issues_closed_6_months,
  a.pull_requests_opened_6_months,
  a.pull_requests_merged_6_months
from project_repos_summary as p
left join contribs_cte as c
  on p.project_id = c.project_id
left join activity_cte as a
  on p.project_id = a.project_id
