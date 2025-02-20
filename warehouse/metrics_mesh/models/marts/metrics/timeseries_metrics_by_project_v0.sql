MODEL (
  name metrics.timeseries_metrics_by_project_v0,
  kind FULL,
  partitioned_by 'sample_date',
  tags (
    'export'
  )
);

WITH all_timeseries_metrics_by_project AS (
  SELECT
    @oso_id('OSO', 'oso', metric) AS metric_id,
    to_project_id AS project_id,
    metrics_sample_date AS sample_date,
    amount AS amount,
    NULL AS unit
  FROM metrics.timeseries_metrics_to_project
)
SELECT
  metric_id::TEXT,
  project_id::TEXT,
  sample_date::DATE,
  amount::DOUBLE,
  unit::TEXT
FROM all_timeseries_metrics_by_project
