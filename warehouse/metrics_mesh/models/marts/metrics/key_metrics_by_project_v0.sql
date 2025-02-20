MODEL (
  name metrics.key_metrics_by_project_v0,
  kind FULL,
  partitioned_by 'sample_date',
  tags (
  'export'
  ),
);

WITH key_metrics_by_project_v0_no_casting AS (
  SELECT
    @oso_id('OSO', 'oso', metric) AS metric_id,
    to_project_id AS project_id,
    metrics_sample_date AS sample_date,
    amount,
    metric,
    NULL AS unit
  FROM metrics.key_metrics_to_project
)

SELECT
  metric_id::TEXT,
  project_id::TEXT,
  sample_date::DATE,
  amount::DOUBLE,
  unit::TEXT
FROM key_metrics_by_project_v0_no_casting
