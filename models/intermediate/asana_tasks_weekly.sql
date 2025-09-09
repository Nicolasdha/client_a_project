WITH date_spine AS (
  SELECT
      DATE_TRUNC('week', generate_series) AS week_start,
      DATE_TRUNC('week', generate_series) + INTERVAL '6 days' AS week_end
  FROM
      generate_series(
          (SELECT DATE_TRUNC('week', MIN(created_at_date)) FROM {{ ref('int_asana_tasks_enriched') }}),
          NOW()::timestamp,
          '1 week'::interval
      ) AS generate_series
),
enriched_tasks AS (
  SELECT *
  FROM {{ ref('int_asana_tasks_enriched') }}
),
tasks_weekly AS (
  SELECT
    ds.week_start,
    ds.week_end,
    et.*
  FROM
    date_spine ds
  JOIN
    enriched_tasks et ON et.created_at_date::DATE <= ds.week_end::DATE
      AND 
    (et.completed_at_date::DATE IS NULL OR et.completed_at_date::DATE > ds.week_start::DATE)
),
final_model as (
  SELECT
    EXTRACT(YEAR FROM week_start::DATE) AS year,
    DATE_TRUNC('month', week_start::DATE)::DATE AS month,
    week_start::DATE AS week,
    parent_task AS "parent task",
    task_id AS "task id",
    task,
    projects,
    assignee,
    cost_center,
    department,
    location,
    notes,
    created_at_date,
    completed_at_date,
    1 AS count
  FROM
    tasks_weekly
  ORDER BY
    task_id
)
SELECT
  *
FROM
  final_model