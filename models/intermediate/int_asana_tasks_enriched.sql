{{ config(materialized='view') }}

WITH tasks_with_assignee AS ( 
  SELECT
    EXTRACT(YEAR FROM t.created_at::timestamp) AS created_at_year,
    DATE_TRUNC('month', t.created_at::DATE)::DATE AS created_at_month,
    t.created_at::DATE AS created_at_date,
    EXTRACT(YEAR FROM t.completed_at::timestamp) AS completed_at_year,
    DATE_TRUNC('month', t.completed_at::DATE)::DATE AS completed_at_month,
    t.completed_at::DATE AS completed_at_date,
    parentTask.task_name AS parent_task,
    t.task_id AS task_id,
    t.task_name AS task,
    t.notes AS notes,
    CONCAT(a.first_name, ' ', a.last_name) AS assignee
  FROM
    {{ ref('stg_asana_task') }} t
  LEFT JOIN
    {{ ref('stg_asana_task') }} parentTask ON t.parent_task_id = parentTask.task_id
  JOIN
    {{ ref('stg_asana_assignees') }} a on t.assignee_id = a.assignee_id
),
tasks_with_projects AS (
  SELECT
    pt.task_id AS task_id,
    STRING_AGG(DISTINCT p.project_name, ', ') AS projects
  FROM
     {{ ref('stg_asana_project_task') }} pt
  JOIN
    {{ ref('stg_asana_project') }} p on pt.project_id = p.project_id
  GROUP BY
    pt.task_id
),
tasks_with_tags AS (
  SELECT
    tt.task_id AS task_id,
    MAX(CASE WHEN t.category = 'Cost Center' THEN t.value END) AS cost_center,
    MAX(CASE WHEN t.category = 'Department' THEN t.value END) AS department,
    MAX(CASE WHEN t.category = 'Location' THEN t.value END) AS location
  FROM
    {{ ref('stg_asana_task_tag') }}  tt
  JOIN
    {{ ref('stg_asana_tag') }} t ON tt.tag_id = t.tag_id
  GROUP BY
    tt.task_id
),
final_model AS (
  SELECT
    ta.created_at_year,
    ta.created_at_month,
    ta.created_at_date,
    ta.completed_at_year,
    ta.completed_at_month,
    ta.completed_at_date,
    ta.parent_task,
    ta.task_id,
    ta.task,
    tp.projects,
    ta.assignee,
    tt.cost_center,
    tt.department,
    tt.location,
    ta.notes,
    1 AS count
  FROM
    tasks_with_assignee ta
  LEFT JOIN
    tasks_with_projects tp ON ta.task_id = tp.task_id
  LEFT JOIN
    tasks_with_tags tt ON ta.task_id = tt.task_id
)
SELECT 
  * 
FROM 
  final_model