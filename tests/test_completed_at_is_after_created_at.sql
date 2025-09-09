SELECT
  task_id
FROM
  {{ ref('int_asana_tasks_enriched') }}
WHERE
  completed_at_date IS NOT NULL
  AND completed_at_date < created_at_date