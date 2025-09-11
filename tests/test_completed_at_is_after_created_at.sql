SELECT
  task_id
FROM
  {{ ref('asana_tasks') }}
WHERE
  completed_at_date IS NOT NULL
  AND completed_at_date < created_at_date