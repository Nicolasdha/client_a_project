SELECT
  id AS task_id,
  name AS task_name,
  created_at,
  completed_at,
  assignee_id,
  due_date,
  notes,
  parent_task_id
FROM
  {{source('asana_connector', 'task')}}