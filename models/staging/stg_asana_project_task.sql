SELECT
  project_id,
  task_id
FROM
  {{source('asana_connector', 'project_task')}}