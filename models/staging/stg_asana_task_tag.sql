SELECT
  task_id,
  tag_id
FROM
  {{source('asana_connector', 'task_tag')}}