SELECT
  id AS project_id,
  name AS project_name,
  created_at,
  archived
FROM
  {{source('asana_connector', 'project')}}