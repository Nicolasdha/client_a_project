SELECT
  id AS assignee_id,
  first_name,
  last_name
FROM
  {{source('asana_connector', 'assignee')}}