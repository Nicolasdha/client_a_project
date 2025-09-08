SELECT
  id AS tag_id,
  category,
  value
FROM
  {{source('asana_connector', 'tag')}}