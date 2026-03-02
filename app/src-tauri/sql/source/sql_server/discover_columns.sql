SELECT
  s.name AS schema_name,
  t.name AS table_name,
  CAST(t.object_id AS BIGINT) AS object_id_local,
  c.name AS column_name,
  c.column_id,
  TYPE_NAME(c.user_type_id) AS data_type,
  c.is_nullable
FROM sys.columns AS c
INNER JOIN sys.tables AS t ON t.object_id = c.object_id
INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id
WHERE s.name NOT IN ('sys', 'INFORMATION_SCHEMA')
ORDER BY s.name, t.name, c.column_id;
