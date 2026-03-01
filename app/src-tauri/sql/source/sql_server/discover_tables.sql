SELECT
  s.name AS schema_name,
  t.name AS table_name,
  CAST(t.object_id AS BIGINT) AS object_id_local,
  CAST(SUM(COALESCE(ps.row_count, 0)) AS BIGINT) AS row_count
FROM sys.tables AS t
INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id
LEFT JOIN sys.dm_db_partition_stats AS ps
  ON ps.object_id = t.object_id
 AND ps.index_id IN (0, 1)
WHERE s.name NOT IN ('sys', 'INFORMATION_SCHEMA')
GROUP BY s.name, t.name, t.object_id
ORDER BY s.name, t.name;
