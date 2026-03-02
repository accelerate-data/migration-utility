-- Discover indexes and constraints for all tables
SELECT
  s.name AS schema_name,
  t.name AS table_name,
  CAST(t.object_id AS BIGINT) AS object_id_local,
  i.name AS index_name,
  i.type_desc AS index_type,
  i.is_unique,
  i.is_primary_key,
  i.is_unique_constraint,
  -- Get indexed columns as JSON array
  (
    SELECT STRING_AGG(
      CONCAT(
        '{"column_name":"', c.name, 
        '","is_descending":', CASE WHEN ic.is_descending_key = 1 THEN 'true' ELSE 'false' END,
        ',"is_included":', CASE WHEN ic.is_included_column = 1 THEN 'true' ELSE 'false' END,
        ',"key_ordinal":', ic.key_ordinal, '}'
      ), ','
    ) WITHIN GROUP (ORDER BY ic.key_ordinal, ic.index_column_id)
    FROM sys.index_columns AS ic
    INNER JOIN sys.columns AS c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
    WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id
  ) AS columns_json
FROM sys.indexes AS i
INNER JOIN sys.tables AS t ON t.object_id = i.object_id
INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id
WHERE s.name NOT IN ('sys', 'INFORMATION_SCHEMA')
  AND i.type > 0  -- Exclude heaps (type 0)
ORDER BY s.name, t.name, i.name;
