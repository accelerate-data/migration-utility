# Source YAML Catalog Enrichment

`setup-target` source YAML is generated from normalized table catalog JSON only. It must not parse raw DDL or infer constraints that are absent from catalog fields.

Confirmed source tables emit catalog columns in `sources.yml`. Column entries preserve available SQL type metadata from `sql_type`, `data_type`, or `type`.

Column tests are deterministic and conservative. Non-nullable catalog columns emit `not_null`; single-column primary keys and unique indexes emit `unique`; composite keys do not mark individual columns unique.

Foreign key relationship tests are source-local only. A foreign key emits a dbt `relationships` test only when both the local and referenced tables are confirmed sources in the same generated YAML and the key maps one local column to one referenced column.

Source freshness uses only `profile.watermark.column` when that column exists in the emitted source columns. Change-capture flags alone do not identify a usable dbt `loaded_at_field`.
