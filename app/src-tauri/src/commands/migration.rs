use rusqlite::{params, OptionalExtension};
use tauri::State;

use crate::db::DbState;
use crate::types::{
    Candidacy, CommandError, ScopeInventoryRow, ScopeRefreshSummary, ScopeTableRef, SelectedTable,
    TableArtifact, TableConfig, TableDetailRow,
};

#[tauri::command]
pub fn migration_save_selected_tables(
    workspace_id: String,
    tables: Vec<SelectedTable>,
    state: State<DbState>,
) -> Result<(), CommandError> {
    log::info!(
        "migration_save_selected_tables: workspace_id={} count={}",
        workspace_id,
        tables.len()
    );
    let conn = state.0.lock().unwrap();
    let tx = conn.unchecked_transaction().map_err(|e| {
        log::error!("migration_save_selected_tables: failed to begin transaction: {e}");
        CommandError::from(e)
    })?;
    for table in &tables {
        log::debug!(
            "migration_save_selected_tables: upserting table id={}",
            table.id
        );
        tx.execute(
            "INSERT OR REPLACE INTO selected_tables(id, workspace_id, warehouse_item_id, schema_name, table_name)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                table.id,
                table.workspace_id,
                table.warehouse_item_id,
                table.schema_name,
                table.table_name,
            ],
        )
        .map_err(|e| {
            log::error!(
                "migration_save_selected_tables: failed to upsert table {}: {e}",
                table.id
            );
            CommandError::from(e)
        })?;
    }
    tx.commit().map_err(|e| {
        log::error!("migration_save_selected_tables: failed to commit: {e}");
        CommandError::from(e)
    })?;
    Ok(())
}

#[tauri::command]
pub fn migration_save_table_artifact(
    artifact: TableArtifact,
    state: State<DbState>,
) -> Result<(), CommandError> {
    log::info!(
        "migration_save_table_artifact: selected_table_id={}",
        artifact.selected_table_id
    );
    let conn = state.0.lock().unwrap();
    conn.execute(
        "INSERT OR REPLACE INTO table_artifacts(selected_table_id, warehouse_item_id, schema_name, procedure_name, pipeline_activity_id, discovery_status)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            artifact.selected_table_id,
            artifact.warehouse_item_id,
            artifact.schema_name,
            artifact.procedure_name,
            artifact.pipeline_activity_id,
            artifact.discovery_status,
        ],
    )
    .map_err(|e| {
        log::error!("migration_save_table_artifact: failed: {e}");
        CommandError::from(e)
    })?;
    Ok(())
}

#[tauri::command]
pub fn migration_save_candidacy(
    candidacy: Candidacy,
    state: State<DbState>,
) -> Result<(), CommandError> {
    log::info!(
        "migration_save_candidacy: warehouse_item_id={} schema={} procedure={}",
        candidacy.warehouse_item_id,
        candidacy.schema_name,
        candidacy.procedure_name
    );
    let conn = state.0.lock().unwrap();
    conn.execute(
        "INSERT OR REPLACE INTO candidacy(warehouse_item_id, schema_name, procedure_name, tier, reasoning, overridden, override_reason)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            candidacy.warehouse_item_id,
            candidacy.schema_name,
            candidacy.procedure_name,
            candidacy.tier,
            candidacy.reasoning,
            candidacy.overridden as i64,
            candidacy.override_reason,
        ],
    )
    .map_err(|e| {
        log::error!("migration_save_candidacy: failed: {e}");
        CommandError::from(e)
    })?;
    Ok(())
}

#[tauri::command]
pub fn migration_override_candidacy(
    warehouse_item_id: String,
    schema_name: String,
    procedure_name: String,
    new_tier: String,
    reason: String,
    state: State<DbState>,
) -> Result<(), CommandError> {
    log::info!(
        "migration_override_candidacy: warehouse_item_id={} schema={} procedure={} new_tier={}",
        warehouse_item_id,
        schema_name,
        procedure_name,
        new_tier
    );
    let conn = state.0.lock().unwrap();
    let rows_affected = conn
        .execute(
            "UPDATE candidacy SET tier=?1, overridden=1, override_reason=?2 WHERE warehouse_item_id=?3 AND schema_name=?4 AND procedure_name=?5",
            params![new_tier, reason, warehouse_item_id, schema_name, procedure_name],
        )
        .map_err(|e| {
            log::error!("migration_override_candidacy: failed: {e}");
            CommandError::from(e)
        })?;
    if rows_affected == 0 {
        log::error!(
            "migration_override_candidacy: not found {}.{}",
            schema_name,
            procedure_name
        );
        return Err(CommandError::NotFound(format!(
            "{}.{}",
            schema_name, procedure_name
        )));
    }
    Ok(())
}

#[tauri::command]
pub fn migration_list_candidacy(
    workspace_id: String,
    state: State<DbState>,
) -> Result<Vec<Candidacy>, CommandError> {
    log::info!("migration_list_candidacy: workspace_id={}", workspace_id);
    let conn = state.0.lock().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT DISTINCT c.warehouse_item_id, c.schema_name, c.procedure_name, c.tier, c.reasoning, c.overridden, c.override_reason
             FROM candidacy c
             INNER JOIN table_artifacts ta
               ON ta.warehouse_item_id = c.warehouse_item_id
               AND ta.schema_name = c.schema_name
               AND ta.procedure_name = c.procedure_name
             INNER JOIN selected_tables st
               ON st.id = ta.selected_table_id
               AND st.workspace_id = ?1",
        )
        .map_err(|e| {
            log::error!("migration_list_candidacy: failed to prepare query: {e}");
            CommandError::from(e)
        })?;

    let rows = stmt
        .query_map(params![workspace_id], |row| {
            let overridden_int: i64 = row.get(5)?;
            Ok(Candidacy {
                warehouse_item_id: row.get(0)?,
                schema_name: row.get(1)?,
                procedure_name: row.get(2)?,
                tier: row.get(3)?,
                reasoning: row.get(4)?,
                overridden: overridden_int != 0,
                override_reason: row.get(6)?,
            })
        })
        .map_err(|e| {
            log::error!("migration_list_candidacy: query failed: {e}");
            CommandError::from(e)
        })?;

    let mut results = Vec::new();
    for row in rows {
        results.push(row.map_err(|e| {
            log::error!("migration_list_candidacy: row error: {e}");
            CommandError::from(e)
        })?);
    }
    Ok(results)
}

#[tauri::command]
pub fn migration_save_table_config(
    config: TableConfig,
    state: State<DbState>,
) -> Result<(), CommandError> {
    log::info!(
        "migration_save_table_config: selected_table_id={}",
        config.selected_table_id
    );
    let conn = state.0.lock().unwrap();
    conn.execute(
        "INSERT OR REPLACE INTO table_config(selected_table_id, table_type, load_strategy, grain_columns, relationships_json, incremental_column, date_column, snapshot_strategy, pii_columns, confirmed_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            config.selected_table_id,
            config.table_type,
            config.load_strategy,
            config.grain_columns,
            config.relationships_json,
            config.incremental_column,
            config.date_column,
            config.snapshot_strategy,
            config.pii_columns,
            config.confirmed_at,
        ],
    )
    .map_err(|e| {
        log::error!("migration_save_table_config: failed: {e}");
        CommandError::from(e)
    })?;
    Ok(())
}

#[tauri::command]
pub fn migration_get_table_config(
    selected_table_id: String,
    state: State<DbState>,
) -> Result<Option<TableConfig>, CommandError> {
    log::info!(
        "migration_get_table_config: selected_table_id={}",
        selected_table_id
    );
    let conn = state.0.lock().unwrap();
    let result = conn
        .query_row(
            "SELECT selected_table_id, table_type, load_strategy, grain_columns, relationships_json, incremental_column, date_column, snapshot_strategy, pii_columns, confirmed_at
             FROM table_config WHERE selected_table_id=?1",
            params![selected_table_id],
            |row| {
                Ok(TableConfig {
                    selected_table_id: row.get(0)?,
                    table_type: row.get(1)?,
                    load_strategy: row.get(2)?,
                    grain_columns: row.get(3)?,
                    relationships_json: row.get(4)?,
                    incremental_column: row.get(5)?,
                    date_column: row.get(6)?,
                    snapshot_strategy: row.get(7)?,
                    pii_columns: row.get(8)?,
                    confirmed_at: row.get(9)?,
                })
            },
        )
        .optional()
        .map_err(|e| {
            log::error!("migration_get_table_config: failed: {e}");
            CommandError::from(e)
        })?;
    Ok(result)
}

#[tauri::command]
pub fn migration_list_scope_inventory(
    workspace_id: String,
    state: State<DbState>,
) -> Result<Vec<ScopeInventoryRow>, CommandError> {
    log::info!("migration_list_scope_inventory: workspace_id={workspace_id}");
    let conn = state.0.lock().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT wt.warehouse_item_id, wt.schema_name, wt.table_name,
                    (
                      SELECT SUM(sp.row_count)
                      FROM data_objects do
                      INNER JOIN namespaces ns
                        ON ns.id = do.namespace_id
                      INNER JOIN containers c
                        ON c.id = ns.container_id
                      INNER JOIN sources s
                        ON s.id = c.source_id
                      LEFT JOIN sqlserver_partitions sp
                        ON sp.data_object_id = do.id
                      WHERE s.workspace_id = ?1
                        AND do.object_type = 'table'
                        AND ns.namespace_name = wt.schema_name
                        AND do.object_name = wt.table_name
                        AND (
                          wt.object_id_local IS NULL
                          OR do.external_object_id = CAST(wt.object_id_local AS TEXT)
                        )
                    ) AS row_count,
                    EXISTS(
                      SELECT 1 FROM selected_tables st
                      WHERE st.workspace_id = ?1
                        AND st.warehouse_item_id = wt.warehouse_item_id
                        AND st.schema_name = wt.schema_name
                        AND st.table_name = wt.table_name
                    ) AS is_selected
             FROM warehouse_tables wt
             INNER JOIN items i
               ON i.id = wt.warehouse_item_id
             WHERE i.workspace_id = ?1
             ORDER BY wt.schema_name, wt.table_name",
        )
        .map_err(CommandError::from)?;

    let rows = stmt
        .query_map(params![workspace_id], |row| {
            Ok(ScopeInventoryRow {
                warehouse_item_id: row.get(0)?,
                schema_name: row.get(1)?,
                table_name: row.get(2)?,
                row_count: row.get(3)?,
                is_selected: row.get::<_, bool>(4)?,
            })
        })
        .map_err(CommandError::from)?;

    let mut inventory = Vec::new();
    for row in rows {
        inventory.push(row.map_err(CommandError::from)?);
    }
    Ok(inventory)
}

fn deterministic_selected_table_id(workspace_id: &str, table: &ScopeTableRef) -> String {
    format!(
        "st:{}:{}:{}:{}",
        workspace_id,
        table.warehouse_item_id,
        table.schema_name.to_lowercase(),
        table.table_name.to_lowercase()
    )
}

#[tauri::command]
pub fn migration_add_tables_to_selection(
    workspace_id: String,
    tables: Vec<ScopeTableRef>,
    state: State<DbState>,
) -> Result<i64, CommandError> {
    log::info!(
        "migration_add_tables_to_selection: workspace_id={} count={}",
        workspace_id,
        tables.len()
    );
    let conn = state.0.lock().unwrap();
    let tx = conn.unchecked_transaction().map_err(CommandError::from)?;
    let mut added: i64 = 0;

    for table in &tables {
        let exists: bool = tx
            .query_row(
                "SELECT EXISTS(
                   SELECT 1 FROM selected_tables
                   WHERE workspace_id = ?1
                     AND warehouse_item_id = ?2
                     AND schema_name = ?3
                     AND table_name = ?4
                 )",
                params![
                    workspace_id,
                    table.warehouse_item_id,
                    table.schema_name,
                    table.table_name
                ],
                |row| row.get(0),
            )
            .map_err(CommandError::from)?;
        if exists {
            continue;
        }

        tx.execute(
            "INSERT INTO selected_tables(id, workspace_id, warehouse_item_id, schema_name, table_name)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                deterministic_selected_table_id(&workspace_id, table),
                workspace_id,
                table.warehouse_item_id,
                table.schema_name,
                table.table_name
            ],
        )
        .map_err(CommandError::from)?;
        added += 1;
    }

    tx.commit().map_err(CommandError::from)?;
    Ok(added)
}

#[tauri::command]
pub fn migration_set_table_selected(
    workspace_id: String,
    table: ScopeTableRef,
    selected: bool,
    state: State<DbState>,
) -> Result<(), CommandError> {
    log::info!(
        "migration_set_table_selected: workspace_id={} {}.{} selected={}",
        workspace_id,
        table.schema_name,
        table.table_name,
        selected
    );
    let conn = state.0.lock().unwrap();
    if selected {
        conn.execute(
            "INSERT OR IGNORE INTO selected_tables(id, workspace_id, warehouse_item_id, schema_name, table_name)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                deterministic_selected_table_id(&workspace_id, &table),
                workspace_id,
                table.warehouse_item_id,
                table.schema_name,
                table.table_name
            ],
        )
        .map_err(CommandError::from)?;
    } else {
        conn.execute(
            "DELETE FROM selected_tables
             WHERE workspace_id = ?1
               AND warehouse_item_id = ?2
               AND schema_name = ?3
               AND table_name = ?4",
            params![
                workspace_id,
                table.warehouse_item_id,
                table.schema_name,
                table.table_name
            ],
        )
        .map_err(CommandError::from)?;
    }
    Ok(())
}

#[tauri::command]
pub fn migration_reset_selected_tables(
    workspace_id: String,
    state: State<DbState>,
) -> Result<i64, CommandError> {
    log::info!("migration_reset_selected_tables: workspace_id={workspace_id}");
    let conn = state.0.lock().unwrap();
    let deleted = conn
        .execute(
            "DELETE FROM selected_tables WHERE workspace_id = ?1",
            params![workspace_id],
        )
        .map_err(CommandError::from)?;
    Ok(i64::try_from(deleted).unwrap_or(0))
}

#[tauri::command]
pub fn migration_list_table_details(
    workspace_id: String,
    state: State<DbState>,
) -> Result<Vec<TableDetailRow>, CommandError> {
    log::info!("migration_list_table_details: workspace_id={workspace_id}");
    let conn = state.0.lock().unwrap();
    list_table_details_for_workspace(&conn, &workspace_id)
}

fn list_table_details_for_workspace(
    conn: &rusqlite::Connection,
    workspace_id: &str,
) -> Result<Vec<TableDetailRow>, CommandError> {
    let mut stmt = conn
        .prepare(
            "SELECT st.id,
                    st.warehouse_item_id,
                    st.schema_name,
                    st.table_name,
                    (
                      SELECT SUM(sp.row_count)
                      FROM data_objects do
                      INNER JOIN namespaces ns
                        ON ns.id = do.namespace_id
                      INNER JOIN containers c
                        ON c.id = ns.container_id
                      INNER JOIN sources s
                        ON s.id = c.source_id
                      LEFT JOIN sqlserver_partitions sp
                        ON sp.data_object_id = do.id
                      WHERE s.workspace_id = ?1
                        AND do.object_type = 'table'
                        AND ns.namespace_name = st.schema_name
                        AND do.object_name = st.table_name
                        AND (
                          wt.object_id_local IS NULL
                          OR do.external_object_id = CAST(wt.object_id_local AS TEXT)
                        )
                    ) AS row_count,
                    COALESCE(
                      tc.table_type,
                      CASE
                        WHEN LOWER(st.table_name) LIKE 'fact%' THEN 'fact'
                        WHEN LOWER(st.table_name) LIKE 'dim%' THEN 'dimension'
                        ELSE 'unknown'
                      END
                    ) AS table_type,
                    COALESCE(
                      tc.load_strategy,
                      CASE
                        WHEN LOWER(st.table_name) LIKE 'dim%' THEN 'snapshot'
                        ELSE 'incremental'
                      END
                    ) AS load_strategy,
                    COALESCE(tc.snapshot_strategy, 'sample_1day') AS snapshot_strategy,
                    COALESCE(
                      tc.incremental_column,
                      (
                        SELECT soc.column_name
                        FROM data_objects do
                        INNER JOIN namespaces ns ON ns.id = do.namespace_id
                        INNER JOIN containers c ON c.id = ns.container_id
                        INNER JOIN sources s ON s.id = c.source_id
                        INNER JOIN sqlserver_object_columns soc ON soc.data_object_id = do.id
                        WHERE s.workspace_id = ?1
                          AND do.object_type = 'table'
                          AND ns.namespace_name = st.schema_name
                          AND do.object_name = st.table_name
                          AND (
                            wt.object_id_local IS NULL
                            OR do.external_object_id = CAST(wt.object_id_local AS TEXT)
                          )
                          AND (
                            LOWER(soc.column_name) LIKE '%load%'
                            OR LOWER(soc.column_name) LIKE '%update%'
                            OR LOWER(soc.column_name) LIKE '%modified%'
                            OR LOWER(soc.column_name) LIKE '%timestamp%'
                            OR LOWER(soc.column_name) LIKE '%date%'
                          )
                        ORDER BY
                          CASE
                            WHEN LOWER(soc.column_name) = 'load_date' THEN 0
                            WHEN LOWER(soc.column_name) = 'updated_at' THEN 1
                            WHEN LOWER(soc.column_name) = 'modified_at' THEN 2
                            WHEN LOWER(soc.column_name) = 'last_modified_at' THEN 3
                            WHEN LOWER(soc.column_name) = 'created_at' THEN 4
                            WHEN LOWER(soc.column_name) = 'event_date' THEN 5
                            ELSE 99
                          END,
                          COALESCE(soc.column_id, 999999),
                          soc.column_name
                        LIMIT 1
                      )
                    ) AS incremental_column,
                    COALESCE(
                      tc.date_column,
                      (
                        SELECT soc.column_name
                        FROM data_objects do
                        INNER JOIN namespaces ns ON ns.id = do.namespace_id
                        INNER JOIN containers c ON c.id = ns.container_id
                        INNER JOIN sources s ON s.id = c.source_id
                        INNER JOIN sqlserver_object_columns soc ON soc.data_object_id = do.id
                        WHERE s.workspace_id = ?1
                          AND do.object_type = 'table'
                          AND ns.namespace_name = st.schema_name
                          AND do.object_name = st.table_name
                          AND (
                            wt.object_id_local IS NULL
                            OR do.external_object_id = CAST(wt.object_id_local AS TEXT)
                          )
                          AND (
                            LOWER(COALESCE(soc.data_type, '')) LIKE '%date%'
                            OR LOWER(COALESCE(soc.data_type, '')) LIKE '%time%'
                            OR LOWER(soc.column_name) LIKE '%date%'
                          )
                        ORDER BY
                          CASE
                            WHEN LOWER(soc.column_name) = 'date_key' THEN 0
                            WHEN LOWER(soc.column_name) = 'business_date' THEN 1
                            WHEN LOWER(soc.column_name) = 'event_date' THEN 2
                            WHEN LOWER(soc.column_name) = 'sale_date' THEN 3
                            WHEN LOWER(soc.column_name) = 'order_date' THEN 4
                            WHEN LOWER(soc.column_name) = 'invoice_date' THEN 5
                            WHEN LOWER(soc.column_name) = 'load_date' THEN 50
                            ELSE 99
                          END,
                          COALESCE(soc.column_id, 999999),
                          soc.column_name
                        LIMIT 1
                      )
                    ) AS date_column,
                    COALESCE(
                      tc.grain_columns,
                      NULLIF(
                        (
                          SELECT json_group_array(pk.column_name)
                          FROM (
                            SELECT soc.column_name
                            FROM data_objects do
                            INNER JOIN namespaces ns ON ns.id = do.namespace_id
                            INNER JOIN containers c ON c.id = ns.container_id
                            INNER JOIN sources s ON s.id = c.source_id
                            INNER JOIN sqlserver_object_columns soc ON soc.data_object_id = do.id
                            WHERE s.workspace_id = ?1
                              AND do.object_type = 'table'
                              AND ns.namespace_name = st.schema_name
                              AND do.object_name = st.table_name
                              AND (
                                wt.object_id_local IS NULL
                                OR do.external_object_id = CAST(wt.object_id_local AS TEXT)
                              )
                              AND COALESCE(soc.is_nullable, 1) = 0
                              AND (
                                LOWER(soc.column_name) = 'id'
                                OR LOWER(soc.column_name) LIKE '%_id'
                              )
                            ORDER BY COALESCE(soc.column_id, 999999), soc.column_name
                          ) pk
                        ),
                        '[]'
                      )
                    ) AS grain_columns,
                    COALESCE(
                      tc.relationships_json,
                      NULLIF(
                        (
                          SELECT json_group_array(
                            json_object(
                              'constraint', fk.constraint_name,
                              'definition', fk.definition_json
                            )
                          )
                          FROM (
                            SELECT sci.constraint_name, sci.definition_json
                            FROM data_objects do
                            INNER JOIN namespaces ns ON ns.id = do.namespace_id
                            INNER JOIN containers c ON c.id = ns.container_id
                            INNER JOIN sources s ON s.id = c.source_id
                            INNER JOIN sqlserver_constraints_indexes sci ON sci.data_object_id = do.id
                            WHERE s.workspace_id = ?1
                              AND do.object_type = 'table'
                              AND ns.namespace_name = st.schema_name
                              AND do.object_name = st.table_name
                              AND (
                                wt.object_id_local IS NULL
                                OR do.external_object_id = CAST(wt.object_id_local AS TEXT)
                              )
                              AND LOWER(COALESCE(sci.constraint_type, '')) LIKE '%foreign%'
                            ORDER BY sci.constraint_name
                          ) fk
                        ),
                        '[]'
                      )
                    ) AS relationships_json,
                    COALESCE(
                      tc.pii_columns,
                      NULLIF(
                        (
                          SELECT json_group_array(pii.column_name)
                          FROM (
                            SELECT soc.column_name
                            FROM data_objects do
                            INNER JOIN namespaces ns ON ns.id = do.namespace_id
                            INNER JOIN containers c ON c.id = ns.container_id
                            INNER JOIN sources s ON s.id = c.source_id
                            INNER JOIN sqlserver_object_columns soc ON soc.data_object_id = do.id
                            WHERE s.workspace_id = ?1
                              AND do.object_type = 'table'
                              AND ns.namespace_name = st.schema_name
                              AND do.object_name = st.table_name
                              AND (
                                wt.object_id_local IS NULL
                                OR do.external_object_id = CAST(wt.object_id_local AS TEXT)
                              )
                              AND (
                                LOWER(soc.column_name) LIKE '%email%'
                                OR LOWER(soc.column_name) LIKE '%phone%'
                                OR LOWER(soc.column_name) LIKE '%ssn%'
                                OR LOWER(soc.column_name) LIKE '%social%'
                                OR LOWER(soc.column_name) LIKE '%dob%'
                                OR LOWER(soc.column_name) LIKE '%birth%'
                                OR LOWER(soc.column_name) LIKE '%address%'
                                OR LOWER(soc.column_name) LIKE '%postal%'
                                OR LOWER(soc.column_name) LIKE '%zip%'
                              )
                            ORDER BY COALESCE(soc.column_id, 999999), soc.column_name
                          ) pii
                        ),
                        '[]'
                      )
                    ) AS pii_columns,
                    tc.confirmed_at
             FROM selected_tables st
             LEFT JOIN warehouse_tables wt
               ON wt.warehouse_item_id = st.warehouse_item_id
              AND wt.schema_name = st.schema_name
              AND wt.table_name = st.table_name
             LEFT JOIN table_config tc
               ON tc.selected_table_id = st.id
             WHERE st.workspace_id = ?1
             ORDER BY st.schema_name, st.table_name",
        )
        .map_err(CommandError::from)?;

    let rows = stmt
        .query_map(params![workspace_id], |row| {
            let table_type: Option<String> = row.get(5)?;
            let load_strategy: Option<String> = row.get(6)?;
            let incremental_column: Option<String> = row.get(8)?;
            let date_column: Option<String> = row.get(9)?;
            let grain_columns: Option<String> = row.get(10)?;
            let relationships_json: Option<String> = row.get(11)?;
            let pii_columns: Option<String> = row.get(12)?;
            let confirmed_at: Option<String> = row.get(13)?;
            let status = if table_type.is_some()
                && load_strategy.is_some()
                && incremental_column.is_some()
                && date_column.is_some()
                && grain_columns.is_some()
                && relationships_json.is_some()
                && pii_columns.is_some()
            {
                "Ready"
            } else {
                "Missing details"
            };
            Ok(TableDetailRow {
                selected_table_id: row.get(0)?,
                warehouse_item_id: row.get(1)?,
                schema_name: row.get(2)?,
                table_name: row.get(3)?,
                row_count: row.get(4)?,
                table_type,
                load_strategy,
                snapshot_strategy: row.get(7)?,
                incremental_column,
                date_column,
                grain_columns,
                relationships_json,
                pii_columns,
                confirmed_at,
                status: status.to_string(),
            })
        })
        .map_err(CommandError::from)?;

    let mut details = Vec::new();
    for row in rows {
        details.push(row.map_err(CommandError::from)?);
    }
    Ok(details)
}

#[tauri::command]
pub fn migration_reconcile_scope_state(
    workspace_id: String,
    state: State<DbState>,
) -> Result<ScopeRefreshSummary, CommandError> {
    log::info!("migration_reconcile_scope_state: workspace_id={workspace_id}");
    let conn = state.0.lock().unwrap();
    let tx = conn.unchecked_transaction().map_err(CommandError::from)?;

    // Prevent duplicate natural keys from accumulating across repeated refresh operations.
    tx.execute(
        "DELETE FROM selected_tables
         WHERE rowid NOT IN (
           SELECT MIN(rowid)
           FROM selected_tables
           GROUP BY workspace_id, warehouse_item_id, schema_name, table_name
         )",
        [],
    )
    .map_err(CommandError::from)?;

    let invalid_selected_ids: Vec<String> = {
        let mut stmt = tx
            .prepare(
                "SELECT st.id
                 FROM selected_tables st
                 LEFT JOIN warehouse_tables wt
                   ON wt.warehouse_item_id = st.warehouse_item_id
                  AND wt.schema_name = st.schema_name
                  AND wt.table_name = st.table_name
                 WHERE st.workspace_id = ?1
                   AND wt.warehouse_item_id IS NULL",
            )
            .map_err(CommandError::from)?;
        let rows = stmt
            .query_map(params![workspace_id], |row| row.get::<_, String>(0))
            .map_err(CommandError::from)?;
        let mut result = Vec::new();
        for row in rows {
            result.push(row.map_err(CommandError::from)?);
        }
        result
    };

    for selected_table_id in &invalid_selected_ids {
        tx.execute(
            "DELETE FROM selected_tables WHERE id = ?1",
            params![selected_table_id],
        )
        .map_err(CommandError::from)?;
    }

    let kept: i64 = tx
        .query_row(
            "SELECT COUNT(*) FROM selected_tables WHERE workspace_id = ?1",
            params![workspace_id],
            |row| row.get(0),
        )
        .map_err(CommandError::from)?;
    let removed = i64::try_from(invalid_selected_ids.len()).unwrap_or(0);

    tx.commit().map_err(CommandError::from)?;
    Ok(ScopeRefreshSummary {
        kept,
        invalidated: removed,
        removed,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;

    fn setup_workspace_and_item(conn: &rusqlite::Connection) -> (String, String) {
        let ws_id = uuid::Uuid::new_v4().to_string();
        let item_id = uuid::Uuid::new_v4().to_string();
        conn.execute(
            "INSERT INTO workspaces(id, display_name, migration_repo_path, created_at) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![ws_id, "Test Workspace", "/tmp/repo", "2026-01-01T00:00:00Z"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO items(id, workspace_id, display_name, item_type) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![item_id, ws_id, "Warehouse", "Warehouse"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO warehouse_schemas(warehouse_item_id, schema_name) VALUES (?1, ?2)",
            rusqlite::params![item_id, "dbo"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO warehouse_procedures(warehouse_item_id, schema_name, procedure_name) VALUES (?1, ?2, ?3)",
            rusqlite::params![item_id, "dbo", "sp_load"],
        )
        .unwrap();
        (ws_id, item_id)
    }

    #[test]
    fn override_candidacy_sets_overridden_flag() {
        let conn = db::open_in_memory().unwrap();
        let (ws_id, item_id) = setup_workspace_and_item(&conn);

        // Insert a selected_table
        let st_id = uuid::Uuid::new_v4().to_string();
        conn.execute(
            "INSERT INTO selected_tables(id, workspace_id, warehouse_item_id, schema_name, table_name) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![st_id, ws_id, item_id, "dbo", "orders"],
        )
        .unwrap();

        // Insert a table_artifact
        conn.execute(
            "INSERT INTO table_artifacts(selected_table_id, warehouse_item_id, schema_name, procedure_name, discovery_status) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![st_id, item_id, "dbo", "sp_load", "resolved"],
        )
        .unwrap();

        // Insert a candidacy record
        conn.execute(
            "INSERT INTO candidacy(warehouse_item_id, schema_name, procedure_name, tier, overridden) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![item_id, "dbo", "sp_load", "migrate", 0],
        )
        .unwrap();

        // Override
        let rows = conn.execute(
            "UPDATE candidacy SET tier=?1, overridden=1, override_reason=?2 WHERE warehouse_item_id=?3 AND schema_name=?4 AND procedure_name=?5",
            rusqlite::params!["reject", "Not suitable", item_id, "dbo", "sp_load"],
        )
        .unwrap();
        assert_eq!(rows, 1);

        let (tier, overridden, reason): (String, i64, Option<String>) = conn
            .query_row(
                "SELECT tier, overridden, override_reason FROM candidacy WHERE warehouse_item_id=?1 AND schema_name=?2 AND procedure_name=?3",
                rusqlite::params![item_id, "dbo", "sp_load"],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(tier, "reject");
        assert_eq!(overridden, 1);
        assert_eq!(reason.as_deref(), Some("Not suitable"));
    }

    #[test]
    fn list_candidacy_returns_items_for_workspace() {
        let conn = db::open_in_memory().unwrap();
        let (ws_id, item_id) = setup_workspace_and_item(&conn);

        // Second workspace — its candidacy should NOT appear
        let ws2_id = uuid::Uuid::new_v4().to_string();
        let item2_id = uuid::Uuid::new_v4().to_string();
        conn.execute(
            "INSERT INTO workspaces(id, display_name, migration_repo_path, created_at) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![ws2_id, "Other Workspace", "/tmp/other", "2026-01-01T00:00:00Z"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO items(id, workspace_id, display_name, item_type) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![item2_id, ws2_id, "Warehouse2", "Warehouse"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO warehouse_schemas(warehouse_item_id, schema_name) VALUES (?1, ?2)",
            rusqlite::params![item2_id, "dbo"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO warehouse_procedures(warehouse_item_id, schema_name, procedure_name) VALUES (?1, ?2, ?3)",
            rusqlite::params![item2_id, "dbo", "sp_other"],
        )
        .unwrap();

        // Insert selected_table + artifact + candidacy for workspace 1
        let st_id = uuid::Uuid::new_v4().to_string();
        conn.execute(
            "INSERT INTO selected_tables(id, workspace_id, warehouse_item_id, schema_name, table_name) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![st_id, ws_id, item_id, "dbo", "orders"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO table_artifacts(selected_table_id, warehouse_item_id, schema_name, procedure_name, discovery_status) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![st_id, item_id, "dbo", "sp_load", "resolved"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO candidacy(warehouse_item_id, schema_name, procedure_name, tier, overridden) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![item_id, "dbo", "sp_load", "migrate", 0],
        )
        .unwrap();

        // Insert selected_table + artifact + candidacy for workspace 2
        let st2_id = uuid::Uuid::new_v4().to_string();
        conn.execute(
            "INSERT INTO selected_tables(id, workspace_id, warehouse_item_id, schema_name, table_name) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![st2_id, ws2_id, item2_id, "dbo", "other_table"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO table_artifacts(selected_table_id, warehouse_item_id, schema_name, procedure_name, discovery_status) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![st2_id, item2_id, "dbo", "sp_other", "resolved"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO candidacy(warehouse_item_id, schema_name, procedure_name, tier, overridden) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![item2_id, "dbo", "sp_other", "reject", 0],
        )
        .unwrap();

        // Query for workspace 1 only
        let mut stmt = conn
            .prepare(
                "SELECT DISTINCT c.warehouse_item_id, c.schema_name, c.procedure_name, c.tier, c.reasoning, c.overridden, c.override_reason
                 FROM candidacy c
                 INNER JOIN selected_tables st ON st.workspace_id = ?1
                 INNER JOIN table_artifacts ta ON ta.selected_table_id = st.id
                   AND ta.warehouse_item_id = c.warehouse_item_id
                   AND ta.schema_name = c.schema_name
                   AND ta.procedure_name = c.procedure_name",
            )
            .unwrap();

        let results: Vec<Candidacy> = stmt
            .query_map(rusqlite::params![ws_id], |row| {
                let overridden_int: i64 = row.get(5)?;
                Ok(Candidacy {
                    warehouse_item_id: row.get(0)?,
                    schema_name: row.get(1)?,
                    procedure_name: row.get(2)?,
                    tier: row.get(3)?,
                    reasoning: row.get(4)?,
                    overridden: overridden_int != 0,
                    override_reason: row.get(6)?,
                })
            })
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].procedure_name, "sp_load");
        assert_eq!(results[0].tier, "migrate");
    }

    #[test]
    fn list_table_details_derives_defaults_from_sqlserver_metadata() {
        let conn = db::open_in_memory().unwrap();
        let (ws_id, item_id) = setup_workspace_and_item(&conn);

        conn.execute(
            "INSERT INTO warehouse_tables(warehouse_item_id, schema_name, table_name, object_id_local)
             VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![item_id, "dbo", "FactSales", 42i64],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO selected_tables(id, workspace_id, warehouse_item_id, schema_name, table_name)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["st-1", ws_id, item_id, "dbo", "FactSales"],
        )
        .unwrap();

        conn.execute(
            "INSERT INTO sources(id, workspace_id, source_type, external_source_id, display_name)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["source-1", ws_id, "sql_server", "sql_server://local", "local"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO containers(id, source_id, container_type, external_container_id, container_name)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["container-1", "source-1", "database", "1", "db"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO namespaces(id, container_id, namespace_name, external_namespace_id)
             VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params!["ns-1", "container-1", "dbo", "1"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO data_objects(id, namespace_id, object_name, object_type, external_object_id)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["obj-1", "ns-1", "FactSales", "table", "42"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO sqlserver_partitions(id, data_object_id, partition_number, row_count)
             VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params!["part-1", "obj-1", 1i64, 12345i64],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO sqlserver_object_columns(id, data_object_id, column_name, column_id, data_type, is_nullable)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params!["col-1", "obj-1", "customer_id", 1i64, "int", 0i64],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO sqlserver_object_columns(id, data_object_id, column_name, column_id, data_type, is_nullable)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params!["col-2", "obj-1", "load_date", 2i64, "datetime2", 0i64],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO sqlserver_object_columns(id, data_object_id, column_name, column_id, data_type, is_nullable)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params!["col-3", "obj-1", "sale_date", 3i64, "date", 1i64],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO sqlserver_object_columns(id, data_object_id, column_name, column_id, data_type, is_nullable)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params!["col-4", "obj-1", "customer_email", 4i64, "nvarchar", 1i64],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO sqlserver_constraints_indexes(id, data_object_id, constraint_name, index_name, constraint_type, definition_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                "fk-1",
                "obj-1",
                "FK_fact_sales_customer",
                Option::<String>::None,
                "FOREIGN KEY",
                "{\"column\":\"customer_id\",\"ref_table\":\"dim_customer\",\"ref_column\":\"customer_id\"}"
            ],
        )
        .unwrap();

        let details = super::list_table_details_for_workspace(&conn, &ws_id).unwrap();
        assert_eq!(details.len(), 1);
        let row = &details[0];
        assert_eq!(row.table_name, "FactSales");
        assert_eq!(row.row_count, Some(12345));
        assert_eq!(row.table_type.as_deref(), Some("fact"));
        assert_eq!(row.load_strategy.as_deref(), Some("incremental"));
        assert_eq!(row.incremental_column.as_deref(), Some("load_date"));
        assert_eq!(row.date_column.as_deref(), Some("sale_date"));
        assert_eq!(row.grain_columns.as_deref(), Some("[\"customer_id\"]"));
        assert!(row
            .relationships_json
            .as_deref()
            .is_some_and(|v| v.contains("FK_fact_sales_customer")));
        assert_eq!(row.pii_columns.as_deref(), Some("[\"customer_email\"]"));
        assert_eq!(row.status, "Ready");
    }

    #[test]
    fn reconcile_scope_state_removes_missing_selected_rows() {
        let conn = db::open_in_memory().unwrap();
        let (ws_id, item_id) = setup_workspace_and_item(&conn);
        conn.execute(
            "INSERT INTO warehouse_tables(warehouse_item_id, schema_name, table_name) VALUES (?1, ?2, ?3)",
            rusqlite::params![item_id, "dbo", "fact_sales"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO selected_tables(id, workspace_id, warehouse_item_id, schema_name, table_name)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["st-1", ws_id, item_id, "dbo", "fact_sales"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO selected_tables(id, workspace_id, warehouse_item_id, schema_name, table_name)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["st-2", ws_id, item_id, "dbo", "missing_table"],
        )
        .unwrap();

        let tx = conn.unchecked_transaction().unwrap();
        let invalid_ids: Vec<String> = {
            let mut stmt = tx
                .prepare(
                    "SELECT st.id
                     FROM selected_tables st
                     LEFT JOIN warehouse_tables wt
                       ON wt.warehouse_item_id = st.warehouse_item_id
                      AND wt.schema_name = st.schema_name
                      AND wt.table_name = st.table_name
                     WHERE st.workspace_id = ?1
                       AND wt.warehouse_item_id IS NULL",
                )
                .unwrap();
            stmt.query_map(rusqlite::params![ws_id], |row| row.get(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };
        for id in &invalid_ids {
            tx.execute(
                "DELETE FROM selected_tables WHERE id=?1",
                rusqlite::params![id],
            )
            .unwrap();
        }
        let kept: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM selected_tables WHERE workspace_id=?1",
                rusqlite::params![ws_id],
                |row| row.get(0),
            )
            .unwrap();
        tx.commit().unwrap();

        assert_eq!(invalid_ids, vec!["st-2".to_string()]);
        assert_eq!(kept, 1);
    }
}
