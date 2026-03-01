use std::fs;
use std::path::PathBuf;

use chrono::Utc;
use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tauri::{AppHandle, Manager, State};

use crate::commands::agent::{launch_agent_with_transcript, SidecarManager};
use crate::db::DbState;
use crate::types::{
    Candidacy, CommandError, ScopeInventoryRow, ScopeRefreshSummary, ScopeTableRef, SelectedTable,
    TableArtifact, TableConfig,
};

const TABLE_DETAILS_AGENT_NAME: &str = "scope-table-details-analyzer";

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
struct AgentTableConfigPayload {
    table_type: Option<String>,
    load_strategy: Option<String>,
    grain_columns: Option<String>,
    relationships_json: Option<String>,
    incremental_column: Option<String>,
    date_column: Option<String>,
    snapshot_strategy: Option<String>,
    pii_columns: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TableDetailsRunHistory {
    run_id: String,
    request_id: String,
    workspace_id: String,
    selected_table_id: String,
    schema_name: String,
    table_name: String,
    started_at: String,
    completed_at: String,
    status: String,
    agent_transcript_path: String,
    raw_agent_response: Value,
    validated_payload: Option<TableConfig>,
    error: Option<String>,
}

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
pub async fn migration_analyze_table_details(
    workspace_id: String,
    selected_table_id: String,
    schema_name: String,
    table_name: String,
    force: Option<bool>,
    state: State<'_, DbState>,
    app: AppHandle,
    sidecar: State<'_, SidecarManager>,
) -> Result<TableConfig, CommandError> {
    let run_id = format!("table-details-{}", uuid::Uuid::new_v4());
    let started_at = Utc::now().to_rfc3339();
    let force = force.unwrap_or(false);
    log::info!(
        "event=table_details_analysis component=migration operation=start run_id={} request_id=pending selected_table_id={} status=started force={}",
        run_id,
        selected_table_id,
        force
    );

    {
        let conn = state.0.lock().unwrap();
        let exists: bool = conn
            .query_row(
                "SELECT EXISTS(
                    SELECT 1
                    FROM selected_tables
                    WHERE id = ?1
                      AND workspace_id = ?2
                      AND schema_name = ?3
                      AND table_name = ?4
                 )",
                params![selected_table_id, workspace_id, schema_name, table_name],
                |row| row.get(0),
            )
            .map_err(CommandError::from)?;
        if !exists {
            return Err(CommandError::NotFound(format!(
                "selected table not found for workspace_id={} selected_table_id={}",
                workspace_id, selected_table_id
            )));
        }

        if !force {
            let cached = load_table_config_for_selected(&conn, &selected_table_id)?;
            if let Some(config) = cached {
                log::info!(
                    "event=table_details_analysis component=migration operation=cache_hit run_id={} request_id=cached selected_table_id={} status=success",
                    run_id,
                    selected_table_id
                );
                return Ok(config);
            }
        }
    }

    let prompt = format!(
        "Use agent `{}`.\nAnalyze table details for migration metadata.\nCONTEXT_START\nworkspace_id: {}\nselected_table_id: {}\nschema_name: {}\ntable_name: {}\nCONTEXT_END\nReturn output strictly according to the agent contract.",
        TABLE_DETAILS_AGENT_NAME, workspace_id, selected_table_id, schema_name, table_name
    );

    log::info!(
        "event=table_details_analysis component=migration operation=agent_call_start run_id={} request_id=pending selected_table_id={} status=started",
        run_id,
        selected_table_id
    );
    let launched = match launch_agent_with_transcript(
        prompt,
        None,
        state.clone(),
        app.clone(),
        sidecar.clone(),
    )
    .await
    {
        Ok(run) => run,
        Err(e) => {
            let message = sanitize_log_message(&e);
            log::error!(
                "event=table_details_analysis component=migration operation=agent_call_end run_id={} request_id=unknown selected_table_id={} status=failure error_code=agent_error message={}",
                run_id,
                selected_table_id,
                message
            );
            return Err(CommandError::Io(e));
        }
    };
    let request_id = launched.request_id.clone();
    log::info!(
        "event=table_details_analysis component=migration operation=agent_call_end run_id={} request_id={} selected_table_id={} status=success",
        run_id,
        request_id,
        selected_table_id
    );

    let raw_json = match parse_agent_json_object(&launched.output_text) {
        Ok(value) => value,
        Err(e) => {
            log::error!(
                "event=table_details_analysis component=migration operation=validate_json run_id={} request_id={} selected_table_id={} status=failure error_code=parse_error message={}",
                run_id,
                request_id,
                selected_table_id,
                sanitize_log_message(&e)
            );
            let completed_at = Utc::now().to_rfc3339();
            let _ = write_table_details_run_history(
                &app,
                &workspace_id,
                TableDetailsRunHistory {
                    run_id: run_id.clone(),
                    request_id: request_id.clone(),
                    workspace_id: workspace_id.clone(),
                    selected_table_id: selected_table_id.clone(),
                    schema_name: schema_name.clone(),
                    table_name: table_name.clone(),
                    started_at,
                    completed_at,
                    status: "failure".to_string(),
                    agent_transcript_path: launched.transcript_path.to_string_lossy().to_string(),
                    raw_agent_response: redact_sensitive_value(Value::String(launched.output_text)),
                    validated_payload: None,
                    error: Some(sanitize_log_message(&e)),
                },
            );
            return Err(CommandError::Io(e));
        }
    };
    let payload: AgentTableConfigPayload = match serde_json::from_value(raw_json.clone()) {
        Ok(value) => value,
        Err(e) => {
            let err = format!("invalid agent contract payload: {e}");
            log::error!(
                "event=table_details_analysis component=migration operation=validate_json run_id={} request_id={} selected_table_id={} status=failure error_code=contract_error message={}",
                run_id,
                request_id,
                selected_table_id,
                sanitize_log_message(&err)
            );
            let completed_at = Utc::now().to_rfc3339();
            let _ = write_table_details_run_history(
                &app,
                &workspace_id,
                TableDetailsRunHistory {
                    run_id: run_id.clone(),
                    request_id: request_id.clone(),
                    workspace_id: workspace_id.clone(),
                    selected_table_id: selected_table_id.clone(),
                    schema_name: schema_name.clone(),
                    table_name: table_name.clone(),
                    started_at,
                    completed_at,
                    status: "failure".to_string(),
                    agent_transcript_path: launched.transcript_path.to_string_lossy().to_string(),
                    raw_agent_response: redact_sensitive_value(raw_json),
                    validated_payload: None,
                    error: Some(sanitize_log_message(&err)),
                },
            );
            return Err(CommandError::Io(err));
        }
    };

    let config = TableConfig {
        selected_table_id: selected_table_id.clone(),
        table_type: payload.table_type,
        load_strategy: payload.load_strategy,
        grain_columns: payload.grain_columns,
        relationships_json: payload.relationships_json,
        incremental_column: payload.incremental_column,
        date_column: payload.date_column,
        snapshot_strategy: payload
            .snapshot_strategy
            .unwrap_or_else(|| "sample_1day".to_string()),
        pii_columns: payload.pii_columns,
        confirmed_at: Some(Utc::now().to_rfc3339()),
    };

    {
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
        .map_err(CommandError::from)?;
    }
    log::info!(
        "event=table_details_analysis component=migration operation=persist_config run_id={} request_id={} selected_table_id={} status=success",
        run_id,
        request_id,
        selected_table_id
    );

    let completed_at = Utc::now().to_rfc3339();
    if let Err(e) = write_table_details_run_history(
        &app,
        &workspace_id,
        TableDetailsRunHistory {
            run_id: run_id.clone(),
            request_id: request_id.clone(),
            workspace_id: workspace_id.clone(),
            selected_table_id: selected_table_id.clone(),
            schema_name,
            table_name,
            started_at,
            completed_at,
            status: "success".to_string(),
            agent_transcript_path: launched.transcript_path.to_string_lossy().to_string(),
            raw_agent_response: redact_sensitive_value(raw_json),
            validated_payload: Some(config.clone()),
            error: None,
        },
    ) {
        log::warn!(
            "event=table_details_analysis component=migration operation=write_run_history run_id={} request_id={} selected_table_id={} status=failure message={}",
            run_id,
            request_id,
            selected_table_id,
            sanitize_log_message(&e)
        );
    }

    Ok(config)
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

fn load_table_config_for_selected(
    conn: &rusqlite::Connection,
    selected_table_id: &str,
) -> Result<Option<TableConfig>, CommandError> {
    conn.query_row(
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
    .map_err(CommandError::from)
}

fn parse_agent_json_object(text: &str) -> Result<Value, String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err("empty agent response".to_string());
    }
    if let Ok(value) = serde_json::from_str::<Value>(trimmed) {
        if value.is_object() {
            return Ok(value);
        }
        return Err("agent response must be a JSON object".to_string());
    }

    let fence_start = trimmed.find('{').ok_or_else(|| {
        "agent response did not contain JSON object start".to_string()
    })?;
    let fence_end = trimmed.rfind('}').ok_or_else(|| {
        "agent response did not contain JSON object end".to_string()
    })?;
    let candidate = &trimmed[fence_start..=fence_end];
    let value: Value = serde_json::from_str(candidate)
        .map_err(|e| format!("failed parsing agent JSON object: {e}"))?;
    if !value.is_object() {
        return Err("agent response must resolve to a JSON object".to_string());
    }
    Ok(value)
}

fn sanitize_log_message(message: &str) -> String {
    message.replace('\n', " ").replace('\r', " ")
}

fn redact_sensitive_value(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let redacted = map
                .into_iter()
                .map(|(k, v)| {
                    let lower = k.to_lowercase();
                    let is_sensitive = ["token", "password", "secret", "authorization", "api_key"]
                        .iter()
                        .any(|needle| lower.contains(needle));
                    if is_sensitive {
                        (k, Value::String("[REDACTED]".to_string()))
                    } else {
                        (k, redact_sensitive_value(v))
                    }
                })
                .collect();
            Value::Object(redacted)
        }
        Value::Array(items) => Value::Array(items.into_iter().map(redact_sensitive_value).collect()),
        other => other,
    }
}

fn working_directory_for_workspace(
    conn: &rusqlite::Connection,
    app: &AppHandle,
    workspace_id: &str,
) -> Result<PathBuf, String> {
    let workspace_dir: Option<String> = conn
        .query_row(
            "SELECT migration_repo_path FROM workspaces WHERE id = ?1 LIMIT 1",
            params![workspace_id],
            |row| row.get(0),
        )
        .ok();
    if let Some(path) = workspace_dir {
        let trimmed = path.trim();
        if !trimmed.is_empty() {
            return Ok(PathBuf::from(trimmed));
        }
    }
    let home = app
        .path()
        .home_dir()
        .map_err(|e| format!("failed to resolve home dir: {e}"))?;
    Ok(home.join(".vibedata").join("migration-utility"))
}

fn write_table_details_run_history(
    app: &AppHandle,
    workspace_id: &str,
    history: TableDetailsRunHistory,
) -> Result<(), String> {
    let conn_state = app
        .state::<DbState>();
    let conn = conn_state
        .0
        .lock()
        .map_err(|e| format!("failed to acquire DB lock for run history: {e}"))?;
    let working_dir = working_directory_for_workspace(&conn, app, workspace_id)?;
    drop(conn);
    let run_dir = working_dir
        .join("logs")
        .join("table-details")
        .join(&history.selected_table_id);
    fs::create_dir_all(&run_dir).map_err(|e| format!("failed to create run history dir: {e}"))?;
    let history_path = run_dir.join(format!("{}.json", history.run_id));
    let serialized =
        serde_json::to_string_pretty(&history).map_err(|e| format!("serialize run history: {e}"))?;
    fs::write(&history_path, serialized).map_err(|e| format!("write run history: {e}"))?;
    Ok(())
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
pub fn migration_reconcile_scope_state(
    workspace_id: String,
    state: State<DbState>,
) -> Result<ScopeRefreshSummary, CommandError> {
    log::info!("migration_reconcile_scope_state: workspace_id={workspace_id}");
    let conn = state.0.lock().unwrap();
    let tx = conn.unchecked_transaction().map_err(CommandError::from)?;

    let invalid_selected_ids: Vec<String> = {
        let mut stmt = tx
            .prepare(
                "SELECT st.id
                 FROM selected_tables st
                 LEFT JOIN warehouse_tables wt
                   ON wt.warehouse_item_id = st.warehouse_item_id
                  AND LOWER(wt.schema_name) = LOWER(st.schema_name)
                  AND LOWER(wt.table_name) = LOWER(st.table_name)
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

    #[test]
    fn reconcile_scope_state_keeps_selected_rows_when_backend_tables_exist() {
        let conn = db::open_in_memory().unwrap();
        let (ws_id, item_id) = setup_workspace_and_item(&conn);
        conn.execute(
            "INSERT INTO warehouse_tables(warehouse_item_id, schema_name, table_name) VALUES (?1, ?2, ?3)",
            rusqlite::params![item_id, "dbo", "fact_sales"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO warehouse_tables(warehouse_item_id, schema_name, table_name) VALUES (?1, ?2, ?3)",
            rusqlite::params![item_id, "dbo", "dim_customer"],
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
            rusqlite::params!["st-2", ws_id, item_id, "dbo", "dim_customer"],
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
        assert!(invalid_ids.is_empty());
        let kept: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM selected_tables WHERE workspace_id=?1",
                rusqlite::params![ws_id],
                |row| row.get(0),
            )
            .unwrap();
        tx.commit().unwrap();

        assert_eq!(kept, 2);
    }

    #[test]
    fn parse_agent_json_object_accepts_plain_json_and_code_fenced_text() {
        let plain = r#"{"table_type":"unknown","load_strategy":"incremental"}"#;
        let plain_parsed = super::parse_agent_json_object(plain).unwrap();
        assert_eq!(
            plain_parsed
                .get("table_type")
                .and_then(serde_json::Value::as_str),
            Some("unknown")
        );

        let fenced = "```json\n{\"table_type\":\"fact\",\"load_strategy\":\"snapshot\"}\n```";
        let fenced_parsed = super::parse_agent_json_object(fenced).unwrap();
        assert_eq!(
            fenced_parsed
                .get("load_strategy")
                .and_then(serde_json::Value::as_str),
            Some("snapshot")
        );
    }

    #[test]
    fn redact_sensitive_value_masks_secret_like_keys() {
        let input = serde_json::json!({
            "api_key": "abc123",
            "tokenValue": "token-xyz",
            "nested": {
                "password": "pw",
                "safe": "ok"
            }
        });

        let redacted = super::redact_sensitive_value(input);
        assert_eq!(redacted.get("api_key").and_then(serde_json::Value::as_str), Some("[REDACTED]"));
        assert_eq!(
            redacted.get("tokenValue").and_then(serde_json::Value::as_str),
            Some("[REDACTED]")
        );
        assert_eq!(
            redacted
                .get("nested")
                .and_then(|v| v.get("password"))
                .and_then(serde_json::Value::as_str),
            Some("[REDACTED]")
        );
        assert_eq!(
            redacted
                .get("nested")
                .and_then(|v| v.get("safe"))
                .and_then(serde_json::Value::as_str),
            Some("ok")
        );
    }
}
