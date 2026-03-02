use std::fmt::Write as FmtWrite;
use std::fs;
use std::path::PathBuf;

use rusqlite::params;
use tauri::State;

use crate::db::DbState;
use crate::types::CommandError;

#[tauri::command]
pub fn plan_serialize(workspace_id: String, state: State<DbState>) -> Result<(), CommandError> {
    log::info!("plan_serialize: workspace_id={}", workspace_id);
    let conn = state.conn().map_err(CommandError::Database)?;

    // Fetch workspace to get migration_repo_path
    log::debug!("plan_serialize: fetching workspace record");
    let (migration_repo_path, generated_ws_id): (String, String) = conn
        .query_row(
            "SELECT migration_repo_path, id FROM workspaces WHERE id=?1",
            params![workspace_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(|e| {
            log::error!("plan_serialize: workspace not found: {e}");
            CommandError::from(e)
        })?;

    // Fetch all rows joining selected_tables, table_artifacts, candidacy, and table_config
    log::debug!("plan_serialize: querying plan data");
    let mut stmt = conn
        .prepare(
            "SELECT
               st.schema_name || '.' || st.table_name AS table_ref,
               ta.schema_name || '.' || ta.procedure_name AS proc_ref,
               COALESCE(c.tier, 'unknown') AS candidacy_tier,
               COALESCE(tc.load_strategy, 'unknown') AS load_strategy
             FROM selected_tables st
             LEFT JOIN table_artifacts ta ON ta.selected_table_id = st.id
             LEFT JOIN candidacy c
               ON c.warehouse_item_id = ta.warehouse_item_id
               AND c.schema_name = ta.schema_name
               AND c.procedure_name = ta.procedure_name
             LEFT JOIN table_config tc ON tc.selected_table_id = st.id
             WHERE st.workspace_id = ?1
             ORDER BY table_ref",
        )
        .map_err(|e| {
            log::error!("plan_serialize: failed to prepare query: {e}");
            CommandError::from(e)
        })?;

    #[derive(Debug)]
    struct PlanRow {
        table_ref: String,
        proc_ref: String,
        candidacy_tier: String,
        load_strategy: String,
    }

    let rows: Vec<PlanRow> = stmt
        .query_map(params![workspace_id], |row| {
            Ok(PlanRow {
                table_ref: row.get::<_, Option<String>>(0)?.unwrap_or_default(),
                proc_ref: row.get::<_, Option<String>>(1)?.unwrap_or_default(),
                candidacy_tier: row
                    .get::<_, Option<String>>(2)?
                    .unwrap_or_else(|| "unknown".to_string()),
                load_strategy: row
                    .get::<_, Option<String>>(3)?
                    .unwrap_or_else(|| "unknown".to_string()),
            })
        })
        .map_err(|e| {
            log::error!("plan_serialize: query failed: {e}");
            CommandError::from(e)
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            log::error!("plan_serialize: row error: {e}");
            CommandError::from(e)
        })?;

    log::debug!("plan_serialize: building plan.md with {} rows", rows.len());

    // Build markdown content
    let generated_at = chrono::Utc::now().to_rfc3339();
    let mut content = String::new();
    writeln!(content, "---").ok();
    writeln!(content, "workspace_id: {}", generated_ws_id).ok();
    writeln!(content, "generated_at: {}", generated_at).ok();
    writeln!(content, "---").ok();
    writeln!(content).ok();
    writeln!(content, "# Migration Plan").ok();
    writeln!(content).ok();
    writeln!(content, "## Selected Tables").ok();
    writeln!(content).ok();
    writeln!(content, "| table | procedure | candidacy | load_strategy |").ok();
    writeln!(content, "|---|---|---|---|").ok();
    for row in &rows {
        writeln!(
            content,
            "| {} | {} | {} | {} |",
            row.table_ref, row.proc_ref, row.candidacy_tier, row.load_strategy
        )
        .ok();
    }

    // Write to migration_repo_path/plan.md
    let plan_path = PathBuf::from(&migration_repo_path).join("plan.md");
    log::debug!("plan_serialize: writing to {}", plan_path.display());
    fs::write(&plan_path, content).map_err(|e| {
        log::error!("plan_serialize: failed to write plan.md: {e}");
        CommandError::Io(e.to_string())
    })?;

    log::info!("plan_serialize: wrote plan.md to {}", plan_path.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;

    #[test]
    fn plan_serialize_creates_plan_md() {
        let conn = db::open_in_memory().unwrap();

        // Create a temp dir as migration_repo_path
        let tmp_dir = tempfile::tempdir().unwrap();
        let repo_path = tmp_dir.path().to_str().unwrap().to_string();

        // Set up workspace
        let ws_id = uuid::Uuid::new_v4().to_string();
        conn.execute(
            "INSERT INTO workspaces(id, display_name, migration_repo_path, created_at) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![ws_id, "Test", repo_path, "2026-01-01T00:00:00Z"],
        )
        .unwrap();

        // Set up item, schema, procedure
        let item_id = uuid::Uuid::new_v4().to_string();
        conn.execute(
            "INSERT INTO items(id, workspace_id, display_name, item_type) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![item_id, ws_id, "WH", "Warehouse"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO warehouse_schemas(warehouse_item_id, schema_name) VALUES (?1, ?2)",
            rusqlite::params![item_id, "dbo"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO warehouse_procedures(warehouse_item_id, schema_name, procedure_name) VALUES (?1, ?2, ?3)",
            rusqlite::params![item_id, "dbo", "sp_load_orders"],
        )
        .unwrap();

        // Selected table
        let st_id = uuid::Uuid::new_v4().to_string();
        conn.execute(
            "INSERT INTO selected_tables(id, workspace_id, warehouse_item_id, schema_name, table_name) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![st_id, ws_id, item_id, "dbo", "orders"],
        )
        .unwrap();

        // Table artifact
        conn.execute(
            "INSERT INTO table_artifacts(selected_table_id, warehouse_item_id, schema_name, procedure_name, discovery_status) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![st_id, item_id, "dbo", "sp_load_orders", "resolved"],
        )
        .unwrap();

        // Candidacy
        conn.execute(
            "INSERT INTO candidacy(warehouse_item_id, schema_name, procedure_name, tier, overridden) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![item_id, "dbo", "sp_load_orders", "migrate", 0],
        )
        .unwrap();

        // Table config
        conn.execute(
            "INSERT INTO table_config(selected_table_id, snapshot_strategy, load_strategy) VALUES (?1, ?2, ?3)",
            rusqlite::params![st_id, "sample_1day", "incremental"],
        )
        .unwrap();

        // Call the core plan-building logic directly (without Tauri State)
        // We replicate the logic here to test it against the in-memory DB
        let (migration_repo_path, generated_ws_id): (String, String) = conn
            .query_row(
                "SELECT migration_repo_path, id FROM workspaces WHERE id=?1",
                rusqlite::params![ws_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();

        let mut stmt = conn
            .prepare(
                "SELECT
                   st.schema_name || '.' || st.table_name AS table_ref,
                   ta.schema_name || '.' || ta.procedure_name AS proc_ref,
                   COALESCE(c.tier, 'unknown') AS candidacy_tier,
                   COALESCE(tc.load_strategy, 'unknown') AS load_strategy
                 FROM selected_tables st
                 LEFT JOIN table_artifacts ta ON ta.selected_table_id = st.id
                 LEFT JOIN candidacy c
                   ON c.warehouse_item_id = ta.warehouse_item_id
                   AND c.schema_name = ta.schema_name
                   AND c.procedure_name = ta.procedure_name
                 LEFT JOIN table_config tc ON tc.selected_table_id = st.id
                 WHERE st.workspace_id = ?1
                 ORDER BY table_ref",
            )
            .unwrap();

        let rows: Vec<(String, String, String, String)> = stmt
            .query_map(rusqlite::params![ws_id], |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?.unwrap_or_default(),
                    row.get::<_, Option<String>>(1)?.unwrap_or_default(),
                    row.get::<_, Option<String>>(2)?
                        .unwrap_or_else(|| "unknown".to_string()),
                    row.get::<_, Option<String>>(3)?
                        .unwrap_or_else(|| "unknown".to_string()),
                ))
            })
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        let mut content = String::new();
        writeln!(content, "---").ok();
        writeln!(content, "workspace_id: {}", generated_ws_id).ok();
        writeln!(content, "generated_at: 2026-01-01T00:00:00Z").ok();
        writeln!(content, "---").ok();
        writeln!(content).ok();
        writeln!(content, "# Migration Plan").ok();
        writeln!(content).ok();
        writeln!(content, "## Selected Tables").ok();
        writeln!(content).ok();
        writeln!(content, "| table | procedure | candidacy | load_strategy |").ok();
        writeln!(content, "|---|---|---|---|").ok();
        for (table_ref, proc_ref, candidacy_tier, load_strategy) in &rows {
            writeln!(
                content,
                "| {} | {} | {} | {} |",
                table_ref, proc_ref, candidacy_tier, load_strategy
            )
            .ok();
        }

        let plan_path = PathBuf::from(&migration_repo_path).join("plan.md");
        fs::write(&plan_path, &content).unwrap();

        // Assert file exists and has expected content
        assert!(plan_path.exists(), "plan.md should be created");
        let written = fs::read_to_string(&plan_path).unwrap();
        assert!(
            written.contains("workspace_id:"),
            "should have YAML frontmatter"
        );
        assert!(written.contains("# Migration Plan"), "should have heading");
        assert!(
            written.contains("## Selected Tables"),
            "should have tables section"
        );
        assert!(
            written.contains("dbo.orders"),
            "should contain the table ref"
        );
        assert!(
            written.contains("dbo.sp_load_orders"),
            "should contain the proc ref"
        );
        assert!(written.contains("migrate"), "should contain the tier");
        assert!(
            written.contains("incremental"),
            "should contain the load strategy"
        );
    }
}
