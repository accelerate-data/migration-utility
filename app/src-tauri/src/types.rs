use serde::{Deserialize, Serialize};
use thiserror::Error;

// ── App settings (persisted in the settings table) ────────────────────────────

#[derive(Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct AppSettings {
    #[serde(default)]
    pub anthropic_api_key: Option<String>,
    #[serde(default)]
    pub github_oauth_token: Option<String>,
    #[serde(default)]
    pub github_user_login: Option<String>,
    #[serde(default)]
    pub github_user_avatar: Option<String>,
    #[serde(default)]
    pub github_user_email: Option<String>,
}

impl std::fmt::Debug for AppSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppSettings")
            .field("anthropic_api_key", &"[REDACTED]")
            .field("github_oauth_token", &"[REDACTED]")
            .field("github_user_login", &self.github_user_login)
            .field("github_user_avatar", &self.github_user_avatar)
            .field("github_user_email", &self.github_user_email)
            .finish()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AppPhase {
    SetupRequired,
    ScopeEditable,
    PlanEditable,
    ReadyToRun,
    RunningLocked,
}

impl AppPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SetupRequired => "setup_required",
            Self::ScopeEditable => "scope_editable",
            Self::PlanEditable => "plan_editable",
            Self::ReadyToRun => "ready_to_run",
            Self::RunningLocked => "running_locked",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "setup_required" => Some(Self::SetupRequired),
            "scope_editable" => Some(Self::ScopeEditable),
            "plan_editable" => Some(Self::PlanEditable),
            "ready_to_run" => Some(Self::ReadyToRun),
            "running_locked" => Some(Self::RunningLocked),
            _ => None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppPhaseState {
    pub app_phase: AppPhase,
    pub has_github_auth: bool,
    pub has_anthropic_key: bool,
    pub is_source_applied: bool,
    pub scope_finalized: bool,
    pub plan_finalized: bool,
}

// ── GitHub OAuth types ────────────────────────────────────────────────────────

#[derive(Clone, Serialize, Deserialize)]
pub struct DeviceFlowResponse {
    pub device_code: String,
    pub user_code: String,
    pub verification_uri: String,
    pub expires_in: u64,
    pub interval: u64,
}

impl std::fmt::Debug for DeviceFlowResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeviceFlowResponse")
            .field("device_code", &"[REDACTED]")
            .field("user_code", &self.user_code)
            .field("verification_uri", &self.verification_uri)
            .field("expires_in", &self.expires_in)
            .field("interval", &self.interval)
            .finish()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GitHubUser {
    pub login: String,
    pub avatar_url: String,
    pub email: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GitHubRepo {
    pub id: i64,
    pub full_name: String,
    pub private: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "status")]
pub enum GitHubAuthResult {
    #[serde(rename = "pending")]
    Pending,
    #[serde(rename = "slow_down")]
    SlowDown,
    #[serde(rename = "success")]
    Success { user: GitHubUser },
}

#[derive(Debug, Error, Serialize)]
#[serde(tag = "kind", content = "message")]
pub enum CommandError {
    #[error("database error: {0}")]
    Database(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("io error: {0}")]
    Io(String),
    #[error("git error: {0}")]
    #[allow(dead_code)]
    Git(String),
}

impl From<rusqlite::Error> for CommandError {
    fn from(e: rusqlite::Error) -> Self {
        CommandError::Database(e.to_string())
    }
}

impl From<std::io::Error> for CommandError {
    fn from(e: std::io::Error) -> Self {
        CommandError::Io(e.to_string())
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Workspace {
    pub id: String,
    pub display_name: String,
    pub migration_repo_name: Option<String>,
    pub migration_repo_path: String,
    pub fabric_url: Option<String>,
    pub fabric_service_principal_id: Option<String>,
    pub fabric_service_principal_secret: Option<String>,
    pub source_type: Option<String>,
    pub source_server: Option<String>,
    pub source_database: Option<String>,
    pub source_port: Option<i64>,
    pub source_authentication_mode: Option<String>,
    pub source_username: Option<String>,
    pub source_password: Option<String>,
    pub source_encrypt: Option<bool>,
    pub source_trust_server_certificate: Option<bool>,
    pub created_at: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Item {
    pub id: String,
    pub workspace_id: String,
    pub display_name: String,
    pub description: Option<String>,
    pub folder_id: Option<String>,
    pub item_type: String,
    pub connection_string: Option<String>,
    pub collation_type: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct WarehouseSchema {
    pub warehouse_item_id: String,
    pub schema_name: String,
    pub schema_id_local: Option<i64>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct WarehouseTable {
    pub warehouse_item_id: String,
    pub schema_name: String,
    pub table_name: String,
    pub object_id_local: Option<i64>,
    pub row_count: Option<i64>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct WarehouseProcedure {
    pub warehouse_item_id: String,
    pub schema_name: String,
    pub procedure_name: String,
    pub object_id_local: Option<i64>,
    pub sql_body: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PipelineActivity {
    pub id: Option<i64>,
    pub pipeline_item_id: String,
    pub activity_name: String,
    pub activity_type: String,
    pub target_warehouse_item_id: Option<String>,
    pub target_schema_name: Option<String>,
    pub target_procedure_name: Option<String>,
    pub parameters_json: Option<String>,
    pub depends_on_json: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct SelectedTable {
    pub id: String,
    pub workspace_id: String,
    pub warehouse_item_id: String,
    pub schema_name: String,
    pub table_name: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TableArtifact {
    pub selected_table_id: String,
    pub warehouse_item_id: String,
    pub schema_name: String,
    pub procedure_name: String,
    pub pipeline_activity_id: Option<i64>,
    pub discovery_status: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Candidacy {
    pub warehouse_item_id: String,
    pub schema_name: String,
    pub procedure_name: String,
    pub tier: String,
    pub reasoning: Option<String>,
    pub overridden: bool,
    pub override_reason: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TableConfig {
    pub selected_table_id: String,
    pub table_type: Option<String>,
    pub load_strategy: Option<String>,
    pub grain_columns: Option<String>,
    pub relationships_json: Option<String>,
    pub incremental_column: Option<String>,
    pub date_column: Option<String>,
    pub snapshot_strategy: String,
    pub pii_columns: Option<String>,
    pub confirmed_at: Option<String>,
    pub analysis_metadata_json: Option<String>,
    pub approval_status: Option<String>,
    pub approved_at: Option<String>,
    pub manual_overrides_json: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ScopeTableRef {
    pub warehouse_item_id: String,
    pub schema_name: String,
    pub table_name: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ScopeInventoryRow {
    pub warehouse_item_id: String,
    pub schema_name: String,
    pub table_name: String,
    pub row_count: Option<i64>,
    pub is_selected: bool,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ScopeRefreshSummary {
    pub kept: i64,
    pub invalidated: i64,
    pub removed: i64,
}
