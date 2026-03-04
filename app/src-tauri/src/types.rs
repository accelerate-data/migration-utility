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
    #[serde(default)]
    pub preferred_model: Option<String>,
    #[serde(default)]
    pub effort: Option<String>,
    #[serde(default)]
    pub log_level: Option<String>,
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

/// Workspace representation safe to return to the renderer process.
/// Omits `source_password` and `fabric_service_principal_secret`.
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct WorkspacePublic {
    pub id: String,
    pub display_name: String,
    pub migration_repo_name: Option<String>,
    pub migration_repo_path: String,
    pub fabric_url: Option<String>,
    pub fabric_service_principal_id: Option<String>,
    pub source_type: Option<String>,
    pub source_server: Option<String>,
    pub source_database: Option<String>,
    pub source_port: Option<i64>,
    pub source_authentication_mode: Option<String>,
    pub source_username: Option<String>,
    pub source_encrypt: Option<bool>,
    pub source_trust_server_certificate: Option<bool>,
    pub created_at: String,
}

impl From<Workspace> for WorkspacePublic {
    fn from(w: Workspace) -> Self {
        WorkspacePublic {
            id: w.id,
            display_name: w.display_name,
            migration_repo_name: w.migration_repo_name,
            migration_repo_path: w.migration_repo_path,
            fabric_url: w.fabric_url,
            fabric_service_principal_id: w.fabric_service_principal_id,
            source_type: w.source_type,
            source_server: w.source_server,
            source_database: w.source_database,
            source_port: w.source_port,
            source_authentication_mode: w.source_authentication_mode,
            source_username: w.source_username,
            source_encrypt: w.source_encrypt,
            source_trust_server_certificate: w.source_trust_server_certificate,
            created_at: w.created_at,
        }
    }
}

/// App settings safe to return to the renderer process.
/// Secret fields (`anthropic_api_key`, `github_oauth_token`) are replaced with bool presence flags.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppSettingsPublic {
    pub has_anthropic_key: bool,
    pub has_github_auth: bool,
    pub github_user_login: Option<String>,
    pub github_user_avatar: Option<String>,
    pub github_user_email: Option<String>,
    pub preferred_model: Option<String>,
    pub effort: Option<String>,
    pub log_level: Option<String>,
}

impl From<AppSettings> for AppSettingsPublic {
    fn from(s: AppSettings) -> Self {
        AppSettingsPublic {
            has_anthropic_key: s.anthropic_api_key.is_some(),
            has_github_auth: s.github_oauth_token.is_some(),
            github_user_login: s.github_user_login,
            github_user_avatar: s.github_user_avatar,
            github_user_email: s.github_user_email,
            preferred_model: s.preferred_model,
            effort: s.effort,
            log_level: s.log_level,
        }
    }
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

