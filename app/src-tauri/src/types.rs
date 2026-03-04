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

/// App settings safe to return to the renderer process.
/// Secret fields are replaced with bool presence flags.
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

// ── App phase ─────────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AppPhase {
    SetupRequired,
    Configured,
    RunningLocked,
}

impl AppPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SetupRequired => "setup_required",
            Self::Configured => "configured",
            Self::RunningLocked => "running_locked",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "setup_required" => Some(Self::SetupRequired),
            "configured" => Some(Self::Configured),
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
    pub has_project: bool,
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

// ── Domain types ──────────────────────────────────────────────────────────────

/// A migration project. `sa_password` is never returned to the frontend.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Project {
    pub id: String,
    pub slug: String,
    pub name: String,
    pub created_at: String,
}

/// One agent run submission.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentRun {
    pub id: i64,
    pub project_id: String,
    pub run_id: String,
    pub action: String,
    pub submitted_ts: String,
    pub github_run_id: Option<String>,
    pub status: String,
}

// ── Error type ────────────────────────────────────────────────────────────────

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
