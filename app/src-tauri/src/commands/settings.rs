use tauri::State;

use crate::db::DbState;
use crate::types::{AppPhase, AppPhaseState, AppSettings};

#[tauri::command]
pub fn get_settings(state: State<'_, DbState>) -> Result<AppSettings, String> {
    log::info!("[get_settings]");
    let conn = state.0.lock().map_err(|e| {
        log::error!("[get_settings] Failed to acquire DB lock: {}", e);
        e.to_string()
    })?;
    crate::db::read_settings(&conn)
}

#[tauri::command]
pub fn save_anthropic_api_key(
    state: State<'_, DbState>,
    api_key: Option<String>,
) -> Result<(), String> {
    log::info!("[save_anthropic_api_key]");
    let conn = state.0.lock().map_err(|e| {
        log::error!("[save_anthropic_api_key] Failed to acquire DB lock: {}", e);
        e.to_string()
    })?;
    let mut settings = crate::db::read_settings(&conn)?;
    settings.anthropic_api_key = api_key;
    crate::db::write_settings(&conn, &settings)?;
    let _ = crate::db::reconcile_and_persist_app_phase(&conn)?;
    Ok(())
}

#[tauri::command]
pub fn app_hydrate_phase(state: State<'_, DbState>) -> Result<AppPhaseState, String> {
    log::info!("[app_hydrate_phase]");
    let conn = state.0.lock().map_err(|e| {
        log::error!("[app_hydrate_phase] Failed to acquire DB lock: {}", e);
        e.to_string()
    })?;
    crate::db::reconcile_and_persist_app_phase(&conn)
}

#[tauri::command]
pub fn app_set_phase(
    state: State<'_, DbState>,
    app_phase: String,
) -> Result<AppPhaseState, String> {
    log::info!("[app_set_phase] {}", app_phase);
    let conn = state.0.lock().map_err(|e| {
        log::error!("[app_set_phase] Failed to acquire DB lock: {}", e);
        e.to_string()
    })?;
    let phase = AppPhase::from_str(app_phase.as_str())
        .ok_or_else(|| format!("Unsupported app phase: {}", app_phase))?;
    crate::db::write_app_phase(&conn, phase)?;
    crate::db::read_current_app_phase_state(&conn)
}

#[tauri::command]
pub fn app_set_phase_flags(
    state: State<'_, DbState>,
    scope_finalized: Option<bool>,
    plan_finalized: Option<bool>,
) -> Result<AppPhaseState, String> {
    log::info!(
        "[app_set_phase_flags] scope_finalized={:?} plan_finalized={:?}",
        scope_finalized,
        plan_finalized
    );
    let conn = state.0.lock().map_err(|e| {
        log::error!("[app_set_phase_flags] Failed to acquire DB lock: {}", e);
        e.to_string()
    })?;
    if let Some(value) = scope_finalized {
        crate::db::write_scope_finalized(&conn, value)?;
    }
    if let Some(value) = plan_finalized {
        crate::db::write_plan_finalized(&conn, value)?;
    }
    crate::db::reconcile_and_persist_app_phase(&conn)
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ModelInfo {
    pub id: String,
    pub display_name: String,
}

#[derive(serde::Deserialize)]
struct ModelsApiResponse {
    data: Vec<ModelsApiItem>,
}

#[derive(serde::Deserialize)]
struct ModelsApiItem {
    id: String,
    display_name: String,
}

/// Fetch models available for the given API key from the Anthropic API.
/// Returns models sorted as returned by the API (newest first).
#[tauri::command]
pub async fn list_models(api_key: String) -> Result<Vec<ModelInfo>, String> {
    log::info!("[list_models]");
    let client = reqwest::Client::new();
    let resp = client
        .get("https://api.anthropic.com/v1/models")
        .header("x-api-key", &api_key)
        .header("anthropic-version", "2023-06-01")
        .send()
        .await
        .map_err(|e| format!("Request failed: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!("API error: {}", resp.status()));
    }
    let body: ModelsApiResponse = resp.json().await.map_err(|e| format!("Parse error: {e}"))?;
    let models = body
        .data
        .into_iter()
        .filter(|m| m.id.starts_with("claude-"))
        .map(|m| ModelInfo { id: m.id, display_name: m.display_name })
        .collect();
    Ok(models)
}

#[tauri::command]
pub fn save_agent_settings(
    state: State<'_, DbState>,
    preferred_model: Option<String>,
    effort: Option<String>,
) -> Result<(), String> {
    log::info!(
        "[save_agent_settings] model={:?} effort={:?}",
        preferred_model,
        effort,
    );
    let conn = state.0.lock().map_err(|e| {
        log::error!("[save_agent_settings] Failed to acquire DB lock: {}", e);
        e.to_string()
    })?;
    let mut settings = crate::db::read_settings(&conn)?;
    settings.preferred_model = preferred_model;
    settings.effort = effort;
    crate::db::write_settings(&conn, &settings)?;
    Ok(())
}

#[tauri::command]
pub async fn test_api_key(api_key: String) -> Result<bool, String> {
    log::info!("[test_api_key]");
    let client = reqwest::Client::new();
    let resp = client
        .post("https://api.anthropic.com/v1/messages")
        .header("x-api-key", &api_key)
        .header("anthropic-version", "2023-06-01")
        .header("content-type", "application/json")
        .body(
            serde_json::json!({
                "model": "claude-haiku-4-5",
                "max_tokens": 1,
                "messages": [{"role": "user", "content": "hi"}]
            })
            .to_string(),
        )
        .send()
        .await
        .map_err(|e| {
            log::error!("[test_api_key] network error: {}", e);
            "Network error while validating API key".to_string()
        })?;

    let status = resp.status().as_u16();
    match status {
        400 | 401 => Err("Invalid API key".to_string()),
        403 => Err("API key is disabled".to_string()),
        _ if resp.status().is_success() => Ok(true),
        _ => Err("Failed to validate API key".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use crate::db;
    use crate::types::AppSettings;

    #[test]
    fn settings_roundtrip_persists_anthropic_key() {
        let conn = db::open_in_memory().unwrap();
        let settings = AppSettings {
            anthropic_api_key: Some("sk-ant-test".to_string()),
            ..AppSettings::default()
        };
        db::write_settings(&conn, &settings).unwrap();
        let read = db::read_settings(&conn).unwrap();
        assert_eq!(read.anthropic_api_key.as_deref(), Some("sk-ant-test"));
    }

    #[test]
    fn agent_settings_roundtrip_persists_model_and_effort() {
        let conn = db::open_in_memory().unwrap();
        let settings = AppSettings {
            preferred_model: Some("claude-haiku-4-5-20251001".to_string()),
            effort: Some("low".to_string()),
            ..AppSettings::default()
        };
        db::write_settings(&conn, &settings).unwrap();
        let read = db::read_settings(&conn).unwrap();
        assert_eq!(read.preferred_model.as_deref(), Some("claude-haiku-4-5-20251001"));
        assert_eq!(read.effort.as_deref(), Some("low"));
    }
}
