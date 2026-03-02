use std::path::PathBuf;
use std::time::Duration;
use std::{fs, io::Write};

use serde::Serialize;
use serde_json::Value;
use tauri::{AppHandle, Emitter, Manager, State};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::Mutex;

use crate::db::DbState;

const DEFAULT_MODEL: &str = "claude-sonnet-4-6";
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const HEARTBEAT_PONG_TIMEOUT: Duration = Duration::from_secs(5);

pub struct SidecarManager(Mutex<Option<PersistentSidecar>>);

impl Default for SidecarManager {
    fn default() -> Self {
        Self(Mutex::new(None))
    }
}

struct PersistentSidecar {
    child: Child,
    stdin: ChildStdin,
    stdout: Lines<BufReader<ChildStdout>>,
}

#[derive(Default)]
struct HeartbeatState {
    awaiting_pong: bool,
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct SidecarConfigPayload {
    prompt: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    system_prompt: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    agent_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    allowed_tools: Option<Vec<String>>,
    api_key: String,
    cwd: String,
}

#[derive(Serialize)]
struct SidecarAgentRequest {
    #[serde(rename = "type")]
    type_name: &'static str,
    request_id: String,
    config: SidecarConfigPayload,
}

#[derive(Clone)]
struct AgentRequest {
    id: String,
    config: SidecarConfigPayload,
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct MonitorStreamEvent {
    request_id: String,
    event_type: String,
    content: Option<String>,
    done: Option<bool>,
    tool_name: Option<String>,
    subtype: Option<String>,
    total_cost_usd: Option<f64>,
    input_tokens: Option<i64>,
    output_tokens: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct AgentRunResult {
    pub request_id: String,
    pub transcript_path: PathBuf,
    pub output_text: String,
}

pub async fn launch_agent_with_transcript(
    prompt: String,
    system_prompt: Option<String>,
    state: State<'_, DbState>,
    app: AppHandle,
    sidecar: State<'_, SidecarManager>,
) -> Result<AgentRunResult, String> {
    launch_agent_with_transcript_config(prompt, system_prompt, None, Some(DEFAULT_MODEL.to_string()), state, app, sidecar).await
}

pub async fn launch_named_agent_with_transcript(
    agent_name: String,
    prompt: String,
    system_prompt: Option<String>,
    state: State<'_, DbState>,
    app: AppHandle,
    sidecar: State<'_, SidecarManager>,
) -> Result<AgentRunResult, String> {
    launch_agent_with_transcript_config(prompt, system_prompt, Some(agent_name), None, state, app, sidecar).await
}

async fn launch_agent_with_transcript_config(
    prompt: String,
    system_prompt: Option<String>,
    agent_name: Option<String>,
    model: Option<String>,
    state: State<'_, DbState>,
    app: AppHandle,
    sidecar: State<'_, SidecarManager>,
) -> Result<AgentRunResult, String> {
    log::info!("monitor_launch_agent");
    let request = {
        let conn = state
            .0
            .lock()
            .map_err(|e| format!("monitor_launch_agent: failed to acquire DB lock: {e}"))?;

        let settings = crate::db::read_settings(&conn)?;
        let api_key = settings
            .anthropic_api_key
            .filter(|k| !k.trim().is_empty())
            .ok_or_else(|| "Anthropic API key is not configured in Settings".to_string())?;

        let working_directory = resolve_working_directory(&app)?;
        let front_matter = agent_name
            .as_deref()
            .and_then(|name| parse_agent_front_matter(&working_directory, name));
        let resolved_model = model.or_else(|| front_matter.as_ref().and_then(|fm| fm.model.clone()));
        let allowed_tools = front_matter.and_then(|fm| fm.allowed_tools);
        build_request(
            prompt,
            system_prompt,
            agent_name,
            resolved_model,
            allowed_tools,
            api_key,
            working_directory,
        )
    };

    let log_path = prepare_log_path(&request.config.cwd, &request.id)?;
    log::info!("monitor_launch_agent: transcript={}", log_path.display());

    let mut guard = sidecar.0.lock().await;
    ensure_sidecar_ready(&mut guard).await?;
    let proc = guard
        .as_mut()
        .ok_or_else(|| "monitor_launch_agent: sidecar unavailable after startup".to_string())?;

    let envelope = SidecarAgentRequest {
        type_name: "agent_request",
        request_id: request.id.clone(),
        config: request.config.clone(),
    };
    let request_json = serde_json::to_string(&envelope)
        .map_err(|e| format!("monitor_launch_agent: failed to serialize sidecar request: {e}"))?;
    proc.stdin
        .write_all(format!("{request_json}\n").as_bytes())
        .await
        .map_err(|e| {
            format!("monitor_launch_agent: failed writing request to sidecar stdin: {e}")
        })?;
    proc.stdin
        .flush()
        .await
        .map_err(|e| format!("monitor_launch_agent: failed flushing sidecar stdin: {e}"))?;

    let mut aggregated = String::new();
    let mut saw_done = false;
    let mut heartbeat = HeartbeatState::default();

    while let Some(line) = {
        match read_sidecar_line_with_heartbeat(
            &mut proc.stdout,
            &mut proc.stdin,
            &mut heartbeat,
            HEARTBEAT_INTERVAL,
            HEARTBEAT_PONG_TIMEOUT,
        )
        .await
        {
            Ok(next) => next,
            Err(e) => {
                log::error!("monitor_launch_agent: failed: {e}");
                let _ = proc.child.kill().await;
                *guard = None;
                append_log_line(
                    &log_path,
                    &serde_json::json!({"type":"error","message":e.clone()}).to_string(),
                )?;
                return Err(e);
            }
        }
    } {
        log::debug!("monitor_launch_agent[sidecar:stdout]: {}", line);
        append_request_scoped_sidecar_line(&log_path, request.id.as_str(), &line)?;
        emit_monitor_stream_event(&app, request.id.as_str(), &line);
        match handle_sidecar_line(&line, request.id.as_str(), &mut aggregated)? {
            SidecarLineResult::Continue => continue,
            SidecarLineResult::Done => {
                saw_done = true;
                break;
            }
        }
    }

    if !saw_done {
        *guard = None;
        append_log_line(
            &log_path,
            &serde_json::json!({"type":"error","message":"sidecar stream ended before completion"})
                .to_string(),
        )?;
        return Err("monitor_launch_agent: sidecar stream ended before completion".to_string());
    }

    let output_text = if aggregated.trim().is_empty() {
        "Agent run completed with no text output".to_string()
    } else {
        aggregated
    };

    Ok(AgentRunResult {
        request_id: request.id,
        transcript_path: log_path,
        output_text,
    })
}

#[tauri::command]
pub async fn monitor_launch_agent(
    prompt: String,
    system_prompt: Option<String>,
    state: State<'_, DbState>,
    app: AppHandle,
    sidecar: State<'_, SidecarManager>,
) -> Result<String, String> {
    let run = launch_agent_with_transcript(prompt, system_prompt, state, app, sidecar).await?;
    Ok(run.output_text)
}

async fn read_sidecar_line_with_heartbeat<R, W>(
    stdout_reader: &mut Lines<R>,
    stdin: &mut W,
    heartbeat: &mut HeartbeatState,
    heartbeat_interval: Duration,
    pong_timeout: Duration,
) -> Result<Option<String>, String>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    loop {
        let wait_duration = if heartbeat.awaiting_pong {
            pong_timeout
        } else {
            heartbeat_interval
        };

        let line_opt = match tokio::time::timeout(wait_duration, stdout_reader.next_line()).await {
            Ok(next_line_result) => next_line_result
                .map_err(|e| format!("monitor_launch_agent: failed reading sidecar stdout: {e}"))?,
            Err(_) => {
                if heartbeat.awaiting_pong {
                    let err = format!(
                        "monitor_launch_agent: sidecar did not emit pong within {}s",
                        pong_timeout.as_secs()
                    );
                    log::error!("{err}");
                    return Err(err);
                }

                let ping = serde_json::json!({ "type": "ping" }).to_string();
                stdin
                    .write_all(format!("{ping}\n").as_bytes())
                    .await
                    .map_err(|e| {
                        format!("monitor_launch_agent: failed writing ping to sidecar stdin: {e}")
                    })?;
                stdin.flush().await.map_err(|e| {
                    format!("monitor_launch_agent: failed flushing ping to sidecar stdin: {e}")
                })?;
                log::info!("monitor_launch_agent: sidecar ping sent");
                heartbeat.awaiting_pong = true;
                continue;
            }
        };

        let Some(line) = line_opt else {
            return Ok(None);
        };

        if parse_message_type(&line).as_deref() == Some("pong") {
            log::info!("monitor_launch_agent: sidecar pong received");
            heartbeat.awaiting_pong = false;
            continue;
        }

        // Any stream activity confirms liveness even if explicit pong was delayed.
        heartbeat.awaiting_pong = false;
        return Ok(Some(line));
    }
}

fn emit_monitor_stream_event(app: &AppHandle, request_id: &str, line: &str) {
    let parsed: Value = match serde_json::from_str(line) {
        Ok(v) => v,
        Err(_) => return,
    };
    let id = parsed
        .get("id")
        .and_then(Value::as_str)
        .or_else(|| parsed.get("request_id").and_then(Value::as_str))
        .unwrap_or_default();
    if id != request_id {
        return;
    }

    let message_type = parsed
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let payload = match message_type {
        "agent_response" => MonitorStreamEvent {
            request_id: request_id.to_string(),
            event_type: "agent_response".to_string(),
            content: parsed
                .get("content")
                .and_then(Value::as_str)
                .map(|s| s.to_string()),
            done: parsed.get("done").and_then(Value::as_bool),
            tool_name: None,
            subtype: None,
            total_cost_usd: None,
            input_tokens: None,
            output_tokens: None,
        },
        "agent_event" => {
            let event = parsed.get("event").unwrap_or(&Value::Null);
            let subtype = event
                .get("type")
                .and_then(Value::as_str)
                .map(|s| s.to_string());
            MonitorStreamEvent {
                request_id: request_id.to_string(),
                event_type: "agent_event".to_string(),
                content: None,
                done: None,
                tool_name: event
                    .get("tool_name")
                    .and_then(Value::as_str)
                    .map(|s| s.to_string()),
                subtype,
                total_cost_usd: event.get("total_cost_usd").and_then(Value::as_f64),
                input_tokens: event
                    .get("usage")
                    .and_then(|u| u.get("input_tokens"))
                    .and_then(Value::as_i64),
                output_tokens: event
                    .get("usage")
                    .and_then(|u| u.get("output_tokens"))
                    .and_then(Value::as_i64),
            }
        }
        "error" | "agent_error" => MonitorStreamEvent {
            request_id: request_id.to_string(),
            event_type: "error".to_string(),
            content: parsed
                .get("message")
                .and_then(Value::as_str)
                .map(|s| s.to_string()),
            done: Some(true),
            tool_name: None,
            subtype: None,
            total_cost_usd: None,
            input_tokens: None,
            output_tokens: None,
        },
        _ => return,
    };

    if let Err(e) = app.emit("monitor-agent-stream", payload) {
        log::warn!("monitor_launch_agent: failed to emit monitor-agent-stream: {e}");
    }
}

async fn ensure_sidecar_ready(slot: &mut Option<PersistentSidecar>) -> Result<(), String> {
    let mut needs_spawn = slot.is_none();
    if let Some(proc) = slot.as_mut() {
        match proc.child.try_wait() {
            Ok(Some(status)) => {
                log::warn!("monitor_launch_agent: sidecar exited, status={status}");
                needs_spawn = true;
            }
            Ok(None) => {}
            Err(e) => {
                log::warn!("monitor_launch_agent: sidecar try_wait failed: {e}");
                needs_spawn = true;
            }
        }
    }
    if !needs_spawn {
        return Ok(());
    }

    let sidecar_entry = resolve_sidecar_entrypoint()?;
    let mut child = Command::new("node")
        .arg(&sidecar_entry)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| format!("monitor_launch_agent: failed to spawn Node sidecar: {e}"))?;

    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| "monitor_launch_agent: missing sidecar stdin".to_string())?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "monitor_launch_agent: missing sidecar stdout".to_string())?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| "monitor_launch_agent: missing sidecar stderr".to_string())?;

    tokio::spawn(async move {
        let mut stderr_reader = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = stderr_reader.next_line().await {
            log::debug!("monitor_launch_agent[sidecar:stderr]: {}", line);
        }
    });

    let mut stdout_reader = BufReader::new(stdout).lines();
    if let Err(e) = wait_for_sidecar_message(&mut stdout_reader, "sidecar_ready", 50).await {
        let _ = child.kill().await;
        return Err(e);
    }
    log::info!("monitor_launch_agent: sidecar_ready received");

    let ping = serde_json::json!({ "type": "ping" }).to_string();
    stdin
        .write_all(format!("{ping}\n").as_bytes())
        .await
        .map_err(|e| format!("monitor_launch_agent: failed writing ping to sidecar stdin: {e}"))?;
    stdin
        .flush()
        .await
        .map_err(|e| format!("monitor_launch_agent: failed flushing ping to sidecar stdin: {e}"))?;
    log::info!("monitor_launch_agent: sidecar ping sent");

    if let Err(e) = wait_for_sidecar_message(&mut stdout_reader, "pong", 20).await {
        let _ = child.kill().await;
        return Err(e);
    }
    log::info!("monitor_launch_agent: sidecar pong received");

    *slot = Some(PersistentSidecar {
        child,
        stdin,
        stdout: stdout_reader,
    });
    Ok(())
}

async fn wait_for_sidecar_message<R>(
    stdout_reader: &mut Lines<R>,
    expected_type: &str,
    max_lines: usize,
) -> Result<(), String>
where
    R: AsyncBufRead + Unpin,
{
    for _ in 0..max_lines {
        if let Some(line) = stdout_reader.next_line().await.map_err(|e| {
            format!("monitor_launch_agent: failed reading sidecar startup line: {e}")
        })? {
            log::debug!("monitor_launch_agent[sidecar:stdout]: {}", line);
            if parse_message_type(&line).as_deref() == Some(expected_type) {
                return Ok(());
            }
        } else {
            break;
        }
    }
    log::error!(
        "monitor_launch_agent: sidecar did not emit expected startup message: {}",
        expected_type
    );
    Err(format!(
        "monitor_launch_agent: sidecar did not emit {}",
        expected_type
    ))
}

fn prepare_log_path(working_directory: &str, request_id: &str) -> Result<PathBuf, String> {
    let logs_dir = PathBuf::from(working_directory).join("logs");
    fs::create_dir_all(&logs_dir)
        .map_err(|e| format!("monitor_launch_agent: failed to create logs dir: {e}"))?;
    Ok(logs_dir.join(format!("agent-{request_id}.jsonl")))
}

fn append_log_line(path: &PathBuf, line: &str) -> Result<(), String> {
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| format!("monitor_launch_agent: failed to open transcript log: {e}"))?;
    writeln!(file, "{line}")
        .map_err(|e| format!("monitor_launch_agent: failed to append transcript line: {e}"))
}


fn append_request_scoped_sidecar_line(
    path: &PathBuf,
    request_id: &str,
    sidecar_line: &str,
) -> Result<(), String> {
    let parsed: Value = match serde_json::from_str(sidecar_line) {
        Ok(v) => v,
        Err(_) => return Ok(()),
    };

    let id = parsed
        .get("id")
        .and_then(|v| v.as_str())
        .or_else(|| parsed.get("request_id").and_then(|v| v.as_str()));
    if id != Some(request_id) {
        return Ok(());
    }

    append_log_line(path, sidecar_line)
}

fn resolve_sidecar_entrypoint() -> Result<PathBuf, String> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("sidecar")
        .join("dist")
        .join("index.js");
    if path.exists() {
        Ok(path)
    } else {
        Err(format!(
            "monitor_launch_agent: Node sidecar entrypoint not found at {} (run `npm run sidecar:build` from app/)",
            path.display()
        ))
    }
}

fn resolve_working_directory(app: &AppHandle) -> Result<String, String> {
    let home = app
        .path()
        .home_dir()
        .map_err(|e| format!("monitor_launch_agent: failed to resolve home dir: {e}"))?;
    Ok(home
        .join(".vibedata")
        .join("migration-utility")
        .to_string_lossy()
        .to_string())
}

fn build_request(
    prompt: String,
    system_prompt: Option<String>,
    agent_name: Option<String>,
    model: Option<String>,
    allowed_tools: Option<Vec<String>>,
    api_key: String,
    working_directory: String,
) -> AgentRequest {
    AgentRequest {
        id: format!("agent-{}", uuid::Uuid::new_v4()),
        config: SidecarConfigPayload {
            prompt,
            system_prompt,
            model,
            agent_name,
            allowed_tools,
            api_key,
            cwd: working_directory,
        },
    }
}

struct AgentFrontMatter {
    model: Option<String>,
    allowed_tools: Option<Vec<String>>,
}

fn parse_agent_front_matter(working_dir: &str, agent_name: &str) -> Option<AgentFrontMatter> {
    let path = PathBuf::from(working_dir)
        .join(".claude")
        .join("agents")
        .join(format!("{agent_name}.md"));
    let content = fs::read_to_string(&path).ok()?;
    let after_open = content.strip_prefix("---\n")?;
    let end = after_open.find("\n---")?;
    let front_matter = &after_open[..end];

    let model = front_matter.lines().find_map(|line| {
        let rest = line.strip_prefix("model:")?.trim();
        if rest.is_empty() { None } else { Some(rest.to_string()) }
    });

    let allowed_tools = front_matter.find("tools:").and_then(|pos| {
        let after_tools = &front_matter[pos + 6..];
        let first = after_tools.lines().next()?.trim();
        if first.starts_with('[') {
            // Inline list: tools: [Bash, Computer]
            let inner = first.trim_start_matches('[').trim_end_matches(']');
            let tools: Vec<String> = inner
                .split(',')
                .map(|s| s.trim().trim_matches('"').to_string())
                .filter(|s| !s.is_empty())
                .collect();
            if tools.is_empty() { None } else { Some(tools) }
        } else {
            // Block list:
            //   - Bash
            //   - Computer
            let tools: Vec<String> = after_tools
                .lines()
                .skip(1)
                .take_while(|l| l.trim().starts_with("- "))
                .map(|l| l.trim().trim_start_matches("- ").trim_matches('"').to_string())
                .collect();
            if tools.is_empty() { None } else { Some(tools) }
        }
    });

    Some(AgentFrontMatter { model, allowed_tools })
}

#[derive(Debug, PartialEq, Eq)]
enum SidecarLineResult {
    Continue,
    Done,
}

fn parse_message_type(line: &str) -> Option<String> {
    let v: Value = serde_json::from_str(line).ok()?;
    Some(v.get("type")?.as_str()?.to_string())
}

fn handle_sidecar_line(
    line: &str,
    request_id: &str,
    aggregated: &mut String,
) -> Result<SidecarLineResult, String> {
    let parsed: Value = serde_json::from_str(line)
        .map_err(|e| format!("monitor_launch_agent: invalid sidecar JSON line: {e}"))?;
    let message_type = parsed
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| "monitor_launch_agent: sidecar line missing type".to_string())?;

    match message_type {
        "agent_response" => {
            let id = parsed
                .get("id")
                .and_then(Value::as_str)
                .or_else(|| parsed.get("request_id").and_then(Value::as_str))
                .unwrap_or_default();
            if id != request_id {
                return Ok(SidecarLineResult::Continue);
            }
            if let Some(content) = parsed.get("content").and_then(Value::as_str) {
                aggregated.push_str(content);
            }
            let done = parsed.get("done").and_then(Value::as_bool).unwrap_or(false);
            if done {
                Ok(SidecarLineResult::Done)
            } else {
                Ok(SidecarLineResult::Continue)
            }
        }
        "agent_event" => {
            let id = parsed
                .get("id")
                .and_then(Value::as_str)
                .or_else(|| parsed.get("request_id").and_then(Value::as_str))
                .unwrap_or_default();
            if id != request_id {
                return Ok(SidecarLineResult::Continue);
            }
            // Extract result from event.result field
            if let Some(event) = parsed.get("event") {
                if let Some(result) = event.get("result").and_then(Value::as_str) {
                    aggregated.push_str(result);
                }
            }
            Ok(SidecarLineResult::Continue)
        }
        "result" => {
            // The SDK v1 query() API emits a top-level {"type":"result",...} message
            // as its final event. Extract the agent's text from the "result" field.
            let id = parsed
                .get("request_id")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if id != request_id {
                return Ok(SidecarLineResult::Continue);
            }
            let is_error = parsed.get("is_error").and_then(Value::as_bool).unwrap_or(false);
            if is_error {
                let message = parsed
                    .get("result")
                    .and_then(Value::as_str)
                    .unwrap_or("agent returned an error result");
                return Err(format!(
                    "monitor_launch_agent: agent result error: {message}"
                ));
            }
            if let Some(text) = parsed.get("result").and_then(Value::as_str) {
                aggregated.push_str(text);
            }
            Ok(SidecarLineResult::Continue)
        }
        "request_complete" => {
            let id = parsed
                .get("request_id")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if id == request_id {
                Ok(SidecarLineResult::Done)
            } else {
                Ok(SidecarLineResult::Continue)
            }
        }
        "error" | "agent_error" => {
            let id = parsed
                .get("id")
                .and_then(Value::as_str)
                .or_else(|| parsed.get("request_id").and_then(Value::as_str))
                .unwrap_or_default();
            if id != request_id {
                return Ok(SidecarLineResult::Continue);
            }
            let message = parsed
                .get("message")
                .or_else(|| parsed.get("error"))
                .and_then(Value::as_str)
                .unwrap_or("Unknown sidecar error");
            Err(format!(
                "monitor_launch_agent: sidecar agent error: {message}"
            ))
        }
        _ => Ok(SidecarLineResult::Continue),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        append_request_scoped_sidecar_line, build_request, handle_sidecar_line, parse_agent_front_matter,
        parse_message_type, prepare_log_path, read_sidecar_line_with_heartbeat,
        wait_for_sidecar_message, HeartbeatState, SidecarLineResult,
    };
    use std::fs;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::time::Duration;

    #[test]
    fn request_serializes_api_key_and_working_directory() {
        let req = build_request(
            "prompt".to_string(),
            Some("system".to_string()),
            None,
            Some("claude-sonnet-4-6".to_string()),
            None,
            "sk-ant-test".to_string(),
            "/tmp/work".to_string(),
        );
        let json = serde_json::to_value(&req.config).unwrap();
        assert_eq!(
            json.get("apiKey").and_then(|v| v.as_str()),
            Some("sk-ant-test")
        );
        assert_eq!(json.get("cwd").and_then(|v| v.as_str()), Some("/tmp/work"));
    }

    #[test]
    fn parse_message_type_extracts_type() {
        assert_eq!(
            parse_message_type(r#"{"type":"sidecar_ready"}"#).as_deref(),
            Some("sidecar_ready")
        );
    }

    #[test]
    fn handle_sidecar_line_aggregates_until_done() {
        let mut aggregated = String::new();
        let id = "agent-1";
        let first =
            r#"{"type":"agent_response","request_id":"agent-1","content":"hello ","done":false}"#
                .to_string();
        let second =
            r#"{"type":"agent_response","request_id":"agent-1","content":"world","done":true}"#
                .to_string();

        let r1 = handle_sidecar_line(&first, id, &mut aggregated).unwrap();
        let r2 = handle_sidecar_line(&second, id, &mut aggregated).unwrap();

        assert_eq!(r1, SidecarLineResult::Continue);
        assert_eq!(r2, SidecarLineResult::Done);
        assert_eq!(aggregated, "hello world");
    }

    #[test]
    fn handle_sidecar_line_extracts_result_from_agent_event() {
        let mut aggregated = String::new();
        let id = "agent-1";
        let event = r#"{"type":"agent_event","request_id":"agent-1","event":{"type":"result","result":"{\"table_type\":\"fact\"}"}}"#.to_string();

        let result = handle_sidecar_line(&event, id, &mut aggregated).unwrap();

        assert_eq!(result, SidecarLineResult::Continue);
        assert_eq!(aggregated, r#"{"table_type":"fact"}"#);
    }

    #[test]
    fn handle_sidecar_line_extracts_result_from_sdk_result_message() {
        let mut aggregated = String::new();
        let id = "agent-1";
        let msg = r#"{"request_id":"agent-1","type":"result","subtype":"success","is_error":false,"result":"{\"table_type\":\"dimension\"}"}"#;

        let result = handle_sidecar_line(msg, id, &mut aggregated).unwrap();

        assert_eq!(result, SidecarLineResult::Continue);
        assert_eq!(aggregated, r#"{"table_type":"dimension"}"#);
    }

    #[test]
    fn handle_sidecar_line_errors_on_sdk_result_is_error() {
        let mut aggregated = String::new();
        let id = "agent-1";
        let msg = r#"{"request_id":"agent-1","type":"result","subtype":"error_max_turns","is_error":true,"result":"max turns exceeded"}"#;

        let err = handle_sidecar_line(msg, id, &mut aggregated).unwrap_err();

        assert!(err.contains("agent result error"));
        assert!(err.contains("max turns exceeded"));
    }

    #[test]
    fn append_request_scoped_sidecar_line_writes_only_matching_request_id() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("t.jsonl");
        let id = "req-123";

        let matching = serde_json::json!({
            "type":"system",
            "request_id": id,
            "subtype":"sdk_ready",
            "timestamp": 1
        })
        .to_string();
        let other = serde_json::json!({
            "type":"system",
            "request_id":"req-other",
            "subtype":"sdk_ready",
            "timestamp": 2
        })
        .to_string();

        append_request_scoped_sidecar_line(&file, id, &other).unwrap();
        append_request_scoped_sidecar_line(&file, id, &matching).unwrap();

        let contents = fs::read_to_string(file).unwrap();
        assert!(contents.contains("\"request_id\":\"req-123\""));
        assert!(!contents.contains("\"request_id\":\"req-other\""));
    }

    #[test]
    fn prepare_log_path_creates_logs_folder_under_working_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let working = tmp.path().join("repo");
        let path = prepare_log_path(&working.to_string_lossy(), "req-123").unwrap();
        assert!(working.join("logs").is_dir());
        assert!(path.ends_with("agent-req-123.jsonl"));
    }

    #[tokio::test]
    async fn wait_for_sidecar_message_detects_pong() {
        let (mut writer, reader) = tokio::io::duplex(256);
        writer
            .write_all(b"{\"type\":\"sidecar_ready\"}\n{\"type\":\"pong\"}\n")
            .await
            .unwrap();
        drop(writer);

        let mut lines = BufReader::new(reader).lines();
        wait_for_sidecar_message(&mut lines, "pong", 5)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn wait_for_sidecar_message_errors_when_missing_expected_type() {
        let (mut writer, reader) = tokio::io::duplex(256);
        writer
            .write_all(b"{\"type\":\"sidecar_ready\"}\n{\"type\":\"agent_event\"}\n")
            .await
            .unwrap();
        drop(writer);

        let mut lines = BufReader::new(reader).lines();
        let err = wait_for_sidecar_message(&mut lines, "pong", 5)
            .await
            .unwrap_err();
        assert_eq!(err, "monitor_launch_agent: sidecar did not emit pong");
    }

    #[tokio::test]
    async fn read_sidecar_line_with_heartbeat_reads_line_without_ping() {
        let (mut stdout_writer, stdout_reader) = tokio::io::duplex(256);
        let (mut stdin_writer, _stdin_reader) = tokio::io::duplex(256);
        stdout_writer
            .write_all(b"{\"type\":\"agent_response\",\"request_id\":\"r1\",\"content\":\"ok\",\"done\":false}\n")
            .await
            .unwrap();
        drop(stdout_writer);

        let mut lines = BufReader::new(stdout_reader).lines();
        let mut heartbeat = HeartbeatState::default();
        let line = read_sidecar_line_with_heartbeat(
            &mut lines,
            &mut stdin_writer,
            &mut heartbeat,
            Duration::from_millis(50),
            Duration::from_millis(20),
        )
        .await
        .unwrap();
        assert!(line.unwrap().contains("\"type\":\"agent_response\""));
    }

    #[tokio::test]
    async fn read_sidecar_line_with_heartbeat_sends_ping_and_accepts_pong() {
        let (mut stdout_writer, stdout_reader) = tokio::io::duplex(256);
        let (mut stdin_writer, stdin_reader) = tokio::io::duplex(256);

        tokio::spawn(async move {
            let mut ping_lines = BufReader::new(stdin_reader).lines();
            let first = ping_lines.next_line().await.unwrap().unwrap();
            assert_eq!(first, r#"{"type":"ping"}"#);
            stdout_writer
                .write_all(
                    b"{\"type\":\"pong\"}\n{\"type\":\"agent_response\",\"request_id\":\"r1\",\"content\":\"ok\",\"done\":false}\n",
                )
                .await
                .unwrap();
        });

        let mut lines = BufReader::new(stdout_reader).lines();
        let mut heartbeat = HeartbeatState::default();
        let line = read_sidecar_line_with_heartbeat(
            &mut lines,
            &mut stdin_writer,
            &mut heartbeat,
            Duration::from_millis(20),
            Duration::from_millis(50),
        )
        .await
        .unwrap();
        assert!(line.unwrap().contains("\"type\":\"agent_response\""));
    }

    #[tokio::test]
    async fn read_sidecar_line_with_heartbeat_errors_when_pong_missing() {
        let (_stdout_writer, stdout_reader) = tokio::io::duplex(256);
        let (mut stdin_writer, stdin_reader) = tokio::io::duplex(256);

        tokio::spawn(async move {
            let mut ping_lines = BufReader::new(stdin_reader).lines();
            let first = ping_lines.next_line().await.unwrap().unwrap();
            assert_eq!(first, r#"{"type":"ping"}"#);
        });

        let mut lines = BufReader::new(stdout_reader).lines();
        let mut heartbeat = HeartbeatState::default();
        let err = read_sidecar_line_with_heartbeat(
            &mut lines,
            &mut stdin_writer,
            &mut heartbeat,
            Duration::from_millis(20),
            Duration::from_millis(20),
        )
        .await
        .unwrap_err();
        assert!(err.contains("did not emit pong within"));
    }

    fn write_agent(agents_dir: &std::path::Path, name: &str, content: &str) {
        fs::write(agents_dir.join(format!("{name}.md")), content).unwrap();
    }

    #[test]
    fn parse_front_matter_block_tools_and_model() {
        let tmp = tempfile::tempdir().unwrap();
        let agents_dir = tmp.path().join(".claude").join("agents");
        fs::create_dir_all(&agents_dir).unwrap();
        write_agent(
            &agents_dir,
            "my-agent",
            "---\nname: my-agent\nmodel: claude-haiku-4-5\ntools:\n  - Bash\n  - Computer\n---\n\nContent.\n",
        );
        let fm = parse_agent_front_matter(tmp.path().to_str().unwrap(), "my-agent").unwrap();
        assert_eq!(fm.model.as_deref(), Some("claude-haiku-4-5"));
        assert_eq!(fm.allowed_tools.unwrap(), vec!["Bash", "Computer"]);
    }

    #[test]
    fn parse_front_matter_inline_tools() {
        let tmp = tempfile::tempdir().unwrap();
        let agents_dir = tmp.path().join(".claude").join("agents");
        fs::create_dir_all(&agents_dir).unwrap();
        write_agent(&agents_dir, "my-agent", "---\ntools: [Bash, Computer]\n---\n\nContent.\n");
        let fm = parse_agent_front_matter(tmp.path().to_str().unwrap(), "my-agent").unwrap();
        assert_eq!(fm.allowed_tools.unwrap(), vec!["Bash", "Computer"]);
    }

    #[test]
    fn parse_front_matter_missing_file_returns_none() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(parse_agent_front_matter(tmp.path().to_str().unwrap(), "no-such-agent").is_none());
    }

    #[test]
    fn parse_front_matter_no_tools_returns_none_tools() {
        let tmp = tempfile::tempdir().unwrap();
        let agents_dir = tmp.path().join(".claude").join("agents");
        fs::create_dir_all(&agents_dir).unwrap();
        write_agent(&agents_dir, "my-agent", "---\nname: my-agent\nmodel: claude-haiku-4-5\n---\n\nContent.\n");
        let fm = parse_agent_front_matter(tmp.path().to_str().unwrap(), "my-agent").unwrap();
        assert_eq!(fm.model.as_deref(), Some("claude-haiku-4-5"));
        assert!(fm.allowed_tools.is_none());
    }

    #[test]
    fn parse_front_matter_no_model_returns_none_model() {
        let tmp = tempfile::tempdir().unwrap();
        let agents_dir = tmp.path().join(".claude").join("agents");
        fs::create_dir_all(&agents_dir).unwrap();
        write_agent(&agents_dir, "my-agent", "---\ntools:\n  - Bash\n---\n\nContent.\n");
        let fm = parse_agent_front_matter(tmp.path().to_str().unwrap(), "my-agent").unwrap();
        assert!(fm.model.is_none());
        assert_eq!(fm.allowed_tools.unwrap(), vec!["Bash"]);
    }

    #[test]
    fn allowed_tools_serialized_in_payload_when_set() {
        let req = build_request(
            "p".to_string(),
            None,
            Some("my-agent".to_string()),
            None,
            Some(vec!["Bash".to_string()]),
            "sk-ant-test".to_string(),
            "/tmp/work".to_string(),
        );
        let json = serde_json::to_value(&req.config).unwrap();
        let tools = json.get("allowedTools").unwrap().as_array().unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].as_str(), Some("Bash"));
    }

    #[test]
    fn allowed_tools_omitted_from_payload_when_none() {
        let req = build_request(
            "p".to_string(),
            None,
            None,
            Some("claude-sonnet-4-6".to_string()),
            None,
            "sk-ant-test".to_string(),
            "/tmp/work".to_string(),
        );
        let json = serde_json::to_value(&req.config).unwrap();
        assert!(json.get("allowedTools").is_none());
    }
}
