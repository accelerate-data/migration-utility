use crate::types::CommandError;

/// Run an external command, returning stdout on success or `CommandError::External` on failure.
/// Async version of `run_cmd` using `tokio::process::Command`.
/// Use this inside `async` Tauri commands so blocking I/O does not starve
/// the Tokio runtime and Tauri event delivery between steps.
pub(crate) async fn run_cmd_async(program: &str, args: &[&str], cwd: Option<&str>, envs: &[(&str, &str)]) -> Result<String, CommandError> {
    let mut cmd = tokio::process::Command::new(program);
    cmd.args(args);
    if let Some(dir) = cwd {
        cmd.current_dir(dir);
    }
    for (k, v) in envs {
        cmd.env(k, v);
    }
    let output = cmd.output().await.map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            CommandError::External(format!("'{program}' not found in PATH — please install it"))
        } else {
            CommandError::External(format!("failed to run '{program}': {e}"))
        }
    })?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let detail = if stderr.is_empty() { stdout } else { stderr };
        log::error!("[run_cmd_async] '{}' exited {}: {}", program, output.status, detail);
        return Err(CommandError::External(format!("'{program}' exited {}: {detail}", output.status)));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

pub(crate) fn run_cmd(program: &str, args: &[&str], cwd: Option<&str>, envs: &[(&str, &str)]) -> Result<String, CommandError> {
    let mut cmd = std::process::Command::new(program);
    cmd.args(args);
    if let Some(dir) = cwd {
        cmd.current_dir(dir);
    }
    for (k, v) in envs {
        cmd.env(k, v);
    }
    let output = cmd.output().map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            CommandError::External(format!(
                "'{program}' not found in PATH — please install it"
            ))
        } else {
            CommandError::External(format!("failed to run '{program}': {e}"))
        }
    })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let detail = if stderr.is_empty() { stdout } else { stderr };
        log::error!("[run_cmd] '{}' exited {}: {}", program, output.status, detail);
        return Err(CommandError::External(format!(
            "'{program}' exited {}: {detail}",
            output.status
        )));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}
