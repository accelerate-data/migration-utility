use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use tauri::{AppHandle, Manager};

const WORKSPACE_DIR: &str = ".vibedata/migration-utility";
const CLAUDE_DIR: &str = ".claude";

pub fn deploy_on_startup(app: &AppHandle) -> Result<(), String> {
    let source = resolve_source_dir(app)?;
    let workspace_root = workspace_target_dir(app)?;
    log::info!(
        "agent_sources: startup deploy begin source={} workspace={}",
        source.display(),
        workspace_root.display()
    );
    deploy_agent_sources(&source, &workspace_root)?;
    log::info!(
        "agent_sources: deployed {} -> {}",
        source.display(),
        workspace_root.display()
    );
    Ok(())
}

fn resolve_source_dir(app: &AppHandle) -> Result<PathBuf, String> {
    let mut candidates: Vec<PathBuf> = Vec::new();

    // In dev builds the Tauri resource_dir resolves to the target artifact directory
    // (target/debug/_up_/_up_/agent-sources), which only reflects the last cargo build and
    // may belong to a different worktree. Always use the live compile-time source instead.
    #[cfg(not(debug_assertions))]
    if let Ok(resource_dir) = app.path().resource_dir() {
        candidates.push(resource_dir.join("agent-sources").join("workspace"));
    }
    #[cfg(debug_assertions)]
    let _ = app; // suppress unused-variable warning in dev

    // Production fallback (or primary dev source): the compile-time workspace root.
    candidates.push(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("agent-sources")
            .join("workspace"),
    );

    resolve_source_dir_from_candidates(&candidates)
}

fn workspace_target_dir(app: &AppHandle) -> Result<PathBuf, String> {
    let home = app
        .path()
        .home_dir()
        .map_err(|e| format!("agent_sources: failed to resolve home dir: {e}"))?;
    Ok(home.join(WORKSPACE_DIR))
}

fn resolve_source_dir_from_candidates(candidates: &[PathBuf]) -> Result<PathBuf, String> {
    for candidate in candidates {
        log::debug!(
            "agent_sources: checking source candidate path={} exists={} is_dir={}",
            candidate.display(),
            candidate.exists(),
            candidate.is_dir()
        );
        if candidate.is_dir() {
            log::info!(
                "agent_sources: selected source directory {}",
                candidate.display()
            );
            return Ok(candidate.clone());
        }
    }
    Err("agent_sources: could not locate bundled or dev agent source directory".to_string())
}

fn deploy_agent_sources(source: &Path, workspace_root: &Path) -> Result<(), String> {
    let start = Instant::now();
    if !source.is_dir() {
        return Err(format!(
            "agent_sources: source directory does not exist: {}",
            source.display()
        ));
    }
    let source_claude = source.join(CLAUDE_DIR);
    if !source_claude.is_dir() {
        return Err(format!(
            "agent_sources: missing required source directory: {}",
            source_claude.display()
        ));
    }

    ensure_dir(workspace_root, "workspace directory")?;
    clear_directory_contents(workspace_root)?;
    copy_dir_recursive(source, workspace_root)?;
    let copied_files = count_files_recursive(workspace_root)?;
    log::info!(
        "agent_sources: synced workspace source={} target={} copied_files={}",
        source.display(),
        workspace_root.display(),
        copied_files
    );

    log::info!(
        "agent_sources: deploy completed in {}ms",
        start.elapsed().as_millis()
    );
    Ok(())
}

fn clear_directory_contents(dir: &Path) -> Result<(), String> {
    for entry in fs::read_dir(dir).map_err(|e| {
        format!(
            "agent_sources: failed to read directory for clearing {}: {}",
            dir.display(),
            e
        )
    })? {
        let entry = entry.map_err(|e| format!("agent_sources: failed to read entry: {e}"))?;
        remove_path(&entry.path(), "workspace content")?;
    }
    Ok(())
}

fn copy_dir_recursive(source: &Path, target: &Path) -> Result<(), String> {
    if !source.is_dir() {
        return Err(format!(
            "agent_sources: source directory does not exist: {}",
            source.display()
        ));
    }

    fs::create_dir_all(target).map_err(|e| {
        format!(
            "agent_sources: failed to create destination directory {}: {}",
            target.display(),
            e
        )
    })?;

    for entry in fs::read_dir(source).map_err(|e| {
        format!(
            "agent_sources: failed to read source directory {}: {}",
            source.display(),
            e
        )
    })? {
        let entry = entry.map_err(|e| format!("agent_sources: failed to read entry: {e}"))?;
        let path = entry.path();
        let destination = target.join(entry.file_name());
        let file_type = entry
            .file_type()
            .map_err(|e| format!("agent_sources: failed to read file type: {e}"))?;

        if file_type.is_dir() {
            copy_dir_recursive(&path, &destination)?;
            continue;
        }

        log::debug!(
            "agent_sources: copying file {} -> {}",
            path.display(),
            destination.display()
        );
        fs::copy(&path, &destination).map_err(|e| {
            format!(
                "agent_sources: failed to copy {} -> {}: {}",
                path.display(),
                destination.display(),
                e
            )
        })?;
    }
    Ok(())
}

fn count_files_recursive(path: &Path) -> Result<usize, String> {
    if !path.is_dir() {
        return Ok(0);
    }
    let mut count = 0usize;
    for entry in fs::read_dir(path).map_err(|e| {
        format!(
            "agent_sources: failed to read directory for counting {}: {}",
            path.display(),
            e
        )
    })? {
        let entry = entry.map_err(|e| format!("agent_sources: failed to read entry: {e}"))?;
        let file_type = entry
            .file_type()
            .map_err(|e| format!("agent_sources: failed to read file type: {e}"))?;
        if file_type.is_dir() {
            count += count_files_recursive(&entry.path())?;
        } else {
            count += 1;
        }
    }
    Ok(count)
}

fn ensure_dir(path: &Path, label: &str) -> Result<(), String> {
    if path.exists() {
        let metadata = fs::symlink_metadata(path).map_err(|e| {
            format!(
                "agent_sources: failed to stat {} {}: {}",
                label,
                path.display(),
                e
            )
        })?;
        if metadata.is_dir() && !metadata.file_type().is_symlink() {
            return Ok(());
        }
        remove_path(path, label)?;
    }

    fs::create_dir_all(path).map_err(|e| {
        format!(
            "agent_sources: failed to create {} {}: {}",
            label,
            path.display(),
            e
        )
    })
}

fn remove_path(path: &Path, label: &str) -> Result<(), String> {
    let metadata = fs::symlink_metadata(path).map_err(|e| {
        format!(
            "agent_sources: failed to stat {} {}: {}",
            label,
            path.display(),
            e
        )
    })?;
    if metadata.is_dir() && !metadata.file_type().is_symlink() {
        fs::remove_dir_all(path).map_err(|e| {
            format!(
                "agent_sources: failed to remove {} {}: {}",
                label,
                path.display(),
                e
            )
        })
    } else {
        fs::remove_file(path).map_err(|e| {
            format!(
                "agent_sources: failed to remove {} {}: {}",
                label,
                path.display(),
                e
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{deploy_agent_sources, resolve_source_dir_from_candidates};
    use std::fs;

    fn source_claude(source: &std::path::Path) -> std::path::PathBuf {
        source.join(".claude")
    }

    fn seed_source(source: &std::path::Path) {
        let claude = source_claude(source);
        fs::create_dir_all(claude.join("skills").join("example")).unwrap();
        fs::create_dir_all(claude.join("agents")).unwrap();
        fs::create_dir_all(claude.join("rules")).unwrap();
        fs::write(source.join("CLAUDE.md"), "# CLAUDE").unwrap();
        fs::write(
            claude.join("skills").join("example").join("SKILL.md"),
            "seed skill",
        )
        .unwrap();
        fs::write(claude.join("agents").join("seed-agent.md"), "seed agent").unwrap();
        fs::write(claude.join("rules").join("seed-rule.md"), "seed rule").unwrap();
    }

    #[test]
    fn deploy_creates_workspace_and_copies_source_content() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("source");
        let workspace = tmp.path().join("workspace");
        seed_source(&source);

        deploy_agent_sources(&source, &workspace).unwrap();

        assert_eq!(
            fs::read_to_string(workspace.join("CLAUDE.md")).unwrap(),
            "# CLAUDE"
        );
        assert_eq!(
            fs::read_to_string(
                workspace
                    .join(".claude")
                    .join("skills")
                    .join("example")
                    .join("SKILL.md")
            )
            .unwrap(),
            "seed skill"
        );
        assert_eq!(
            fs::read_to_string(workspace.join(".claude").join("agents").join("seed-agent.md"))
                .unwrap(),
            "seed agent"
        );
        assert_eq!(
            fs::read_to_string(
                workspace
                    .join(".claude")
                    .join("rules")
                    .join("seed-rule.md")
            )
            .unwrap(),
            "seed rule"
        );
    }

    #[test]
    fn deploy_replaces_all_existing_workspace_content() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("source");
        let workspace = tmp.path().join("workspace");
        seed_source(&source);

        fs::create_dir_all(workspace.join(".claude")).unwrap();
        fs::write(workspace.join(".claude").join("custom.md"), "remove me").unwrap();
        fs::write(workspace.join("CLAUDE.md"), "old").unwrap();
        fs::write(workspace.join("stale.txt"), "stale").unwrap();

        deploy_agent_sources(&source, &workspace).unwrap();

        assert!(workspace.join(".claude").exists());
        assert!(!workspace.join(".claude").join("custom.md").exists());
        assert!(!workspace.join("stale.txt").exists());
        assert_eq!(fs::read_to_string(workspace.join("CLAUDE.md")).unwrap(), "# CLAUDE");
        assert!(workspace
            .join(".claude")
            .join("agents")
            .join("seed-agent.md")
            .exists());
        assert!(workspace
            .join(".claude")
            .join("skills")
            .join("example")
            .join("SKILL.md")
            .exists());
        assert!(workspace
            .join(".claude")
            .join("rules")
            .join("seed-rule.md")
            .exists());
    }

    #[test]
    fn deploy_errors_when_source_missing_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("missing");
        let workspace = tmp.path().join("workspace");

        let err = deploy_agent_sources(&source, &workspace).unwrap_err();
        assert!(err.contains("source directory does not exist"));
    }

    #[test]
    fn deploy_errors_when_source_missing_dot_claude() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("source");
        let workspace = tmp.path().join("workspace");
        fs::create_dir_all(&source).unwrap();
        fs::write(source.join("CLAUDE.md"), "# CLAUDE").unwrap();

        let err = deploy_agent_sources(&source, &workspace).unwrap_err();
        assert!(err.contains("missing required source directory"));
    }

    #[test]
    fn deploy_handles_workspace_path_existing_as_file() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("source");
        let workspace = tmp.path().join("workspace");
        seed_source(&source);

        fs::write(&workspace, "not-a-dir").unwrap();
        deploy_agent_sources(&source, &workspace).unwrap();
        assert!(workspace.is_dir());
    }

    #[test]
    fn deploy_is_idempotent_across_repeated_runs() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("source");
        let workspace = tmp.path().join("workspace");
        seed_source(&source);

        deploy_agent_sources(&source, &workspace).unwrap();
        fs::write(
            workspace
                .join(".claude")
                .join("skills")
                .join("transient.md"),
            "remove me",
        )
        .unwrap();
        deploy_agent_sources(&source, &workspace).unwrap();

        assert!(!workspace
            .join(".claude")
            .join("skills")
            .join("transient.md")
            .exists());
        assert_eq!(fs::read_to_string(workspace.join("CLAUDE.md")).unwrap(), "# CLAUDE");
    }

    #[test]
    fn resolve_source_dir_uses_first_existing_candidate() {
        let tmp = tempfile::tempdir().unwrap();
        let first = tmp.path().join("first");
        let second = tmp.path().join("second");
        fs::create_dir_all(&second).unwrap();
        fs::create_dir_all(&first).unwrap();

        let resolved = resolve_source_dir_from_candidates(&[first.clone(), second]).unwrap();
        assert_eq!(resolved, first);
    }

    #[test]
    fn resolve_source_dir_errors_when_no_candidate_exists() {
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("missing");
        let err = resolve_source_dir_from_candidates(&[missing]).unwrap_err();
        assert!(err.contains("could not locate bundled or dev agent source directory"));
    }
}
