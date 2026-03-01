use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use tauri::{AppHandle, Manager};

const WORKSPACE_DIR: &str = ".vibedata/migration-utility";
const CLAUDE_DIR: &str = ".claude";
const CLAUDE_FILE: &str = "CLAUDE.md";
const MANAGED_SUBDIRS: [&str; 2] = ["agents", "skills"];

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
        workspace_root.join(CLAUDE_DIR).display()
    );
    Ok(())
}

fn resolve_source_dir(app: &AppHandle) -> Result<PathBuf, String> {
    let mut candidates: Vec<PathBuf> = Vec::new();

    if let Ok(resource_dir) = app.path().resource_dir() {
        candidates.push(resource_dir.join("agent-sources").join("workspace"));
    }

    // Local dev fallback when running unbundled.
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

    ensure_dir(workspace_root, "workspace directory")?;

    let target_claude_dir = workspace_root.join(CLAUDE_DIR);
    ensure_dir(&target_claude_dir, ".claude directory")?;

    let source_claude = source.join(CLAUDE_FILE);
    if !source_claude.exists() {
        return Err(format!(
            "agent_sources: missing required source file: {}",
            source_claude.display()
        ));
    }
    let target_claude = target_claude_dir.join(CLAUDE_FILE);
    if !target_claude.exists() {
        log::info!(
            "agent_sources: copying CLAUDE.md {} -> {}",
            source_claude.display(),
            target_claude.display()
        );
        fs::copy(&source_claude, &target_claude).map_err(|e| {
            format!(
                "agent_sources: failed to copy {} -> {}: {}",
                source_claude.display(),
                target_claude.display(),
                e
            )
        })?;
    } else {
        log::debug!(
            "agent_sources: preserving existing CLAUDE.md at {}",
            target_claude.display()
        );
    }

    for dir in MANAGED_SUBDIRS {
        let source_dir = source.join(dir);
        if !source_dir.exists() {
            return Err(format!(
                "agent_sources: missing required source directory: {}",
                source_dir.display()
            ));
        }
        let target_dir = target_claude_dir.join(dir);
        let source_files = count_files_recursive(&source_dir)?;
        log::info!(
            "agent_sources: syncing managed dir '{}' source={} target={} source_files={}",
            dir,
            source_dir.display(),
            target_dir.display(),
            source_files
        );
        replace_directory(&source_dir, &target_dir)?;
        let copied_files = count_files_recursive(&target_dir)?;
        log::info!(
            "agent_sources: synced managed dir '{}' copied_files={}",
            dir,
            copied_files
        );
    }

    log::info!(
        "agent_sources: deploy completed in {}ms",
        start.elapsed().as_millis()
    );
    Ok(())
}

fn replace_directory(source: &Path, target: &Path) -> Result<(), String> {
    if !source.is_dir() {
        return Err(format!(
            "agent_sources: source directory does not exist: {}",
            source.display()
        ));
    }
    if target.exists() {
        log::debug!(
            "agent_sources: removing existing managed directory {}",
            target.display()
        );
        remove_path(target, "existing directory")?;
    }
    copy_dir_recursive(source, target)
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

    fn seed_source(source: &std::path::Path) {
        fs::create_dir_all(source.join("skills")).unwrap();
        fs::create_dir_all(source.join("agents")).unwrap();
        fs::write(source.join("CLAUDE.md"), "# CLAUDE").unwrap();
        fs::write(source.join("skills").join("seed-skill.md"), "seed skill").unwrap();
        fs::write(source.join("agents").join("seed-agent.md"), "seed agent").unwrap();
    }

    #[test]
    fn deploy_creates_claude_and_copies_managed_content() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("source");
        let workspace = tmp.path().join("workspace");
        let nested_skill = source.join("skills").join("example");
        let nested_agent = source.join("agents");

        fs::create_dir_all(&nested_skill).unwrap();
        fs::create_dir_all(&nested_agent).unwrap();
        fs::write(source.join("CLAUDE.md"), "# CLAUDE").unwrap();
        fs::write(nested_skill.join("SKILL.md"), "skill").unwrap();
        fs::write(nested_agent.join("agent.md"), "agent").unwrap();

        deploy_agent_sources(&source, &workspace).unwrap();

        assert_eq!(
            fs::read_to_string(workspace.join(".claude").join("CLAUDE.md")).unwrap(),
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
            "skill"
        );
        assert_eq!(
            fs::read_to_string(workspace.join(".claude").join("agents").join("agent.md")).unwrap(),
            "agent"
        );
    }

    #[test]
    fn deploy_replaces_agents_and_skills_but_keeps_other_claude_files() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("source");
        let workspace = tmp.path().join("workspace");
        let existing_claude = workspace.join(".claude");

        fs::create_dir_all(source.join("skills")).unwrap();
        fs::create_dir_all(source.join("agents")).unwrap();
        fs::write(source.join("CLAUDE.md"), "new").unwrap();
        fs::write(source.join("skills").join("new-skill.md"), "new skill").unwrap();
        fs::write(source.join("agents").join("new-agent.md"), "new agent").unwrap();

        fs::create_dir_all(existing_claude.join("skills")).unwrap();
        fs::create_dir_all(existing_claude.join("agents")).unwrap();
        fs::write(existing_claude.join("CLAUDE.md"), "old").unwrap();
        fs::write(existing_claude.join("skills").join("old-skill.md"), "old").unwrap();
        fs::write(existing_claude.join("agents").join("old-agent.md"), "old").unwrap();
        fs::write(existing_claude.join("custom.md"), "keep").unwrap();

        deploy_agent_sources(&source, &workspace).unwrap();

        assert_eq!(
            fs::read_to_string(existing_claude.join("CLAUDE.md")).unwrap(),
            "old"
        );
        assert!(!existing_claude.join("skills").join("old-skill.md").exists());
        assert!(!existing_claude.join("agents").join("old-agent.md").exists());
        assert_eq!(
            fs::read_to_string(existing_claude.join("skills").join("new-skill.md")).unwrap(),
            "new skill"
        );
        assert_eq!(
            fs::read_to_string(existing_claude.join("agents").join("new-agent.md")).unwrap(),
            "new agent"
        );
        assert_eq!(
            fs::read_to_string(existing_claude.join("custom.md")).unwrap(),
            "keep"
        );
    }

    #[test]
    fn deploy_errors_when_source_missing_claude_md() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("source");
        let workspace = tmp.path().join("workspace");

        fs::create_dir_all(source.join("skills")).unwrap();
        fs::create_dir_all(source.join("agents")).unwrap();

        let err = deploy_agent_sources(&source, &workspace).unwrap_err();
        assert!(err.contains("missing required source file"));
    }

    #[test]
    fn deploy_errors_when_source_missing_agents_or_skills() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("source");
        let workspace = tmp.path().join("workspace");

        fs::create_dir_all(source.join("skills")).unwrap();
        fs::write(source.join("CLAUDE.md"), "seed").unwrap();
        let err_agents = deploy_agent_sources(&source, &workspace).unwrap_err();
        assert!(err_agents.contains("missing required source directory"));

        fs::remove_dir_all(source.join("skills")).unwrap();
        fs::create_dir_all(source.join("agents")).unwrap();
        let err_skills = deploy_agent_sources(&source, &workspace).unwrap_err();
        assert!(err_skills.contains("missing required source directory"));
    }

    #[test]
    fn deploy_replaces_managed_directories_when_they_are_files() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("source");
        let workspace = tmp.path().join("workspace");
        seed_source(&source);

        let claude_dir = workspace.join(".claude");
        fs::create_dir_all(&claude_dir).unwrap();
        fs::write(claude_dir.join("skills"), "bad").unwrap();
        fs::write(claude_dir.join("agents"), "bad").unwrap();

        deploy_agent_sources(&source, &workspace).unwrap();

        assert!(claude_dir.join("skills").is_dir());
        assert!(claude_dir.join("agents").is_dir());
        assert!(claude_dir.join("skills").join("seed-skill.md").exists());
        assert!(claude_dir.join("agents").join("seed-agent.md").exists());
    }

    #[test]
    fn deploy_handles_workspace_or_claude_path_existing_as_file() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("source");
        let workspace = tmp.path().join("workspace");
        seed_source(&source);

        fs::write(&workspace, "not-a-dir").unwrap();
        deploy_agent_sources(&source, &workspace).unwrap();
        assert!(workspace.is_dir());

        let claude_path = workspace.join(".claude");
        fs::remove_dir_all(&claude_path).unwrap();
        fs::write(&claude_path, "not-a-dir").unwrap();
        deploy_agent_sources(&source, &workspace).unwrap();
        assert!(claude_path.is_dir());
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
        assert_eq!(
            fs::read_to_string(workspace.join(".claude").join("CLAUDE.md")).unwrap(),
            "# CLAUDE"
        );
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
