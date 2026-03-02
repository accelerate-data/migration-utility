use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use tauri::{AppHandle, Manager};

const WORKSPACE_DIR: &str = ".vibedata/migration-utility";
const CLAUDE_DIR: &str = ".claude";
const CLAUDE_FILE: &str = "CLAUDE.md";
const MANAGED_SUBDIRS: [&str; 3] = ["agents", "skills", "rules"];
const CUSTOMIZATION_SENTINEL: &str = "\n## Customization";

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
    // CLAUDE.md lives at workspace root, not inside .claude/
    // The Claude Agent SDK auto-loads from {cwd}/CLAUDE.md
    let target_claude = workspace_root.join(CLAUDE_FILE);
    merge_claude_md(&source_claude, &target_claude)?;

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

fn merge_claude_md(source_path: &Path, target_path: &Path) -> Result<(), String> {
    let source_content = fs::read_to_string(source_path).map_err(|e| {
        format!(
            "agent_sources: failed to read source CLAUDE.md {}: {}",
            source_path.display(),
            e
        )
    })?;

    if !target_path.exists() {
        log::info!(
            "agent_sources: creating CLAUDE.md {} -> {}",
            source_path.display(),
            target_path.display()
        );
        return fs::write(target_path, &source_content).map_err(|e| {
            format!(
                "agent_sources: failed to write CLAUDE.md {}: {}",
                target_path.display(),
                e
            )
        });
    }

    let existing_content = fs::read_to_string(target_path).map_err(|e| {
        format!(
            "agent_sources: failed to read existing CLAUDE.md {}: {}",
            target_path.display(),
            e
        )
    })?;

    let merged = merge_claude_content(&source_content, &existing_content);

    if merged == existing_content {
        log::debug!(
            "agent_sources: CLAUDE.md unchanged, skipping write {}",
            target_path.display()
        );
        return Ok(());
    }

    log::info!(
        "agent_sources: updating CLAUDE.md managed section {}",
        target_path.display()
    );
    fs::write(target_path, merged).map_err(|e| {
        format!(
            "agent_sources: failed to write CLAUDE.md {}: {}",
            target_path.display(),
            e
        )
    })
}

fn merge_claude_content(source: &str, existing: &str) -> String {
    let managed = match source.find(CUSTOMIZATION_SENTINEL) {
        Some(pos) => &source[..pos],
        None => source,
    };
    match existing.find(CUSTOMIZATION_SENTINEL) {
        Some(pos) => {
            let user_section = &existing[pos..];
            let mut result = managed.trim_end().to_string();
            result.push('\n');
            result.push_str(user_section);
            if !result.ends_with('\n') {
                result.push('\n');
            }
            result
        }
        None => source.to_string(),
    }
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
    use super::{deploy_agent_sources, merge_claude_content, merge_claude_md, resolve_source_dir_from_candidates};
    use std::fs;

    fn seed_source(source: &std::path::Path) {
        fs::create_dir_all(source.join("skills")).unwrap();
        fs::create_dir_all(source.join("agents")).unwrap();
        fs::create_dir_all(source.join("rules")).unwrap();
        fs::write(source.join("CLAUDE.md"), "# CLAUDE").unwrap();
        fs::write(source.join("skills").join("seed-skill.md"), "seed skill").unwrap();
        fs::write(source.join("agents").join("seed-agent.md"), "seed agent").unwrap();
        fs::write(source.join("rules").join("seed-rule.md"), "seed rule").unwrap();
    }

    #[test]
    fn deploy_creates_claude_and_copies_managed_content() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("source");
        let workspace = tmp.path().join("workspace");
        let nested_skill = source.join("skills").join("example");
        let nested_agent = source.join("agents");
        let source_rules = source.join("rules");

        fs::create_dir_all(&nested_skill).unwrap();
        fs::create_dir_all(&nested_agent).unwrap();
        fs::create_dir_all(&source_rules).unwrap();
        fs::write(source.join("CLAUDE.md"), "# CLAUDE").unwrap();
        fs::write(nested_skill.join("SKILL.md"), "skill").unwrap();
        fs::write(nested_agent.join("agent.md"), "agent").unwrap();
        fs::write(source_rules.join("source-sql-server.md"), "rule").unwrap();

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
            "skill"
        );
        assert_eq!(
            fs::read_to_string(workspace.join(".claude").join("agents").join("agent.md")).unwrap(),
            "agent"
        );
        assert_eq!(
            fs::read_to_string(
                workspace
                    .join(".claude")
                    .join("rules")
                    .join("source-sql-server.md")
            )
            .unwrap(),
            "rule"
        );
    }

    #[test]
    fn deploy_replaces_managed_dirs_but_keeps_other_claude_files() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("source");
        let workspace = tmp.path().join("workspace");
        let existing_claude = workspace.join(".claude");

        fs::create_dir_all(source.join("skills")).unwrap();
        fs::create_dir_all(source.join("agents")).unwrap();
        fs::create_dir_all(source.join("rules")).unwrap();
        fs::write(source.join("CLAUDE.md"), "# New\n\n## Customization\n").unwrap();
        fs::write(source.join("skills").join("new-skill.md"), "new skill").unwrap();
        fs::write(source.join("agents").join("new-agent.md"), "new agent").unwrap();
        fs::write(source.join("rules").join("new-rule.md"), "new rule").unwrap();

        fs::create_dir_all(existing_claude.join("skills")).unwrap();
        fs::create_dir_all(existing_claude.join("agents")).unwrap();
        fs::create_dir_all(existing_claude.join("rules")).unwrap();
        // CLAUDE.md lives at workspace root, not inside .claude/
        fs::write(workspace.join("CLAUDE.md"), "# Old\n\n## Customization\n\nmy note\n").unwrap();
        fs::write(existing_claude.join("skills").join("old-skill.md"), "old").unwrap();
        fs::write(existing_claude.join("agents").join("old-agent.md"), "old").unwrap();
        fs::write(existing_claude.join("rules").join("old-rule.md"), "old").unwrap();
        fs::write(existing_claude.join("custom.md"), "keep").unwrap();

        deploy_agent_sources(&source, &workspace).unwrap();

        assert_eq!(
            fs::read_to_string(workspace.join("CLAUDE.md")).unwrap(),
            "# New\n\n## Customization\n\nmy note\n"
        );
        assert!(!existing_claude.join("skills").join("old-skill.md").exists());
        assert!(!existing_claude.join("agents").join("old-agent.md").exists());
        assert!(!existing_claude.join("rules").join("old-rule.md").exists());
        assert_eq!(
            fs::read_to_string(existing_claude.join("skills").join("new-skill.md")).unwrap(),
            "new skill"
        );
        assert_eq!(
            fs::read_to_string(existing_claude.join("agents").join("new-agent.md")).unwrap(),
            "new agent"
        );
        assert_eq!(
            fs::read_to_string(existing_claude.join("rules").join("new-rule.md")).unwrap(),
            "new rule"
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
    fn deploy_errors_when_source_missing_managed_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("source");
        let workspace = tmp.path().join("workspace");

        // Missing agents
        fs::create_dir_all(source.join("skills")).unwrap();
        fs::create_dir_all(source.join("rules")).unwrap();
        fs::write(source.join("CLAUDE.md"), "seed").unwrap();
        let err = deploy_agent_sources(&source, &workspace).unwrap_err();
        assert!(err.contains("missing required source directory"));

        // Missing skills
        fs::remove_dir_all(source.join("skills")).unwrap();
        fs::create_dir_all(source.join("agents")).unwrap();
        let err = deploy_agent_sources(&source, &workspace).unwrap_err();
        assert!(err.contains("missing required source directory"));

        // Missing rules
        fs::create_dir_all(source.join("skills")).unwrap();
        fs::remove_dir_all(source.join("rules")).unwrap();
        let err = deploy_agent_sources(&source, &workspace).unwrap_err();
        assert!(err.contains("missing required source directory"));
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
        fs::write(claude_dir.join("rules"), "bad").unwrap();

        deploy_agent_sources(&source, &workspace).unwrap();

        assert!(claude_dir.join("skills").is_dir());
        assert!(claude_dir.join("agents").is_dir());
        assert!(claude_dir.join("rules").is_dir());
        assert!(claude_dir.join("skills").join("seed-skill.md").exists());
        assert!(claude_dir.join("agents").join("seed-agent.md").exists());
        assert!(claude_dir.join("rules").join("seed-rule.md").exists());
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
            fs::read_to_string(workspace.join("CLAUDE.md")).unwrap(),
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

    #[test]
    fn merge_claude_content_updates_managed_preserves_user_section() {
        let source = "# App\n\n## Customization\n";
        let existing = "# Old\n\n## Customization\n\nuser stuff\n";
        let result = merge_claude_content(source, existing);
        assert_eq!(result, "# App\n\n## Customization\n\nuser stuff\n");
    }

    #[test]
    fn merge_claude_content_replaces_entirely_when_no_sentinel_in_existing() {
        let source = "# App\n\n## Customization\n";
        let existing = "old content without sentinel";
        let result = merge_claude_content(source, existing);
        assert_eq!(result, source);
    }

    #[test]
    fn merge_claude_md_creates_file_on_first_run() {
        let tmp = tempfile::tempdir().unwrap();
        let source_path = tmp.path().join("CLAUDE.md");
        let target_path = tmp.path().join("target").join("CLAUDE.md");
        fs::create_dir_all(target_path.parent().unwrap()).unwrap();
        fs::write(&source_path, "# App\n\n## Customization\n").unwrap();

        assert!(!target_path.exists());
        merge_claude_md(&source_path, &target_path).unwrap();
        assert_eq!(
            fs::read_to_string(&target_path).unwrap(),
            "# App\n\n## Customization\n"
        );
    }

    #[test]
    fn merge_claude_md_preserves_user_section_on_update() {
        let tmp = tempfile::tempdir().unwrap();
        let source_path = tmp.path().join("source.md");
        let target_path = tmp.path().join("target.md");
        fs::write(&source_path, "# New\n\n## Customization\n").unwrap();
        fs::write(&target_path, "# Old\n\n## Customization\n\nmy note\n").unwrap();

        merge_claude_md(&source_path, &target_path).unwrap();
        assert_eq!(
            fs::read_to_string(&target_path).unwrap(),
            "# New\n\n## Customization\n\nmy note\n"
        );
    }
}
