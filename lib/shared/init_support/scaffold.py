"""Project file and hook scaffolding for migration project init."""

from __future__ import annotations

import json
import logging
import os
import stat
import subprocess
from pathlib import Path

from shared.init_support.source_config import get_source_config
from shared.output_models.init import ScaffoldHooksOutput, ScaffoldProjectOutput

logger = logging.getLogger(__name__)

GITIGNORE_ENTRIES = [
    "# Staging files from setup-ddl (intermediate MCP query results)",
    ".staging/",
    "",
    "# Intermediate CLI-ready test spec JSON (committed artifact is .yml)",
    "test-specs/*.json",
    "",
    "# Batch command run metadata",
    ".migration-runs/",
    "",
    "# Python",
    "__pycache__/",
    "*.pyc",
    ".venv/",
    "",
    "# OS",
    ".DS_Store",
    "Thumbs.db",
    "",
    "# Environment",
    ".env",
    ".env.*",
    "!.env.example",
    "",
]

GIT_WORKFLOW_MD = """\
# Git Workflow

## Worktrees

Worktree base path: `{worktree_base}`

Batch commands create or reuse worktrees automatically through deterministic plugin runtime helpers.

Commands create worktrees at `<base>/<run-slug>` where `<run-slug>` is generated from the command name and table names (e.g. `feature/scope-dimcustomer-dimproduct`).

For manual worktrees, use standard `git worktree add` commands in the shell.

## Cleanup

Run `/cleanup-worktrees` after PRs are merged to remove worktrees and branches.
"""

CLAUDE_MD_REQUIRED_SECTIONS = [
    "Domain",
    "Stack",
    "Directory Layout",
    "Skills",
    "MCP Servers",
    "Guardrails",
    "Skill Reasoning",
    "Output Framing",
    "Maintenance",
    "Commit Discipline",
]

ENVRC_DOTENV_LINE = "source_env_if_exists .env"


def is_executable_file(path_str: str) -> bool:
    path = Path(path_str)
    return path.is_file() and os.access(path, os.X_OK)


def run_scaffold_project(project_root: Path, technology: str = "sql_server") -> ScaffoldProjectOutput:
    """Scaffold project files. Idempotent: skips existing, merges .gitignore."""
    config = get_source_config(technology)
    project_root.mkdir(parents=True, exist_ok=True)
    files_created: list[str] = []
    files_updated: list[str] = []
    files_skipped: list[str] = []
    written_paths: list[str] = []

    claude_md_path = project_root / "CLAUDE.md"
    if not claude_md_path.exists():
        claude_md_path.write_text(config.claude_md_fn(), encoding="utf-8")
        files_created.append("CLAUDE.md")
        written_paths.append("CLAUDE.md")
        logger.info("event=scaffold_file file=CLAUDE.md status=created technology=%s", technology)
    else:
        content = claude_md_path.read_text(encoding="utf-8")
        missing = [s for s in CLAUDE_MD_REQUIRED_SECTIONS if f"## {s}" not in content]
        if missing:
            files_skipped.append(f"CLAUDE.md (missing sections: {', '.join(missing)})")
            logger.warning(
                "event=scaffold_file file=CLAUDE.md status=skipped missing_sections=%s",
                missing,
            )
        else:
            files_skipped.append("CLAUDE.md")

    readme_path = project_root / "README.md"
    if not readme_path.exists():
        readme_path.write_text(config.readme_md_fn(), encoding="utf-8")
        files_created.append("README.md")
        written_paths.append("README.md")
        logger.info("event=scaffold_file file=README.md status=created technology=%s", technology)
    else:
        files_skipped.append("README.md")

    repo_map_path = project_root / "repo-map.json"
    if not repo_map_path.exists():
        repo_map_path.write_text(
            json.dumps(config.repo_map_fn(), indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        files_created.append("repo-map.json")
        written_paths.append("repo-map.json")
        logger.info("event=scaffold_file file=repo-map.json status=created technology=%s", technology)
    else:
        files_skipped.append("repo-map.json")

    gitignore_path = project_root / ".gitignore"
    if not gitignore_path.exists():
        gitignore_path.write_text("\n".join(GITIGNORE_ENTRIES) + "\n", encoding="utf-8")
        files_created.append(".gitignore")
        written_paths.append(".gitignore")
        logger.info("event=scaffold_file file=.gitignore status=created")
    else:
        existing = gitignore_path.read_text(encoding="utf-8")
        existing_lines = {line.strip() for line in existing.splitlines()}
        new_entries: list[str] = []
        for entry in GITIGNORE_ENTRIES:
            stripped = entry.strip()
            if stripped and not stripped.startswith("#") and stripped not in existing_lines:
                new_entries.append(entry)
        if new_entries:
            addition = "\n" + "\n".join(new_entries) + "\n"
            gitignore_path.write_text(existing.rstrip("\n") + addition, encoding="utf-8")
            files_updated.append(f".gitignore (+{len(new_entries)} entries)")
            written_paths.append(".gitignore")
            logger.info(
                "event=scaffold_file file=.gitignore status=updated entries_added=%d",
                len(new_entries),
            )
        else:
            files_skipped.append(".gitignore")

    envrc_path = project_root / ".envrc"
    if not envrc_path.exists():
        envrc_path.write_text(config.envrc_fn(), encoding="utf-8")
        files_created.append(".envrc")
        written_paths.append(".envrc")
        logger.info("event=scaffold_file file=.envrc status=created technology=%s", technology)
    else:
        envrc_text = envrc_path.read_text(encoding="utf-8")
        if ENVRC_DOTENV_LINE not in envrc_text:
            updated_envrc = envrc_text.rstrip("\n")
            if updated_envrc:
                updated_envrc += "\n\n"
            updated_envrc += f"{ENVRC_DOTENV_LINE}\n"
            envrc_path.write_text(updated_envrc, encoding="utf-8")
            files_updated.append(".envrc (+local .env loader)")
            written_paths.append(".envrc")
            logger.info(
                "event=scaffold_file file=.envrc status=updated technology=%s",
                technology,
            )
        else:
            files_skipped.append(".envrc")

    workflow_path = project_root / ".claude" / "rules" / "git-workflow.md"
    if not workflow_path.exists():
        workflow_path.parent.mkdir(parents=True, exist_ok=True)
        content = GIT_WORKFLOW_MD.format(worktree_base="../worktrees")
        workflow_path.write_text(content, encoding="utf-8")
        files_created.append(".claude/rules/git-workflow.md")
        written_paths.append(".claude/rules/git-workflow.md")
        logger.info(
            "event=scaffold_file file=.claude/rules/git-workflow.md status=created"
        )
    else:
        files_skipped.append(".claude/rules/git-workflow.md")

    return ScaffoldProjectOutput(
        files_created=files_created,
        files_updated=files_updated,
        files_skipped=files_skipped,
        written_paths=written_paths,
    )


def run_scaffold_hooks(project_root: Path, technology: str = "sql_server") -> ScaffoldHooksOutput:
    """Create .githooks/pre-commit and configure git hooks path. Idempotent."""
    config = get_source_config(technology)
    hook_dir = project_root / ".githooks"
    hook_path = hook_dir / "pre-commit"
    hook_created = False
    hooks_path_configured = False
    written_paths: list[str] = []

    if not hook_path.exists():
        hook_dir.mkdir(parents=True, exist_ok=True)
        hook_path.write_text(config.pre_commit_hook_fn(), encoding="utf-8")
        hook_path.chmod(hook_path.stat().st_mode | stat.S_IEXEC)
        hook_created = True
        written_paths.append(".githooks/pre-commit")
        logger.info("event=scaffold_hook file=.githooks/pre-commit status=created technology=%s", technology)
    else:
        logger.info("event=scaffold_hook file=.githooks/pre-commit status=skipped")

    try:
        subprocess.run(
            ["git", "config", "core.hooksPath", ".githooks"],
            cwd=project_root,
            check=True,
            capture_output=True,
            text=True,
        )
        hooks_path_configured = True
        logger.info("event=configure_hooks_path status=success")
    except subprocess.CalledProcessError as exc:
        logger.warning(
            "event=configure_hooks_path status=failed error=%s",
            exc.stderr.strip(),
        )

    return ScaffoldHooksOutput(
        hook_created=hook_created,
        hooks_path_configured=hooks_path_configured,
        written_paths=written_paths,
    )
