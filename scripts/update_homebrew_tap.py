"""Render the Homebrew formula for the public ad-migration CLI."""

from __future__ import annotations

import argparse
import logging
import tomllib
import uuid
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
TEMPLATE_PATH = REPO_ROOT / "scripts" / "templates" / "homebrew" / "Formula" / "ad-migration.rb.tmpl"
LOCK_PATH = REPO_ROOT / "lib" / "uv.lock"
logger = logging.getLogger(__name__)


def _load_lock(*, run_id: str = "-") -> dict[str, object]:
    logger.info(
        "event=homebrew_lock_load component=homebrew_tap operation=load_lock status=start run_id=%s lock_path=%s",
        run_id,
        LOCK_PATH,
    )
    with LOCK_PATH.open("rb") as handle:
        lock = tomllib.load(handle)
    logger.info(
        "event=homebrew_lock_load component=homebrew_tap operation=load_lock status=success run_id=%s package_count=%s",
        run_id,
        len(lock.get("package", [])),
    )
    return lock


def _runtime_resource_packages(*, run_id: str = "-") -> list[dict[str, str]]:
    lock = _load_lock(run_id=run_id)
    packages = {pkg["name"]: pkg for pkg in lock["package"]}
    resources: list[dict[str, str]] = []
    seen = {"ad-migration-shared"}
    queue = ["ad-migration-shared"]

    while queue:
        package_name = queue.pop(0)
        package = packages[package_name]
        for dependency in package.get("dependencies", []):
            dep_name = dependency["name"]
            if dep_name in seen:
                continue
            seen.add(dep_name)
            dep_package = packages[dep_name]
            queue.append(dep_name)
            sdist = dep_package.get("sdist")
            if not sdist:
                raise RuntimeError(f"No sdist metadata found for {dep_name}")
            resources.append(
                {
                    "name": dep_name,
                    "url": sdist["url"],
                    "sha256": sdist["hash"].removeprefix("sha256:"),
                }
            )

    resources = sorted(resources, key=lambda resource: resource["name"])
    logger.info(
        "event=homebrew_resource_resolution component=homebrew_tap operation=resolve_resources status=success run_id=%s resource_count=%s",
        run_id,
        len(resources),
    )
    return resources


def _render_python_resources(resources: list[dict[str, str]]) -> str:
    blocks = []
    for resource in resources:
        blocks.append(
            (
                f'resource "{resource["name"]}" do\n'
                f'  url "{resource["url"]}"\n'
                f'  sha256 "{resource["sha256"]}"\n'
                "end"
            )
        )
    return "\n\n".join(blocks)


def render_formula(
    *,
    version: str,
    cli_url: str,
    cli_sha256: str,
    shared_url: str,
    shared_sha256: str,
    resources: list[dict[str, str]] | None = None,
) -> str:
    template = TEMPLATE_PATH.read_text(encoding="utf-8")
    resolved_resources = resources if resources is not None else _runtime_resource_packages()
    return (
        template.replace("<%= version %>", version)
        .replace("<%= cli_url %>", cli_url)
        .replace("<%= cli_sha256 %>", cli_sha256)
        .replace("<%= shared_url %>", shared_url)
        .replace("<%= shared_sha256 %>", shared_sha256)
        .replace("<%= python_resources %>", _render_python_resources(resolved_resources))
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    parser.add_argument("--cli-url", required=True)
    parser.add_argument("--cli-sha256", required=True)
    parser.add_argument("--shared-url", required=True)
    parser.add_argument("--shared-sha256", required=True)
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = parse_args()
    run_id = uuid.uuid4().hex[:12]
    logger.info(
        "event=homebrew_formula_render component=homebrew_tap operation=main status=start run_id=%s output=%s",
        run_id,
        args.output,
    )
    resources = _runtime_resource_packages(run_id=run_id)
    formula = render_formula(
        version=args.version,
        cli_url=args.cli_url,
        cli_sha256=args.cli_sha256,
        shared_url=args.shared_url,
        shared_sha256=args.shared_sha256,
        resources=resources,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    logger.info(
        "event=homebrew_formula_write component=homebrew_tap operation=write_formula status=start run_id=%s output=%s",
        run_id,
        args.output,
    )
    args.output.write_text(formula, encoding="utf-8")
    logger.info(
        "event=homebrew_formula_write component=homebrew_tap operation=write_formula status=success run_id=%s output=%s resource_count=%s",
        run_id,
        args.output,
        len(resources),
    )
    logger.info(
        "event=homebrew_formula_render component=homebrew_tap operation=main status=success run_id=%s output=%s",
        run_id,
        args.output,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
