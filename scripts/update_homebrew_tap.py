"""Render the Homebrew formula for the public ad-migration CLI."""

from __future__ import annotations

import argparse
import tomllib
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
TEMPLATE_PATH = REPO_ROOT / "scripts" / "templates" / "homebrew" / "Formula" / "ad-migration.rb.tmpl"
LOCK_PATH = REPO_ROOT / "lib" / "uv.lock"


def _load_lock() -> dict[str, object]:
    with LOCK_PATH.open("rb") as handle:
        return tomllib.load(handle)


def _runtime_resource_packages() -> list[dict[str, str]]:
    lock = _load_lock()
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

    return sorted(resources, key=lambda resource: resource["name"])


def _render_python_resources() -> str:
    blocks = []
    for resource in _runtime_resource_packages():
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
) -> str:
    template = TEMPLATE_PATH.read_text(encoding="utf-8")
    return (
        template.replace("<%= version %>", version)
        .replace("<%= cli_url %>", cli_url)
        .replace("<%= cli_sha256 %>", cli_sha256)
        .replace("<%= shared_url %>", shared_url)
        .replace("<%= shared_sha256 %>", shared_sha256)
        .replace("<%= python_resources %>", _render_python_resources())
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
    args = parse_args()
    formula = render_formula(
        version=args.version,
        cli_url=args.cli_url,
        cli_sha256=args.cli_sha256,
        shared_url=args.shared_url,
        shared_sha256=args.shared_sha256,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(formula, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
