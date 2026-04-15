"""Render the Homebrew formula for the public ad-migration CLI."""

from __future__ import annotations

import argparse
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
TEMPLATE_PATH = REPO_ROOT / "scripts" / "templates" / "homebrew" / "Formula" / "ad-migration.rb.tmpl"


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
