#!/usr/bin/env python3
"""Check that docs/design/README.md indexes every design subdirectory."""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path


INDEX_LINK_RE = re.compile(r"^- \[[^\]]+\]\(([^)]+)\)")


@dataclass(frozen=True)
class DesignIndexCheck:
    missing_links: list[str]
    unindexed_dirs: list[str]
    stale_index_dirs: list[str]

    @property
    def ok(self) -> bool:
        return not (
            self.missing_links or self.unindexed_dirs or self.stale_index_dirs
        )


def _index_links(index_path: Path) -> list[str]:
    links: list[str] = []
    for line in index_path.read_text(encoding="utf-8").splitlines():
        match = INDEX_LINK_RE.match(line)
        if match:
            links.append(match.group(1))
    return links


def check_design_index(design_root: Path) -> DesignIndexCheck:
    design_root = design_root.resolve()
    index_path = design_root / "README.md"
    links = _index_links(index_path)

    missing_links = [link for link in links if not (design_root / link).exists()]
    indexed_dirs = sorted({Path(link).parts[0] for link in links})
    actual_dirs = sorted(path.name for path in design_root.iterdir() if path.is_dir())

    return DesignIndexCheck(
        missing_links=missing_links,
        unindexed_dirs=sorted(set(actual_dirs) - set(indexed_dirs)),
        stale_index_dirs=sorted(set(indexed_dirs) - set(actual_dirs)),
    )


def _print_result(result: DesignIndexCheck) -> None:
    print(f"missing_links: {result.missing_links}")
    print(f"unindexed_dirs: {result.unindexed_dirs}")
    print(f"stale_index_dirs: {result.stale_index_dirs}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "design_root",
        nargs="?",
        default="docs/design",
        type=Path,
        help="Path to the design docs directory.",
    )
    args = parser.parse_args(argv)

    result = check_design_index(args.design_root)
    _print_result(result)

    if result.ok:
        return 0

    print(
        "::error::docs/design/README.md index does not match docs/design subdirectories."
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
