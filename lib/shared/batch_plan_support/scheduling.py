"""Phase and dependency scheduling helpers for batch plans."""

from __future__ import annotations

from collections import deque


def _topological_batches(
    fqns: list[str],
    blocking: dict[str, set[str]],
) -> list[list[str]]:
    """Compute maximally-parallel execution batches using Kahn's algorithm.

    Objects with no blocking deps go in batch 0.
    Objects whose blocking deps are all in batch 0 go in batch 1, etc.

    Any objects remaining after the algorithm (cycle members) are omitted;
    callers detect them by comparing the output to the input list.
    """
    fqn_set = set(fqns)
    restricted: dict[str, set[str]] = {
        fqn: blocking.get(fqn, set()) & fqn_set for fqn in fqns
    }
    in_degree = {fqn: len(restricted[fqn]) for fqn in fqns}

    dependents: dict[str, set[str]] = {fqn: set() for fqn in fqns}
    for fqn in fqns:
        for dep in restricted[fqn]:
            dependents[dep].add(fqn)

    ready: deque[str] = deque(fqn for fqn in fqns if in_degree[fqn] == 0)
    batches: list[list[str]] = []

    while ready:
        batch: list[str] = []
        next_ready: deque[str] = deque()
        while ready:
            fqn = ready.popleft()
            batch.append(fqn)
        batches.append(sorted(batch))
        for fqn in batch:
            for dependent in dependents.get(fqn, set()):
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    next_ready.append(dependent)
        ready = next_ready

    return batches


def _classify_phases(
    all_objects: list[tuple[str, str]],
    statuses: dict[str, str],
) -> tuple[list[str], list[str], list[str], list[str], list[str]]:
    """Sort objects into pipeline phases based on their status.

    Returns (scope_phase, profile_phase, migrate_candidates, completed, n_a).
    """
    scope: list[str] = []
    profile: list[str] = []
    migrate: list[str] = []
    completed: list[str] = []
    n_a: list[str] = []

    for fqn, _ in all_objects:
        s = statuses[fqn]
        if s == "scope_needed":
            scope.append(fqn)
        elif s == "profile_needed":
            profile.append(fqn)
        elif s in ("test_gen_needed", "refactor_needed", "migrate_needed"):
            migrate.append(fqn)
        elif s == "complete":
            completed.append(fqn)
        elif s == "n_a":
            n_a.append(fqn)

    return scope, profile, migrate, completed, n_a


def _compute_blocking_deps(
    migrate_candidates: list[str],
    raw_deps: dict[str, set[str]],
    dbt_status: dict[str, bool],
    writerless_fqns: set[str],
) -> dict[str, set[str]]:
    """Compute blocking deps for each migration candidate.

    A dep is blocking if it has no dbt model and is not covered by a complete
    intermediate node. Writerless tables are source tables referenced through
    dbt source() calls, so they never block.
    """
    blocking: dict[str, set[str]] = {}
    for fqn in migrate_candidates:
        covered: set[str] = set()
        for dep in raw_deps.get(fqn, set()):
            if dbt_status.get(dep, False):
                covered |= raw_deps.get(dep, set())
        blocking[fqn] = {
            d
            for d in raw_deps.get(fqn, set())
            if not dbt_status.get(d, False)
            and d not in covered
            and d not in writerless_fqns
        }
    return blocking
