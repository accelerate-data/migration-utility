---
name: test-invariants
description: >
  Use when editing, generating, reviewing, or debugging migrated dbt unit test
  specs and review artifacts that must stay consistent across paired test
  skills.
user-invocable: false
argument-hint: "<schema.object> — optional object context"
---

# Test Invariants

Shared contract for `generating-tests` and `reviewing-tests`. Keep these rules stable so the pair can evolve without drifting.

## When to Use

- shared test-generation and test-review rules are repeated in multiple docs
- a spec, review artifact, or repair pass looks self-contradictory
- you are editing either test skill and need the pair-level contract

Do not use this skill for command orchestration, readiness checks, or fixture-quality heuristics that belong only to review.

## Quick Reference

| Invariant | Rule |
|---|---|
| Source of truth | Re-derive branches from current SQL; stored manifests may be stale |
| Artifact ownership | Generator owns `TestSpec`; reviewer owns `TestReviewOutput` |
| Cross-artifact IDs | Branch, scenario, uncovered, and feedback IDs must resolve within the owning artifact |
| Repair scope | On reviewer-driven reruns, repair named gaps first and preserve non-targeted approved scenarios and `expect` blocks |
| Evidence standard | Coverage and verdict claims must be derivable from artifact content, not prose |

## Common Mistakes

- Treating an older `branch_manifest` as authoritative.
- Letting reviewer feedback expand into broad regeneration without an explicit request.
- Filling review coverage fields from generator-owned branches instead of reviewer-owned evidence.
- Reporting complete coverage while unresolved uncovered or untestable branches still exist in the artifact.
