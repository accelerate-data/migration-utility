---
issue: VU-1125
title: Rationalize analyze-data-domains reference files
date: 2026-04-20
branch: analyze-domain
---

# Rationalize classifying-data-domains Reference Files

## Problem

The `classifying-data-domains` skill has 22 reference files. Only two (`21_domain_taxonomy.md` and `22_dw_table_patterns.md`) are wired into the main skill with load conditions. The remaining 20 files exist as on-demand deep dives but have no load conditions in SKILL.md, making them unreachable during classification. Files 21 and 22 also carry a stale skill name (`table-domain-classifier`) in their opening description lines.

## Goal

Make the reference set intentional: every file is either reachable from SKILL.md with a clear load condition or explicitly removed.

## Scope

- **In:** SKILL.md reference section, header lines in files 21 and 22.
- **Out:** No content rewrites of dimensional modeling guidance, no new eval infrastructure, no changes to other skills.

## Design

### 1. SKILL.md — Deep-dive cluster block

Extend the existing `## Reference Files` section with a `### Deep-dive clusters` subsection. No other changes to the skill.

Three clusters, each with a single load condition:

**Fact deep dives** — load when a table is Fact or Aggregate but subtype or grain is ambiguous:
`01_fact_table.md`, `03_grain.md`, `04_transaction_fact_table.md`, `05_periodic_snapshot_fact_table.md`, `06_accumulating_snapshot_fact_table.md`, `07_factless_fact_table.md`, `19_aggregate_tables.md`

**Dimension deep dives** — load when a table is Dimension but subtype is ambiguous (SCD, conformed, junk, role-playing, etc.):
`02_dimension_table.md`, `08_slowly_changing_dimensions.md`, `09_surrogate_keys.md`, `10_conformed_dimensions.md`, `11_degenerate_dimension.md`, `12_junk_dimension.md`, `13_role_playing_dimension.md`, `14_minidimension.md`, `18_date_dimension.md`, `20_heterogeneous_products.md`

**Structural patterns** — load when a table is Bridge, or bus/matrix architecture context is needed:
`15_bridge_table.md`, `16_bus_architecture.md`, `17_bus_matrix.md`

### 2. Files 21 and 22 — Remove stale name

Remove the opening line `"Used by Step N of the table-domain-classifier skill."` from `21_domain_taxonomy.md` and `22_dw_table_patterns.md`. The second description line is accurate and stays.

### 3. Files 01–20 — No content changes

All 20 files are retained as-is. They become reachable via the cluster load conditions above.

## Rationale

Files 01–20 are on-demand modeling deep dives, not legacy material. They were intentionally authored for use during classification but never wired up with load conditions. Grouping them into 3 role-aligned clusters mirrors the role classification step already in the skill, so the load trigger is unambiguous: when the primary role is confirmed but the subtype is not, load the matching cluster.

## Acceptance Verification

- Every reference file is listed in SKILL.md under a load condition.
- SKILL.md explains when to load each cluster.
- The stale `table-domain-classifier` name is removed from files 21 and 22.
- No files are deleted.
- `markdownlint` passes on SKILL.md and files 21 and 22.
