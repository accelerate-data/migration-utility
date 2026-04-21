# Rationalize classifying-data-domains Reference Files Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make all 22 reference files in `skills/classifying-data-domains/references/` reachable from SKILL.md by adding cluster load conditions and removing a stale skill name from two files.

**Architecture:** Three targeted edits — remove one stale line from each of `21_domain_taxonomy.md` and `22_dw_table_patterns.md`, then append a deep-dive cluster block to the Reference Files section of `SKILL.md`. No content changes to files 01–20.

**Tech Stack:** Markdown only. Verify with `markdownlint` if available; otherwise manual read-back.

---

## File Map

| Action | File |
|---|---|
| Modify (remove stale line) | `skills/classifying-data-domains/references/22_dw_table_patterns.md` |
| Modify (remove stale line) | `skills/classifying-data-domains/references/21_domain_taxonomy.md` |
| Modify (append cluster block) | `skills/classifying-data-domains/SKILL.md` |

---

### Task 1: Remove stale skill name from `22_dw_table_patterns.md`

**Files:**

- Modify: `skills/classifying-data-domains/references/22_dw_table_patterns.md:3`

- [ ] **Step 1: Read the current file header**

Open `skills/classifying-data-domains/references/22_dw_table_patterns.md`. Confirm lines 1–5 look like this:

```markdown
# DW Table Patterns Reference

Used by Step 2 of the table-domain-classifier skill.
Contains the full pattern library and decision tree for classifying a table's dimensional modeling role.
```

- [ ] **Step 2: Remove the stale attribution line**

Delete only line 3 (`Used by Step 2 of the table-domain-classifier skill.`) and the blank line between it and the description line, so the result is:

```markdown
# DW Table Patterns Reference

Contains the full pattern library and decision tree for classifying a table's dimensional modeling role.
```

Everything from line 5 onward is unchanged.

- [ ] **Step 3: Verify the edit**

Read lines 1–6 of the file. Confirm the stale line is gone and the description line immediately follows the heading with one blank line between them.

- [ ] **Step 4: Commit**

```bash
git add skills/classifying-data-domains/references/22_dw_table_patterns.md
git commit -m "VU-1125: remove stale table-domain-classifier reference from 22_dw_table_patterns"
```

---

### Task 2: Remove stale skill name from `21_domain_taxonomy.md`

**Files:**

- Modify: `skills/classifying-data-domains/references/21_domain_taxonomy.md:3`

- [ ] **Step 1: Read the current file header**

Open `skills/classifying-data-domains/references/21_domain_taxonomy.md`. Confirm lines 1–5 look like this:

```markdown
# Domain Taxonomy Reference

Used by Step 3 of the table-domain-classifier skill.
Contains canonical domain definitions, keyword lists, and industry-specific variants.
```

- [ ] **Step 2: Remove the stale attribution line**

Delete only line 3 (`Used by Step 3 of the table-domain-classifier skill.`) and the blank line between it and the description line, so the result is:

```markdown
# Domain Taxonomy Reference

Contains canonical domain definitions, keyword lists, and industry-specific variants.
```

Everything from line 5 onward is unchanged.

- [ ] **Step 3: Verify the edit**

Read lines 1–6 of the file. Confirm the stale line is gone and the description line immediately follows the heading with one blank line between them.

- [ ] **Step 4: Commit**

```bash
git add skills/classifying-data-domains/references/21_domain_taxonomy.md
git commit -m "VU-1125: remove stale table-domain-classifier reference from 21_domain_taxonomy"
```

---

### Task 3: Add deep-dive cluster block to SKILL.md

**Files:**

- Modify: `skills/classifying-data-domains/SKILL.md` (end of `## Reference Files` section)

- [ ] **Step 1: Read the current Reference Files section**

Open `skills/classifying-data-domains/SKILL.md` and find `## Reference Files` (near the bottom of the file). Confirm it currently ends with:

```markdown
## Reference Files

Load these only when needed:

| File | When to Load |
|---|---|
| `references/22_dw_table_patterns.md` | Dimensional role classification |
| `references/21_domain_taxonomy.md` | Business-domain assignment |
```

There should be nothing after this table.

- [ ] **Step 2: Append the deep-dive cluster block**

Immediately after the closing `|` of the reference table, add the following (preserve the blank line separating the table from the new heading):

```markdown
### Deep-dive clusters

Load a cluster when the primary role is confirmed but a subtype or specific pattern requires deeper evidence.

**Fact deep dives** — load when a table is Fact or Aggregate but subtype or grain is ambiguous:

- `references/01_fact_table.md`
- `references/03_grain.md`
- `references/04_transaction_fact_table.md`
- `references/05_periodic_snapshot_fact_table.md`
- `references/06_accumulating_snapshot_fact_table.md`
- `references/07_factless_fact_table.md`
- `references/19_aggregate_tables.md`

**Dimension deep dives** — load when a table is Dimension but subtype is ambiguous (SCD, conformed, junk, role-playing, etc.):

- `references/02_dimension_table.md`
- `references/08_slowly_changing_dimensions.md`
- `references/09_surrogate_keys.md`
- `references/10_conformed_dimensions.md`
- `references/11_degenerate_dimension.md`
- `references/12_junk_dimension.md`
- `references/13_role_playing_dimension.md`
- `references/14_minidimension.md`
- `references/18_date_dimension.md`
- `references/20_heterogeneous_products.md`

**Structural patterns** — load when a table is Bridge, or bus/matrix architecture context is needed:

- `references/15_bridge_table.md`
- `references/16_bus_architecture.md`
- `references/17_bus_matrix.md`
```

- [ ] **Step 3: Verify the edit**

Read the full `## Reference Files` section. Confirm:

- The original two-row table is untouched above the new block.
- All three cluster headings are present (`**Fact deep dives**`, `**Dimension deep dives**`, `**Structural patterns**`).
- File counts: 7 fact files, 10 dimension files, 3 structural files = 20 total. Verify by counting the bullet points in each cluster.
- No file from 01–20 is missing or duplicated across clusters.

- [ ] **Step 4: Run markdownlint if available**

```bash
markdownlint skills/classifying-data-domains/SKILL.md
```

If `markdownlint` is not installed, skip this step — the manual read-back in Step 3 is sufficient.

- [ ] **Step 5: Commit**

```bash
git add skills/classifying-data-domains/SKILL.md
git commit -m "VU-1125: add deep-dive cluster load conditions to classifying-data-domains SKILL.md"
```

---

## Acceptance Checklist

Run these checks after all tasks complete:

- [ ] `skills/classifying-data-domains/SKILL.md` reference section lists all 22 files (2 primary + 20 in clusters)
- [ ] The phrase `table-domain-classifier` does not appear anywhere in `21_domain_taxonomy.md` or `22_dw_table_patterns.md`
- [ ] Every file in `skills/classifying-data-domains/references/` is listed in SKILL.md

```bash
# Verify no stale name remains
grep -r "table-domain-classifier" skills/classifying-data-domains/references/

# Verify all reference files are listed in SKILL.md
ls skills/classifying-data-domains/references/ | while read f; do
  grep -q "$f" skills/classifying-data-domains/SKILL.md && echo "OK: $f" || echo "MISSING: $f"
done
```

Expected: `grep` returns no output. Every `ls` entry prints `OK`.
