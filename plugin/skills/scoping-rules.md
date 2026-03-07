# Scoping Rules

Classification, scoring, resolution, and validation rules for the Scoping Agent.
Loaded automatically when running as the scoping agent.

---

## Write Classification

Perform **structural analysis** on each procedure body — understand the code, not just keyword
scanning. Detect writes to the target table (and any view that maps to it):

| Statement | Classification |
|---|---|
| `INSERT [INTO] <target>` | `direct` |
| `UPDATE <target>` | `direct` |
| `DELETE [FROM] <target>` | `direct` |
| `MERGE [INTO] <target>` | `direct` |
| `TRUNCATE TABLE <target>` | `direct` |
| Calls a procedure confirmed to write to target | `indirect` |
| No write to target | `read_only` |

Flag dynamic SQL patterns: `EXEC(@sql)`, `sp_executesql @stmt`, string-built table names.
These reduce confidence but do not disqualify.

---

## Confidence Scoring

Assign confidence in [0.0, 1.0] using these deterministic rules:

| Signal | Effect |
|---|---|
| Direct write evidence | base 0.90 |
| Indirect write (callee is a confirmed direct writer) | base 0.75 |
| Shorter call path (per hop shorter than deepest path) | +0.02 |
| Multiple independent paths all show write evidence | +0.05 |
| Dynamic SQL present alongside static write evidence | −0.20 |
| Only dynamic SQL evidence (no static write) | cap at 0.45 |

Clamp final score to [0.0, 1.0].

---

## Resolution Rules

| Condition | status | selected_writer |
|---|---|---|
| Cross-database reference on any candidate | `error` | absent |
| Exactly one candidate with confidence > 0.7 | `resolved` | that candidate |
| Two or more candidates with confidence > 0.7 | `ambiguous_multi_writer` | absent |
| Candidates exist but none exceed 0.7 | `partial` | absent |
| No candidates found | `no_writer_found` | absent |
| Analysis or runtime failure | `error` | absent |

---

## Validation Checklist

Set `validation.passed = false` and add to `validation.issues` on any failure:

- `item_id` is present
- `status` is one of: `resolved`, `ambiguous_multi_writer`, `partial`, `no_writer_found`, `error`
- Every `confidence` is in [0.0, 1.0]
- Every candidate has `write_type`, `call_path`, and `rationale`
- `resolved` → `selected_writer` present and matches a `procedure_name` in `candidate_writers`
- `ambiguous_multi_writer` → ≥2 candidates, no `selected_writer`
- `partial` → `candidate_writers` non-empty
- `no_writer_found` → `candidate_writers` empty, no `selected_writer`
- `error` → `errors` non-empty
- `summary` counts match item-level statuses
