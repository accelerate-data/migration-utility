---
name: scoping-writers
description: Scoping agent rules covering input/output contract, write classification,
  confidence scoring, resolution precedence, and output validation. Load when running
  as the scoping agent or when detecting writer procedures, scoring candidates, or
  resolving CandidateWriters output.
user-invocable: false
---

# Scoping Writers

Rules and reference for the Scoping Agent.

---

## Input Schema

```json
{
  "schema_version": "1.0",
  "run_id": "<uuid>",
  "items": [
    {
      "item_id": "<schema>.<table>",
      "search_depth": 2
    }
  ]
}
```

### Field semantics

- `item_id` — schema-qualified target table or view name
- `search_depth` — maximum call-graph traversal depth (integer `0..5`, default `2`)
  - `0` = candidate procedure bodies only, no callee traversal
  - `1` = direct callees of candidates
  - `2+` = recursive up to N hops

---

## Output Schema

Write only valid JSON to the output file. No markdown fences, no explanation.

```json
{
  "schema_version": "1.0",
  "run_id": "<run_id from input>",
  "results": [
    {
      "item_id": "schema.table",
      "status": "resolved",
      "selected_writer": "schema.proc_name",
      "candidate_writers": [
        {
          "procedure_name": "schema.proc_name",
          "write_type": "direct",
          "call_path": ["schema.proc_name"],
          "rationale": "Direct INSERT INTO target table found in procedure body.",
          "confidence": 0.90
        }
      ],
      "warnings": [],
      "validation": {"passed": true, "issues": []},
      "errors": []
    }
  ],
  "summary": {
    "total": 1,
    "resolved": 1,
    "ambiguous_multi_writer": 0,
    "no_writer_found": 0,
    "partial": 0,
    "error": 0
  }
}
```

### Field notes

- Omit `selected_writer` entirely when status is not `resolved`
- `call_path` — ordered list from entry-point candidate to the procedure that performs the write
- `summary` counts must match item-level statuses exactly

---

## Write Classification

Perform **structural analysis** on each procedure body — understand the code, not just
keyword scanning. Detect writes to the target table and any view that maps to it.

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

Set `validation.passed = false` and add a description to `validation.issues` on any failure:

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
