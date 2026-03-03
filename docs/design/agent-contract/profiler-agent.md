# Profiler Agent Contract

The profiler agent introspects SQL Server and returns a deterministic `PlannerReadyProfile`.
This output is machine-readable evidence for the planner agent.

## Required Input

```json
{
  "procedure": {
    "name": "dbo.usp_load_fact_sales"
  },
  "target": {
    "name": "dbo.fact_sales",
    "intended_kind": "auto|dim_non_scd|dim_scd1|dim_scd2|dim_junk|fact_transaction|fact_periodic_snapshot|fact_accumulating_snapshot|fact_aggregate"
  },
  "constraints": {
    "business_context": "",
    "fde_overrides": []
  }
}
```

## Output Schema (PlannerReadyProfile)

```json
{
  "schema_version": "1.0",
  "contract_version": "1.0.0",
  "profile_id": "uuid",
  "idempotency_key": "sha256:<hash>",
  "generated_at_utc": "2026-03-03T00:00:00Z",

  "request": {
    "procedure": { "name": "dbo.usp_load_fact_sales" },
    "target": { "name": "dbo.fact_sales", "intended_kind": "auto" },
    "constraints": { "business_context": "", "fde_overrides": [] }
  },

  "source_fingerprint": {
    "server_name": "fabric-sql-prod-01",
    "database_name": "warehouse",
    "compatibility_level": 160,
    "snapshot_marker": "lsn-or-timestamp"
  },

  "permission_scope": {
    "principal": "svc_migrator",
    "missing_permissions": []
  },

  "completeness": {
    "status": "complete",
    "missing_sections": [],
    "planner_requery_required": false
  },

  "source_introspection": {
    "procedure_sql": "CREATE PROCEDURE dbo.usp_load_fact_sales AS ...",
    "procedure_signature": {
      "params": [
        { "name": "@as_of_date", "type": "date", "nullable": false }
      ]
    },
    "dependencies": {
      "tables": ["dbo.sales_stage", "dbo.dim_customer", "dbo.dim_product"],
      "views": [],
      "procedures": [],
      "dynamic_sql_detected": false
    }
  },

  "object_catalog": [
    {
      "object_type": "table",
      "schema": "dbo",
      "name": "fact_sales",
      "object_id": 123456,
      "definition_hash": "sha256:<hash>"
    }
  ],

  "target_introspection": {
    "target_schema": {
      "columns": [
        { "name": "sale_id", "type": "bigint", "nullable": false },
        { "name": "customer_email", "type": "varchar(320)", "nullable": true },
        { "name": "load_date", "type": "datetime2", "nullable": false }
      ]
    },
    "constraints": {
      "primary_keys": [],
      "foreign_keys": [],
      "checks": []
    },
    "indexes": [
      { "name": "ix_fact_sales_sale_id", "is_unique": true, "columns": ["sale_id"] }
    ]
  },

  "data_profile": {
    "rowcounts": {
      "target_estimated": 12450893,
      "sources_estimated": [
        { "table": "dbo.sales_stage", "rows": 13022011 }
      ]
    },
    "column_stats": [
      { "column": "sale_id", "null_pct": 0.0, "distinct_count": 12450893 },
      { "column": "customer_email", "null_pct": 12.4, "distinct_count": 4021133 }
    ],
    "key_candidates": [
      { "columns": ["sale_id"], "confidence": 0.97 }
    ],
    "watermark_candidates": [
      { "column": "load_date", "confidence": 0.94 }
    ],
    "scd_signals": {
      "has_effective_dates": false,
      "has_current_flag": false,
      "has_hashdiff_pattern": false
    }
  },

  "classification": {
    "resolved_kind": "fact_transaction",
    "confidence": 0.88,
    "rationale": [
      "Procedure inserts transaction-level rows keyed by sale_id."
    ],
    "declared_grain": {
      "human": "One row per sale_id.",
      "columns": ["sale_id"]
    },
    "business_key_candidates": ["sale_id"],
    "surrogate_key_candidates": [],
    "unique_key_candidates": [["sale_id"]]
  },

  "scenario_profile": {
    "dim_scd": null,
    "fact_snapshot": null,
    "fact_accumulating": null,
    "fact_aggregate": null
  },

  "pii_candidates": [
    {
      "column": "customer_email",
      "entity": "email",
      "confidence": 0.93,
      "reason": "column_name_pattern+value_pattern",
      "sample_evidence": ["a***@example.com"]
    }
  ],

  "governance": {
    "access_tier": "restricted",
    "retention_policy": "",
    "legal_basis": "",
    "contains_sensitive_metadata": true
  },

  "validation": {
    "passed": true,
    "violations": []
  },

  "risk_register": [],
  "warnings": [],
  "errors": [],

  "evidence": [
    {
      "type": "sqlserver_metadata",
      "source": "sys.columns",
      "detail": "customer_email varchar(320)",
      "sample_window_start": "2026-02-01",
      "sample_window_end": "2026-02-28",
      "sample_row_count": 100000
    }
  ]
}
```

## Required Invariants

- If `completeness.status` is `complete`, then `missing_sections` must be empty.
- If `completeness.planner_requery_required` is `false`, all planner-required fields must exist.
- `profile_id` is immutable and `idempotency_key` is stable for the same request + source snapshot.
- All objects referenced in dependencies should resolve to `object_catalog` when available.

## What Profiler Must Not Output

- Final dbt SQL or Jinja model content.
- Final materialization/test decisions.
- Unbounded raw payloads (full execution plans/log dumps) inline.
