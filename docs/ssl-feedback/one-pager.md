# Showcase Migration: From Target Facts to Working dbt Project

## Executive Summary

The migration utility's purpose is to minimize the effort required to bring a small subset of tables — typically 2-3 fact tables and all their dimensional dependencies — to Fabric Lakehouse, so vibeData can be showcased on top of real, correctly-migrated data. We propose a reverse-dependency flow: the user specifies target facts, the agent traces the full dependency tree, and autonomously migrates everything needed to produce exactly the same output as the source system. The output is foundation quality — these models become the starting point for the customer's actual migration.

## Current State

The existing pipeline has 8 discrete commands (init, setup-ddl, scope, profile, test, refactor, init-dbt, generate-model), each requiring manual invocation and per-object specification. A user attempting to migrate a single table spent an entire session fighting four prerequisite gates — missing statement analysis, missing profile, missing sandbox, missing test spec — none requiring human judgment. The pipeline was designed for full warehouse migration with domain classification, triage phases, and foundation layer detection. This is the wrong abstraction for the actual use case: showcasing vibeData on 2-3 fact tables.

## Proposed Solution

The redesigned flow has one user input beyond project init: "which fact tables do you want to showcase?" From there, the agent works backwards. It traces the dependency tree from each target fact through their writer procedures to every table those procedures read from, recursively, until it reaches leaf nodes (raw source tables with no writers). The complete tree — typically 10-15 objects — is the entire migration scope.

The agent presents the resolved tree for confirmation, then executes the full pipeline autonomously in dependency order: seeds and sources first, then dimensions, then facts. Each step has [machine-verifiable self-checks](proposed-agentic-flow.md) — deterministic postconditions that catch errors without human involvement. Writer targets are verified against SQL parse trees. Profile classifications are validated against column types, not names. Refactored SQL is compiled and its source table set compared against the original. Generated models must pass both `dbt compile` and `dbt test`.

Static reference data (dim_date, dim_currency — tables with no writer and no dynamic content) becomes dbt seeds rather than fully migrated models. Staging tables become dbt sources. Everything else gets full migration with foundation-quality test coverage: branch-covering test generation with ground truth capture, validating that migrated models produce exactly the same output as source procedures. This is not throwaway work — these models and tests become the starting point for the customer's production migration.

For ambiguous patterns like SCD2 current-plus-history table pairs or multi-table writers, the agent detects and summarizes using [structural signals only](open-questions-analysis.md) — shared business keys, column type supersets, UPDATE-plus-INSERT behavioral patterns. All detection is naming-agnostic because names are opinions and structure is physics. The agent presents its finding and recommendation; the human makes the gate decision. This balances agentic capability with appropriate caution on structural decisions affecting data integrity.

Beyond migration, the utility generates two separate manifest packages. The **ingestion manifest** lists every source table needed in the lakehouse, grouped by origin (database, app, file, API) with a recommended method per group (Mirroring, Data Factory, or Shortcuts). The **parallel run manifest** specifies every table pair to compare over the 3-5 day validation window — primary keys, measure columns, SCD handling, expected daily deltas — paired with a setup prompt for building the Fabric Semantic Model and comparison report. Each manifest ships in two formats: agent-ready YAML (consumable by another agent or automation) and human-readable markdown (setup guide with rationale, checklists, and escalation criteria). The semantic model DirectQueries into both the source DW and the lakehouse simultaneously, avoiding the cross-database problem that blocks dbt-audit-helper, and building the comparison on Fabric is itself proof that vibeData works.

The ideal UX is a single command — `/showcase fact.fct_sales fact.fct_returns` — with the agent stopping only to confirm the dependency tree, ask the sourcing and comparison questionnaire, escalate self-check failures, present SCD2 or multi-table-writer decisions, and deliver the final review.

## Risks

The primary risk is dependency resolution correctness: if the agent misses a transitive dependency or misclassifies ETL plumbing as a real dependency, the showcase either breaks (missing data) or includes unnecessary objects (wasted effort). At 10-15 objects the blast radius is small, but the distinction between "needed for output correctness" and "ETL orchestration metadata" requires judgment. The self-check after scoping — verifying that the selected writer's reference graph matches the catalog — catches most structural errors, but novel ETL patterns may require human input.

## Decisions

All design decisions have been [resolved](open-questions-analysis.md) — the team can proceed to implementation.
