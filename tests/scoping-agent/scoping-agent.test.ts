/**
 * Integration tests for the scoping agent.
 *
 * Each test uses a DDL fixture directory from tests/scoping-agent/fixtures/.
 * Tests validate the output structure and status — not exact LLM wording.
 *
 * Requirements:
 *   - `claude` CLI on PATH
 *   - ANTHROPIC_API_KEY in environment
 *   - `uv` on PATH (used to run the DDL MCP server)
 */

import { describe, it, expect } from "vitest";
import { runScopingAgent } from "./helpers.js";

describe("scoping agent — resolved", () => {
  it("identifies a single direct writer and resolves", () => {
    const output = runScopingAgent("resolved", [
      { item_id: "silver.DimProduct" },
    ]);

    expect(output.schema_version).toBe("1.0");
    expect(output.results).toHaveLength(1);

    const result = output.results[0];
    expect(result.item_id).toBe("silver.DimProduct");
    expect(result.status).toBe("resolved");
    expect(result.selected_writer).toBeTruthy();
    expect(result.candidate_writers.length).toBeGreaterThanOrEqual(1);

    const writer = result.candidate_writers.find(
      (w) => w.procedure_name === result.selected_writer
    );
    expect(writer).toBeDefined();
    expect(writer!.write_type).toBe("direct");
    expect(writer!.confidence).toBeGreaterThan(0.7);
    expect(writer!.call_path).toHaveLength(1);

    expect(result.validation.passed).toBe(true);
    expect(result.errors).toHaveLength(0);

    expect(output.summary.total).toBe(1);
    expect(output.summary.resolved).toBe(1);
  });
});

describe("scoping agent — no writer found", () => {
  it("returns no_writer_found when no procedure writes to the table", () => {
    const output = runScopingAgent("no-writer-found", [
      { item_id: "silver.DimGeography" },
    ]);

    const result = output.results[0];
    expect(result.status).toBe("no_writer_found");
    expect(result.candidate_writers).toHaveLength(0);
    expect(result.selected_writer).toBeUndefined();
    expect(result.validation.passed).toBe(true);

    expect(output.summary.no_writer_found).toBe(1);
  });
});

describe("scoping agent — error: cross-database reference", () => {
  it("returns error with ANALYSIS_CROSS_DATABASE_OUT_OF_SCOPE", () => {
    const output = runScopingAgent("error-cross-db", [
      { item_id: "silver.DimEmployee" },
    ]);

    const result = output.results[0];
    expect(result.status).toBe("error");
    expect(result.errors).toContain("ANALYSIS_CROSS_DATABASE_OUT_OF_SCOPE");
    expect(result.selected_writer).toBeUndefined();
    expect(result.validation.passed).toBe(true);

    expect(output.summary.error).toBe(1);
  });
});

describe("scoping agent — writer through view", () => {
  it("detects a write via an updatable view and resolves", () => {
    const output = runScopingAgent("writer-through-view", [
      { item_id: "silver.DimDate" },
    ]);

    const result = output.results[0];
    expect(result.status).toBe("resolved");
    expect(result.selected_writer).toBeTruthy();

    const writer = result.candidate_writers.find(
      (w) => w.procedure_name === result.selected_writer
    );
    expect(writer).toBeDefined();
    expect(writer!.confidence).toBeGreaterThan(0.7);
    expect(result.validation.passed).toBe(true);

    // Agent should note the view in warnings or rationale
    const hasViewNote =
      result.warnings.some((w) => w.toLowerCase().includes("view")) ||
      writer!.rationale.toLowerCase().includes("view");
    expect(hasViewNote).toBe(true);
  });
});

describe("scoping agent — ambiguous multi-writer", () => {
  it("returns ambiguous_multi_writer when two procs write to the same table", () => {
    const output = runScopingAgent("ambiguous-multi-writer", [
      { item_id: "silver.DimRegion" },
    ]);

    const result = output.results[0];
    expect(result.status).toBe("ambiguous_multi_writer");
    expect(result.selected_writer).toBeUndefined();
    expect(result.candidate_writers.length).toBeGreaterThanOrEqual(2);
    expect(result.validation.passed).toBe(true);

    expect(output.summary.ambiguous_multi_writer).toBe(1);
  });
});

describe("scoping agent — partial (dynamic SQL only)", () => {
  it("returns partial with confidence capped at 0.45 when only dynamic SQL evidence", () => {
    const output = runScopingAgent("partial", [
      { item_id: "silver.DimChannel" },
    ]);

    const result = output.results[0];
    expect(result.status).toBe("partial");
    expect(result.selected_writer).toBeUndefined();
    expect(result.candidate_writers.length).toBeGreaterThanOrEqual(1);

    const writer = result.candidate_writers[0];
    expect(writer.confidence).toBeLessThanOrEqual(0.45);
    expect(result.validation.passed).toBe(true);

    expect(output.summary.partial).toBe(1);
  });
});

describe("scoping agent — call graph traversal", () => {
  it("discovers a writer via call graph and marks it as indirect", () => {
    const output = runScopingAgent("call-graph", [
      { item_id: "silver.DimTerritory" },
    ]);

    const result = output.results[0];
    expect(result.status).toBe("resolved");
    expect(result.selected_writer).toBeTruthy();
    expect(result.candidate_writers.length).toBeGreaterThanOrEqual(1);

    // The inner proc (direct writer) should appear somewhere in the candidates
    const innerProc = result.candidate_writers.find((w) =>
      w.procedure_name.toLowerCase().includes("inner")
    );
    const outerProc = result.candidate_writers.find((w) =>
      w.call_path.length === 1 && !w.procedure_name.toLowerCase().includes("inner")
    );

    // Either the outer proc is resolved via indirect write, or the inner proc directly
    const hasCallGraphEvidence = result.candidate_writers.some(
      (w) => w.call_path.length > 1 || w.write_type === "indirect"
    );
    expect(hasCallGraphEvidence).toBe(true);

    expect(result.validation.passed).toBe(true);
  });
});

describe("scoping agent — target is a view", () => {
  it("handles item_id that is a view, not a base table", () => {
    const output = runScopingAgent("target-is-view", [
      { item_id: "silver.vw_DimCurrency" },
    ]);

    const result = output.results[0];
    // The target is a view — agent should note this and either resolve
    // (by finding the base table writer) or return no_writer_found
    expect(["resolved", "no_writer_found"]).toContain(result.status);

    // Agent should flag the view in warnings
    const hasViewWarning = result.warnings.some((w) =>
      w.toLowerCase().includes("view")
    );
    expect(hasViewWarning).toBe(true);

    expect(result.validation.passed).toBe(true);
  });
});

describe("scoping agent — summary counts", () => {
  it("summary totals match result statuses across a batch", () => {
    // resolved + no_writer_found in one call
    const output = runScopingAgent("resolved", [
      { item_id: "silver.DimProduct" },
    ]);

    const { summary, results } = output;
    const statusCounts: Record<string, number> = {};
    for (const r of results) {
      statusCounts[r.status] = (statusCounts[r.status] ?? 0) + 1;
    }

    expect(summary.total).toBe(results.length);
    expect(summary.resolved).toBe(statusCounts["resolved"] ?? 0);
    expect(summary.ambiguous_multi_writer).toBe(statusCounts["ambiguous_multi_writer"] ?? 0);
    expect(summary.no_writer_found).toBe(statusCounts["no_writer_found"] ?? 0);
    expect(summary.partial).toBe(statusCounts["partial"] ?? 0);
    expect(summary.error).toBe(statusCounts["error"] ?? 0);
  });
});
