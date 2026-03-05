import { describe, it, expect } from "vitest";
import { runScopingAgent } from "./helpers.js";

// ─────────────────────────────────────────────────────────────────────────────
// Must Have scenarios
// ─────────────────────────────────────────────────────────────────────────────

describe("Scoping Agent — Must Have", () => {
  it("resolved: single direct MERGE writer for silver.DimProduct", async () => {
    const out = await runScopingAgent("resolved");
    const r = out.results[0];

    expect(r.status).toBe("resolved");
    expect(r.selected_writer).toBe("silver.usp_load_DimProduct");
    expect(r.candidate_writers[0].confidence).toBeGreaterThan(0.7);
    expect(r.candidate_writers[0].write_type).toBe("direct");
    expect(r.validation.passed).toBe(true);
  });

  it("no_writer_found: no proc writes to silver.DimGeography", async () => {
    const out = await runScopingAgent("no-writer-found");
    const r = out.results[0];

    expect(r.status).toBe("no_writer_found");
    expect(r.candidate_writers).toHaveLength(0);
    expect(r.selected_writer).toBeUndefined();
    expect(r.validation.passed).toBe(true);
  });

  it("error: cross-database reference detected for silver.DimEmployee", async () => {
    const out = await runScopingAgent("error-cross-db");
    const r = out.results[0];

    expect(r.status).toBe("error");
    expect(r.errors).toContain("ANALYSIS_CROSS_DATABASE_OUT_OF_SCOPE");
    expect(r.selected_writer).toBeUndefined();
    expect(r.validation.passed).toBe(true);
  });

  it("resolved: proc writing via updateable view is identified as writer of silver.DimPromotion", async () => {
    const out = await runScopingAgent("writer-through-view");
    const r = out.results[0];

    expect(r.status).toBe("resolved");
    expect(r.selected_writer).toBe("silver.usp_load_DimPromotion");
    expect(r.validation.passed).toBe(true);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// Should Have scenarios
// ─────────────────────────────────────────────────────────────────────────────

describe("Scoping Agent — Should Have", () => {
  it("ambiguous_multi_writer: two procs compete for silver.DimCustomer", async () => {
    const out = await runScopingAgent("ambiguous-multi-writer");
    const r = out.results[0];

    expect(r.status).toBe("ambiguous_multi_writer");
    expect(r.candidate_writers.length).toBeGreaterThanOrEqual(2);
    expect(r.selected_writer).toBeUndefined();

    const names = r.candidate_writers.map((c) => c.procedure_name);
    expect(names).toContain("silver.usp_load_DimCustomer_Full");
    expect(names).toContain("silver.usp_load_DimCustomer_Delta");
    expect(r.validation.passed).toBe(true);
  });

  it("partial: dynamic-SQL-only writer caps confidence for silver.DimCurrency", async () => {
    const out = await runScopingAgent("partial");
    const r = out.results[0];

    expect(r.status).toBe("partial");
    expect(r.candidate_writers.length).toBeGreaterThan(0);
    r.candidate_writers.forEach((c) =>
      expect(c.confidence).toBeLessThanOrEqual(0.7)
    );
    expect(r.selected_writer).toBeUndefined();
    expect(r.validation.passed).toBe(true);
  });

  it("resolved: call-graph traversal finds silver.usp_stage_FactInternetSales as writer", async () => {
    const out = await runScopingAgent("call-graph");
    const r = out.results[0];

    expect(r.status).toBe("resolved");
    // The staging proc is the direct writer; orchestrator has no direct write
    expect(r.selected_writer).toBe("silver.usp_stage_FactInternetSales");
    expect(r.validation.passed).toBe(true);
  });

  it("no_writer_found: indexed-view target silver.DimSalesTerritory has no loader proc", async () => {
    const out = await runScopingAgent("mv-as-target");
    const r = out.results[0];

    expect(r.status).toBe("no_writer_found");
    expect(r.selected_writer).toBeUndefined();
    expect(r.validation.passed).toBe(true);
  });
});
