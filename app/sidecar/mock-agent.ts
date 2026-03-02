import type { SidecarConfig } from "./config.ts";

/**
 * Fixed fixture result for the scope-table-details-analyzer agent.
 * Matches the full output contract from classify-source-object/SKILL.md.
 * Emitted as a top-level "result" message so the Rust "result" arm captures it.
 */
const SCOPE_ANALYZER_FIXTURE = JSON.stringify({
  table_type: "dimension",
  load_strategy: "full_refresh",
  grain_columns: '["CurrencyKey"]',
  incremental_column: "",
  date_column: "",
  snapshot_strategy: "",
  pii_columns: "[]",
  relationships_json: "[]",
  analysis_metadata: {
    table_type: {
      value: "dimension",
      confidence: 95,
      reasoning: "Mock: Dim prefix indicates dimension table per Kimball convention",
    },
    load_strategy: {
      value: "full_refresh",
      confidence: 90,
      reasoning: "Mock: dimension tables default to full_refresh",
    },
    grain_columns: {
      value: '["CurrencyKey"]',
      confidence: 85,
      reasoning: "Mock: surrogate key is the grain for dimension tables",
    },
    relationships: {
      value: "[]",
      confidence: 80,
      reasoning: "Mock: no FK signals in test fixture",
    },
    incremental_column: {
      value: "",
      confidence: 90,
      reasoning: "Mock: full_refresh strategy, no incremental column needed",
    },
    date_column: {
      value: "",
      confidence: 90,
      reasoning: "Mock: currency dimension has no canonical date column",
    },
    pii_columns: {
      value: "[]",
      confidence: 95,
      reasoning: "Mock: currency data contains no PII",
    },
  },
});

/**
 * Mock agent for testing without real API calls.
 *
 * When MOCK_AGENTS=true, emits a fixed "result" message so the full
 * sidecar → Rust → DB → frontend pipeline can be exercised without LLM calls.
 *
 * For the scope-table-details-analyzer agent, emits the full fixture JSON.
 * All other agents emit a generic stub.
 */
export async function runMockAgent(
  config: SidecarConfig,
  onMessage: (message: Record<string, unknown>) => void,
  _externalSignal?: AbortSignal,
): Promise<void> {
  const isScopeAnalyzer = config.agentName === "scope-table-details-analyzer";

  onMessage({
    type: "result",
    subtype: "success",
    is_error: false,
    result: isScopeAnalyzer ? SCOPE_ANALYZER_FIXTURE : '{"mock":true}',
    duration_ms: 0,
    num_turns: 0,
    usage: { input_tokens: 0, output_tokens: 0 },
  });
}
