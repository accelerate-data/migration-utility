import type { SidecarConfig } from "./config.ts";

/**
 * Minimal mock agent for testing without real API calls.
 * Emits a single assistant message then returns.
 */
export async function runMockAgent(
  _config: SidecarConfig,
  onMessage: (message: Record<string, unknown>) => void,
  _externalSignal?: AbortSignal,
): Promise<void> {
  onMessage({
    type: "assistant",
    message: {
      role: "assistant",
      content: [{ type: "text", text: "Mock agent response." }],
      stop_reason: "end_turn",
    },
  });
}
