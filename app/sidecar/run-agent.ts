import { query } from "@anthropic-ai/claude-agent-sdk";
import type { SidecarConfig } from "./config.ts";
import { runMockAgent } from "./mock-agent.ts";
import { buildQueryOptions } from "./options.ts";
import { createAbortState, linkExternalSignal } from "./shutdown.ts";

/**
 * Emit a system-level progress event (not an SDK message).
 * These events let the UI show granular status during initialization.
 */
export function emitSystemEvent(
  onMessage: (message: Record<string, unknown>) => void,
  subtype: string,
): void {
  onMessage({ type: "system", subtype, timestamp: Date.now() });
}

/**
 * Run a single agent request using the SDK.
 *
 * Streams each SDK message to the provided `onMessage` callback.
 * The callback receives raw SDK message objects (the caller is responsible
 * for any wrapping, e.g., adding `request_id`).
 *
 * @param config          The sidecar config for this request
 * @param onMessage       Called for each message from the SDK conversation
 * @param externalSignal  Optional AbortSignal to cancel from outside (e.g., when persistent-mode
 *                        aborts a stuck request to start a new one)
 */
export async function runAgentRequest(
  config: SidecarConfig,
  onMessage: (message: Record<string, unknown>) => void,
  externalSignal?: AbortSignal,
): Promise<void> {
  if (process.env.MOCK_AGENTS === "true") {
    process.stderr.write("[sidecar] Mock agent mode\n");
    return runMockAgent(config, onMessage, externalSignal);
  }

  const state = createAbortState();
  if (externalSignal) {
    linkExternalSignal(state, externalSignal);
  }

  // Route SDK subprocess stderr through onMessage so it gets wrapped with
  // request_id and written to the JSONL transcript (not the app log).
  const stderrHandler = (data: string) => {
    onMessage({ type: "system", subtype: "sdk_stderr", data: data.trimEnd(), timestamp: Date.now() });
  };

  const options = buildQueryOptions(config, state.abortController, stderrHandler);

  // Notify the UI that we're about to initialize the SDK
  emitSystemEvent(onMessage, "init_start");

  process.stderr.write("[sidecar] Starting SDK query\n");
  const conversation = query({
    prompt: config.prompt,
    options,
  });

  // SDK is loaded and connected — ready to stream messages
  emitSystemEvent(onMessage, "sdk_ready");

  for await (const message of conversation) {
    if (state.abortController.signal.aborted) break;
    onMessage(message as Record<string, unknown>);
  }
}
