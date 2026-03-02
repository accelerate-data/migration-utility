import { query } from "@anthropic-ai/claude-agent-sdk";
import { readFileSync } from "fs";
import { join } from "path";
import type { SidecarConfig } from "./config.ts";
import { runMockAgent } from "./mock-agent.ts";
import { buildQueryOptions } from "./options.ts";
import { createAbortState, linkExternalSignal } from "./shutdown.ts";

interface AgentFrontMatter {
  model?: string;
  allowedTools?: string[];
}

/**
 * Parse `model:` and `tools:` from a YAML front-matter block (--- ... ---).
 */
function parseAgentFrontMatter(content: string): AgentFrontMatter {
  const fmMatch = content.match(/^---\r?\n([\s\S]*?)\r?\n---/);
  if (!fmMatch) return {};

  const fm = fmMatch[1];

  const modelMatch = fm.match(/^model:\s*(.+)$/m);
  const model = modelMatch?.[1]?.trim();

  // tools: is a YAML list under a `tools:` key:
  //   tools:
  //     - Bash
  //     - Read
  const toolsBlockMatch = fm.match(/^tools:\s*\n((?:[ \t]*-[ \t]*.+\n?)*)/m);
  let allowedTools: string[] | undefined;
  if (toolsBlockMatch) {
    allowedTools = toolsBlockMatch[1]
      .split("\n")
      .map((line) => line.replace(/^[ \t]*-[ \t]*/, "").trim())
      .filter(Boolean);
  }

  return { model, allowedTools };
}

/**
 * Read an agent's front-matter and return the fields that should be applied
 * when the caller did not supply them explicitly.  Returns an empty object
 * if the agent file cannot be read.
 */
function resolveAgentFrontMatter(agentName: string, cwd: string): AgentFrontMatter {
  const agentPath = join(cwd, ".claude", "agents", `${agentName}.md`);
  try {
    return parseAgentFrontMatter(readFileSync(agentPath, "utf-8"));
  } catch {
    return {};
  }
}

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

  // When an agent name is provided, fill in any fields the caller omitted
  // (model, allowedTools) from the agent's front-matter.  Caller-supplied
  // values always win; front-matter is a fallback only.
  let resolvedConfig = config;
  if (config.agentName && (!config.model || !config.allowedTools)) {
    const fm = resolveAgentFrontMatter(config.agentName, config.cwd);
    const mergedModel = config.model ?? fm.model;
    const mergedTools = config.allowedTools ?? fm.allowedTools;
    if (mergedModel !== config.model || mergedTools !== config.allowedTools) {
      if (mergedModel && mergedModel !== config.model) {
        process.stderr.write(
          `[sidecar] resolved model from front-matter: agent=${config.agentName} model=${mergedModel}\n`,
        );
      }
      if (mergedTools && mergedTools !== config.allowedTools) {
        process.stderr.write(
          `[sidecar] resolved tools from front-matter: agent=${config.agentName} tools=${mergedTools.join(",")}\n`,
        );
      }
      resolvedConfig = { ...config, model: mergedModel, allowedTools: mergedTools };
    }
  }

  // Route SDK subprocess stderr through onMessage so it gets wrapped with
  // request_id and written to the JSONL transcript (not the app log).
  const stderrHandler = (data: string) => {
    onMessage({ type: "system", subtype: "sdk_stderr", data: data.trimEnd(), timestamp: Date.now() });
  };

  const options = buildQueryOptions(resolvedConfig, state.abortController, stderrHandler);

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
