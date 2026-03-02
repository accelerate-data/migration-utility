import { query } from "@anthropic-ai/claude-agent-sdk";
import type { SidecarConfig } from "./config.ts";
import { buildQueryOptions } from "./options.ts";
import { createAbortState, linkExternalSignal } from "./shutdown.ts";
import { emitSystemEvent } from "./run-agent.ts";

/** Sentinel used to close the async generator cleanly. */
const CLOSE_SENTINEL = Symbol("close");

/**
 * A streaming session that wraps the SDK's streaming input mode.
 *
 * The SDK's `query()` receives an `AsyncGenerator` as its prompt.
 * The generator yields user messages on demand — the first from the config,
 * subsequent ones pushed via `pushMessage()`. The SDK maintains full
 * conversation state (tool_use, tool_result, assistant messages) across yields.
 */
export class StreamSession {
  private currentRequestId: string;
  private pendingResolve: ((value: string | typeof CLOSE_SENTINEL) => void) | null = null;
  private messageQueue: string[] = [];
  private closed = false;
  private sessionId: string;

  constructor(
    sessionId: string,
    firstRequestId: string,
    config: SidecarConfig,
    onMessage: (requestId: string, message: Record<string, unknown>) => void,
    externalSignal?: AbortSignal,
  ) {
    this.sessionId = sessionId;
    this.currentRequestId = firstRequestId;

    // Start the streaming query in background — don't await
    this.runQuery(config, onMessage, externalSignal);
  }

  /**
   * Push a follow-up user message into the streaming session.
   * Resolves the pending promise so the generator yields to the SDK.
   */
  pushMessage(requestId: string, userMessage: string): void {
    if (this.closed) {
      throw new Error(`StreamSession ${this.sessionId} is closed`);
    }
    this.currentRequestId = requestId;
    if (this.pendingResolve) {
      this.pendingResolve(userMessage);
      this.pendingResolve = null;
    } else {
      // Generator hasn't reached its await yet — queue the message
      // so it's consumed on the next iteration instead of being dropped.
      this.messageQueue.push(userMessage);
    }
  }

  /**
   * Close the streaming session. The generator exits, query() finishes.
   */
  close(): void {
    this.closed = true;
    if (this.pendingResolve) {
      this.pendingResolve(CLOSE_SENTINEL);
      this.pendingResolve = null;
    }
  }

  private async runQuery(
    config: SidecarConfig,
    onMessage: (requestId: string, message: Record<string, unknown>) => void,
    externalSignal?: AbortSignal,
  ): Promise<void> {
    if (process.env.MOCK_AGENTS === "true") {
      process.stderr.write("[stream-session] Mock mode not supported for streaming sessions\n");
      onMessage(this.currentRequestId, {
        type: "error",
        message: "Streaming sessions are not supported in mock mode",
      });
      onMessage(this.currentRequestId, { type: "request_complete" });
      return;
    }

    const state = createAbortState();
    if (externalSignal) {
      linkExternalSignal(state, externalSignal);
    }

    // Route SDK stderr through onMessage for JSONL transcripts
    const stderrHandler = (data: string) => {
      onMessage(this.currentRequestId, {
        type: "system",
        subtype: "sdk_stderr",
        data: data.trimEnd(),
        timestamp: Date.now(),
      });
    };

    const options = buildQueryOptions(config, state.abortController, stderrHandler);

    // Build the async generator that feeds messages to the SDK
    const self = this;
    async function* messageGenerator() {
      // First message: the full prompt from config
      yield {
        type: "user" as const,
        message: {
          role: "user" as const,
          content: config.prompt,
        },
      };

      // Subsequent messages: wait for pushMessage() calls
      while (!self.closed) {
        // Check for queued messages that arrived before we could await
        if (self.messageQueue.length > 0) {
          const message = self.messageQueue.shift()!;
          yield {
            type: "user" as const,
            message: { role: "user" as const, content: message },
          };
          continue;
        }

        const nextMessage = await new Promise<string | typeof CLOSE_SENTINEL>(
          (resolve) => {
            // Before parking, drain any message that arrived during the yield
            if (self.messageQueue.length > 0) {
              const message = self.messageQueue.shift()!;
              resolve(message);
              return;
            }
            self.pendingResolve = resolve;
          },
        );

        if (nextMessage === CLOSE_SENTINEL || self.closed) {
          return;
        }

        yield {
          type: "user" as const,
          message: {
            role: "user" as const,
            content: nextMessage,
          },
        };
      }
    }

    emitSystemEvent(
      (msg) => onMessage(this.currentRequestId, msg),
      "init_start",
    );

    process.stderr.write(`[stream-session] Starting streaming query for session ${this.sessionId}\n`);

    const conversation = query({
      prompt: messageGenerator(),
      options,
    });

    emitSystemEvent(
      (msg) => onMessage(this.currentRequestId, msg),
      "sdk_ready",
    );

    try {
      for await (const message of conversation) {
        if (state.abortController.signal.aborted) break;

        const msg = message as Record<string, unknown>;
        onMessage(this.currentRequestId, msg);

        // Detect turn completion: emit for any non-tool_use stop reason.
        // This is more robust than checking only "end_turn" since the SDK
        // may use other stop reasons (e.g., "max_tokens", "stop_sequence").
        if (msg.type === "assistant" && msg.message) {
          const innerMsg = msg.message as Record<string, unknown>;
          const stopReason = innerMsg.stop_reason as string | undefined;
          if (stopReason && stopReason !== "tool_use") {
            onMessage(this.currentRequestId, { type: "request_complete" });
          }
        }
      }
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : String(err);
      process.stderr.write(`[stream-session] Query error: ${errorMessage}\n`);
      onMessage(this.currentRequestId, {
        type: "error",
        message: errorMessage,
      });
    }

    // Query finished — either all turns exhausted or generator closed
    if (!this.closed) {
      // Turns exhausted naturally (not user-initiated close)
      process.stderr.write(
        `[stream-session] Session ${this.sessionId} exhausted (query completed without close)\n`,
      );
      onMessage(this.currentRequestId, { type: "request_complete" });
    }

    process.stderr.write(`[stream-session] Session ${this.sessionId} ended\n`);
  }
}
