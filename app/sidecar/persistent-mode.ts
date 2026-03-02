import { createInterface, type Interface } from "node:readline";
import { type SidecarConfig, parseSidecarConfig } from "./config.ts";
import { runAgentRequest } from "./run-agent.ts";
import { StreamSession } from "./stream-session.ts";

/** Incoming request envelope: run an agent. */
interface AgentRequest {
  type: "agent_request";
  request_id: string;
  config: SidecarConfig;
}

/** Incoming shutdown envelope. */
interface ShutdownRequest {
  type: "shutdown";
}

/** Incoming ping envelope for heartbeat health checks. */
interface PingRequest {
  type: "ping";
}

/** Incoming cancel envelope: abort a specific in-flight request. */
interface CancelRequest {
  type: "cancel";
  request_id: string;
}

/** Start a streaming session (first message). */
interface StreamStartRequest {
  type: "stream_start";
  request_id: string;
  session_id: string;
  config: SidecarConfig;
}

/** Push a follow-up message into an active streaming session. */
interface StreamMessageRequest {
  type: "stream_message";
  request_id: string;
  session_id: string;
  user_message: string;
}

/** Close a streaming session. */
interface StreamEndRequest {
  type: "stream_end";
  session_id: string;
}

/** Union of all valid incoming messages. */
type IncomingMessage =
  | AgentRequest
  | ShutdownRequest
  | PingRequest
  | CancelRequest
  | StreamStartRequest
  | StreamMessageRequest
  | StreamEndRequest;

/**
 * Write a single JSON line to stdout.
 */
function writeLine(obj: Record<string, unknown>): void {
  process.stdout.write(JSON.stringify(obj) + "\n");
}

/**
 * Parse and validate an incoming JSON line.
 * Returns the parsed message or null if invalid.
 */
export function parseIncomingMessage(line: string): IncomingMessage | null {
  const trimmed = line.trim();
  if (!trimmed) return null;

  let parsed: unknown;
  try {
    parsed = JSON.parse(trimmed);
  } catch {
    return null;
  }

  if (typeof parsed !== "object" || parsed === null) return null;

  const obj = parsed as Record<string, unknown>;

  if (obj.type === "shutdown") {
    return { type: "shutdown" };
  }

  if (obj.type === "ping") {
    return { type: "ping" };
  }

  if (obj.type === "cancel") {
    if (typeof obj.request_id !== "string" || !obj.request_id) return null;
    return { type: "cancel", request_id: obj.request_id };
  }

  if (obj.type === "agent_request") {
    if (typeof obj.request_id !== "string" || !obj.request_id) return null;
    if (typeof obj.config !== "object" || obj.config === null) return null;
    try {
      return {
        type: "agent_request",
        request_id: obj.request_id,
        config: parseSidecarConfig(obj.config),
      };
    } catch {
      return null;
    }
  }

  if (obj.type === "stream_start") {
    if (typeof obj.request_id !== "string" || !obj.request_id) return null;
    if (typeof obj.session_id !== "string" || !obj.session_id) return null;
    if (typeof obj.config !== "object" || obj.config === null) return null;
    try {
      return {
        type: "stream_start",
        request_id: obj.request_id,
        session_id: obj.session_id,
        config: parseSidecarConfig(obj.config),
      };
    } catch {
      return null;
    }
  }

  if (obj.type === "stream_message") {
    if (typeof obj.request_id !== "string" || !obj.request_id) return null;
    if (typeof obj.session_id !== "string" || !obj.session_id) return null;
    if (typeof obj.user_message !== "string") return null;
    return {
      type: "stream_message",
      request_id: obj.request_id,
      session_id: obj.session_id,
      user_message: obj.user_message,
    };
  }

  if (obj.type === "stream_end") {
    if (typeof obj.session_id !== "string" || !obj.session_id) return null;
    return {
      type: "stream_end",
      session_id: obj.session_id,
    };
  }

  return null;
}

/**
 * Wrap an SDK message with a request_id prefix.
 */
export function wrapWithRequestId(
  requestId: string,
  message: Record<string, unknown>,
): Record<string, unknown> {
  return { request_id: requestId, ...message };
}

/**
 * Run the sidecar in persistent mode.
 *
 * - Emits `{"type":"sidecar_ready"}` on startup
 * - Reads stdin line-by-line for `agent_request` and `shutdown` messages
 * - Each `agent_request` runs the SDK and streams responses with `request_id` prefix
 * - `shutdown` causes a clean exit
 * - stdin close (pipe broken) causes a clean exit
 *
 * @param input   Readable stream (defaults to process.stdin)
 * @param exitFn  Exit function (defaults to process.exit)
 */
export async function runPersistent(
  input: NodeJS.ReadableStream = process.stdin,
  exitFn: (code: number) => void = (code) => process.exit(code),
): Promise<void> {
  // Signal readiness
  writeLine({ type: "sidecar_ready" });
  process.stderr.write("[sidecar] Persistent mode ready\n");

  const rl: Interface = createInterface({
    input,
    crlfDelay: Infinity,
  });

  // Track in-flight requests so we can wait for them before shutdown.
  // Also track the current request's AbortController and ID so we can cancel
  // a stuck request when a new one arrives or Rust sends a cancel message.
  const inFlight = new Set<Promise<void>>();
  let currentAbort: AbortController | null = null;
  let currentRequestId: string | null = null;

  // Active streaming sessions (refine chat uses these for multi-turn conversations)
  const activeSessions = new Map<string, StreamSession>();

  for await (const line of rl) {
    const message = parseIncomingMessage(line);

    if (!message) {
      // Unrecognized input — emit an error line (no request_id since we couldn't parse one)
      writeLine({
        type: "error",
        message: `Unrecognized input: ${line.trim().substring(0, 200)}`,
      });
      continue;
    }

    if (message.type === "ping") {
      writeLine({ type: "pong" });
      continue;
    }

    if (message.type === "shutdown") {
      process.stderr.write("[sidecar] Shutdown requested\n");
      // Wait for any in-flight requests to finish
      if (inFlight.size > 0) {
        await Promise.allSettled(inFlight);
      }
      rl.close();
      exitFn(0);
      return;
    }

    if (message.type === "cancel") {
      process.stderr.write(`[sidecar] Cancel request for ${message.request_id}\n`);
      // Rust sends cancel when a request times out.
      // Abort the matching in-flight request so the SDK stops waiting.
      if (currentAbort && currentRequestId === message.request_id) {
        currentAbort.abort();
      }
      continue;
    }

    if (message.type === "stream_start") {
      const { request_id, session_id, config } = message;
      process.stderr.write(
        `[sidecar] Stream start: session=${session_id} request=${request_id}\n`,
      );

      if (activeSessions.has(session_id)) {
        writeLine(
          wrapWithRequestId(request_id, {
            type: "error",
            message: `Stream session '${session_id}' already exists`,
          }),
        );
        continue;
      }

      const session = new StreamSession(
        session_id,
        request_id,
        config,
        (reqId, msg) => {
          writeLine(wrapWithRequestId(reqId, msg));
        },
      );
      activeSessions.set(session_id, session);
      continue;
    }

    if (message.type === "stream_message") {
      const { request_id, session_id, user_message } = message;
      process.stderr.write(
        `[sidecar] Stream message: session=${session_id} request=${request_id}\n`,
      );

      const session = activeSessions.get(session_id);
      if (!session) {
        writeLine(
          wrapWithRequestId(request_id, {
            type: "error",
            message: `No stream session found for '${session_id}'`,
          }),
        );
        continue;
      }

      try {
        session.pushMessage(request_id, user_message);
      } catch (err) {
        const errorMessage = err instanceof Error ? err.message : String(err);
        writeLine(
          wrapWithRequestId(request_id, {
            type: "error",
            message: errorMessage,
          }),
        );
      }
      continue;
    }

    if (message.type === "stream_end") {
      const { session_id } = message;
      process.stderr.write(`[sidecar] Stream end: session=${session_id}\n`);

      const session = activeSessions.get(session_id);
      if (session) {
        session.close();
        activeSessions.delete(session_id);
      }
      continue;
    }

    if (message.type === "agent_request") {
      // If a previous request is still in-flight (e.g., SDK hanging on API),
      // abort it before starting the new one.
      if (inFlight.size > 0 && currentAbort) {
        currentAbort.abort();
        // Fire-and-forget: don't block the readline loop while the aborted
        // request tears down. The stdout writer routes by request_id so
        // concurrent requests with different IDs are safe.
        void Promise.allSettled([...inFlight]);
      }

      const { request_id, config } = message;
      process.stderr.write(`[sidecar] Agent request: ${request_id}\n`);
      const abortController = new AbortController();
      currentAbort = abortController;
      currentRequestId = request_id;

      // Run the agent request without blocking the readline loop.
      // This lets ping/shutdown messages be processed while the agent runs.
      const requestPromise = (async () => {
        try {
          await runAgentRequest(config, (msg) => {
            writeLine(wrapWithRequestId(request_id, msg));
          }, abortController.signal);
        } catch (err) {
          const errorMessage =
            err instanceof Error ? err.message : String(err);
          writeLine(
            wrapWithRequestId(request_id, {
              type: "error",
              message: errorMessage,
            }),
          );
        } finally {
          // Signal to Rust that this request is fully complete and the sidecar
          // is ready for the next one.
          writeLine(
            wrapWithRequestId(request_id, { type: "request_complete" }),
          );
        }
      })();

      inFlight.add(requestPromise);
      requestPromise.finally(() => {
        inFlight.delete(requestPromise);
        if (currentAbort === abortController) {
          currentAbort = null;
          currentRequestId = null;
        }
      });
    }
  }

  // stdin closed (pipe broken) — exit gracefully
  if (inFlight.size > 0) {
    await Promise.allSettled(inFlight);
  }
  exitFn(0);
}
