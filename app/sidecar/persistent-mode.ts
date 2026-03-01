import {
  unstable_v2_createSession,
  type SDKSession,
} from '@anthropic-ai/claude-agent-sdk';
import { createInterface } from 'node:readline';
import type { SidecarConfig } from './config.ts';
import { buildInitialPrompt, buildSessionOptions, redactSessionOptionsForLog } from './options.ts';
import { parseIncomingMessage, writeLine } from './protocol.ts';
import { StreamSession } from './stream-session.ts';

function assistantTextFromEvent(event: unknown): string {
  if (!event || typeof event !== 'object') return '';
  const e = event as {
    type?: string;
    message?: { content?: Array<{ type?: string; text?: string }> };
  };
  if (e.type !== 'assistant') return '';
  const blocks = e.message?.content;
  if (!Array.isArray(blocks)) return '';
  return blocks
    .filter((b) => b?.type === 'text' && typeof b.text === 'string')
    .map((b) => b.text as string)
    .join('');
}

async function runSingleRequest(
  requestId: string,
  config: SidecarConfig,
  abortSignal: AbortSignal,
): Promise<void> {
  const sessionOptions = buildSessionOptions(config);
  console.error(
    `[sidecar] request_id=${requestId} sdk_session_options=${JSON.stringify(redactSessionOptionsForLog(sessionOptions))}`,
  );
  const session: SDKSession = unstable_v2_createSession(sessionOptions);
  try {
    writeLine({
      type: 'system',
      request_id: requestId,
      subtype: 'init_start',
      timestamp: Date.now(),
    });
    await session.send(buildInitialPrompt(config));
    writeLine({
      type: 'system',
      request_id: requestId,
      subtype: 'sdk_ready',
      timestamp: Date.now(),
    });

    for await (const event of session.stream()) {
      if (abortSignal.aborted) {
        writeLine({
          type: 'error',
          request_id: requestId,
          message: 'Request aborted',
        });
        break;
      }
      writeLine({ type: 'agent_event', request_id: requestId, event });
      const textChunk = assistantTextFromEvent(event);
      if (textChunk.length > 0) {
        writeLine({
          type: 'agent_response',
          request_id: requestId,
          content: textChunk,
          done: false,
        });
      }

      const eventType =
        typeof event === 'object' && event !== null && 'type' in event
          ? (event as { type?: string }).type
          : undefined;
      if (eventType === 'result' || eventType === 'error') {
        break;
      }
    }

    writeLine({
      type: 'agent_response',
      request_id: requestId,
      content: '',
      done: true,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    writeLine({ type: 'error', request_id: requestId, message });
  } finally {
    session.close();
    writeLine({ type: 'request_complete', request_id: requestId });
  }
}

export async function runPersistent(): Promise<void> {
  writeLine({ type: 'sidecar_ready' });
  console.error('[sidecar] persistent mode ready');

  const rl = createInterface({ input: process.stdin, crlfDelay: Infinity });
  const activeSessions = new Map<string, StreamSession>();
  const activeRequests = new Map<string, AbortController>();
  const inFlight = new Set<Promise<void>>();

  for await (const line of rl) {
    const message = parseIncomingMessage(line);
    if (!message) {
      console.error('[sidecar] unrecognized input');
      continue;
    }

    if (message.type === 'ping') {
      writeLine({ type: 'pong' });
      continue;
    }
    if (message.type === 'shutdown') {
      console.error('[sidecar] shutdown requested');
      break;
    }
    if (message.type === 'cancel') {
      activeRequests.get(message.request_id)?.abort();
      continue;
    }

    if (message.type === 'agent_request') {
      const controller = new AbortController();
      activeRequests.set(message.request_id, controller);
      const p = runSingleRequest(message.request_id, message.config, controller.signal).finally(
        () => {
          activeRequests.delete(message.request_id);
          inFlight.delete(p);
        },
      );
      inFlight.add(p);
      continue;
    }

    if (message.type === 'stream_start') {
      if (activeSessions.has(message.session_id)) {
        writeLine({
          type: 'error',
          request_id: message.request_id,
          message: `stream session '${message.session_id}' already exists`,
        });
        writeLine({ type: 'request_complete', request_id: message.request_id });
        continue;
      }
      const session = new StreamSession(message.config);
      activeSessions.set(message.session_id, session);
      const p = session.start(message.request_id, message.config).catch((err: unknown) => {
        const msg = err instanceof Error ? err.message : String(err);
        writeLine({ type: 'error', request_id: message.request_id, message: msg });
        writeLine({ type: 'request_complete', request_id: message.request_id });
      });
      inFlight.add(p);
      void p.finally(() => inFlight.delete(p));
      continue;
    }

    if (message.type === 'stream_message') {
      const session = activeSessions.get(message.session_id);
      if (!session) {
        writeLine({
          type: 'error',
          request_id: message.request_id,
          message: `no stream session found for '${message.session_id}'`,
        });
        writeLine({ type: 'request_complete', request_id: message.request_id });
        continue;
      }
      const p = session.sendTurn(message.request_id, message.user_message).catch((err: unknown) => {
        const msg = err instanceof Error ? err.message : String(err);
        writeLine({ type: 'error', request_id: message.request_id, message: msg });
        writeLine({ type: 'request_complete', request_id: message.request_id });
      });
      inFlight.add(p);
      void p.finally(() => inFlight.delete(p));
      continue;
    }

    if (message.type === 'stream_end') {
      activeSessions.get(message.session_id)?.close();
      activeSessions.delete(message.session_id);
    }
  }

  for (const session of activeSessions.values()) {
    session.close();
  }
  activeSessions.clear();
  for (const controller of activeRequests.values()) {
    controller.abort();
  }
  activeRequests.clear();
  if (inFlight.size > 0) {
    await Promise.allSettled([...inFlight]);
  }
}
