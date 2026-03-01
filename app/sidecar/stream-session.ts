import {
  unstable_v2_createSession,
  type SDKSession,
} from '@anthropic-ai/claude-agent-sdk';
import type { SidecarConfig } from './config.ts';
import { buildInitialPrompt, buildSessionOptions, redactSessionOptionsForLog } from './options.ts';
import { writeLine } from './protocol.ts';

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

export class StreamSession {
  private readonly session: SDKSession;
  private readonly redactedSessionOptions: Record<string, unknown>;
  private queue: Promise<void> = Promise.resolve();
  private closed = false;
  private loggedOptions = false;

  constructor(config: SidecarConfig) {
    const opts = buildSessionOptions(config);
    this.redactedSessionOptions = redactSessionOptionsForLog(opts);
    this.session = unstable_v2_createSession(opts);
  }

  start(requestId: string, config: SidecarConfig): Promise<void> {
    if (!this.loggedOptions) {
      console.error(
        `[sidecar] request_id=${requestId} sdk_session_options=${JSON.stringify(this.redactedSessionOptions)}`,
      );
      this.loggedOptions = true;
    }
    return this.sendTurn(requestId, buildInitialPrompt(config));
  }

  sendTurn(requestId: string, text: string): Promise<void> {
    this.queue = this.queue.then(async () => {
      if (this.closed) {
        throw new Error('stream session is closed');
      }
      writeLine({
        type: 'system',
        request_id: requestId,
        subtype: 'init_start',
        timestamp: Date.now(),
      });
      await this.session.send(text);
      writeLine({
        type: 'system',
        request_id: requestId,
        subtype: 'sdk_ready',
        timestamp: Date.now(),
      });

      for await (const event of this.session.stream()) {
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
      writeLine({ type: 'request_complete', request_id: requestId });
    });
    return this.queue;
  }

  close(): void {
    this.closed = true;
    this.session.close();
  }
}
