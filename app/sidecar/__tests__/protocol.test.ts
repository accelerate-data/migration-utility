import { describe, expect, it } from 'vitest';
import { parseIncomingMessage } from '../persistent-mode.ts';

describe('parseIncomingMessage', () => {
  it('parses ping and shutdown', () => {
    expect(parseIncomingMessage('{"type":"ping"}')).toEqual({ type: 'ping' });
    expect(parseIncomingMessage('{"type":"shutdown"}')).toEqual({ type: 'shutdown' });
  });

  it('parses agent_request with config', () => {
    const result = parseIncomingMessage(
      JSON.stringify({
        type: 'agent_request',
        request_id: 'req-1',
        config: {
          prompt: 'hello',
          apiKey: 'sk-ant-test',
          cwd: '/tmp/work',
        },
      }),
    );
    expect(result).toMatchObject({
      type: 'agent_request',
      request_id: 'req-1',
      config: { prompt: 'hello', cwd: '/tmp/work' },
    });
  });

  it('parses stream_message', () => {
    const result = parseIncomingMessage(
      '{"type":"stream_message","request_id":"r1","session_id":"s1","user_message":"next"}',
    );
    expect(result).toEqual({
      type: 'stream_message',
      request_id: 'r1',
      session_id: 's1',
      user_message: 'next',
    });
  });

  it('returns null for invalid input', () => {
    expect(parseIncomingMessage('not-json')).toBeNull();
    expect(parseIncomingMessage('{"type":"agent_request"}')).toBeNull();
  });
});

