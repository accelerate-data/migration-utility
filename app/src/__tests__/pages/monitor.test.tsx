import { render, screen, fireEvent, waitFor, act } from '@testing-library/react';
import { MemoryRouter } from 'react-router';
import { describe, it, expect, beforeEach, vi } from 'vitest';
import MonitorSurface from '../../routes/monitor';
import { useWorkflowStore } from '../../stores/workflow-store';
import { mockInvoke, mockInvokeCommands, resetTauriMocks } from '../../test/mocks/tauri';

type MonitorStreamPayload = {
  requestId: string;
  eventType: string;
  content?: string | null;
  done?: boolean | null;
  subtype?: string | null;
  toolName?: string | null;
  totalCostUsd?: number | null;
  inputTokens?: number | null;
  outputTokens?: number | null;
};

let monitorStreamListener: ((event: { payload: MonitorStreamPayload }) => void) | null = null;

vi.mock('@tauri-apps/api/event', () => ({
  listen: vi.fn((_name: string, handler: (event: { payload: MonitorStreamPayload }) => void) => {
    monitorStreamListener = handler;
    return Promise.resolve(() => {
      monitorStreamListener = null;
    });
  }),
}));

function renderPage() {
  return render(
    <MemoryRouter initialEntries={['/monitor']}>
      <MonitorSurface />
    </MemoryRouter>,
  );
}

type Deferred<T> = {
  promise: Promise<T>;
  resolve: (value: T) => void;
};

function deferred<T>(): Deferred<T> {
  let resolve: ((value: T) => void) | null = null;
  const promise = new Promise<T>((res) => {
    resolve = res;
  });
  return { promise, resolve: (value: T) => resolve?.(value) };
}

describe('MonitorSurface', () => {
  beforeEach(() => {
    resetTauriMocks();
    monitorStreamListener = null;
    mockInvokeCommands({
      app_set_phase: {
        appPhase: 'running_locked',
        hasGithubAuth: true,
        hasAnthropicKey: true,
        isSourceApplied: true,
      },
      app_hydrate_phase: {
        appPhase: 'ready_to_run',
        hasGithubAuth: true,
        hasAnthropicKey: true,
        isSourceApplied: true,
      },
      monitor_launch_agent: 'agent output',
    });
    useWorkflowStore.setState((s) => ({ ...s, appPhase: 'ready_to_run', migrationStatus: 'idle' }));
  });

  it('renders ready state when appPhase is ready_to_run', () => {
    renderPage();
    expect(screen.getByTestId('monitor-ready-state')).toBeInTheDocument();
    expect(screen.queryByTestId('monitor-running-state')).not.toBeInTheDocument();
  });

  it('renders running state when appPhase is running_locked', () => {
    useWorkflowStore.setState((s) => ({ ...s, appPhase: 'running_locked', migrationStatus: 'running' }));
    renderPage();
    expect(screen.getByTestId('monitor-running-state')).toBeInTheDocument();
    expect(screen.queryByTestId('monitor-ready-state')).not.toBeInTheDocument();
  });

  it('Launch Migration button transitions appPhase to running_locked', async () => {
    renderPage();
    fireEvent.click(screen.getByTestId('btn-launch-migration'));
    await waitFor(() => {
      expect(useWorkflowStore.getState().appPhase).toBe('running_locked');
    });
  });

  it('running state shows log stream', () => {
    useWorkflowStore.setState((s) => ({ ...s, appPhase: 'running_locked', migrationStatus: 'running' }));
    renderPage();
    expect(screen.getByTestId('monitor-log-stream')).toBeInTheDocument();
  });

  it('shows agent response in running log stream after launch', async () => {
    renderPage();
    fireEvent.click(screen.getByTestId('btn-launch-migration'));
    await waitFor(() => {
      expect(screen.getByTestId('monitor-log-stream')).toHaveTextContent('agent output');
    });
  });

  it('does not duplicate response text when stream already emitted the final content', async () => {
    const launchResult = deferred<string>();
    mockInvoke.mockImplementation((cmd: string) => {
      if (cmd === 'app_set_phase') {
        return Promise.resolve({
          appPhase: 'running_locked',
          hasGithubAuth: true,
          hasAnthropicKey: true,
          isSourceApplied: true,
        });
      }
      if (cmd === 'monitor_launch_agent') {
        return launchResult.promise;
      }
      return Promise.reject(new Error(`Unmocked command: ${cmd}`));
    });

    renderPage();
    fireEvent.click(screen.getByTestId('btn-launch-migration'));

    await waitFor(() => {
      expect(monitorStreamListener).not.toBeNull();
    });

    await act(async () => {
      monitorStreamListener?.({
        payload: {
          requestId: 'req-1',
          eventType: 'agent_response',
          content: 'agent output',
        },
      });
      launchResult.resolve('agent output');
    });

    await waitFor(() => {
      const logText = screen.getByTestId('monitor-log-stream').textContent ?? '';
      const occurrences = logText.split('agent output').length - 1;
      expect(occurrences).toBe(1);
    });
  });
});
