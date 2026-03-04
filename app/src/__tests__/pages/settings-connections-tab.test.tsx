import { act, render, screen, waitFor } from '@testing-library/react';
import { describe, it, expect, beforeEach, vi } from 'vitest';
import { MemoryRouter } from 'react-router';
import ConnectionsTab from '../../routes/settings/connections-tab';
import { mockInvokeCommands, resetTauriMocks } from '../../test/mocks/tauri';
import { useAuthStore } from '@/stores/auth-store';
import { useWorkflowStore } from '@/stores/workflow-store';

vi.mock('sonner', () => ({
  toast: {
    success: vi.fn(),
    error: vi.fn(),
  },
}));

const MOCK_USER = {
  login: 'octocat',
  avatar_url: 'https://github.com/octocat.png',
  email: 'octocat@github.com',
};
const initialAuthState = useAuthStore.getState();
const PHASE_STATE = {
  appPhase: 'setup_required',
  hasGithubAuth: false,
  hasAnthropicKey: false,
  hasProject: false,
};

function renderTab() {
  return render(
    <MemoryRouter>
      <ConnectionsTab />
    </MemoryRouter>,
  );
}

beforeEach(() => {
  resetTauriMocks();
  useAuthStore.setState({
    user: null,
    isLoggedIn: false,
    isLoading: false,
    lastCheckedAt: null,
    loadUser: initialAuthState.loadUser,
    setUser: initialAuthState.setUser,
    logout: initialAuthState.logout,
    reset: initialAuthState.reset,
  });
  useWorkflowStore.setState((s) => ({ ...s, migrationStatus: 'idle' }));
});

describe('ConnectionsTab — GitHub card', () => {
  it('shows Sign in button when not connected', async () => {
    mockInvokeCommands({ github_get_user: null, get_settings: { anthropicApiKey: null }, app_hydrate_phase: PHASE_STATE });
    renderTab();
    await waitFor(() => {
      expect(screen.getByTestId('btn-connect-github')).toBeVisible();
    });
    expect(screen.getByTestId('settings-panel-connections')).toBeInTheDocument();
    expect(screen.getByTestId('settings-connections-group-label')).toHaveTextContent(
      'One-time setup · Safe to update at any time',
    );
    expect(screen.getAllByText('Not connected').length).toBeGreaterThan(0);
  });

  it('shows github URL and Disconnect when connected', async () => {
    mockInvokeCommands({ github_get_user: MOCK_USER, get_settings: { anthropicApiKey: null }, app_hydrate_phase: PHASE_STATE });
    renderTab();
    await waitFor(() => {
      expect(screen.getByText('@octocat')).toBeVisible();
    });
    expect(screen.getByText('Connected')).toBeVisible();
    expect(screen.getByText(/Last checked/i)).toBeVisible();
    expect(screen.getByTestId('btn-disconnect-github')).toBeVisible();
  });

  it('Disconnect button is disabled when app phase is running_locked', async () => {
    useAuthStore.setState({
      user: MOCK_USER,
      isLoggedIn: true,
      isLoading: false,
      lastCheckedAt: null,
      loadUser: async () => {},
    });
    mockInvokeCommands({ github_get_user: MOCK_USER, get_settings: { anthropicApiKey: null }, app_hydrate_phase: PHASE_STATE });
    act(() => {
      useWorkflowStore.setState((s) => ({ ...s, appPhase: 'running_locked' }));
    });
    renderTab();
    await waitFor(() => {
      expect(screen.getByTestId('btn-disconnect-github')).toBeDisabled();
    });
  });

  it('shows checking state while auth is loading', async () => {
    mockInvokeCommands({
      github_get_user: new Promise(() => {}),
      get_settings: { anthropicApiKey: null },
      app_hydrate_phase: PHASE_STATE,
    });
    renderTab();
    await waitFor(() => {
      expect(screen.getByText('Checking')).toBeVisible();
    });
    expect(screen.getByText('Checking GitHub connection...')).toBeVisible();
    expect(screen.queryByTestId('btn-connect-github')).not.toBeInTheDocument();
  });

  it('Anthropic key input and Update button are present', async () => {
    mockInvokeCommands({ github_get_user: null, get_settings: { anthropicApiKey: null }, app_hydrate_phase: PHASE_STATE });
    renderTab();
    await waitFor(() => {
      expect(screen.getByTestId('input-anthropic-key')).toBeInTheDocument();
    });
    expect(screen.getByTestId('btn-update-anthropic-key')).toBeInTheDocument();
  });
});
