import { render, screen, waitFor } from '@testing-library/react';
import { describe, it, expect, beforeEach, vi } from 'vitest';
import { MemoryRouter } from 'react-router';
import ConnectionsTab from '../../routes/settings/connections-tab';
import { mockInvokeCommands, resetTauriMocks } from '../../test/mocks/tauri';
import { useAuthStore } from '@/stores/auth-store';

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
});

describe('ConnectionsTab — GitHub card', () => {
  it('shows Sign in button when not connected', async () => {
    mockInvokeCommands({ github_get_user: null, get_settings: { anthropicApiKey: null } });
    renderTab();
    await waitFor(() => {
      expect(screen.getByTestId('btn-connect-github')).toBeVisible();
    });
    expect(screen.getByTestId('settings-panel-connections')).toBeInTheDocument();
    expect(screen.getAllByText('Not connected').length).toBeGreaterThan(0);
  });

  it('shows github URL and Disconnect when connected', async () => {
    mockInvokeCommands({ github_get_user: MOCK_USER, get_settings: { anthropicApiKey: null } });
    renderTab();
    await waitFor(() => {
      expect(screen.getByText('@octocat')).toBeVisible();
    });
    expect(screen.getByText('Connected')).toBeVisible();
    expect(screen.getByText(/Last checked/i)).toBeVisible();
    expect(screen.getByTestId('btn-disconnect-github')).toBeVisible();
  });

  it('shows checking state while auth is loading', async () => {
    mockInvokeCommands({
      github_get_user: new Promise(() => {}),
      get_settings: { anthropicApiKey: null },
    });
    renderTab();
    await waitFor(() => {
      expect(screen.getByText('Checking')).toBeVisible();
    });
    expect(screen.getByText('Checking GitHub connection...')).toBeVisible();
    expect(screen.queryByTestId('btn-connect-github')).not.toBeInTheDocument();
  });

  it('Anthropic key input and Update button are present', async () => {
    mockInvokeCommands({ github_get_user: null, get_settings: { anthropicApiKey: null } });
    renderTab();
    await waitFor(() => {
      expect(screen.getByTestId('input-anthropic-key')).toBeInTheDocument();
    });
    expect(screen.getByTestId('btn-update-anthropic-key')).toBeInTheDocument();
  });
});
