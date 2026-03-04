import { render, screen, waitFor } from '@testing-library/react';
import { describe, it, beforeEach, expect } from 'vitest';
import App from '@/App';
import { mockInvokeCommands, resetTauriMocks } from '@/test/mocks/tauri';

if (!window.matchMedia) {
  Object.defineProperty(window, 'matchMedia', {
    writable: true,
    value: (query: string) => ({
      matches: false,
      media: query,
      onchange: null,
      addListener: () => {},
      removeListener: () => {},
      addEventListener: () => {},
      removeEventListener: () => {},
      dispatchEvent: () => false,
    }),
  });
}

describe('App routing guards', () => {
  beforeEach(() => {
    resetTauriMocks();
    window.history.pushState({}, '', '/');
  });

  it('redirects startup to home and renders dashboard', async () => {
    mockInvokeCommands({
      workspace_get: { id: 'ws-1', displayName: 'Test' },
      app_hydrate_phase: {
        appPhase: 'scope_editable',
        hasGithubAuth: true,
        hasAnthropicKey: true,
        isSourceApplied: true,
      },
      github_get_user: { login: 'user', avatarUrl: '' },
      get_settings: { anthropicApiKey: 'key' },
    });

    render(<App />);

    await waitFor(() => {
      expect(screen.getByTestId('home-dashboard-state')).toBeInTheDocument();
      expect(screen.getByTestId('nav-home')).not.toBeDisabled();
    });
  });

});
