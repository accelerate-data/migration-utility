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

  it('redirects startup to home when appPhase is setup_required', async () => {
    mockInvokeCommands({
      app_hydrate_phase: {
        appPhase: 'setup_required',
        hasGithubAuth: false,
        hasAnthropicKey: false,
        hasProject: false,
      },
      github_get_user: null,
      get_settings: { anthropicApiKey: null },
    });

    render(<App />);

    await waitFor(() => {
      expect(screen.getByTestId('home-setup-state')).toBeInTheDocument();
      expect(screen.getByTestId('nav-home')).not.toBeDisabled();
    });
  });

});
