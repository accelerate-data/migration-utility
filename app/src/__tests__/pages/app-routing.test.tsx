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

describe('App routing', () => {
  beforeEach(() => {
    resetTauriMocks();
    window.history.pushState({}, '', '/');
  });

  it('redirects from / to /home on startup', async () => {
    mockInvokeCommands({
      github_get_user: null,
      get_settings: { anthropicApiKey: null },
    });

    render(<App />);

    await waitFor(() => {
      expect(screen.getByTestId('home-dashboard')).toBeInTheDocument();
    });
  });
});
