import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, it, expect, beforeEach, vi } from 'vitest';
import { MemoryRouter } from 'react-router';
import ProfileTab from '../../routes/settings/profile-tab';
import { setFrontendLogLevel } from '@/lib/logger';
import { mockInvoke, mockInvokeCommands, resetTauriMocks } from '../../test/mocks/tauri';

vi.mock('next-themes', () => ({
  useTheme: () => ({ theme: 'system', setTheme: vi.fn() }),
}));

function renderTab() {
  return render(
    <MemoryRouter>
      <ProfileTab />
    </MemoryRouter>,
  );
}

async function renderTabReady() {
  renderTab();
  await waitFor(() => {
    expect(screen.getByTestId('path-log-file')).toHaveTextContent('/tmp/migration-utility.log');
    expect(screen.getByTestId('path-data-dir')).toHaveTextContent('/tmp/data');
  });
}

beforeEach(() => {
  setFrontendLogLevel('info');
  resetTauriMocks();
  mockInvokeCommands({
    get_log_file_path: '/tmp/migration-utility.log',
    get_data_dir_path: '/tmp/data',
    set_log_level: undefined,
    get_settings: {},
  });
});

describe('ProfileTab', () => {
  it('renders the profile tab container', async () => {
    await renderTabReady();
    expect(screen.getByTestId('settings-profile-tab')).toBeInTheDocument();
    expect(screen.getByTestId('settings-panel-profile')).toBeInTheDocument();
    expect(screen.getByTestId('settings-profile-logging-card')).toBeInTheDocument();
    expect(screen.getByTestId('settings-profile-directories-card')).toBeInTheDocument();
  });

  it('renders log level select with DB-loaded level', async () => {
    mockInvokeCommands({
      get_log_file_path: '/tmp/migration-utility.log',
      get_data_dir_path: '/tmp/data',
      set_log_level: undefined,
      get_settings: { logLevel: 'warn' },
    });
    renderTab();
    await waitFor(() => {
      const select = screen.getByTestId('select-log-level') as HTMLSelectElement;
      expect(select.value).toBe('warn');
    });
  });

  it('defaults to info when settings has no logLevel', async () => {
    await renderTabReady();
    const select = screen.getByTestId('select-log-level') as HTMLSelectElement;
    expect(select.value).toBe('info');
  });

  it('changing log level calls set_log_level command', async () => {
    const user = userEvent.setup();
    await renderTabReady();
    const select = screen.getByTestId('select-log-level');
    await user.selectOptions(select, 'debug');
    expect(mockInvoke).toHaveBeenCalledWith('set_log_level', { level: 'debug' });
  });

  it('does not render fire test logs button', async () => {
    await renderTabReady();
    expect(screen.queryByTestId('btn-fire-test-logs')).not.toBeInTheDocument();
  });

  it('renders all three theme toggle buttons', async () => {
    await renderTabReady();
    expect(screen.getByTestId('theme-system')).toBeInTheDocument();
    expect(screen.getByTestId('theme-light')).toBeInTheDocument();
    expect(screen.getByTestId('theme-dark')).toBeInTheDocument();
  });

  it('active theme button has bg-background class', async () => {
    await renderTabReady();
    const systemBtn = screen.getByTestId('theme-system');
    expect(systemBtn.className).toContain('bg-background');
  });

  it('renders working directory path', async () => {
    await renderTabReady();
    expect(screen.getByTestId('path-working-dir')).toHaveTextContent('~/.vibedata/migration-utility');
  });

  it('renders log file path from backend', async () => {
    await renderTabReady();
  });

  it('renders data directory path from backend', async () => {
    await renderTabReady();
  });
});
