import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router';
import { describe, it, expect, beforeEach, vi } from 'vitest';
import WorkspaceTab from '../../routes/settings/workspace-tab';
import { mockInvoke, mockInvokeCommands, resetTauriMocks } from '../../test/mocks/tauri';
import { useWorkflowStore } from '../../stores/workflow-store';

vi.mock('@tauri-apps/plugin-dialog', () => ({
  open: vi.fn().mockResolvedValue('/selected/path'),
}));

function renderPage() {
  return render(
    <MemoryRouter initialEntries={['/settings/workspace']}>
      <WorkspaceTab />
    </MemoryRouter>,
  );
}

describe('WorkspaceTab (Settings)', () => {
  const phaseState = {
    appPhase: 'scope_editable',
    hasGithubAuth: true,
    hasAnthropicKey: true,
    isSourceApplied: true,
  };

  beforeEach(() => {
    resetTauriMocks();
    mockInvokeCommands({
      workspace_get: null,
      github_list_repos: [],
      workspace_apply_start: 'job-1',
      workspace_apply_status: {
        jobId: 'job-1',
        state: 'succeeded',
        isAlive: false,
        stage: 'completed',
        percent: 100,
        message: 'Apply completed.',
        error: null,
      },
      workspace_test_source_connection: 'Connection successful',
      workspace_discover_source_databases: ['AdventureWorks', 'master'],
      workspace_reset_state: undefined,
      app_hydrate_phase: phaseState,
    });
    useWorkflowStore.setState((s) => ({
      ...s,
      appPhase: 'scope_editable',
      migrationStatus: 'idle',
      workspaceId: null,
    }));
  });

  it('renders source connection and workspace actions', () => {
    renderPage();

    expect(screen.getByTestId('settings-panel-workspace')).toBeInTheDocument();
    expect(screen.getByTestId('select-source-type')).toBeInTheDocument();
    expect(screen.getByTestId('input-source-server')).toBeInTheDocument();
    expect(screen.getByTestId('input-source-database')).toBeInTheDocument();
    expect(screen.getByTestId('btn-test-connection')).toBeInTheDocument();
    expect(screen.getByTestId('btn-apply')).toBeInTheDocument();
    expect(screen.getByTestId('settings-workspace-danger-zone')).toBeInTheDocument();
  });

  it('discovers databases after test connection', async () => {
    const user = userEvent.setup();
    renderPage();

    await user.type(screen.getByTestId('input-source-server'), 'sql.acme.local');
    await user.type(screen.getByTestId('input-source-username'), 'sa');
    await user.type(screen.getByTestId('input-source-password'), 'secret');
    await user.click(screen.getByTestId('btn-test-connection'));

    await waitFor(() => {
      expect(mockInvoke).toHaveBeenCalledWith('workspace_discover_source_databases', {
        args: {
          sourceType: 'sql_server',
          sourceServer: 'sql.acme.local',
          sourcePort: 1433,
          sourceAuthenticationMode: 'sql_password',
          sourceUsername: 'sa',
          sourcePassword: 'secret',
          sourceEncrypt: true,
          sourceTrustServerCertificate: false,
        },
      });
      const dbSelect = screen.getByTestId('input-source-database') as HTMLSelectElement;
      expect(dbSelect.options.length).toBeGreaterThan(1);
      expect(dbSelect.value).toBe('AdventureWorks');
    });
  });

  it('keeps apply disabled until db test passed + repo selected + folder selected + db selected', async () => {
    const user = userEvent.setup();
    mockInvokeCommands({
      workspace_get: null,
      workspace_apply_start: 'job-1',
      workspace_apply_status: {
        jobId: 'job-1',
        state: 'succeeded',
        isAlive: false,
        stage: 'completed',
        percent: 100,
        message: 'Apply completed.',
        error: null,
      },
      workspace_test_source_connection: 'Connection successful',
      workspace_discover_source_databases: ['AdventureWorks'],
      github_list_repos: [{ id: 1, fullName: 'acme/data-platform', private: true }],
      workspace_reset_state: undefined,
      app_hydrate_phase: phaseState,
    });
    renderPage();

    const apply = screen.getByTestId('btn-apply');
    expect(apply).toBeDisabled();

    await user.type(screen.getByTestId('input-source-server'), 'sql.acme.local');
    await user.type(screen.getByTestId('input-source-username'), 'sa');
    await user.type(screen.getByTestId('input-source-password'), 'secret');
    await user.click(screen.getByTestId('btn-pick-repo-path'));

    await user.click(screen.getByTestId('input-repo-name'));
    await user.selectOptions(screen.getByTestId('input-repo-name'), 'acme/data-platform');

    expect(apply).toBeDisabled();

    await user.click(screen.getByTestId('btn-test-connection'));
    await waitFor(() => expect(apply).toBeEnabled());
  });

  it('invalidates passed connection test and clears discovered databases when source fields change', async () => {
    const user = userEvent.setup();
    renderPage();

    await user.type(screen.getByTestId('input-source-server'), 'sql.acme.local');
    await user.type(screen.getByTestId('input-source-username'), 'sa');
    await user.type(screen.getByTestId('input-source-password'), 'secret');
    await user.click(screen.getByTestId('btn-test-connection'));

    await waitFor(() => {
      expect(screen.getByTestId('workspace-test-connection-success')).toBeInTheDocument();
      expect((screen.getByTestId('input-source-database') as HTMLSelectElement).value).toBe(
        'AdventureWorks',
      );
    });

    await user.type(screen.getByTestId('input-source-server'), '2');

    expect(screen.queryByTestId('workspace-test-connection-success')).not.toBeInTheDocument();
    expect((screen.getByTestId('input-source-database') as HTMLSelectElement).value).toBe('');
  });

  it('sends credential-based payload for test connection', async () => {
    const user = userEvent.setup();
    renderPage();

    await user.type(screen.getByTestId('input-source-server'), 'sql.acme.local');
    await user.clear(screen.getByTestId('input-source-port'));
    await user.type(screen.getByTestId('input-source-port'), '1433');
    await user.type(screen.getByTestId('input-source-username'), 'sa');
    await user.type(screen.getByTestId('input-source-password'), 'top-secret');

    await user.click(screen.getByTestId('btn-test-connection'));

    await waitFor(() => {
      expect(mockInvoke).toHaveBeenCalledWith('workspace_test_source_connection', {
        args: {
          sourceType: 'sql_server',
          sourceServer: 'sql.acme.local',
          sourcePort: 1433,
          sourceAuthenticationMode: 'sql_password',
          sourceUsername: 'sa',
          sourcePassword: 'top-secret',
          sourceEncrypt: true,
          sourceTrustServerCertificate: false,
        },
      });
    });
  });

  it('shows SQL Server only in source type selector', () => {
    renderPage();

    const sourceTypeSelect = screen.getByTestId('select-source-type') as HTMLSelectElement;
    expect(sourceTypeSelect).toBeDisabled();
    expect(
      Array.from(sourceTypeSelect.options).map((option) => option.value),
    ).toEqual(['sql_server']);
  });

  it('enforces browse-only working folder input', async () => {
    const user = userEvent.setup();
    renderPage();

    const pathInput = screen.getByTestId('input-repo-path');
    expect(pathInput).toBeDisabled();

    await user.click(screen.getByTestId('btn-pick-repo-path'));
    expect(pathInput).toHaveValue('/selected/path');
  });

  it('locks the page after successful apply', async () => {
    const user = userEvent.setup();
    mockInvokeCommands({
      workspace_get: null,
      workspace_apply_start: 'job-1',
      workspace_apply_status: {
        jobId: 'job-1',
        state: 'succeeded',
        isAlive: false,
        stage: 'completed',
        percent: 100,
        message: 'Apply completed.',
        error: null,
      },
      workspace_test_source_connection: 'Connection successful',
      workspace_discover_source_databases: ['AdventureWorks'],
      github_list_repos: [{ id: 1, fullName: 'acme/data-platform', private: true }],
      workspace_reset_state: undefined,
      app_hydrate_phase: phaseState,
    });
    renderPage();

    await user.type(screen.getByTestId('input-source-server'), 'sql.acme.local');
    await user.type(screen.getByTestId('input-source-username'), 'sa');
    await user.type(screen.getByTestId('input-source-password'), 'secret');
    await user.click(screen.getByTestId('btn-pick-repo-path'));
    await user.click(screen.getByTestId('input-repo-name'));
    await user.selectOptions(screen.getByTestId('input-repo-name'), 'acme/data-platform');
    await user.click(screen.getByTestId('btn-test-connection'));

    await waitFor(() => expect(screen.getByTestId('btn-apply')).toBeEnabled());
    await user.click(screen.getByTestId('btn-apply'));

    await waitFor(() => {
      expect(screen.getByTestId('workspace-apply-success')).toHaveTextContent(
        'Workspace applied successfully. Repository cloned locally.',
      );
      expect(
        screen.getByText('Workspace is locked after apply. Reset Migration to edit settings.'),
      ).toBeInTheDocument();
      expect(screen.getByTestId('btn-apply')).toBeDisabled();
      expect(screen.getByTestId('btn-test-connection')).toBeDisabled();
      expect(screen.getByTestId('btn-pick-repo-path')).toBeDisabled();
    });
  });

  it('apply button sends SQL Server source payload', async () => {
    const user = userEvent.setup();
    mockInvokeCommands({
      workspace_get: null,
      workspace_apply_start: 'job-1',
      workspace_apply_status: {
        jobId: 'job-1',
        state: 'succeeded',
        isAlive: false,
        stage: 'completed',
        percent: 100,
        message: 'Apply completed.',
        error: null,
      },
      workspace_test_source_connection: 'Connection successful',
      workspace_discover_source_databases: ['AdventureWorks'],
      github_list_repos: [{ id: 1, fullName: 'acme/data-platform', private: true }],
      workspace_reset_state: undefined,
      app_hydrate_phase: phaseState,
    });
    renderPage();

    await user.type(screen.getByTestId('input-source-server'), 'sql.acme.local');
    await user.clear(screen.getByTestId('input-source-port'));
    await user.type(screen.getByTestId('input-source-port'), '1433');
    await user.type(screen.getByTestId('input-source-username'), 'sa');
    await user.type(screen.getByTestId('input-source-password'), 'secret');
    await user.click(screen.getByTestId('btn-pick-repo-path'));
    await user.click(screen.getByTestId('input-repo-name'));
    await user.selectOptions(screen.getByTestId('input-repo-name'), 'acme/data-platform');
    await user.click(screen.getByTestId('btn-test-connection'));
    await waitFor(() => expect(screen.getByTestId('btn-apply')).toBeEnabled());

    await user.click(screen.getByTestId('btn-apply'));

    await waitFor(() => {
      expect(mockInvoke).toHaveBeenCalledWith('workspace_apply_start', {
        args: {
          name: 'Migration Workspace',
          migrationRepoName: 'acme/data-platform',
          migrationRepoPath: '/selected/path',
          fabricUrl: null,
          fabricServicePrincipalId: null,
          fabricServicePrincipalSecret: null,
          sourceType: 'sql_server',
          sourceServer: 'sql.acme.local',
          sourceDatabase: 'AdventureWorks',
          sourcePort: 1433,
          sourceAuthenticationMode: 'sql_password',
          sourceUsername: 'sa',
          sourcePassword: 'secret',
          sourceEncrypt: true,
          sourceTrustServerCertificate: false,
        },
      });
    });
  });

  it('requires exact high-friction confirmation token for reset', async () => {
    const user = userEvent.setup();
    renderPage();

    await user.type(screen.getByTestId('input-source-server'), 'sql.acme.local');
    await user.type(screen.getByTestId('input-source-username'), 'sa');
    await user.type(screen.getByTestId('input-source-password'), 'secret');
    await user.click(screen.getByTestId('btn-test-connection'));
    await waitFor(() => {
      expect((screen.getByTestId('input-source-database') as HTMLSelectElement).value).toBe(
        'AdventureWorks',
      );
    });

    await user.click(screen.getByTestId('btn-open-reset-migration-dialog'));

    expect(
      screen.getByText('This clears local migration state and workspace settings. Remote GitHub repository is not touched.'),
    ).toBeInTheDocument();

    const confirmButton = screen.getByTestId('btn-confirm-reset-migration');
    expect(confirmButton).toBeDisabled();

    await user.type(screen.getByTestId('input-reset-confirmation'), 'RESET wrong-db');
    expect(confirmButton).toBeDisabled();

    await user.clear(screen.getByTestId('input-reset-confirmation'));
    await user.type(screen.getByTestId('input-reset-confirmation'), 'RESET AdventureWorks');
    expect(confirmButton).toBeEnabled();
  });

  it('reset clears persisted workspace state and stays on workspace page', async () => {
    const user = userEvent.setup();
    renderPage();

    await user.type(screen.getByTestId('input-source-server'), 'sql.acme.local');
    await user.type(screen.getByTestId('input-source-username'), 'sa');
    await user.type(screen.getByTestId('input-source-password'), 'secret');
    await user.click(screen.getByTestId('btn-test-connection'));
    await waitFor(() => {
      expect((screen.getByTestId('input-source-database') as HTMLSelectElement).value).toBe(
        'AdventureWorks',
      );
    });

    await user.click(screen.getByTestId('btn-open-reset-migration-dialog'));
    await user.type(screen.getByTestId('input-reset-confirmation'), 'RESET AdventureWorks');
    await user.click(screen.getByTestId('btn-confirm-reset-migration'));

    await waitFor(() => {
      expect(mockInvoke).toHaveBeenCalledWith('workspace_reset_state');
      expect(screen.getByTestId('settings-panel-workspace')).toBeInTheDocument();
      expect(screen.getByTestId('input-source-server')).toHaveValue('');
      expect((screen.getByTestId('input-source-database') as HTMLSelectElement).value).toBe('');
    });
  });
});
