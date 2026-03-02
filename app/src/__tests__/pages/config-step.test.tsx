import { describe, it, expect, vi, beforeEach } from 'vitest';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router';
import ConfigStep from '@/routes/scope/config-step';
import { useWorkflowStore } from '@/stores/workflow-store';

const tauriMocks = vi.hoisted(() => ({
  migrationListScopeInventory: vi.fn(),
  migrationGetTableConfig: vi.fn(),
  migrationAnalyzeTableDetails: vi.fn(),
  migrationSaveTableConfig: vi.fn(),
  appSetPhaseFlags: vi.fn(),
  workspaceGet: vi.fn(),
  workspaceApplyStart: vi.fn(),
  workspaceApplyStatus: vi.fn(),
  migrationReconcileScopeState: vi.fn(),
}));

vi.mock('@/lib/tauri', () => tauriMocks);

function renderStep() {
  return render(
    <MemoryRouter initialEntries={['/scope/config']}>
      <ConfigStep />
    </MemoryRouter>,
  );
}

describe('ConfigStep', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    useWorkflowStore.setState((s) => ({
      ...s,
      workspaceId: 'ws-1',
      appPhase: 'scope_editable',
      phaseFacts: { ...s.phaseFacts, scopeFinalized: false },
    }));
    tauriMocks.migrationListScopeInventory.mockResolvedValue([
      {
        warehouseItemId: 'wh-1',
        schemaName: 'dbo',
        tableName: 'fact_sales',
        rowCount: 1_250_000,
        isSelected: true,
      },
    ]);
    tauriMocks.migrationGetTableConfig.mockResolvedValue(null);
    tauriMocks.migrationAnalyzeTableDetails.mockResolvedValue({
      selectedTableId: 'st:ws-1:wh-1:dbo:fact_sales',
      tableType: 'unknown',
      loadStrategy: 'incremental',
      grainColumns: '[]',
      relationshipsJson: '[]',
      incrementalColumn: '',
      dateColumn: '',
      snapshotStrategy: 'sample_1day',
      piiColumns: '[]',
      confirmedAt: null,
    });
    tauriMocks.migrationSaveTableConfig.mockResolvedValue(undefined);
    tauriMocks.appSetPhaseFlags.mockResolvedValue({
      appPhase: 'plan_editable',
      hasGithubAuth: true,
      hasAnthropicKey: true,
      isSourceApplied: true,
      scopeFinalized: true,
      planFinalized: false,
    });
    tauriMocks.workspaceGet.mockResolvedValue({
      id: 'ws-1',
      displayName: 'Workspace',
      migrationRepoName: 'acme/repo',
      migrationRepoPath: '/tmp/repo',
      sourceType: 'sql_server',
      sourceServer: 'localhost',
      sourceDatabase: 'master',
      sourcePort: 1433,
      sourceAuthenticationMode: 'sql_password',
      sourceUsername: 'sa',
      sourcePassword: 'secret',
      sourceEncrypt: true,
      sourceTrustServerCertificate: false,
    });
    tauriMocks.workspaceApplyStart.mockResolvedValue('job-1');
    tauriMocks.workspaceApplyStatus.mockResolvedValue({ state: 'succeeded', error: null });
    tauriMocks.migrationReconcileScopeState.mockResolvedValue({ kept: 1, invalidated: 0, removed: 0 });
  });

  it('renders selected table details panel', async () => {
    renderStep();
    await screen.findByText('fact_sales');
    expect(screen.getByText(/dbo.fact_sales/i)).toBeInTheDocument();
  });

  it('analyzes table on first load and supports analyze again', async () => {
    renderStep();
    await screen.findByText('fact_sales');

    await waitFor(() => {
      expect(tauriMocks.migrationAnalyzeTableDetails).toHaveBeenCalledWith({
        workspaceId: 'ws-1',
        selectedTableId: 'st:ws-1:wh-1:dbo:fact_sales',
        schemaName: 'dbo',
        tableName: 'fact_sales',
        force: false,
      });
    });

    fireEvent.click(screen.getByRole('button', { name: 'Analyze again' }));
    await waitFor(() => {
      expect(tauriMocks.migrationAnalyzeTableDetails).toHaveBeenCalledWith({
        workspaceId: 'ws-1',
        selectedTableId: 'st:ws-1:wh-1:dbo:fact_sales',
        schemaName: 'dbo',
        tableName: 'fact_sales',
        force: true,
      });
    });
  });

  it('autosaves when changing table type', async () => {
    renderStep();
    await screen.findByText('fact_sales');
    const tableTypeSelect = screen.getByRole('combobox', { name: /Table type/i });
    fireEvent.change(tableTypeSelect, { target: { value: 'fact' } });
    await waitFor(() => {
      expect(tauriMocks.migrationSaveTableConfig).toHaveBeenCalled();
    });
  });

  it('refresh schema runs apply + reconciliation', async () => {
    renderStep();
    await screen.findByText('fact_sales');
    fireEvent.click(screen.getByRole('button', { name: 'Refresh schema' }));
    await waitFor(() => {
      expect(tauriMocks.workspaceApplyStart).toHaveBeenCalled();
      expect(tauriMocks.migrationReconcileScopeState).toHaveBeenCalledWith('ws-1');
    });
  });
});
