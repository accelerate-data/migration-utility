import { describe, it, expect, vi, beforeEach } from 'vitest';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router';
import ScopeStep from '@/routes/scope/scope-step';
import { useWorkflowStore } from '@/stores/workflow-store';

const tauriMocks = vi.hoisted(() => ({
  migrationListScopeInventory: vi.fn(),
  migrationAddTablesToSelection: vi.fn(),
  migrationSetTableSelected: vi.fn(),
  migrationResetSelectedTables: vi.fn(),
  workspaceGet: vi.fn(),
  workspaceApplyStart: vi.fn(),
  workspaceApplyStatus: vi.fn(),
  migrationReconcileScopeState: vi.fn(),
  appSetPhaseFlags: vi.fn(),
}));

vi.mock('@/lib/tauri', () => tauriMocks);

function renderStep() {
  return render(
    <MemoryRouter initialEntries={['/scope']}>
      <ScopeStep />
    </MemoryRouter>,
  );
}

describe('ScopeStep', () => {
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
        isSelected: false,
      },
      {
        warehouseItemId: 'wh-1',
        schemaName: 'dbo',
        tableName: 'dim_customer',
        rowCount: 810_000,
        isSelected: true,
      },
    ]);
    tauriMocks.migrationAddTablesToSelection.mockResolvedValue(1);
    tauriMocks.migrationSetTableSelected.mockResolvedValue(undefined);
    tauriMocks.migrationResetSelectedTables.mockResolvedValue(1);
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
    tauriMocks.appSetPhaseFlags.mockResolvedValue({
      appPhase: 'plan_editable',
      hasGithubAuth: true,
      hasAnthropicKey: true,
      isSourceApplied: true,
      scopeFinalized: true,
      planFinalized: false,
    });
  });

  it('renders scope inventory and selected count', async () => {
    renderStep();
    await screen.findByText('fact_sales');
    expect(screen.getByText('1 tables selected')).toBeInTheDocument();
    expect(screen.getByText('1.3M')).toBeInTheDocument();
    expect(screen.getByText('810K')).toBeInTheDocument();
  });

  it('adds visible rows to selection', async () => {
    renderStep();
    await screen.findByText('fact_sales');
    fireEvent.click(screen.getByRole('button', { name: 'Add to selection' }));
    await waitFor(() => {
      expect(tauriMocks.migrationAddTablesToSelection).toHaveBeenCalled();
    });
  });

  it('refresh schema triggers apply flow and reconciliation', async () => {
    renderStep();
    await screen.findByText('fact_sales');
    fireEvent.click(screen.getByTestId('scope-refresh-schema'));
    await waitFor(() => {
      expect(tauriMocks.workspaceApplyStart).toHaveBeenCalled();
      expect(tauriMocks.migrationReconcileScopeState).toHaveBeenCalledWith('ws-1');
    });
  });
});
