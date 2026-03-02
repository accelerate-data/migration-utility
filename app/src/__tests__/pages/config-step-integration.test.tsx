/**
 * Component integration tests for ConfigStep (task 5.5)
 *
 * Tests cross-component interactions:
 * - Table dropdown ↔ detail panel (table selection)
 * - Header counts ↔ approval state
 * - Multi-table navigation
 */
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
  migrationApproveTableConfig: vi.fn(),
}));

vi.mock('@/lib/tauri', () => tauriMocks);

const BASE_WORKSPACE = {
  id: 'ws-1',
  displayName: 'W',
  migrationRepoName: 'a/b',
  migrationRepoPath: '/tmp',
  sourceType: 'sql_server',
  sourceServer: 'localhost',
  sourceDatabase: 'db',
  sourcePort: 1433,
  sourceAuthenticationMode: 'sql_password',
  sourceUsername: 'sa',
  sourcePassword: 'pw',
  sourceEncrypt: true,
  sourceTrustServerCertificate: false,
};

function makeConfig(selectedTableId: string, overrides: Record<string, unknown> = {}) {
  return {
    selectedTableId,
    tableType: 'fact',
    loadStrategy: 'incremental',
    grainColumns: null,
    relationshipsJson: null,
    incrementalColumn: null,
    dateColumn: null,
    snapshotStrategy: 'sample_1day',
    piiColumns: null,
    confirmedAt: null,
    analysisMetadataJson: null,
    approvalStatus: null,
    approvedAt: null,
    manualOverridesJson: null,
    ...overrides,
  };
}

function renderStep() {
  return render(
    <MemoryRouter initialEntries={['/scope/config']}>
      <ConfigStep />
    </MemoryRouter>,
  );
}

describe('ConfigStep — multi-table navigation', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    useWorkflowStore.setState((s) => ({
      ...s,
      workspaceId: 'ws-1',
      appPhase: 'scope_editable',
      phaseFacts: { ...s.phaseFacts, scopeFinalized: false },
    }));
    tauriMocks.migrationListScopeInventory.mockResolvedValue([
      { warehouseItemId: 'wh-1', schemaName: 'dbo', tableName: 'fact_sales', rowCount: 0, isSelected: true },
      { warehouseItemId: 'wh-2', schemaName: 'dbo', tableName: 'dim_customer', rowCount: 0, isSelected: true },
    ]);
    tauriMocks.migrationGetTableConfig.mockResolvedValue(null);
    tauriMocks.migrationAnalyzeTableDetails.mockImplementation(({ selectedTableId }: { selectedTableId: string }) =>
      Promise.resolve(makeConfig(selectedTableId)),
    );
    tauriMocks.migrationSaveTableConfig.mockResolvedValue(undefined);
    tauriMocks.appSetPhaseFlags.mockResolvedValue({ appPhase: 'scope_editable', hasGithubAuth: true, hasAnthropicKey: true, isSourceApplied: true, scopeFinalized: false, planFinalized: false });
    tauriMocks.workspaceGet.mockResolvedValue(BASE_WORKSPACE);
    tauriMocks.workspaceApplyStart.mockResolvedValue('job-1');
    tauriMocks.workspaceApplyStatus.mockResolvedValue({ state: 'succeeded', error: null });
    tauriMocks.migrationReconcileScopeState.mockResolvedValue({ kept: 2, invalidated: 0, removed: 0 });
  });

  it('shows both tables in dropdown and switches detail panel on selection', async () => {
    renderStep();
    await screen.findByText('Selected table');
    const selectedTable = screen.getAllByRole('combobox')[0];
    expect(screen.getByText(/dbo\.dim_customer -/i)).toBeInTheDocument();

    // Initially fact_sales is active — detail panel shows it
    await waitFor(() => {
      expect(screen.getAllByText(/dbo\.fact_sales/i).length).toBeGreaterThan(0);
    });

    // Select dim_customer in dropdown
    fireEvent.change(selectedTable, { target: { value: 'st:ws-1:wh-2:dbo:dim_customer' } });
    await waitFor(() => {
      expect(screen.getAllByText(/dbo\.dim_customer/i).length).toBeGreaterThan(0);
    });
  });

  it('header shows correct total count for multiple tables', async () => {
    renderStep();
    await screen.findByText('Selected table');
    await waitFor(() => {
      expect(screen.getByText(/0\s*\/\s*2 tables ready/i)).toBeInTheDocument();
    });
  });
});

describe('ConfigStep — header count ↔ approval state integration', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    useWorkflowStore.setState((s) => ({
      ...s,
      workspaceId: 'ws-1',
      appPhase: 'scope_editable',
      phaseFacts: { ...s.phaseFacts, scopeFinalized: false },
    }));
    tauriMocks.migrationListScopeInventory.mockResolvedValue([
      { warehouseItemId: 'wh-1', schemaName: 'dbo', tableName: 'fact_sales', rowCount: 0, isSelected: true },
    ]);
    tauriMocks.migrationSaveTableConfig.mockResolvedValue(undefined);
    tauriMocks.appSetPhaseFlags.mockResolvedValue({ appPhase: 'scope_editable', hasGithubAuth: true, hasAnthropicKey: true, isSourceApplied: true, scopeFinalized: false, planFinalized: false });
    tauriMocks.workspaceGet.mockResolvedValue(BASE_WORKSPACE);
    tauriMocks.workspaceApplyStart.mockResolvedValue('job-1');
    tauriMocks.workspaceApplyStatus.mockResolvedValue({ state: 'succeeded', error: null });
    tauriMocks.migrationReconcileScopeState.mockResolvedValue({ kept: 1, invalidated: 0, removed: 0 });
  });

  it('header approved count increments after approval', async () => {
    const sid = 'st:ws-1:wh-1:dbo:fact_sales';
    tauriMocks.migrationGetTableConfig.mockResolvedValue(
      makeConfig(sid, { confirmedAt: '2026-01-01T10:00:00Z', approvalStatus: 'pending' }),
    );
    tauriMocks.migrationAnalyzeTableDetails = vi.fn();
    tauriMocks.migrationApproveTableConfig.mockResolvedValue(undefined);
    // After approve, getTableConfig returns approved state
    tauriMocks.migrationGetTableConfig
      .mockResolvedValueOnce(makeConfig(sid, { confirmedAt: '2026-01-01T10:00:00Z', approvalStatus: 'pending' }))
      .mockResolvedValue(makeConfig(sid, { confirmedAt: '2026-01-01T10:00:00Z', approvalStatus: 'approved', approvedAt: '2026-01-15T10:00:00Z' }));

    renderStep();
    await screen.findByText('Selected table');

    // Initially 0 approved
    await waitFor(() => {
      expect(screen.getByText(/0\s+approved/i)).toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole('button', { name: 'Approve Configuration' }));

    await waitFor(() => {
      expect(screen.getByText(/1\s+approved/i)).toBeInTheDocument();
    });
  });

  it('shows approved badge after approval', async () => {
    const sid = 'st:ws-1:wh-1:dbo:fact_sales';
    tauriMocks.migrationGetTableConfig.mockResolvedValue(
      makeConfig(sid, { confirmedAt: '2026-01-01T10:00:00Z', approvalStatus: 'pending' }),
    );
    tauriMocks.migrationAnalyzeTableDetails = vi.fn();
    tauriMocks.migrationApproveTableConfig.mockResolvedValue(undefined);
    tauriMocks.migrationGetTableConfig
      .mockResolvedValueOnce(makeConfig(sid, { confirmedAt: '2026-01-01T10:00:00Z', approvalStatus: 'pending' }))
      .mockResolvedValue(makeConfig(sid, { confirmedAt: '2026-01-01T10:00:00Z', approvalStatus: 'approved', approvedAt: '2026-01-15T10:00:00Z' }));

    renderStep();
    await screen.findByText('Selected table');
    await waitFor(() => screen.getByRole('button', { name: 'Approve Configuration' }));

    fireEvent.click(screen.getByRole('button', { name: 'Approve Configuration' }));

    // After approval the detail panel shows ✓ Approved
    await waitFor(() => {
      expect(screen.getByText('✓ Approved')).toBeInTheDocument();
    });
  });
});

describe('ConfigStep — locked state integration', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    useWorkflowStore.setState((s) => ({
      ...s,
      workspaceId: 'ws-1',
      appPhase: 'scope_editable',
      phaseFacts: { ...s.phaseFacts, scopeFinalized: true },
    }));
    tauriMocks.migrationListScopeInventory.mockResolvedValue([
      { warehouseItemId: 'wh-1', schemaName: 'dbo', tableName: 'fact_sales', rowCount: 0, isSelected: true },
    ]);
    tauriMocks.migrationGetTableConfig.mockResolvedValue(
      makeConfig('st:ws-1:wh-1:dbo:fact_sales', { confirmedAt: '2026-01-01T10:00:00Z', approvalStatus: 'approved', approvedAt: '2026-01-15T10:00:00Z' }),
    );
    tauriMocks.migrationAnalyzeTableDetails = vi.fn();
    tauriMocks.migrationSaveTableConfig.mockResolvedValue(undefined);
    tauriMocks.appSetPhaseFlags.mockResolvedValue({ appPhase: 'plan_editable', hasGithubAuth: true, hasAnthropicKey: true, isSourceApplied: true, scopeFinalized: true, planFinalized: false });
    tauriMocks.workspaceGet.mockResolvedValue(BASE_WORKSPACE);
    tauriMocks.workspaceApplyStart.mockResolvedValue('job-1');
    tauriMocks.workspaceApplyStatus.mockResolvedValue({ state: 'succeeded', error: null });
    tauriMocks.migrationReconcileScopeState.mockResolvedValue({ kept: 1, invalidated: 0, removed: 0 });
  });

  it('disables form fields when scope is locked', async () => {
    renderStep();
    await screen.findByText('Selected table');
    await waitFor(() => {
      const tableTypeSelect = screen.getByRole('combobox', { name: /Table type/i });
      expect(tableTypeSelect).toBeDisabled();
    });
  });

  it('does not autosave when scope is locked', async () => {
    renderStep();
    await screen.findByText('Selected table');
    // Scope is locked — no save should be triggered
    await waitFor(() => {
      expect(tauriMocks.migrationSaveTableConfig).not.toHaveBeenCalled();
    });
  });
});
