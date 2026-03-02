import { describe, it, expect, vi, beforeEach } from 'vitest';
import { fireEvent, render, screen, waitFor, act } from '@testing-library/react';
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
      grainColumns: [],
      relationshipsJson: [],
      incrementalColumn: '',
      dateColumn: '',
      snapshotStrategy: 'sample_1day',
      piiColumns: [],
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
    await screen.findAllByRole('combobox');
    expect(screen.getAllByText(/dbo\.fact_sales/i).length).toBeGreaterThan(0);
  });

  it('analyzes table on first load and supports analyze again', async () => {
    renderStep();
    await screen.findAllByRole('combobox');

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
    await screen.findAllByRole('combobox');
    const tableTypeSelect = screen.getByRole('combobox', { name: /Table type/i });
    fireEvent.change(tableTypeSelect, { target: { value: 'fact' } });
    await waitFor(() => {
      expect(tauriMocks.migrationSaveTableConfig).toHaveBeenCalled();
    });
  });

  it('refresh schema runs apply + reconciliation', async () => {
    renderStep();
    await screen.findAllByRole('combobox');
    fireEvent.click(screen.getByRole('button', { name: 'Refresh schema' }));
    await waitFor(() => {
      expect(tauriMocks.workspaceApplyStart).toHaveBeenCalled();
      expect(tauriMocks.migrationReconcileScopeState).toHaveBeenCalledWith('ws-1');
    });
  });
});

// ── Manual override tracking (7.1.3) ────────────────────────────────────────

describe('ConfigStep — manual override tracking', () => {
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
    tauriMocks.migrationGetTableConfig.mockResolvedValue(null);
    tauriMocks.migrationAnalyzeTableDetails.mockResolvedValue({
      selectedTableId: 'st:ws-1:wh-1:dbo:fact_sales',
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
    });
    tauriMocks.migrationSaveTableConfig.mockResolvedValue(undefined);
    tauriMocks.appSetPhaseFlags.mockResolvedValue({ appPhase: 'scope_editable', hasGithubAuth: true, hasAnthropicKey: true, isSourceApplied: true, scopeFinalized: false, planFinalized: false });
    tauriMocks.workspaceGet.mockResolvedValue({ id: 'ws-1', displayName: 'W', migrationRepoName: 'a/b', migrationRepoPath: '/tmp', sourceType: 'sql_server', sourceServer: 'localhost', sourceDatabase: 'db', sourcePort: 1433, sourceAuthenticationMode: 'sql_password', sourceUsername: 'sa', sourcePassword: 'pw', sourceEncrypt: true, sourceTrustServerCertificate: false });
    tauriMocks.workspaceApplyStart.mockResolvedValue('job-1');
    tauriMocks.workspaceApplyStatus.mockResolvedValue({ state: 'succeeded', error: null });
    tauriMocks.migrationReconcileScopeState.mockResolvedValue({ kept: 1, invalidated: 0, removed: 0 });
  });

  it('saves manualOverridesJson when a field is changed', async () => {
    render(<MemoryRouter initialEntries={['/scope/config']}><ConfigStep /></MemoryRouter>);
    await screen.findAllByRole('combobox');

    const tableTypeSelect = screen.getByRole('combobox', { name: /Table type/i });
    fireEvent.change(tableTypeSelect, { target: { value: 'dimension' } });

    await waitFor(() => {
      const call = tauriMocks.migrationSaveTableConfig.mock.calls.at(-1)?.[0];
      expect(call).toBeDefined();
      const overrides = JSON.parse(call.manualOverridesJson ?? '[]');
      expect(overrides).toContain('tableType');
    });
  });

  it('accumulates multiple field overrides', async () => {
    render(<MemoryRouter initialEntries={['/scope/config']}><ConfigStep /></MemoryRouter>);
    await screen.findAllByRole('combobox');

    const tableTypeSelect = screen.getByRole('combobox', { name: /Table type/i });
    fireEvent.change(tableTypeSelect, { target: { value: 'dimension' } });

    await waitFor(() => expect(tableTypeSelect).toHaveValue('dimension'));
    await waitFor(() => {
      const call = tauriMocks.migrationSaveTableConfig.mock.calls.at(-1)?.[0];
      const overrides = JSON.parse(call.manualOverridesJson ?? '[]');
      expect(overrides).toContain('tableType');
    });
    tauriMocks.migrationSaveTableConfig.mockClear();

    const loadStrategySelect = screen.getByRole('combobox', { name: /Load strategy/i });
    fireEvent.change(loadStrategySelect, { target: { value: 'full_refresh' } });

    await waitFor(() => {
      const call = tauriMocks.migrationSaveTableConfig.mock.calls.at(-1)?.[0];
      const overrides = JSON.parse(call.manualOverridesJson ?? '[]');
      expect(overrides).toContain('tableType');
      expect(overrides).toContain('loadStrategy');
    });
  });
});

// ── Manual override persistence (7.2.3) ─────────────────────────────────────

describe('ConfigStep — manual override persistence', () => {
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
    // Return a config that already has manualOverridesJson set
    tauriMocks.migrationGetTableConfig.mockResolvedValue({
      selectedTableId: 'st:ws-1:wh-1:dbo:fact_sales',
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
      manualOverridesJson: JSON.stringify(['tableType', 'loadStrategy']),
    });
    tauriMocks.migrationSaveTableConfig.mockResolvedValue(undefined);
    tauriMocks.appSetPhaseFlags.mockResolvedValue({ appPhase: 'scope_editable', hasGithubAuth: true, hasAnthropicKey: true, isSourceApplied: true, scopeFinalized: false, planFinalized: false });
    tauriMocks.workspaceGet.mockResolvedValue({ id: 'ws-1', displayName: 'W', migrationRepoName: 'a/b', migrationRepoPath: '/tmp', sourceType: 'sql_server', sourceServer: 'localhost', sourceDatabase: 'db', sourcePort: 1433, sourceAuthenticationMode: 'sql_password', sourceUsername: 'sa', sourcePassword: 'pw', sourceEncrypt: true, sourceTrustServerCertificate: false });
    tauriMocks.workspaceApplyStart.mockResolvedValue('job-1');
    tauriMocks.workspaceApplyStatus.mockResolvedValue({ state: 'succeeded', error: null });
    tauriMocks.migrationReconcileScopeState.mockResolvedValue({ kept: 1, invalidated: 0, removed: 0 });
  });

  it('deserializes manualOverridesJson on load and shows Manual chips', async () => {
    render(<MemoryRouter initialEntries={['/scope/config']}><ConfigStep /></MemoryRouter>);
    await screen.findAllByRole('combobox');
    // Both tableType and loadStrategy are in manualOverrides → both show Manual chip
    await waitFor(() => {
      const manualChips = screen.getAllByText('Manual');
      expect(manualChips.length).toBeGreaterThanOrEqual(2);
    });
  });
});

// ── Agent/Manual chip display (7.3.4) ────────────────────────────────────────

describe('ConfigStep — Agent/Manual chip display', () => {
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
    tauriMocks.workspaceGet.mockResolvedValue({ id: 'ws-1', displayName: 'W', migrationRepoName: 'a/b', migrationRepoPath: '/tmp', sourceType: 'sql_server', sourceServer: 'localhost', sourceDatabase: 'db', sourcePort: 1433, sourceAuthenticationMode: 'sql_password', sourceUsername: 'sa', sourcePassword: 'pw', sourceEncrypt: true, sourceTrustServerCertificate: false });
    tauriMocks.workspaceApplyStart.mockResolvedValue('job-1');
    tauriMocks.workspaceApplyStatus.mockResolvedValue({ state: 'succeeded', error: null });
    tauriMocks.migrationReconcileScopeState.mockResolvedValue({ kept: 1, invalidated: 0, removed: 0 });
  });

  it('shows Agent chip for agent-suggested fields with no manual override', async () => {
    tauriMocks.migrationGetTableConfig.mockResolvedValue({
      selectedTableId: 'st:ws-1:wh-1:dbo:fact_sales',
      tableType: 'fact',
      loadStrategy: 'incremental',
      grainColumns: null, relationshipsJson: null, incrementalColumn: null, dateColumn: null,
      snapshotStrategy: 'sample_1day', piiColumns: null, confirmedAt: null,
      analysisMetadataJson: null, approvalStatus: null, approvedAt: null,
      manualOverridesJson: '[]',
    });
    render(<MemoryRouter initialEntries={['/scope/config']}><ConfigStep /></MemoryRouter>);
    await screen.findAllByRole('combobox');
    await waitFor(() => {
      expect(screen.getAllByText('Agent').length).toBeGreaterThanOrEqual(1);
    });
  });

  it('shows Manual chip after user edits a field', async () => {
    tauriMocks.migrationGetTableConfig.mockResolvedValue(null);
    tauriMocks.migrationAnalyzeTableDetails.mockResolvedValue({
      selectedTableId: 'st:ws-1:wh-1:dbo:fact_sales',
      tableType: 'fact', loadStrategy: 'incremental', grainColumns: null,
      relationshipsJson: null, incrementalColumn: null, dateColumn: null,
      snapshotStrategy: 'sample_1day', piiColumns: null, confirmedAt: null,
      analysisMetadataJson: null, approvalStatus: null, approvedAt: null, manualOverridesJson: null,
    });
    render(<MemoryRouter initialEntries={['/scope/config']}><ConfigStep /></MemoryRouter>);
    await screen.findAllByRole('combobox');

    const tableTypeSelect = screen.getByRole('combobox', { name: /Table type/i });
    await act(async () => { fireEvent.change(tableTypeSelect, { target: { value: 'dimension' } }); });

    await waitFor(() => {
      expect(screen.getAllByText('Manual').length).toBeGreaterThanOrEqual(1);
    });
  });
});

// ── Approval logic (8.1.5) ───────────────────────────────────────────────────

describe('ConfigStep — approval logic', () => {
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
    tauriMocks.migrationGetTableConfig.mockResolvedValue({
      selectedTableId: 'st:ws-1:wh-1:dbo:fact_sales',
      tableType: 'fact', loadStrategy: 'incremental', grainColumns: null,
      relationshipsJson: null, incrementalColumn: null, dateColumn: null,
      snapshotStrategy: 'sample_1day', piiColumns: null,
      confirmedAt: '2026-01-01T10:00:00Z',
      analysisMetadataJson: null, approvalStatus: 'pending', approvedAt: null,
      manualOverridesJson: null,
    });
    tauriMocks.migrationSaveTableConfig.mockResolvedValue(undefined);
    tauriMocks.appSetPhaseFlags.mockResolvedValue({ appPhase: 'scope_editable', hasGithubAuth: true, hasAnthropicKey: true, isSourceApplied: true, scopeFinalized: false, planFinalized: false });
    tauriMocks.workspaceGet.mockResolvedValue({ id: 'ws-1', displayName: 'W', migrationRepoName: 'a/b', migrationRepoPath: '/tmp', sourceType: 'sql_server', sourceServer: 'localhost', sourceDatabase: 'db', sourcePort: 1433, sourceAuthenticationMode: 'sql_password', sourceUsername: 'sa', sourcePassword: 'pw', sourceEncrypt: true, sourceTrustServerCertificate: false });
    tauriMocks.workspaceApplyStart.mockResolvedValue('job-1');
    tauriMocks.workspaceApplyStatus.mockResolvedValue({ state: 'succeeded', error: null });
    tauriMocks.migrationReconcileScopeState.mockResolvedValue({ kept: 1, invalidated: 0, removed: 0 });
  });

  it('shows Approve Configuration button when confirmedAt is set', async () => {
    tauriMocks.migrationAnalyzeTableDetails = vi.fn(); // prevent auto-analyze
    render(<MemoryRouter initialEntries={['/scope/config']}><ConfigStep /></MemoryRouter>);
    await screen.findAllByRole('combobox');
    await waitFor(() => {
      expect(screen.getByRole('button', { name: 'Approve Configuration' })).toBeInTheDocument();
    });
  });

  it('calls migrationApproveTableConfig when approve button clicked', async () => {
    const approveMock = vi.fn().mockResolvedValue(undefined);
    tauriMocks.migrationAnalyzeTableDetails = vi.fn();
    Object.assign(tauriMocks, { migrationApproveTableConfig: approveMock });

    render(<MemoryRouter initialEntries={['/scope/config']}><ConfigStep /></MemoryRouter>);
    await screen.findAllByRole('combobox');
    await waitFor(() => screen.getByRole('button', { name: 'Approve Configuration' }));

    fireEvent.click(screen.getByRole('button', { name: 'Approve Configuration' }));
    await waitFor(() => {
      expect(approveMock).toHaveBeenCalledWith('st:ws-1:wh-1:dbo:fact_sales');
    });
  });
});

// ── Approval UI state (8.3.4) ────────────────────────────────────────────────

describe('ConfigStep — approval UI state', () => {
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
    tauriMocks.workspaceGet.mockResolvedValue({ id: 'ws-1', displayName: 'W', migrationRepoName: 'a/b', migrationRepoPath: '/tmp', sourceType: 'sql_server', sourceServer: 'localhost', sourceDatabase: 'db', sourcePort: 1433, sourceAuthenticationMode: 'sql_password', sourceUsername: 'sa', sourcePassword: 'pw', sourceEncrypt: true, sourceTrustServerCertificate: false });
    tauriMocks.workspaceApplyStart.mockResolvedValue('job-1');
    tauriMocks.workspaceApplyStatus.mockResolvedValue({ state: 'succeeded', error: null });
    tauriMocks.migrationReconcileScopeState.mockResolvedValue({ kept: 1, invalidated: 0, removed: 0 });
  });

  it('shows approved badge when approvalStatus is approved', async () => {
    tauriMocks.migrationGetTableConfig.mockResolvedValue({
      selectedTableId: 'st:ws-1:wh-1:dbo:fact_sales',
      tableType: 'fact', loadStrategy: 'incremental', grainColumns: null,
      relationshipsJson: null, incrementalColumn: null, dateColumn: null,
      snapshotStrategy: 'sample_1day', piiColumns: null,
      confirmedAt: '2026-01-01T10:00:00Z',
      analysisMetadataJson: null,
      approvalStatus: 'approved',
      approvedAt: '2026-01-15T10:30:00Z',
      manualOverridesJson: null,
    });
    tauriMocks.migrationAnalyzeTableDetails = vi.fn();
    render(<MemoryRouter initialEntries={['/scope/config']}><ConfigStep /></MemoryRouter>);
    await screen.findAllByRole('combobox');
    await waitFor(() => {
      expect(screen.getByText('✓ Approved')).toBeInTheDocument();
    });
  });

  it('shows pending state when approvalStatus is pending', async () => {
    tauriMocks.migrationGetTableConfig.mockResolvedValue({
      selectedTableId: 'st:ws-1:wh-1:dbo:fact_sales',
      tableType: 'fact', loadStrategy: 'incremental', grainColumns: null,
      relationshipsJson: null, incrementalColumn: null, dateColumn: null,
      snapshotStrategy: 'sample_1day', piiColumns: null,
      confirmedAt: '2026-01-01T10:00:00Z',
      analysisMetadataJson: null, approvalStatus: 'pending', approvedAt: null,
      manualOverridesJson: null,
    });
    tauriMocks.migrationAnalyzeTableDetails = vi.fn();
    render(<MemoryRouter initialEntries={['/scope/config']}><ConfigStep /></MemoryRouter>);
    await screen.findAllByRole('combobox');
    await waitFor(() => {
      expect(screen.getByRole('button', { name: 'Approve Configuration' })).toBeInTheDocument();
    });
  });

  it('does not show approval panel when confirmedAt is null', async () => {
    tauriMocks.migrationGetTableConfig.mockResolvedValue({
      selectedTableId: 'st:ws-1:wh-1:dbo:fact_sales',
      tableType: 'fact', loadStrategy: 'incremental', grainColumns: null,
      relationshipsJson: null, incrementalColumn: null, dateColumn: null,
      snapshotStrategy: 'sample_1day', piiColumns: null,
      confirmedAt: null,
      analysisMetadataJson: null, approvalStatus: null, approvedAt: null,
      manualOverridesJson: null,
    });
    tauriMocks.migrationAnalyzeTableDetails = vi.fn();
    render(<MemoryRouter initialEntries={['/scope/config']}><ConfigStep /></MemoryRouter>);
    await screen.findAllByRole('combobox');
    await waitFor(() => {
      expect(screen.getByRole('button', { name: 'Approve Configuration' })).toBeDisabled();
    });
  });
});
