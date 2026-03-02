import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MemoryRouter, Route, Routes } from 'react-router';
import ScopeStep from '@/routes/scope/scope-step';
import ConfigStep from '@/routes/scope/config-step';
import ScopeSurface from '@/routes/scope';
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
  appSetPhase: vi.fn(),
  migrationGetTableConfig: vi.fn(),
  migrationAnalyzeTableDetails: vi.fn(),
  migrationSaveTableConfig: vi.fn(),
}));

vi.mock('@/lib/tauri', () => tauriMocks);

function renderScopeSelect() {
  return render(
    <MemoryRouter initialEntries={['/scope']}>
      <Routes>
        <Route path="/scope" element={<ScopeStep />} />
      </Routes>
    </MemoryRouter>,
  );
}

function renderScopeDetails() {
  return render(
    <MemoryRouter initialEntries={['/scope/config']}>
      <Routes>
        <Route path="/scope/config" element={<ConfigStep />} />
      </Routes>
    </MemoryRouter>,
  );
}

function renderScopeSurface() {
  return render(
    <MemoryRouter initialEntries={['/scope']}>
      <Routes>
        <Route path="/scope/*" element={<ScopeSurface />} />
      </Routes>
    </MemoryRouter>,
  );
}

describe('Scope UI mockup contract', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    useWorkflowStore.setState((s) => ({
      ...s,
      workspaceId: 'ws-1',
      appPhase: 'scope_editable',

    }));

    tauriMocks.migrationListScopeInventory.mockResolvedValue([
      {
        warehouseItemId: 'wh-1',
        schemaName: 'dbo',
        tableName: 'fact_sales',
        rowCount: 12_400_000,
        isSelected: true,
      },
      {
        warehouseItemId: 'wh-1',
        schemaName: 'dbo',
        tableName: 'dim_customer',
        rowCount: 1_100_000,
        isSelected: false,
      },
      {
        warehouseItemId: 'wh-1',
        schemaName: 'reporting',
        tableName: 'gold_summary',
        rowCount: 420_000,
        isSelected: false,
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
    tauriMocks.appSetPhase.mockResolvedValue({
      appPhase: 'plan_editable',
      hasGithubAuth: true,
      hasAnthropicKey: true,
      isSourceApplied: true,
    });

    tauriMocks.migrationGetTableConfig.mockResolvedValue(null);
    tauriMocks.migrationAnalyzeTableDetails.mockResolvedValue({
      selectedTableId: 'st:ws-1:wh-1:dbo:fact_sales',
      tableType: 'unknown',
      loadStrategy: 'incremental',
      snapshotStrategy: 'sample_1day',
      incrementalColumn: '',
      dateColumn: '',
      grainColumns: [],
      relationshipsJson: [],
      piiColumns: [],
      confirmedAt: null,
    });
    tauriMocks.migrationSaveTableConfig.mockResolvedValue(undefined);
  });

  it('matches select-tables mockup contract for header, tab labels, filter ordering, and action labels', async () => {
    renderScopeSelect();
    await screen.findByText('fact_sales');

    expect(screen.getByText(/Select Tables for migration/i)).toBeInTheDocument();
    expect(screen.getByText(/\d+\s+selected/i)).toBeInTheDocument();
    expect(screen.getByText(/\d+\s*\/\s*\d+\s+tables ready/i)).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Refresh schema' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Finalize Scope' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: '1. Select Tables' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: '2. Table Details' })).toBeInTheDocument();

    const filters = screen.getAllByRole('textbox');
    expect(filters[0]).toHaveAttribute('placeholder', 'Search schema...');
    expect(filters[1]).toHaveAttribute('placeholder', 'Search tables...');

    const actionButtons = [
      screen.getByRole('button', { name: 'Add to selection' }),
      screen.getByRole('button', { name: 'Clear filters' }),
      screen.getByRole('button', { name: 'Reset selection' }),
    ];
    expect(actionButtons.map((b) => b.textContent)).toEqual([
      'Add to selection',
      'Clear filters',
      'Reset selection',
    ]);

    const sortButtons = screen.getAllByRole('button').map((b) => b.textContent ?? '');
    expect(sortButtons.some((text) => text.includes('Schema'))).toBe(true);
    expect(sortButtons.some((text) => text.includes('Table'))).toBe(true);
    expect(screen.getByText('Rows')).toBeInTheDocument();
  });

  it('renders scope surface without the legacy left steps rail', async () => {
    renderScopeSurface();
    await screen.findByText('fact_sales');
    expect(screen.queryByText('Steps')).not.toBeInTheDocument();
    expect(screen.queryByText('Candidacy Review')).not.toBeInTheDocument();
  });

  it('matches table-details contract for summary chips, tabs, grouped schema list, and detail field labels', async () => {
    renderScopeDetails();
    await screen.findAllByRole('combobox');

    expect(screen.getByText(/Select Tables for migration/i)).toBeInTheDocument();
    expect(screen.getByText(/\d+ \/ \d+ tables ready/i)).toBeInTheDocument();

    expect(screen.getByRole('button', { name: '1. Select Tables' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: '2. Table Details' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Refresh schema' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Finalize Scope' })).toBeInTheDocument();

    expect(screen.getAllByRole('combobox').length).toBeGreaterThan(0);
    expect(screen.queryByRole('radio')).not.toBeInTheDocument();

    expect(screen.getByText('Table type')).toBeInTheDocument();
    expect(screen.getByText('Load strategy')).toBeInTheDocument();
    expect(screen.getByText('CDC column')).toBeInTheDocument();
    expect(screen.getByText('Canonical date column')).toBeInTheDocument();
    expect(screen.getByText('PII columns (required for fixture masking)')).toBeInTheDocument();
    expect(screen.getByText('Keys and grain')).toBeInTheDocument();
    expect(screen.getByText('Relationships (required for tests)')).toBeInTheDocument();
    expect(screen.getByLabelText('SCD (dimensions only)')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Analyze again' })).toBeInTheDocument();
  });
});
