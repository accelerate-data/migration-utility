import { useEffect, useMemo, useState } from 'react';
import { useNavigate } from 'react-router';
import { Input } from '@/components/ui/input';
import { ConfigStepHeader } from '@/components/scope/config-step-header';
import { Button } from '@/components/ui/button';
import {
  appSetPhase,
  migrationAddTablesToSelection,
  migrationGetTableConfig,
  migrationListScopeInventory,
  migrationReconcileScopeState,
  migrationResetSelectedTables,
  migrationSetTableSelected,
  workspaceApplyStart,
  workspaceApplyStatus,
  workspaceGet,
} from '@/lib/tauri';
import { logger } from '@/lib/logger';
import type { ScopeInventoryRow, TableConfigPayload } from '@/lib/types';
import { useWorkflowStore } from '@/stores/workflow-store';

type SortKey = 'schema' | 'table';
type SortDirection = 'asc' | 'desc';

function keyForRow(row: ScopeInventoryRow): string {
  return `${row.warehouseItemId}::${row.schemaName}::${row.tableName}`;
}

function formatRowCount(value: number | null): string {
  if (value === null || value === undefined) return '--';
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M`;
  if (value >= 1_000) return `${Math.round(value / 1_000)}K`;
  return value.toLocaleString();
}

function selectedTableId(workspaceId: string, row: ScopeInventoryRow): string {
  return `st:${workspaceId}:${row.warehouseItemId}:${row.schemaName.toLowerCase()}:${row.tableName.toLowerCase()}`;
}

function isFilled(value: string | null): boolean {
  return value !== null && value.trim().length > 0;
}

function isFilledArray(value: unknown[] | null | undefined): boolean {
  return value != null && value.length > 0;
}

function isProvided(value: unknown[] | null | undefined): boolean {
  return value != null;
}

function isReady(config: TableConfigPayload | null | undefined): boolean {
  if (!config) return false;
  return (
    isFilled(config.tableType) &&
    isFilled(config.loadStrategy) &&
    isFilled(config.incrementalColumn) &&
    isFilled(config.dateColumn) &&
    isFilledArray(config.grainColumns) &&
    isProvided(config.relationshipsJson) &&
    isProvided(config.piiColumns)
  );
}

export default function ScopeStep() {
  const navigate = useNavigate();
  const { workspaceId, appPhase, setAppPhaseState } = useWorkflowStore();
  const isLocked = appPhase !== 'scope_editable';
  const [rows, setRows] = useState<ScopeInventoryRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [, setMessage] = useState<string>('Saved just now');
  const [error, setError] = useState<string | null>(null);
  const [schemaSearch, setSchemaSearch] = useState('');
  const [tableSearch, setTableSearch] = useState('');
  const [sortKey, setSortKey] = useState<SortKey>('schema');
  const [sortDirection, setSortDirection] = useState<SortDirection>('asc');
  const [refreshing, setRefreshing] = useState(false);
  const [readyCount, setReadyCount] = useState(0);

  async function loadInventory() {
    if (!workspaceId) {
      setRows([]);
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const data = await migrationListScopeInventory(workspaceId);
      setRows(data);
      const selected = data.filter((row) => row.isSelected);
      if (selected.length === 0) {
        setReadyCount(0);
      } else {
        const configs = await Promise.all(
          selected.map((row) => migrationGetTableConfig(selectedTableId(workspaceId, row))),
        );
        setReadyCount(configs.filter((config) => isReady(config)).length);
      }
    } catch (err) {
      logger.error('failed loading scope inventory', err);
      setError(err instanceof Error ? err.message : String(err));
      setReadyCount(0);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void loadInventory();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [workspaceId]);

  const selectedCount = rows.filter((row) => row.isSelected).length;

  const visibleRows = useMemo(() => {
    const schemaQuery = schemaSearch.trim().toLowerCase();
    const tableQuery = tableSearch.trim().toLowerCase();
    const filtered = rows.filter((row) => {
      const schemaMatches = !schemaQuery || row.schemaName.toLowerCase().includes(schemaQuery);
      const tableMatches = !tableQuery || row.tableName.toLowerCase().includes(tableQuery);
      return schemaMatches && tableMatches;
    });
    const dir = sortDirection === 'asc' ? 1 : -1;
    filtered.sort((a, b) => {
      const av = sortKey === 'schema' ? a.schemaName.toLowerCase() : a.tableName.toLowerCase();
      const bv = sortKey === 'schema' ? b.schemaName.toLowerCase() : b.tableName.toLowerCase();
      if (av < bv) return -1 * dir;
      if (av > bv) return 1 * dir;
      return 0;
    });
    return filtered;
  }, [rows, schemaSearch, tableSearch, sortKey, sortDirection]);

  function updateSort(next: SortKey) {
    if (sortKey === next) {
      setSortDirection((prev) => (prev === 'asc' ? 'desc' : 'asc'));
      return;
    }
    setSortKey(next);
    setSortDirection('asc');
  }

  async function addVisibleToSelection() {
    if (!workspaceId || isLocked) return;
    const toAdd = visibleRows
      .filter((row) => !row.isSelected)
      .map((row) => ({
        warehouseItemId: row.warehouseItemId,
        schemaName: row.schemaName,
        tableName: row.tableName,
      }));
    if (toAdd.length === 0) return;
    await migrationAddTablesToSelection(workspaceId, toAdd);
    setMessage(`Added ${toAdd.length} table(s) to selection`);
    await loadInventory();
  }

  async function resetSelection() {
    if (!workspaceId || isLocked) return;
    const removed = await migrationResetSelectedTables(workspaceId);
    setMessage(`Reset selection (${removed} removed)`);
    await loadInventory();
  }

  async function setSelected(row: ScopeInventoryRow, selected: boolean) {
    if (!workspaceId || isLocked) return;
    await migrationSetTableSelected(
      workspaceId,
      {
        warehouseItemId: row.warehouseItemId,
        schemaName: row.schemaName,
        tableName: row.tableName,
      },
      selected,
    );
    await loadInventory();
  }

  async function refreshSchema() {
    if (!workspaceId || isLocked || refreshing) return;
    setRefreshing(true);
    setError(null);
    try {
      const workspace = await workspaceGet();
      if (!workspace || !workspace.migrationRepoName) {
        throw new Error('Workspace is not configured for refresh');
      }
      const jobId = await workspaceApplyStart({
        name: workspace.displayName,
        migrationRepoName: workspace.migrationRepoName,
        migrationRepoPath: workspace.migrationRepoPath,
        sourceType: workspace.sourceType ?? 'sql_server',
        sourceServer: workspace.sourceServer ?? undefined,
        sourceDatabase: workspace.sourceDatabase ?? undefined,
        sourcePort: workspace.sourcePort ?? undefined,
        sourceAuthenticationMode: workspace.sourceAuthenticationMode ?? undefined,
        sourceUsername: workspace.sourceUsername ?? undefined,
        sourcePassword: workspace.sourcePassword ?? undefined,
        sourceEncrypt: workspace.sourceEncrypt ?? undefined,
        sourceTrustServerCertificate: workspace.sourceTrustServerCertificate ?? undefined,
      });

      for (let i = 0; i < 120; i += 1) {
        const status = await workspaceApplyStatus(jobId);
        if (status.state === 'running') {
          await new Promise((resolve) => setTimeout(resolve, 500));
          continue;
        }
        if (status.state !== 'succeeded') {
          throw new Error(status.error || 'Schema refresh failed');
        }
        break;
      }

      const summary = await migrationReconcileScopeState(workspaceId);
      setMessage(
        `Schema refreshed just now · kept ${summary.kept} · invalidated ${summary.invalidated} · removed ${summary.removed}`,
      );
      await loadInventory();
    } catch (err) {
      logger.error('scope refresh failed', err);
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setRefreshing(false);
    }
  }

  async function finalizeScope() {
    if (isLocked) return;
    logger.info('scope: finalizing scope');
    try {
      const phase = await appSetPhase('plan_editable');
      setAppPhaseState(phase);
      setMessage('Scope finalized just now');
      logger.info('scope: finalize scope succeeded');
    } catch (err) {
      logger.error('finalize scope failed', err);
      setError(err instanceof Error ? err.message : String(err));
    }
  }

  return (
    <section className="flex h-full min-h-0 flex-col gap-4" data-testid="scope-select-step">
      <div className="bg-background pb-4">
        <ConfigStepHeader
          selectedCount={selectedCount}
          readyCount={readyCount}
          totalCount={selectedCount}
          activeStep="select"
          isLocked={isLocked}
          refreshing={refreshing}
          anyAnalyzing={false}
          onRefreshSchema={() => void refreshSchema()}
          onFinalizeScope={() => void finalizeScope()}
          onNavigateToSelect={() => navigate('/scope')}
          onNavigateToConfig={() => navigate('/scope/config')}
        />
      </div>

      <div className="flex min-h-0 flex-1 flex-col rounded-md border bg-card">
        <div className="flex flex-wrap items-center justify-between gap-3 border-b p-3">
          <div className="flex flex-wrap items-center gap-2">
            <Input
              placeholder="Search schema..."
              value={schemaSearch}
              onChange={(e) => setSchemaSearch(e.target.value)}
              disabled={isLocked}
              className="h-8 w-[180px]"
            />
            <Input
              placeholder="Search tables..."
              value={tableSearch}
              onChange={(e) => setTableSearch(e.target.value)}
              disabled={isLocked}
              className="h-8 w-[220px]"
            />
          </div>
          <div className="flex items-center gap-2">
            <Button type="button" size="sm" onClick={() => void addVisibleToSelection()} disabled={isLocked}>
              Add to selection
            </Button>
            <Button type="button" variant="outline" size="sm" onClick={() => { setSchemaSearch(''); setTableSearch(''); }}>
              Clear filters
            </Button>
            <Button type="button" variant="ghost" size="sm" onClick={() => void resetSelection()} disabled={isLocked}>
              Reset selection
            </Button>
          </div>
        </div>

        <div className="min-h-0 flex-1 overflow-auto">
          {loading && <p className="p-3 text-sm text-muted-foreground">Loading tables...</p>}
          {!loading && error && <p className="p-3 text-sm text-destructive">{error}</p>}
          {!loading && !error && visibleRows.length === 0 && (
            <p className="p-3 text-sm text-muted-foreground">No tables match current filters.</p>
          )}
          {!loading && !error && visibleRows.length > 0 && (
            <table className="w-full table-fixed border-collapse">
              <colgroup>
                <col className="w-9" />
                <col className="w-36" />
                <col />
                <col className="w-28" />
              </colgroup>
              <thead className="sticky top-0 z-10 bg-card">
                <tr className="border-y text-xs font-medium text-muted-foreground">
                  <th className="px-3 py-2 text-left" />
                  <th className="px-3 py-2 text-left">
                    <button type="button" className="text-left" onClick={() => updateSort('schema')}>
                      Schema {sortKey === 'schema' ? (sortDirection === 'asc' ? '↑' : '↓') : '↕'}
                    </button>
                  </th>
                  <th className="px-3 py-2 text-left">
                    <button type="button" className="text-left" onClick={() => updateSort('table')}>
                      Table {sortKey === 'table' ? (sortDirection === 'asc' ? '↑' : '↓') : '↕'}
                    </button>
                  </th>
                  <th className="px-3 py-2 text-left">Rows</th>
                </tr>
              </thead>
              <tbody>
                {visibleRows.map((row) => (
                  <tr key={keyForRow(row)} className="border-b text-sm">
                    <td className="px-3 py-2 align-middle">
                      <input
                        type="checkbox"
                        checked={row.isSelected}
                        disabled={isLocked}
                        onChange={(e) => void setSelected(row, e.target.checked)}
                      />
                    </td>
                    <td className="px-3 py-2 font-mono text-muted-foreground">{row.schemaName}</td>
                    <td className="px-3 py-2 font-mono">
                      <span className="block truncate">{row.tableName}</span>
                    </td>
                    <td className="px-3 py-2 font-mono text-muted-foreground">{formatRowCount(row.rowCount)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>
    </section>
  );
}
