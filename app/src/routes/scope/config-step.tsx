import { useEffect, useMemo, useRef, useState } from 'react';
import { useNavigate } from 'react-router';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import {
  appSetPhaseFlags,
  migrationListTableDetails,
  migrationSaveTableConfig,
  workspaceApplyStart,
  workspaceApplyStatus,
  workspaceGet,
  migrationReconcileScopeState,
} from '@/lib/tauri';
import { logger } from '@/lib/logger';
import type { TableDetailRow, TableConfigPayload } from '@/lib/types';
import { useWorkflowStore } from '@/stores/workflow-store';

function toPayload(detail: TableDetailRow): TableConfigPayload {
  return {
    selectedTableId: detail.selectedTableId,
    tableType: detail.tableType,
    loadStrategy: detail.loadStrategy,
    grainColumns: detail.grainColumns,
    relationshipsJson: detail.relationshipsJson,
    incrementalColumn: detail.incrementalColumn,
    dateColumn: detail.dateColumn,
    snapshotStrategy: detail.snapshotStrategy || 'sample_1day',
    piiColumns: detail.piiColumns,
    confirmedAt: detail.confirmedAt,
  };
}

export default function ConfigStep() {
  const navigate = useNavigate();
  const { workspaceId, appPhase, phaseFacts, setAppPhaseState } = useWorkflowStore();
  const isLocked = phaseFacts.scopeFinalized || appPhase === 'running_locked';
  const [rows, setRows] = useState<TableDetailRow[]>([]);
  const [activeId, setActiveId] = useState<string | null>(null);
  const [draft, setDraft] = useState<TableDetailRow | null>(null);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [message, setMessage] = useState('Saved just now');
  const [refreshing, setRefreshing] = useState(false);
  const autosaveTimerRef = useRef<number | null>(null);

  async function loadDetails(preferredId?: string | null) {
    if (!workspaceId) {
      setRows([]);
      setActiveId(null);
      setDraft(null);
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const details = await migrationListTableDetails(workspaceId);
      setRows(details);
      const nextActiveId = preferredId && details.some((d) => d.selectedTableId === preferredId)
        ? preferredId
        : details[0]?.selectedTableId ?? null;
      setActiveId(nextActiveId);
      setDraft(details.find((d) => d.selectedTableId === nextActiveId) ?? null);
    } catch (err) {
      logger.error('failed to load table details', err);
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void loadDetails(activeId);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [workspaceId]);

  useEffect(
    () => () => {
      if (autosaveTimerRef.current !== null) {
        window.clearTimeout(autosaveTimerRef.current);
      }
    },
    [],
  );

  const grouped = useMemo(() => {
    const map = new Map<string, TableDetailRow[]>();
    for (const row of rows) {
      const current = map.get(row.schemaName) ?? [];
      current.push(row);
      map.set(row.schemaName, current);
    }
    return Array.from(map.entries()).sort(([a], [b]) => a.localeCompare(b));
  }, [rows]);
  const readyCount = rows.filter((r) => r.status === 'Ready').length;
  const needsDetails = rows.length - readyCount;

  async function persist(next: TableDetailRow) {
    setSaving(true);
    setError(null);
    try {
      await migrationSaveTableConfig(toPayload(next));
      setMessage('Saved just now');
      await loadDetails(next.selectedTableId);
    } catch (err) {
      logger.error('failed to save table config', err);
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSaving(false);
    }
  }

  function updateDraft<K extends keyof TableDetailRow>(key: K, value: TableDetailRow[K]) {
    if (!draft || isLocked) return;
    const next = { ...draft, [key]: value };
    setDraft(next);
    if (autosaveTimerRef.current !== null) {
      window.clearTimeout(autosaveTimerRef.current);
    }
    autosaveTimerRef.current = window.setTimeout(() => {
      void persist(next);
    }, 500);
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
      await loadDetails(activeId);
    } catch (err) {
      logger.error('scope details refresh failed', err);
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setRefreshing(false);
    }
  }

  async function finalizeScope() {
    if (isLocked) return;
    try {
      const phase = await appSetPhaseFlags({ scopeFinalized: true });
      setAppPhaseState(phase);
      setMessage('Scope finalized just now');
    } catch (err) {
      logger.error('finalize scope failed', err);
      setError(err instanceof Error ? err.message : String(err));
    }
  }

  return (
    <section className="space-y-4" data-testid="scope-table-details-step">
      <header className="rounded-md border bg-card p-4">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div className="space-y-1">
            <p className="text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">
              Scope — Table details capture
            </p>
            <div className="flex flex-wrap items-center gap-2">
              <span className="rounded-full bg-primary/20 px-2 py-0.5 text-xs font-medium text-primary">
                {readyCount} / {rows.length} tables ready
              </span>
              <span className="text-xs text-muted-foreground">Needs details for {needsDetails} tables</span>
              <span className="text-xs text-muted-foreground">{message}</span>
              <span className="text-xs text-muted-foreground">
                {isLocked ? 'Scope finalized (read-only)' : 'Scope editable'}
              </span>
            </div>
          </div>
          <div className="flex items-center gap-2">
            <Button
              type="button"
              variant="outline"
              size="sm"
              disabled={isLocked || refreshing}
              onClick={() => void refreshSchema()}
            >
              {refreshing ? 'Refreshing...' : 'Refresh schema'}
            </Button>
            <Button type="button" size="sm" disabled={isLocked} onClick={() => void finalizeScope()}>
              {isLocked ? 'Scope Finalized' : 'Finalize Scope'}
            </Button>
          </div>
        </div>
        <div className="mt-4 border-b border-border">
          <div className="flex items-center gap-6">
            <button
              type="button"
              className="border-b-2 border-transparent pb-2 text-sm font-medium text-muted-foreground"
              onClick={() => navigate('/scope')}
            >
              1. Select Tables
            </button>
            <button
              type="button"
              className="border-b-2 border-primary pb-2 text-sm font-medium text-primary"
              onClick={() => navigate('/scope/config')}
            >
              2. Table Details
            </button>
          </div>
        </div>
      </header>

      <div className="grid gap-4 lg:grid-cols-[40%_60%]">
        <div className="rounded-md border bg-card">
          <div className="max-h-[560px] overflow-auto">
            {loading && <p className="p-3 text-sm text-muted-foreground">Loading details...</p>}
            {!loading && rows.length === 0 && (
              <p className="p-3 text-sm text-muted-foreground">No selected tables yet.</p>
            )}
            {!loading &&
              grouped.map(([schema, schemaRows]) => (
                <details key={schema} open className="border-b">
                  <summary className="flex cursor-pointer items-center justify-between bg-muted/50 px-3 py-2 text-xs">
                    <span className="font-medium">{schema}</span>
                    <span className="text-muted-foreground">{schemaRows.length} selected</span>
                  </summary>
                  {schemaRows.map((row) => (
                    <button
                      key={row.selectedTableId}
                      type="button"
                      className={`w-full border-t px-3 py-2 text-left text-sm ${
                        row.selectedTableId === activeId ? 'bg-primary/10' : ''
                      }`}
                      onClick={() => {
                        setActiveId(row.selectedTableId);
                        setDraft(row);
                      }}
                    >
                      <span className="font-mono">{row.tableName}</span>
                    </button>
                  ))}
                </details>
              ))}
          </div>
        </div>

        <div className="rounded-md border bg-card p-4 lg:pr-8">
          {!draft && <p className="text-sm text-muted-foreground">Select a table to edit details.</p>}
          {draft && (
            <div className="space-y-4">
              <div>
                <p className="font-mono text-sm font-semibold">
                  {draft.schemaName}.{draft.tableName}
                </p>
                <p className="text-xs text-muted-foreground">Migration metadata required for build and tests.</p>
              </div>

              <div className="grid gap-3 md:grid-cols-2">
                <label className="space-y-1 text-sm">
                  <span>Table type</span>
                  <select
                    className="h-9 w-full rounded-md border bg-background px-3 text-sm"
                    value={draft.tableType ?? ''}
                    disabled={isLocked}
                    onChange={(e) => updateDraft('tableType', e.target.value || null)}
                  >
                    <option value="">Select...</option>
                    <option value="fact">fact</option>
                    <option value="dimension">dimension</option>
                    <option value="unknown">unknown</option>
                  </select>
                </label>
                <label className="space-y-1 text-sm">
                  <span>Load strategy</span>
                  <select
                    className="h-9 w-full rounded-md border bg-background px-3 text-sm"
                    value={draft.loadStrategy ?? ''}
                    disabled={isLocked}
                    onChange={(e) => updateDraft('loadStrategy', e.target.value || null)}
                  >
                    <option value="">Select...</option>
                    <option value="incremental">incremental</option>
                    <option value="full_refresh">full_refresh</option>
                    <option value="snapshot">snapshot</option>
                  </select>
                </label>
                <label className="space-y-1 text-sm">
                  <span>CDC column</span>
                  <Input
                    value={draft.incrementalColumn ?? ''}
                    disabled={isLocked}
                    onChange={(e) => updateDraft('incrementalColumn', e.target.value || null)}
                  />
                </label>
                <label className="space-y-1 text-sm">
                  <span>Canonical date column</span>
                  <Input
                    value={draft.dateColumn ?? ''}
                    disabled={isLocked}
                    onChange={(e) => updateDraft('dateColumn', e.target.value || null)}
                  />
                </label>
                <label className="space-y-1 text-sm md:col-span-2">
                  <span>PII columns (required for fixture masking)</span>
                  <Input
                    value={draft.piiColumns ?? ''}
                    disabled={isLocked}
                    onChange={(e) => updateDraft('piiColumns', e.target.value || null)}
                  />
                </label>
                <label className="space-y-1 text-sm">
                  <span>Grain columns</span>
                  <Input
                    value={draft.grainColumns ?? ''}
                    disabled={isLocked}
                    onChange={(e) => updateDraft('grainColumns', e.target.value || null)}
                  />
                </label>
                <label className="space-y-1 text-sm md:col-span-2">
                  <span>Relationships (required for tests)</span>
                  <Input
                    value={draft.relationshipsJson ?? ''}
                    disabled={isLocked}
                    onChange={(e) => updateDraft('relationshipsJson', e.target.value || null)}
                  />
                </label>
                <label className="space-y-1 text-sm md:col-span-2">
                  <span>SCD (dimensions only)</span>
                  <select
                    className="h-9 w-full rounded-md border bg-background px-3 text-sm"
                    value={draft.snapshotStrategy}
                    disabled={isLocked || draft.tableType !== 'dimension'}
                    onChange={(e) => updateDraft('snapshotStrategy', e.target.value)}
                  >
                    <option value="sample_1day">sample_1day</option>
                    <option value="full">full</option>
                    <option value="full_flagged">full_flagged</option>
                  </select>
                </label>
              </div>

              {error && <p className="text-sm text-destructive">{error}</p>}
              {saving && <span className="text-xs text-muted-foreground">Saving...</span>}
            </div>
          )}
        </div>
      </div>
    </section>
  );
}
