import { useEffect, useMemo, useRef, useState } from 'react';
import { useNavigate } from 'react-router';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import {
  appSetPhaseFlags,
  migrationAnalyzeTableDetails,
  migrationGetTableConfig,
  migrationListScopeInventory,
  migrationReconcileScopeState,
  migrationSaveTableConfig,
  workspaceApplyStart,
  workspaceApplyStatus,
  workspaceGet,
} from '@/lib/tauri';
import { logger } from '@/lib/logger';
import type { ScopeInventoryRow, TableConfigPayload } from '@/lib/types';
import { useWorkflowStore } from '@/stores/workflow-store';

type SelectedTableRow = ScopeInventoryRow & {
  selectedTableId: string;
};

function selectedTableId(workspaceId: string, row: ScopeInventoryRow): string {
  return `st:${workspaceId}:${row.warehouseItemId}:${row.schemaName.toLowerCase()}:${row.tableName.toLowerCase()}`;
}

function defaultConfig(selectedTableIdValue: string): TableConfigPayload {
  return {
    selectedTableId: selectedTableIdValue,
    tableType: null,
    loadStrategy: null,
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
  };
}

function isFilled(value: string | null): boolean {
  return value !== null && value.trim().length > 0;
}

function isReady(config: TableConfigPayload | null | undefined): boolean {
  if (!config) return false;
  return (
    isFilled(config.tableType) &&
    isFilled(config.loadStrategy) &&
    isFilled(config.incrementalColumn) &&
    isFilled(config.dateColumn) &&
    isFilled(config.grainColumns) &&
    isFilled(config.relationshipsJson) &&
    isFilled(config.piiColumns)
  );
}

export default function ConfigStep() {
  const navigate = useNavigate();
  const { workspaceId, appPhase, phaseFacts, setAppPhaseState } = useWorkflowStore();
  const isLocked = phaseFacts.scopeFinalized || appPhase === 'running_locked';

  const [rows, setRows] = useState<SelectedTableRow[]>([]);
  const [configsById, setConfigsById] = useState<Record<string, TableConfigPayload | null>>({});
  const [activeId, setActiveId] = useState<string | null>(null);
  const [draft, setDraft] = useState<TableConfigPayload | null>(null);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [message, setMessage] = useState('Saved just now');
  const [refreshing, setRefreshing] = useState(false);
  const [analyzingById, setAnalyzingById] = useState<Record<string, boolean>>({});
  const [analyzeErrorById, setAnalyzeErrorById] = useState<Record<string, string | null>>({});
const autosaveTimerRef = useRef<number | null>(null);
  const autoAnalyzeAttemptedRef = useRef<Set<string>>(new Set());

  async function loadSelectedTables(preferredId?: string | null) {
    if (!workspaceId) {
      setRows([]);
      setConfigsById({});
      setActiveId(null);
      setDraft(null);
      return;
    }

    setLoading(true);
    setError(null);
    try {
      const inventory = await migrationListScopeInventory(workspaceId);
      const selected = inventory
        .filter((row) => row.isSelected)
        .map((row) => ({ ...row, selectedTableId: selectedTableId(workspaceId, row) }));
      setRows(selected);

      const configEntries = await Promise.all(
        selected.map(async (row) => [row.selectedTableId, await migrationGetTableConfig(row.selectedTableId)] as const),
      );
      const nextConfigMap: Record<string, TableConfigPayload | null> = {};
      for (const [id, config] of configEntries) {
        nextConfigMap[id] = config;
      }
      setConfigsById(nextConfigMap);

      const nextActiveId =
        preferredId && selected.some((row) => row.selectedTableId === preferredId)
          ? preferredId
          : selected[0]?.selectedTableId ?? null;
      setActiveId(nextActiveId);
      setDraft(nextActiveId ? nextConfigMap[nextActiveId] ?? defaultConfig(nextActiveId) : null);
    } catch (err) {
      logger.error('failed to load selected tables for details', err);
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void loadSelectedTables(activeId);
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

  useEffect(() => {
    if (!activeId) {
      setDraft(null);
      return;
    }
    const config = configsById[activeId] ?? null;
    setDraft(config ?? defaultConfig(activeId));
  }, [activeId, configsById]);

  const grouped = useMemo(() => {
    const map = new Map<string, SelectedTableRow[]>();
    for (const row of rows) {
      const current = map.get(row.schemaName) ?? [];
      current.push(row);
      map.set(row.schemaName, current);
    }
    return Array.from(map.entries()).sort(([a], [b]) => a.localeCompare(b));
  }, [rows]);

  const activeRow = useMemo(
    () => rows.find((row) => row.selectedTableId === activeId) ?? null,
    [rows, activeId],
  );
  const activeIsAnalyzing = activeRow ? analyzingById[activeRow.selectedTableId] === true : false;
  const anyAnalyzing = Object.values(analyzingById).some((v) => v === true);

  const readyCount = useMemo(
    () => rows.filter((row) => isReady(configsById[row.selectedTableId])).length,
    [rows, configsById],
  );
  const needsDetails = rows.length - readyCount;

  async function analyzeTable(row: SelectedTableRow, force: boolean) {
    if (!workspaceId || isLocked) return;
    setAnalyzeErrorById((prev) => ({ ...prev, [row.selectedTableId]: null }));
    setAnalyzingById((prev) => ({ ...prev, [row.selectedTableId]: true }));
    try {
      const analyzed = await migrationAnalyzeTableDetails({
        workspaceId,
        selectedTableId: row.selectedTableId,
        schemaName: row.schemaName,
        tableName: row.tableName,
        force,
      });
      setConfigsById((prev) => ({ ...prev, [row.selectedTableId]: analyzed }));
      if (activeId === row.selectedTableId) {
        setDraft(analyzed);
      }
      setMessage(force ? 'Re-analyzed just now' : 'Analyzed just now');
    } catch (err) {
      logger.error('table details analysis failed', err);
      const msg = err instanceof Error ? err.message : String(err);
      setAnalyzeErrorById((prev) => ({ ...prev, [row.selectedTableId]: msg }));
      setError(msg);
    } finally {
      setAnalyzingById((prev) => ({ ...prev, [row.selectedTableId]: false }));
    }
  }

  useEffect(() => {
    if (!activeRow || isLocked) return;
    if (configsById[activeRow.selectedTableId]) return;
    if (analyzingById[activeRow.selectedTableId]) return;
    if (autoAnalyzeAttemptedRef.current.has(activeRow.selectedTableId)) return;
    autoAnalyzeAttemptedRef.current.add(activeRow.selectedTableId);
    void analyzeTable(activeRow, false);
  }, [activeRow, isLocked, configsById, analyzingById]);

  async function persist(next: TableConfigPayload) {
    setSaving(true);
    setError(null);
    try {
      const payload = {
        ...next,
        confirmedAt: new Date().toISOString(),
      };
      await migrationSaveTableConfig(payload);
      setConfigsById((prev) => ({ ...prev, [next.selectedTableId]: payload }));
      setMessage('Saved just now');
    } catch (err) {
      logger.error('failed to save table config', err);
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSaving(false);
    }
  }

  function updateDraft<K extends keyof TableConfigPayload>(key: K, value: TableConfigPayload[K]) {
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
      autoAnalyzeAttemptedRef.current.clear();
      await loadSelectedTables(activeId);
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
    <section className="relative space-y-4" data-testid="scope-table-details-step">
      {anyAnalyzing && (
        <div className="absolute inset-0 z-20 cursor-wait rounded-md bg-background/15 backdrop-blur-[0.5px]" />
      )}
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
              disabled={isLocked || refreshing || anyAnalyzing}
              onClick={() => void refreshSchema()}
            >
              {refreshing ? 'Refreshing...' : 'Refresh schema'}
            </Button>
            <Button type="button" size="sm" disabled={isLocked || anyAnalyzing} onClick={() => void finalizeScope()}>
              {isLocked ? 'Scope Finalized' : 'Finalize Scope'}
            </Button>
          </div>
        </div>
        <div className="mt-4 border-b border-border">
          <div className="flex items-center gap-6">
            <button
              type="button"
              className="border-b-2 border-transparent pb-2 text-sm font-medium text-muted-foreground"
              disabled={anyAnalyzing}
              onClick={() => navigate('/scope')}
            >
              1. Select Tables
            </button>
            <button
              type="button"
              className="border-b-2 border-primary pb-2 text-sm font-medium text-primary"
              disabled={anyAnalyzing}
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
                      disabled={anyAnalyzing}
                      onClick={() => setActiveId(row.selectedTableId)}
                    >
                      <span className="font-mono">{row.tableName}</span>
                    </button>
                  ))}
                </details>
              ))}
          </div>
        </div>

        <div className="rounded-md border bg-card p-4 lg:pr-8">
          {!activeRow && <p className="text-sm text-muted-foreground">Select a table to edit details.</p>}
          {activeRow && draft && (
            <div className="space-y-4">
              <div className="flex items-start justify-between gap-3">
                <div>
                  <p className="font-mono text-sm font-semibold">
                    {activeRow.schemaName}.{activeRow.tableName}
                  </p>
                  <p className="text-xs text-muted-foreground">Migration metadata required for build and tests.</p>
                </div>
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  disabled={isLocked || activeIsAnalyzing}
                  onClick={() => void analyzeTable(activeRow, true)}
                >
                  {activeIsAnalyzing ? 'Analyzing...' : 'Analyze again'}
                </Button>
              </div>

              <div className="grid gap-3 md:grid-cols-2">
                <label className="space-y-1 text-sm">
                  <span>Table type</span>
                  <select
                    className="h-9 w-full rounded-md border bg-background px-3 text-sm"
                    value={draft.tableType ?? ''}
                    disabled={isLocked || activeIsAnalyzing}
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
                    disabled={isLocked || activeIsAnalyzing}
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
                    disabled={isLocked || activeIsAnalyzing}
                    onChange={(e) => updateDraft('incrementalColumn', e.target.value || null)}
                  />
                </label>
                <label className="space-y-1 text-sm">
                  <span>Canonical date column</span>
                  <Input
                    value={draft.dateColumn ?? ''}
                    disabled={isLocked || activeIsAnalyzing}
                    onChange={(e) => updateDraft('dateColumn', e.target.value || null)}
                  />
                </label>
                <label className="space-y-1 text-sm md:col-span-2">
                  <span>PII columns (required for fixture masking)</span>
                  <Input
                    value={draft.piiColumns ?? ''}
                    disabled={isLocked || activeIsAnalyzing}
                    onChange={(e) => updateDraft('piiColumns', e.target.value || null)}
                  />
                </label>
                <label className="space-y-1 text-sm">
                  <span>Grain columns</span>
                  <Input
                    value={draft.grainColumns ?? ''}
                    disabled={isLocked || activeIsAnalyzing}
                    onChange={(e) => updateDraft('grainColumns', e.target.value || null)}
                  />
                </label>
                <label className="space-y-1 text-sm md:col-span-2">
                  <span>Relationships (required for tests)</span>
                  <Input
                    value={draft.relationshipsJson ?? ''}
                    disabled={isLocked || activeIsAnalyzing}
                    onChange={(e) => updateDraft('relationshipsJson', e.target.value || null)}
                  />
                </label>
                <label className="space-y-1 text-sm md:col-span-2">
                  <span>SCD (dimensions only)</span>
                  <select
                    className="h-9 w-full rounded-md border bg-background px-3 text-sm"
                    value={draft.snapshotStrategy}
                    disabled={isLocked || activeIsAnalyzing || draft.tableType !== 'dimension'}
                    onChange={(e) => updateDraft('snapshotStrategy', e.target.value)}
                  >
                    <option value="sample_1day">sample_1day</option>
                    <option value="full">full</option>
                    <option value="full_flagged">full_flagged</option>
                  </select>
                </label>
              </div>

              {analyzeErrorById[activeRow.selectedTableId] && (
                <p className="text-sm text-destructive">{analyzeErrorById[activeRow.selectedTableId]}</p>
              )}
              {error && <p className="text-sm text-destructive">{error}</p>}
              {saving && <span className="text-xs text-muted-foreground">Saving...</span>}
            </div>
          )}
        </div>
      </div>
    </section>
  );
}
