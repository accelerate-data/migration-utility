import { useEffect, useMemo, useRef, useState } from 'react';
import { useNavigate } from 'react-router';
import { Button } from '@/components/ui/button';
import {
  appSetPhaseFlags,
  migrationAnalyzeTableDetails,
  migrationApproveTableConfig,
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
import { ConfigStepHeader } from '@/components/scope/config-step-header';
import { TableListSidebar } from '@/components/scope/table-list-sidebar';
import { CoreFieldsSection } from '@/components/scope/core-fields-section';
import { PiiSection } from '@/components/scope/pii-section';
import { RelationshipsSection } from '@/components/scope/relationships-section';
import { ScdSection } from '@/components/scope/scd-section';
import { AgentRationaleSection } from '@/components/scope/agent-rationale-section';
import { ApprovalActions } from '@/components/scope/approval-actions';

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
  const approvedCount = useMemo(
    () => rows.filter((row) => configsById[row.selectedTableId]?.approvalStatus === 'approved').length,
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
      let msg = 'Analysis failed';
      if (err instanceof Error) {
        msg = err.message;
      } else if (typeof err === 'object' && err !== null) {
        msg = JSON.stringify(err);
      } else {
        msg = String(err);
      }
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
    
    // Track manual override
    let manualOverrides: string[] = [];
    try {
      if (draft.manualOverridesJson) {
        manualOverrides = JSON.parse(draft.manualOverridesJson);
      }
    } catch {
      // Invalid JSON, start fresh
    }
    
    // Add field to manual overrides if not already tracked
    const fieldName = String(key);
    if (!manualOverrides.includes(fieldName) && fieldName !== 'manualOverridesJson' && fieldName !== 'confirmedAt') {
      manualOverrides.push(fieldName);
    }
    
    const next = { 
      ...draft, 
      [key]: value,
      manualOverridesJson: JSON.stringify(manualOverrides)
    };
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

  async function handleApprove() {
    if (!activeRow) return;
    try {
      await migrationApproveTableConfig(activeRow.selectedTableId);
      const updated = await migrationGetTableConfig(activeRow.selectedTableId);
      if (updated) {
        setConfigsById((prev) => ({ ...prev, [activeRow.selectedTableId]: updated }));
        setDraft(updated);
        setMessage('Approved just now');
      }
    } catch (err) {
      logger.error('failed to approve table config', err);
      setError(err instanceof Error ? err.message : String(err));
    }
  }

  return (
    <section className="relative space-y-4" data-testid="scope-table-details-step">
      {anyAnalyzing && (
        <div className="absolute inset-0 z-20 cursor-wait rounded-md bg-background/15 backdrop-blur-[0.5px]" />
      )}

      <div className="sticky top-0 z-10 bg-background pb-4">
        <ConfigStepHeader
          readyCount={readyCount}
          approvedCount={approvedCount}
          totalCount={rows.length}
          needsDetails={needsDetails}
          message={message}
          isLocked={isLocked}
          refreshing={refreshing}
          anyAnalyzing={anyAnalyzing}
          onRefreshSchema={() => void refreshSchema()}
          onFinalizeScope={() => void finalizeScope()}
          onNavigateToSelect={() => navigate('/scope')}
          onNavigateToConfig={() => navigate('/scope/config')}
        />
      </div>

      <div className="grid gap-4 lg:grid-cols-[300px_1fr]">
        <TableListSidebar
          grouped={grouped}
          activeId={activeId}
          loading={loading}
          anyAnalyzing={anyAnalyzing}
          onSelectTable={setActiveId}
        />

        <div className="min-w-0 space-y-4">
          {!activeRow && (
            <div className="rounded-md border bg-card p-6">
              <p className="text-sm text-muted-foreground">Select a table to edit details.</p>
            </div>
          )}
          {activeRow && draft && (
            <>
              {/* Fields Panel */}
              <div className="rounded-md border bg-card p-6">
                <div className="mb-6 flex items-start justify-between gap-3">
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

                <div className="space-y-6">
                  <CoreFieldsSection
                    tableType={draft.tableType}
                    loadStrategy={draft.loadStrategy}
                    incrementalColumn={draft.incrementalColumn}
                    dateColumn={draft.dateColumn}
                    disabled={isLocked || activeIsAnalyzing}
                    manualOverrides={(() => {
                      try {
                        return draft.manualOverridesJson ? JSON.parse(draft.manualOverridesJson) : [];
                      } catch {
                        return [];
                      }
                    })()}
                    onUpdate={updateDraft}
                  />
                  <PiiSection
                    piiColumns={draft.piiColumns}
                    disabled={isLocked || activeIsAnalyzing}
                    onUpdate={(value) => updateDraft('piiColumns', value)}
                  />
                  <RelationshipsSection
                    relationshipsJson={draft.relationshipsJson}
                    grainColumns={draft.grainColumns}
                    disabled={isLocked || activeIsAnalyzing}
                    onUpdateRelationships={(value) => updateDraft('relationshipsJson', value)}
                    onUpdateGrain={(value) => updateDraft('grainColumns', value)}
                  />
                  <ScdSection
                    tableType={draft.tableType}
                    snapshotStrategy={draft.snapshotStrategy}
                    disabled={isLocked || activeIsAnalyzing}
                    onUpdate={(value) => updateDraft('snapshotStrategy', value)}
                  />
                </div>

                {analyzeErrorById[activeRow.selectedTableId] && (
                  <p className="mt-4 text-sm text-destructive">{analyzeErrorById[activeRow.selectedTableId]}</p>
                )}
                {error && <p className="mt-4 text-sm text-destructive">{error}</p>}
                {saving && <span className="mt-4 block text-xs text-muted-foreground">Saving...</span>}
              </div>

              {/* Agent Analysis & Approval Panel */}
              {draft.confirmedAt && (
                <div className="rounded-md border bg-card p-6">
                  <AgentRationaleSection analysisMetadataJson={draft.analysisMetadataJson} />

                  <div className="mt-6">
                    <ApprovalActions
                      approvalStatus={draft.approvalStatus}
                      approvedAt={draft.approvedAt}
                      confirmedAt={draft.confirmedAt}
                      isLocked={isLocked}
                      onApprove={handleApprove}
                    />
                  </div>
                </div>
              )}
            </>
          )}
        </div>
      </div>
    </section>
  );
}
