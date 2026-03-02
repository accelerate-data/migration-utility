import { useEffect, useMemo, useRef, useState } from 'react';
import { useNavigate } from 'react-router';
import { AlertCircle } from 'lucide-react';
import { Button } from '@/components/ui/button';
import {
  appSetPhase,
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
import { CoreFieldsSection } from '@/components/scope/core-fields-section';
import { PiiSection } from '@/components/scope/pii-section';
import { RelationshipsSection } from '@/components/scope/relationships-section';
import { ScdSection } from '@/components/scope/scd-section';
import { AgentRationaleSection } from '@/components/scope/agent-rationale-section';

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

function isFilledArray(value: unknown[] | null | undefined): boolean {
  return value != null && value.length > 0;
}

// null = not yet analyzed; [] = analyzed and confirmed empty (valid)
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

export default function ConfigStep() {
  const navigate = useNavigate();
  const { workspaceId, appPhase, appPhaseHydrated, setAppPhaseState } = useWorkflowStore();
  const isLocked = appPhaseHydrated && appPhase !== 'scope_editable';

  const [rows, setRows] = useState<SelectedTableRow[]>([]);
  const [configsById, setConfigsById] = useState<Record<string, TableConfigPayload | null>>({});
  const [activeId, setActiveId] = useState<string | null>(null);
  const [draft, setDraft] = useState<TableConfigPayload | null>(null);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [, setMessage] = useState('Saved just now');
  const [refreshing, setRefreshing] = useState(false);
  const [analyzingById, setAnalyzingById] = useState<Record<string, boolean>>({});
  const [analyzeErrorById, setAnalyzeErrorById] = useState<Record<string, string | null>>({});
  const [validationErrorsById, setValidationErrorsById] = useState<Record<string, number>>({});
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

  function handleValidationChange(tableId: string, errorCount: number) {
    setValidationErrorsById((prev) => ({
      ...prev,
      [tableId]: errorCount,
    }));
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
        // sourcePassword not returned by workspaceGet (security) — backend reads it from storage
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
    <section className="relative flex h-full min-h-0 flex-col gap-4" data-testid="scope-table-details-step">
      {(anyAnalyzing || refreshing) && (
        <div className="absolute inset-0 z-20 cursor-wait rounded-md bg-background/15 backdrop-blur-[0.5px]" />
      )}

      <div className="sticky top-0 z-20 bg-background pb-4">
        <ConfigStepHeader
          selectedCount={rows.length}
          readyCount={readyCount}
          totalCount={rows.length}
          activeStep="details"
          isLocked={isLocked}
          refreshing={refreshing}
          anyAnalyzing={anyAnalyzing}
          onRefreshSchema={() => void refreshSchema()}
          onFinalizeScope={() => void finalizeScope()}
          onNavigateToSelect={() => navigate('/scope')}
          onNavigateToConfig={() => navigate('/scope/config')}
        />
      </div>

      <div className="min-h-0 min-w-0 flex-1">
        {!activeRow && (
          <div className="rounded-md border bg-card p-6">
            <p className="text-sm text-muted-foreground">Select a table to view and edit its details.</p>
          </div>
        )}
        {activeRow && draft && (
          <div className="flex h-full min-h-0 flex-col rounded-md border bg-card">
            <div className="border-b p-6">
              <div className="flex flex-wrap items-start justify-between gap-4">
                <div className="w-full max-w-[560px]">
                  <select
                    value={activeId ?? ''}
                    disabled={loading || anyAnalyzing || refreshing}
                    onChange={(e) => setActiveId(e.target.value || null)}
                    className="h-10 w-full rounded-md border bg-background px-3 text-sm font-mono"
                  >
                    {rows.map((row) => (
                      <option key={row.selectedTableId} value={row.selectedTableId}>
                        {row.schemaName}.{row.tableName}
                      </option>
                    ))}
                  </select>
                </div>

                <div className="flex items-center gap-2">
                  {draft.approvalStatus === 'approved' ? (
                    <span className="rounded-md border bg-muted px-3 py-1.5 text-sm font-medium text-muted-foreground">
                      ✓ Approved
                    </span>
                  ) : (
                    <Button
                      type="button"
                      size="sm"
                      disabled={
                        isLocked ||
                        activeIsAnalyzing ||
                        !draft.confirmedAt ||
                        (validationErrorsById[activeRow.selectedTableId] || 0) > 0
                      }
                      onClick={() => void handleApprove()}
                    >
                      Approve Configuration
                    </Button>
                  )}
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
              </div>
            </div>

            <div className="min-h-0 flex-1 space-y-6 overflow-auto p-6">
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
                availableColumns={draft.availableColumns}
                onUpdate={updateDraft}
              />

              <p className="text-sm text-muted-foreground">
                Core fields stay clean; all agent inferences and reasons are listed in Agent rationale below.
              </p>

              <RelationshipsSection
                relationshipsJson={draft.relationshipsJson}
                grainColumns={draft.grainColumns}
                disabled={isLocked || activeIsAnalyzing}
                workspaceId={workspaceId ?? undefined}
                selectedTableId={activeRow.selectedTableId}
                availableColumns={draft.availableColumns}
                onUpdateGrain={(value) => updateDraft('grainColumns', value)}
                onValidationChange={(errorCount) => handleValidationChange(activeRow.selectedTableId, errorCount)}
              />

              <PiiSection
                piiColumns={draft.piiColumns}
                disabled={isLocked || activeIsAnalyzing}
                availableColumns={draft.availableColumns}
                onUpdate={(value) => updateDraft('piiColumns', value)}
              />

              <ScdSection
                tableType={draft.tableType}
                snapshotStrategy={draft.snapshotStrategy}
                disabled={isLocked || activeIsAnalyzing}
                onUpdate={(value) => updateDraft('snapshotStrategy', value)}
              />

              {draft.confirmedAt && (
                <AgentRationaleSection
                  analysisMetadataJson={draft.analysisMetadataJson}
                  manualOverrides={(() => {
                    try {
                      return draft.manualOverridesJson ? JSON.parse(draft.manualOverridesJson) : [];
                    } catch {
                      return [];
                    }
                  })()}
                />
              )}

              {analyzeErrorById[activeRow.selectedTableId] && (
                <div className="flex items-start gap-2 text-sm text-destructive">
                  <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
                  <span>{analyzeErrorById[activeRow.selectedTableId]}</span>
                </div>
              )}
              {error && (
                <div className="flex items-start gap-2 text-sm text-destructive">
                  <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
                  <span>{error}</span>
                </div>
              )}
              {saving && <span className="block text-xs text-muted-foreground">Saving...</span>}
            </div>
          </div>
        )}
      </div>
    </section>
  );
}
