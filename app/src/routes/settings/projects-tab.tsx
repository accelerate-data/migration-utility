import { useEffect, useRef, useState } from 'react';
import { CheckCircle2, FolderOpen, Loader2, Plus, RefreshCw, Search, Trash2, XCircle } from 'lucide-react';
import { toast } from 'sonner';
import { open as openDialog } from '@tauri-apps/plugin-dialog';
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '@/components/ui/alert-dialog';
import { Button } from '@/components/ui/button';
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';

const SQL_SERVER_VERSIONS = [
  { value: 'SQL Server 2022', label: 'SQL Server 2022 (16.x)' },
  { value: 'SQL Server 2019', label: 'SQL Server 2019 (15.x)' },
  { value: 'SQL Server 2017', label: 'SQL Server 2017 (14.x)' },
  { value: 'SQL Server 2016', label: 'SQL Server 2016 (13.x)' },
  { value: 'SQL Server 2014', label: 'SQL Server 2014 (12.x)' },
];
import SettingsPanelShell from '@/components/settings/settings-panel-shell';
import { projectCreateFull, projectDetectDatabases, projectDeleteFull, projectInit, projectResetLocal, listenProjectInitStep, tauriErrorMessage } from '@/lib/tauri';
import { INIT_STEP_LABEL } from '@/lib/types';
import { logger } from '@/lib/logger';
import { useProjectStore } from '@/stores/project-store';
import type { InitStep } from '@/lib/types';

// ── Init progress ─────────────────────────────────────────────────────────────

function InitProgress() {
  const { initSteps, isInitRunning } = useProjectStore();
  if (!isInitRunning && initSteps.length === 0) return null;

  return (
    <div className="rounded-lg border border-border bg-card p-4 flex flex-col gap-2">
      <p className="text-sm font-semibold text-foreground">Initializing project…</p>
      {initSteps.map(({ step, status }) => {
        const label = INIT_STEP_LABEL[step as InitStep];
        const icon = !status || status.kind === 'running'
          ? <Loader2 className="h-4 w-4 animate-spin shrink-0" style={{ color: 'var(--color-pacific)' }} />
          : status.kind === 'ok'
            ? <CheckCircle2 className="h-4 w-4 shrink-0" style={{ color: 'var(--color-seafoam)' }} />
            : <XCircle className="h-4 w-4 shrink-0 text-destructive" />;

        return (
          <div key={step} className="flex items-start gap-2">
            {icon}
            <div className="flex flex-col min-w-0">
              <span className="text-sm text-foreground">{label}</span>
              {status?.kind === 'error' && (
                <span className="text-xs text-destructive break-all">{status.message}</span>
              )}
            </div>
          </div>
        );
      })}
    </div>
  );
}

function toSlugPreview(name: string): string {
  return name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

// ── Create project dialog ─────────────────────────────────────────────────────

interface CreateDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onCreated: () => void;
}

function CreateProjectDialog({ open, onOpenChange, onCreated }: CreateDialogProps) {
  const [name, setName] = useState('');
  const [saPassword, setSaPassword] = useState('');
  const [dacpacPath, setDacpacPath] = useState('');
  const [sqlServerVersion, setSqlServerVersion] = useState('SQL Server 2022');
  const [customer, setCustomer] = useState('');
  const [system, setSystem] = useState('');
  const [dbName, setDbName] = useState('');
  const [detectedDbs, setDetectedDbs] = useState<string[]>([]);
  const [detecting, setDetecting] = useState(false);
  const [detectError, setDetectError] = useState('');
  const [extractionDatetime, setExtractionDatetime] = useState('');
  const [creating, setCreating] = useState(false);
  const { startInit, finishInit, applyInitStep, loadProjects } = useProjectStore();
  const unlistenRef = useRef<(() => void) | null>(null);

  function reset() {
    setName('');
    setSaPassword('');
    setDacpacPath('');
    setSqlServerVersion('SQL Server 2022');
    setCustomer('');
    setSystem('');
    setDbName('');
    setDetectedDbs([]);
    setDetecting(false);
    setDetectError('');
    setExtractionDatetime('');
  }

  async function handleDetect() {
    setDetecting(true);
    setDetectError('');
    setDetectedDbs([]);
    setDbName('');
    try {
      logger.debug('projects-tab: detecting databases for', name);
      const dbs = await projectDetectDatabases(name.trim(), saPassword, dacpacPath);
      setDetectedDbs(dbs);
      if (dbs.length === 1) setDbName(dbs[0]);
    } catch (err) {
      logger.error('projects-tab: detect databases failed', err);
      setDetectError(tauriErrorMessage(err));
    } finally {
      setDetecting(false);
    }
  }

  async function pickDacpac() {
    const selected = await openDialog({
      filters: [{ name: 'DacPac', extensions: ['dacpac'] }],
      multiple: false,
    });
    if (typeof selected === 'string') setDacpacPath(selected);
  }

  async function handleCreate() {
    if (!name.trim() || !saPassword || !dacpacPath || !customer.trim() || !system.trim() || !dbName.trim() || !extractionDatetime) {
      toast.error('All fields are required');
      return;
    }
    setCreating(true);
    try {
      logger.debug('projects-tab: creating project', name);
      const project = await projectCreateFull(
        name.trim(),
        saPassword,
        dacpacPath,
        sqlServerVersion,
        customer.trim(),
        system.trim(),
        dbName.trim(),
        extractionDatetime,
      );
      toast.success(`Project "${project.name}" created`);
      await loadProjects();
      onOpenChange(false);
      reset();
      onCreated();

      startInit();
      unlistenRef.current = await listenProjectInitStep((ev) => applyInitStep(ev));
      await projectInit(project.id);
      finishInit();
      toast.success('Project initialized successfully');
    } catch (err) {
      logger.error('projects-tab: create failed', err);
      toast.error(tauriErrorMessage(err));
      finishInit();
    } finally {
      setCreating(false);
      unlistenRef.current?.();
      unlistenRef.current = null;
    }
  }

  const busy = detecting || creating;

  return (
    <Dialog open={open} onOpenChange={(v) => { if (!busy) { onOpenChange(v); if (!v) reset(); } }}>
      <DialogContent className="sm:max-w-xl">
        <DialogHeader>
          <DialogTitle>New project</DialogTitle>
        </DialogHeader>

        {/* Detecting overlay — blocks the form while Docker/DacPac restore runs */}
        {detecting && (
          <div className="absolute inset-0 z-10 rounded-lg flex flex-col items-center justify-center gap-3 bg-background/80 backdrop-blur-sm">
            <Loader2 className="h-8 w-8 animate-spin" style={{ color: 'var(--color-pacific)' }} />
            <div className="text-center">
              <p className="text-sm font-semibold text-foreground">Detecting databases…</p>
              <p className="text-xs text-muted-foreground mt-1">Starting SQL Server and restoring DacPac.<br />This may take a minute.</p>
            </div>
          </div>
        )}

        <div className="flex flex-col gap-4 py-2">
          {/* Row 1: Name + SQL Server version */}
          <div className="grid grid-cols-2 gap-3">
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="proj-name" className="text-xs font-medium text-muted-foreground">Project name</Label>
              <Input
                id="proj-name"
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="e.g. Contoso Migration"
                disabled={busy}
              />
              {name.trim() && (
                <p className="text-xs text-muted-foreground font-mono">
                  slug: {toSlugPreview(name.trim())}
                </p>
              )}
            </div>
            <div className="flex flex-col gap-1.5">
              <Label className="text-xs font-medium text-muted-foreground">SQL Server version</Label>
              <Select value={sqlServerVersion} onValueChange={setSqlServerVersion} disabled={busy}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {SQL_SERVER_VERSIONS.map((v) => (
                    <SelectItem key={v.value} value={v.value}>{v.label}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>

          {/* Row 2: DacPac file */}
          <div className="flex flex-col gap-1.5">
            <Label className="text-xs font-medium text-muted-foreground">DacPac file</Label>
            <div className="flex items-center gap-2">
              <Input
                value={dacpacPath}
                readOnly
                placeholder="Select a .dacpac file…"
                className="cursor-pointer"
                onClick={pickDacpac}
                disabled={busy}
              />
              <Button variant="outline" size="icon" onClick={pickDacpac} disabled={busy}>
                <FolderOpen className="h-4 w-4" />
              </Button>
            </div>
          </div>

          {/* Row 3: SA password */}
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="proj-sa" className="text-xs font-medium text-muted-foreground">SA password</Label>
            <Input
              id="proj-sa"
              type="password"
              value={saPassword}
              onChange={(e) => setSaPassword(e.target.value)}
              placeholder="Strong SQL Server SA password"
              disabled={busy}
            />
          </div>

          {/* Row 4: Customer + System */}
          <div className="grid grid-cols-2 gap-3">
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="proj-customer" className="text-xs font-medium text-muted-foreground">Customer</Label>
              <Input
                id="proj-customer"
                value={customer}
                onChange={(e) => setCustomer(e.target.value)}
                placeholder="e.g. Contoso"
                disabled={busy}
              />
            </div>
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="proj-system" className="text-xs font-medium text-muted-foreground">Source system</Label>
              <Input
                id="proj-system"
                value={system}
                onChange={(e) => setSystem(e.target.value)}
                placeholder="e.g. ERP"
                disabled={busy}
              />
            </div>
          </div>

          {/* Row 5: Database detection + Extraction date */}
          <div className="grid grid-cols-2 gap-3">
            <div className="flex flex-col gap-1.5">
              <Label className="text-xs font-medium text-muted-foreground">Database name</Label>
              <div className="flex flex-col gap-1.5">
                {detectedDbs.length === 0 ? (
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={handleDetect}
                    disabled={busy || !name.trim() || !saPassword || !dacpacPath}
                    data-testid="project-detect-databases"
                  >
                    <Search className="h-3.5 w-3.5 mr-1.5" />
                    Detect databases
                  </Button>
                ) : (
                  <div className="flex items-center gap-1.5">
                    <Select value={dbName} onValueChange={setDbName} disabled={busy}>
                      <SelectTrigger data-testid="project-dbname-select">
                        <SelectValue placeholder="Select database…" />
                      </SelectTrigger>
                      <SelectContent>
                        {detectedDbs.map((db) => (
                          <SelectItem key={db} value={db}>{db}</SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                    <Button
                      variant="ghost"
                      size="icon"
                      onClick={handleDetect}
                      disabled={busy}
                      title="Re-detect databases"
                      data-testid="project-redetect-databases"
                    >
                      <RefreshCw className="h-3.5 w-3.5" />
                    </Button>
                  </div>
                )}
                {detectError && (
                  <p className="text-xs text-destructive break-all" data-testid="project-detect-error">{detectError}</p>
                )}
              </div>
            </div>
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="proj-extraction" className="text-xs font-medium text-muted-foreground">Extraction date</Label>
              <Input
                id="proj-extraction"
                type="date"
                value={extractionDatetime}
                onChange={(e) => setExtractionDatetime(e.target.value)}
                disabled={busy}
              />
            </div>
          </div>
        </div>

        <DialogFooter>
          <Button variant="ghost" size="sm" onClick={() => onOpenChange(false)} disabled={busy}>
            Cancel
          </Button>
          <Button size="sm" onClick={handleCreate} disabled={busy || !name.trim() || !saPassword || !dacpacPath || !customer || !system || !dbName || !extractionDatetime}>
            {creating ? <Loader2 className="h-3.5 w-3.5 animate-spin mr-1.5" /> : null}
            {creating ? 'Creating…' : 'Create'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

// ── Project row ───────────────────────────────────────────────────────────────

interface ProjectRowProps {
  id: string;
  name: string;
  slug: string;
  isActive: boolean;
  onRefresh: () => void;
}

function ProjectRow({ id, name, slug, isActive, onRefresh }: ProjectRowProps) {
  const [deleteOpen, setDeleteOpen] = useState(false);
  const [resetOpen, setResetOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const { setActive, startInit, finishInit, applyInitStep } = useProjectStore();
  const unlistenRef = useRef<(() => void) | null>(null);

  async function handleSelect() {
    if (isActive) return;
    setBusy(true);
    try {
      await setActive(id);
      startInit();
      unlistenRef.current = await listenProjectInitStep((ev) => applyInitStep(ev));
      await projectInit(id);
      finishInit();
      toast.success(`Switched to "${name}"`);
    } catch (err) {
      logger.error('projects-tab: switch failed', err);
      toast.error(`Switch failed: ${tauriErrorMessage(err)}`);
      finishInit();
    } finally {
      setBusy(false);
      unlistenRef.current?.();
      unlistenRef.current = null;
    }
  }

  async function handleDelete() {
    setBusy(true);
    setDeleteOpen(false);
    try {
      await projectDeleteFull(id);
      toast.success(`Project "${name}" deleted`);
      onRefresh();
    } catch (err) {
      logger.error('projects-tab: delete failed', err);
      toast.error(`Delete failed: ${tauriErrorMessage(err)}`);
    } finally {
      setBusy(false);
    }
  }

  async function handleReset() {
    setBusy(true);
    setResetOpen(false);
    try {
      await projectResetLocal(id);
      startInit();
      unlistenRef.current = await listenProjectInitStep((ev) => applyInitStep(ev));
      await projectInit(id);
      finishInit();
      toast.success(`"${name}" reset and reinitialized`);
    } catch (err) {
      logger.error('projects-tab: reset failed', err);
      toast.error(`Reset failed: ${tauriErrorMessage(err)}`);
      finishInit();
    } finally {
      setBusy(false);
      unlistenRef.current?.();
      unlistenRef.current = null;
    }
  }

  return (
    <>
      <tr
        className="group hover:bg-muted/40 transition-colors duration-150"
        data-testid={`project-row-${slug}`}
      >
        {/* Active indicator + name */}
        <td className="pl-4 py-2.5 border-b border-border">
          <div className="flex items-center gap-2.5">
            <div
              className="w-1.5 h-1.5 rounded-full shrink-0"
              style={{ background: isActive ? 'var(--color-pacific)' : 'var(--color-border, #e5e7eb)' }}
            />
            <div className="min-w-0">
              <p className="text-sm font-semibold text-foreground truncate">{name}</p>
            </div>
          </div>
        </td>

        {/* Slug */}
        <td className="py-2.5 border-b border-border">
          <span className="text-xs text-muted-foreground font-mono">{slug}</span>
        </td>

        {/* Status */}
        <td className="py-2.5 border-b border-border">
          {isActive ? (
            <span
              className="inline-flex items-center gap-1 text-xs font-medium px-2 py-0.5 rounded-full"
              style={{
                color: 'var(--color-pacific)',
                background: 'color-mix(in oklch, var(--color-pacific), transparent 85%)',
              }}
            >
              Active
            </span>
          ) : (
            <span className="text-xs text-muted-foreground">Inactive</span>
          )}
        </td>

        {/* Actions */}
        <td className="pr-4 py-2.5 border-b border-border">
          <div className="flex items-center gap-1 justify-end">
            {!isActive && (
              <Button
                variant="outline"
                size="sm"
                onClick={handleSelect}
                disabled={busy}
                data-testid={`project-select-${slug}`}
              >
                {busy ? <Loader2 className="h-3 w-3 animate-spin" /> : 'Select'}
              </Button>
            )}
            {isActive && (
              <Button
                variant="ghost"
                size="icon"
                onClick={() => setResetOpen(true)}
                disabled={busy}
                title="Reset local state"
                data-testid={`project-reset-${slug}`}
              >
                <RefreshCw className="h-3.5 w-3.5 text-muted-foreground" />
              </Button>
            )}
            <Button
              variant="ghost"
              size="icon"
              onClick={() => setDeleteOpen(true)}
              disabled={busy}
              title="Delete project"
              data-testid={`project-delete-${slug}`}
            >
              <Trash2 className="h-3.5 w-3.5 text-destructive" />
            </Button>
          </div>
        </td>
      </tr>

      {/* Delete confirmation */}
      <AlertDialog open={deleteOpen} onOpenChange={setDeleteOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete "{name}"?</AlertDialogTitle>
            <AlertDialogDescription>
              This will permanently remove:
              <ul className="list-disc list-inside mt-2 space-y-1 text-sm">
                <li>The <code>{slug}/</code> directory from the migration repository</li>
                <li>The local project directory and SQL container data</li>
                <li>The <code>SA_PASSWORD_{slug.replace(/-/g, '_').toUpperCase()}</code> GitHub secret</li>
              </ul>
              <span className="block mt-2 font-medium text-destructive">
                This action cannot be undone.
              </span>
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={handleDelete}
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
              data-testid={`project-delete-confirm-${slug}`}
            >
              Delete permanently
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      {/* Reset confirmation */}
      <AlertDialog open={resetOpen} onOpenChange={setResetOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Reset local state for "{name}"?</AlertDialogTitle>
            <AlertDialogDescription>
              <span className="block font-medium mb-1">Will be removed locally:</span>
              <ul className="list-disc list-inside space-y-1 text-sm">
                <li>Local project directory (restored from git on reinit)</li>
                <li>SQL Server Docker container and its data volume</li>
              </ul>
              <span className="block font-medium mt-2 mb-1">Will be kept:</span>
              <ul className="list-disc list-inside space-y-1 text-sm">
                <li>GitHub repository artifacts, DacPac, and metadata</li>
                <li>Project record and SA secret</li>
              </ul>
              <span className="block mt-2 text-sm text-muted-foreground">
                The project will be reinitialized immediately after reset.
              </span>
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={handleReset}
              data-testid={`project-reset-confirm-${slug}`}
            >
              Reset and reinitialize
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  );
}

// ── Projects tab ──────────────────────────────────────────────────────────────

export default function ProjectsTab() {
  const { projects, activeProject, isLoading, loadProjects } = useProjectStore();
  const [createOpen, setCreateOpen] = useState(false);

  useEffect(() => {
    void loadProjects();
  }, [loadProjects]);

  return (
    <SettingsPanelShell>
      <div className="flex flex-col gap-5">
        <div className="flex items-center justify-between">
          <div>
            <p className="text-sm font-semibold text-foreground">Projects</p>
            <p className="text-xs text-muted-foreground mt-0.5">
              Manage migration projects. One project is active at a time.
            </p>
          </div>
          <Button variant="outline" size="sm" onClick={() => setCreateOpen(true)} className="gap-1.5">
            <Plus className="h-3.5 w-3.5" />
            New project
          </Button>
        </div>

        <InitProgress />

        {isLoading && (
          <div className="flex items-center gap-2 text-sm text-muted-foreground">
            <Loader2 className="h-4 w-4 animate-spin" style={{ color: 'var(--color-pacific)' }} />
            Loading projects…
          </div>
        )}

        {!isLoading && projects.length === 0 && (
          <div className="rounded-lg border border-dashed border-border p-6 text-center">
            <p className="text-sm text-muted-foreground">No projects yet.</p>
            <p className="text-xs text-muted-foreground mt-1">
              Create one to get started.
            </p>
          </div>
        )}

        {!isLoading && projects.length > 0 && (
          <table className="w-full table-auto border-separate border-spacing-0" data-testid="project-list">
            <thead>
              <tr>
                <th scope="col" className="pl-4 py-1.5 text-left text-xs font-semibold text-muted-foreground border-b-2 border-border">
                  Name
                </th>
                <th scope="col" className="py-1.5 text-left text-xs font-semibold text-muted-foreground border-b-2 border-border">
                  Slug
                </th>
                <th scope="col" className="py-1.5 text-left text-xs font-semibold text-muted-foreground border-b-2 border-border">
                  Status
                </th>
                <th scope="col" className="pr-4 py-1.5 text-right text-xs font-semibold text-muted-foreground border-b-2 border-border">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody>
              {projects.map((p) => (
                <ProjectRow
                  key={p.id}
                  id={p.id}
                  name={p.name}
                  slug={p.slug}
                  isActive={activeProject?.id === p.id}
                  onRefresh={loadProjects}
                />
              ))}
            </tbody>
          </table>
        )}
      </div>

      <CreateProjectDialog
        open={createOpen}
        onOpenChange={setCreateOpen}
        onCreated={loadProjects}
      />
    </SettingsPanelShell>
  );
}
