import { useEffect, useState } from 'react';
import { FolderOpen, Loader2, Plus, RefreshCw, Search, Trash2 } from 'lucide-react';
import { Switch } from '@/components/ui/switch';
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
import StepProgress from '@/components/step-progress';
import SettingsPanelShell from '@/components/settings/settings-panel-shell';
import { projectCreateFull, projectDetectDatabases, projectDeleteFull, projectResetLocal, tauriErrorMessage } from '@/lib/tauri';
import { TECHNOLOGY_LABEL } from '@/lib/types';
import type { Technology } from '@/lib/types';
import { logger } from '@/lib/logger';
import { useProjectInit } from '@/hooks/use-project-init';
import { useProjectStore } from '@/stores/project-store';

const TECHNOLOGIES: { value: Technology; label: string }[] = [
  { value: 'sql_server', label: TECHNOLOGY_LABEL['sql_server'] },
  { value: 'fabric_warehouse', label: TECHNOLOGY_LABEL['fabric_warehouse'] },
  { value: 'fabric_lakehouse', label: TECHNOLOGY_LABEL['fabric_lakehouse'] },
  { value: 'snowflake', label: TECHNOLOGY_LABEL['snowflake'] },
];

function toSlugPreview(name: string): string {
  return name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

/** Today's date as a YYYY-MM-DD string in the local timezone. */
function localTodayString(): string {
  const now = new Date();
  const y = now.getFullYear();
  const m = String(now.getMonth() + 1).padStart(2, '0');
  const d = String(now.getDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
}

/** Convert a local YYYY-MM-DD date string (midnight local) to a UTC ISO string for storage. */
function localDateToUtc(localDate: string): string {
  const [y, mo, d] = localDate.split('-').map(Number);
  return new Date(y, mo - 1, d, 0, 0, 0, 0).toISOString();
}

/** Convert a stored UTC ISO string back to a local YYYY-MM-DD string for display. */
export function utcToLocalDate(utcString: string): string {
  const dt = new Date(utcString);
  const y = dt.getFullYear();
  const m = String(dt.getMonth() + 1).padStart(2, '0');
  const d = String(dt.getDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
}

// ── Create project dialog ─────────────────────────────────────────────────────

interface CreateDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onCreated: () => void;
}

function CreateProjectDialog({ open, onOpenChange, onCreated }: CreateDialogProps) {
  const [name, setName] = useState('');
  const [technology, setTechnology] = useState<Technology>('sql_server');
  const [sourcePath, setSourcePath] = useState('');
  const [dbName, setDbName] = useState('');
  const [detectedDbs, setDetectedDbs] = useState<string[]>([]);
  const [detecting, setDetecting] = useState(false);
  const [detectError, setDetectError] = useState('');
  const [extractionDate, setExtractionDate] = useState(localTodayString);
  const [creating, setCreating] = useState(false);
  const { loadProjects } = useProjectStore();
  const { runInit } = useProjectInit();

  const isSqlServer = technology === 'sql_server';

  function reset() {
    setName('');
    setTechnology('sql_server');
    setSourcePath('');
    setDbName('');
    setDetectedDbs([]);
    setDetecting(false);
    setDetectError('');
    setExtractionDate(localTodayString());
  }

  async function handleDetect() {
    if (!sourcePath) return;
    setDetecting(true);
    setDetectError('');
    setDetectedDbs([]);
    setDbName('');
    try {
      logger.debug('projects-tab: detecting databases', sourcePath);
      const dbs = await projectDetectDatabases(sourcePath);
      setDetectedDbs(dbs);
      if (dbs.length === 1) setDbName(dbs[0]);
    } catch (err) {
      logger.error('projects-tab: detect databases failed', err);
      setDetectError(tauriErrorMessage(err));
    } finally {
      setDetecting(false);
    }
  }

  async function pickSourceFile() {
    const filters = isSqlServer
      ? [{ name: 'DacPac', extensions: ['dacpac'] }]
      : [{ name: 'DDL Archive', extensions: ['zip'] }];
    const selected = await openDialog({ filters, multiple: false });
    if (typeof selected === 'string') {
      setSourcePath(selected);
      setDetectedDbs([]);
      setDbName('');
      setDetectError('');
    }
  }

  async function handleCreate() {
    if (!name.trim() || !sourcePath || !dbName.trim() || !extractionDate) {
      toast.error('All fields are required', { duration: Infinity });
      return;
    }
    setCreating(true);
    try {
      logger.debug('projects-tab: creating project', name, technology);
      const project = await projectCreateFull(
        name.trim(),
        technology,
        sourcePath,
        dbName.trim(),
        localDateToUtc(extractionDate),
      );
      toast.success(`Project "${project.name}" created`);
      await loadProjects();
      onOpenChange(false);
      reset();
      onCreated();

      await runInit(project.id);
      toast.success('Project initialized successfully');
    } catch (err) {
      logger.error('projects-tab: create failed', err);
      toast.error(tauriErrorMessage(err), { duration: Infinity });
    } finally {
      setCreating(false);
    }
  }

  // Auto-detect DB name when a DacPac is selected (SQL Server only).
  useEffect(() => {
    if (sourcePath && isSqlServer) {
      void handleDetect();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sourcePath]);

  // Clear source path when technology changes (different file type).
  useEffect(() => {
    setSourcePath('');
    setDetectedDbs([]);
    setDbName('');
    setDetectError('');
  }, [technology]);

  const busy = detecting || creating;
  const canCreate = !!name.trim() && !!sourcePath && !!dbName.trim() && !!extractionDate && !busy;

  return (
    <Dialog open={open} onOpenChange={(v) => { if (!busy) { onOpenChange(v); if (!v) reset(); } }}>
      <DialogContent className="sm:max-w-xl">
        <DialogHeader>
          <DialogTitle>New project</DialogTitle>
        </DialogHeader>

        {detecting && (
          <div className="absolute inset-0 z-10 rounded-lg flex flex-col items-center justify-center gap-3 bg-background/80 backdrop-blur-sm">
            <Loader2 className="h-8 w-8 animate-spin" style={{ color: 'var(--color-pacific)' }} />
            <div className="text-center">
              <p className="text-sm font-semibold text-foreground">Reading DacPac…</p>
              <p className="text-xs text-muted-foreground mt-1">Extracting database name from metadata.</p>
            </div>
          </div>
        )}

        <div className="flex flex-col gap-4 py-2">
          {/* Row 1: Name + Technology */}
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
              <Label className="text-xs font-medium text-muted-foreground">Technology</Label>
              <Select value={technology} onValueChange={(v) => setTechnology(v as Technology)} disabled={busy}>
                <SelectTrigger data-testid="project-technology-select">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {TECHNOLOGIES.map((t) => (
                    <SelectItem key={t.value} value={t.value}>{t.label}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>

          {/* Row 2: Source file */}
          <div className="flex flex-col gap-1.5">
            <Label className="text-xs font-medium text-muted-foreground">
              {isSqlServer ? 'DacPac file' : 'DDL archive (.zip)'}
            </Label>
            <div className="flex items-center gap-2">
              <Input
                value={sourcePath}
                readOnly
                placeholder={isSqlServer ? 'Select a .dacpac file…' : 'Select a .zip archive…'}
                className="cursor-pointer"
                onClick={pickSourceFile}
                disabled={busy}
              />
              <Button variant="outline" size="icon" onClick={pickSourceFile} disabled={busy}>
                <FolderOpen className="h-4 w-4" />
              </Button>
            </div>
          </div>

          {/* Row 3: Database name */}
          <div className="flex flex-col gap-1.5">
            <Label className="text-xs font-medium text-muted-foreground">Database name</Label>
            <div className="flex flex-col gap-1.5">
              {isSqlServer && detectedDbs.length === 0 ? (
                <div className="flex items-center gap-2">
                  <Input
                    value={dbName}
                    onChange={(e) => setDbName(e.target.value)}
                    placeholder="e.g. Contoso_DW"
                    disabled={busy}
                  />
                  <Button
                    variant="outline"
                    size="icon"
                    onClick={handleDetect}
                    disabled={busy || !sourcePath}
                    title="Detect from DacPac"
                    data-testid="project-detect-databases"
                  >
                    <Search className="h-3.5 w-3.5" />
                  </Button>
                </div>
              ) : isSqlServer ? (
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
                    title="Re-detect from DacPac"
                    data-testid="project-redetect-databases"
                  >
                    <RefreshCw className="h-3.5 w-3.5" />
                  </Button>
                </div>
              ) : (
                <Input
                  value={dbName}
                  onChange={(e) => setDbName(e.target.value)}
                  placeholder="e.g. Contoso_DW"
                  disabled={busy}
                />
              )}
              {detectError && (
                <p className="text-xs text-destructive break-all" data-testid="project-detect-error">{detectError}</p>
              )}
            </div>
          </div>

          {/* Row 4: Extraction date */}
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="proj-extraction" className="text-xs font-medium text-muted-foreground">Extraction date</Label>
            <Input
              id="proj-extraction"
              type="date"
              value={extractionDate}
              onChange={(e) => setExtractionDate(e.target.value)}
              disabled={busy}
            />
          </div>
        </div>

        <DialogFooter>
          <Button variant="ghost" size="sm" onClick={() => onOpenChange(false)} disabled={busy}>
            Cancel
          </Button>
          <Button size="sm" onClick={handleCreate} disabled={!canCreate}>
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
  technology: string;
  isActive: boolean;
  onRefresh: () => void;
}

function ProjectRow({ id, name, slug, technology, isActive, onRefresh }: ProjectRowProps) {
  const [deleteOpen, setDeleteOpen] = useState(false);
  const [resetOpen, setResetOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const { setActive } = useProjectStore();
  const { runInit } = useProjectInit();

  async function handleSelect() {
    if (isActive) return;
    setBusy(true);
    try {
      await setActive(id);
      await runInit(id);
      toast.success(`Switched to "${name}"`);
    } catch (err) {
      logger.error('projects-tab: switch failed', err);
      toast.error(`Switch failed: ${tauriErrorMessage(err)}`, { duration: Infinity });
    } finally {
      setBusy(false);
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
      toast.error(`Delete failed: ${tauriErrorMessage(err)}`, { duration: Infinity });
    } finally {
      setBusy(false);
    }
  }

  async function handleReset() {
    setBusy(true);
    setResetOpen(false);
    try {
      await projectResetLocal(id);
      await runInit(id);
      toast.success(`"${name}" reset and reinitialized`);
    } catch (err) {
      logger.error('projects-tab: reset failed', err);
      toast.error(`Reset failed: ${tauriErrorMessage(err)}`, { duration: Infinity });
    } finally {
      setBusy(false);
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

        {/* Technology */}
        <td className="py-2.5 border-b border-border">
          <span className="text-xs font-medium px-2 py-0.5 rounded-full"
            style={{ background: 'color-mix(in oklch, var(--color-pacific), transparent 85%)', color: 'var(--color-pacific)' }}>
            {TECHNOLOGY_LABEL[technology as keyof typeof TECHNOLOGY_LABEL] ?? technology}
          </span>
        </td>

        {/* Active toggle */}
        <td className="py-2.5 border-b border-border">
          <div className="flex items-center gap-2">
            {busy && !isActive ? (
              <Loader2 className="h-3.5 w-3.5 animate-spin" style={{ color: 'var(--color-pacific)' }} />
            ) : (
              <Switch
                size="sm"
                checked={isActive}
                disabled={busy || isActive}
                onCheckedChange={(checked) => { if (checked) void handleSelect(); }}
                aria-label={isActive ? 'Active project' : 'Set as active project'}
                data-testid={`project-select-${slug}`}
              />
            )}
            <span className="text-xs text-muted-foreground">
              {isActive ? 'Active' : 'Inactive'}
            </span>
          </div>
        </td>

        {/* Actions */}
        <td className="pr-4 py-2.5 border-b border-border">
          <div className="flex items-center gap-1 justify-end">
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
                <li>The local project directory</li>
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
                <li>Local project directory (DDL files will be re-extracted from source on reinit)</li>
              </ul>
              <span className="block font-medium mt-2 mb-1">Will be kept:</span>
              <ul className="list-disc list-inside space-y-1 text-sm">
                <li>GitHub repository artifacts, source binary, and metadata</li>
                <li>Project record in database</li>
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
  const { projects, activeProject, isLoading, loadProjects, initSteps, isInitRunning, dismissInit } = useProjectStore();
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

        <StepProgress steps={initSteps} isRunning={isInitRunning} onDismiss={dismissInit} />

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
                  Technology
                </th>
                <th scope="col" className="py-1.5 text-left text-xs font-semibold text-muted-foreground border-b-2 border-border">
                  Active
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
                  technology={p.technology}
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
