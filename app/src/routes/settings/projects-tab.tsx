import { useEffect, useRef, useState } from 'react';
import { CheckCircle2, FolderOpen, Loader2, Plus, RefreshCw, Trash2, XCircle } from 'lucide-react';
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
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import SettingsPanelShell from '@/components/settings/settings-panel-shell';
import { projectCreateFull, projectDeleteFull, projectInit, projectResetLocal, listenProjectInitStep } from '@/lib/tauri';
import { INIT_STEP_LABEL } from '@/lib/types';
import { logger } from '@/lib/logger';
import { useProjectStore } from '@/stores/project-store';
import type { InitStep } from '@/lib/types';

// ── Init progress row ─────────────────────────────────────────────────────────

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

// ── Create project form ───────────────────────────────────────────────────────

interface CreateFormProps {
  onCreated: () => void;
}

function CreateProjectForm({ onCreated }: CreateFormProps) {
  const [open, setOpen] = useState(false);
  const [name, setName] = useState('');
  const [saPassword, setSaPassword] = useState('');
  const [dacpacPath, setDacpacPath] = useState('');
  const [creating, setCreating] = useState(false);
  const { startInit, finishInit, applyInitStep, loadProjects } = useProjectStore();
  const unlistenRef = useRef<(() => void) | null>(null);

  async function pickDacpac() {
    const selected = await openDialog({
      filters: [{ name: 'DacPac', extensions: ['dacpac'] }],
      multiple: false,
    });
    if (typeof selected === 'string') setDacpacPath(selected);
  }

  async function handleCreate() {
    if (!name.trim() || !saPassword || !dacpacPath) {
      toast.error('Name, SA password, and DacPac file are required');
      return;
    }
    setCreating(true);
    try {
      logger.debug('projects-tab: creating project', name);
      const project = await projectCreateFull(name.trim(), saPassword, dacpacPath);
      toast.success(`Project "${project.name}" created`);
      await loadProjects();
      setOpen(false);
      setName('');
      setSaPassword('');
      setDacpacPath('');
      onCreated();

      // Start init
      startInit();
      unlistenRef.current = await listenProjectInitStep((ev) => applyInitStep(ev));
      await projectInit(project.id);
      finishInit();
      toast.success('Project initialized successfully');
    } catch (err) {
      logger.error('projects-tab: create failed', err);
      toast.error(String(err));
      finishInit();
    } finally {
      setCreating(false);
      unlistenRef.current?.();
      unlistenRef.current = null;
    }
  }

  if (!open) {
    return (
      <Button variant="outline" size="sm" onClick={() => setOpen(true)} className="gap-1.5">
        <Plus className="h-3.5 w-3.5" />
        New project
      </Button>
    );
  }

  return (
    <div className="rounded-lg border border-border bg-card p-4 flex flex-col gap-3">
      <p className="text-sm font-semibold text-foreground">New project</p>

      <div className="flex flex-col gap-1.5">
        <Label htmlFor="proj-name" className="text-xs font-medium text-muted-foreground">Name</Label>
        <Input
          id="proj-name"
          value={name}
          onChange={(e) => setName(e.target.value)}
          placeholder="e.g. Contoso Migration"
          disabled={creating}
        />
      </div>

      <div className="flex flex-col gap-1.5">
        <Label htmlFor="proj-sa" className="text-xs font-medium text-muted-foreground">SA password</Label>
        <Input
          id="proj-sa"
          type="password"
          value={saPassword}
          onChange={(e) => setSaPassword(e.target.value)}
          placeholder="Strong SQL Server SA password"
          disabled={creating}
        />
      </div>

      <div className="flex flex-col gap-1.5">
        <Label className="text-xs font-medium text-muted-foreground">DacPac file</Label>
        <div className="flex items-center gap-2">
          <Input
            value={dacpacPath}
            readOnly
            placeholder="Select a .dacpac file…"
            className="cursor-pointer"
            onClick={pickDacpac}
            disabled={creating}
          />
          <Button variant="outline" size="icon" onClick={pickDacpac} disabled={creating}>
            <FolderOpen className="h-4 w-4" />
          </Button>
        </div>
      </div>

      <div className="flex gap-2 justify-end">
        <Button variant="ghost" size="sm" onClick={() => setOpen(false)} disabled={creating}>
          Cancel
        </Button>
        <Button size="sm" onClick={handleCreate} disabled={creating || !name || !saPassword || !dacpacPath}>
          {creating ? <Loader2 className="h-3.5 w-3.5 animate-spin mr-1.5" /> : null}
          {creating ? 'Creating…' : 'Create'}
        </Button>
      </div>
    </div>
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
      toast.error(`Switch failed: ${String(err)}`);
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
      toast.error(`Delete failed: ${String(err)}`);
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
      toast.error(`Reset failed: ${String(err)}`);
      finishInit();
    } finally {
      setBusy(false);
      unlistenRef.current?.();
      unlistenRef.current = null;
    }
  }

  return (
    <>
      <div
        className="flex items-center gap-3 rounded-lg border px-4 py-3 bg-card transition-colors duration-150"
        style={isActive ? { borderColor: 'var(--color-pacific)' } : undefined}
        data-testid={`project-row-${slug}`}
      >
        {/* Active indicator */}
        <div className="w-2 h-2 rounded-full shrink-0"
          style={{ background: isActive ? 'var(--color-pacific)' : 'var(--color-border, #e5e7eb)' }} />

        <div className="flex-1 min-w-0">
          <p className="text-sm font-semibold text-foreground truncate">{name}</p>
          <p className="text-xs text-muted-foreground font-mono truncate">{slug}</p>
        </div>

        <div className="flex items-center gap-1.5 shrink-0">
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
      </div>

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
          <CreateProjectForm onCreated={loadProjects} />
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
          <div className="flex flex-col gap-2" data-testid="project-list">
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
          </div>
        )}
      </div>
    </SettingsPanelShell>
  );
}
