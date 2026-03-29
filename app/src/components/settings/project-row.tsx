import { useState } from 'react';
import { Loader2, RefreshCw, Trash2 } from 'lucide-react';
import { toast } from 'sonner';
import { Button } from '@/components/ui/button';
import { Switch } from '@/components/ui/switch';
import { projectDeleteFull, projectResetLocal, tauriErrorMessage } from '@/lib/tauri';
import { TECHNOLOGY_LABEL } from '@/lib/types';
import { logger } from '@/lib/logger';
import { useProjectInit } from '@/hooks/use-project-init';
import { useProjectStore } from '@/stores/project-store';
import ProjectDeleteDialog from './project-delete-dialog';
import ProjectResetDialog from './project-reset-dialog';

interface ProjectRowProps {
  id: string;
  name: string;
  slug: string;
  technology: string;
  isActive: boolean;
  onRefresh: () => void;
}

export default function ProjectRow({ id, name, slug, technology, isActive, onRefresh }: ProjectRowProps) {
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

      <ProjectDeleteDialog
        open={deleteOpen}
        onOpenChange={setDeleteOpen}
        name={name}
        slug={slug}
        onConfirm={handleDelete}
      />
      <ProjectResetDialog
        open={resetOpen}
        onOpenChange={setResetOpen}
        name={name}
        slug={slug}
        onConfirm={handleReset}
      />
    </>
  );
}
