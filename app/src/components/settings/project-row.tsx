import { useState } from 'react';
import { Loader2, RefreshCw, Trash2 } from 'lucide-react';
import { toast } from 'sonner';
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
import { Switch } from '@/components/ui/switch';
import { projectDeleteFull, projectResetLocal, tauriErrorMessage } from '@/lib/tauri';
import { TECHNOLOGY_LABEL } from '@/lib/types';
import { logger } from '@/lib/logger';
import { useProjectInit } from '@/hooks/use-project-init';
import { useProjectStore } from '@/stores/project-store';

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
