import { useEffect, useState } from 'react';
import { Loader2, Plus } from 'lucide-react';
import { Button } from '@/components/ui/button';
import StepProgress from '@/components/step-progress';
import SettingsPanelShell from '@/components/settings/settings-panel-shell';
import CreateProjectDialog from '@/components/settings/create-project-dialog';
import ProjectRow from '@/components/settings/project-row';
import { useProjectStore } from '@/stores/project-store';

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
