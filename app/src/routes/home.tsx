import { useProjectStore } from '@/stores/project-store';

// ── Dashboard ─────────────────────────────────────────────────────────────────

export default function HomeSurface() {
  const activeProject = useProjectStore((s) => s.activeProject);

  return (
    <div className="h-full flex flex-col">
      <div className="flex-1 overflow-auto">
        <div className="px-8 py-6">
          <div className="w-full md:w-[60%] md:min-w-[520px] md:max-w-[960px] md:resize-x overflow-auto flex flex-col gap-5" data-testid="home-dashboard">

            {/* Active Migration card */}
            <div>
              <p className="text-sm font-medium text-muted-foreground mb-2">
                Active Migration
              </p>
              <div className="rounded-lg border border-border bg-card p-4 flex flex-col gap-3">
                <p className="text-sm font-semibold">
                  {activeProject ? activeProject.name : '—'}
                </p>
                {activeProject && (
                  <p className="text-xs text-muted-foreground font-mono">{activeProject.slug}</p>
                )}
                <div className="flex items-center gap-3">
                  <div className="flex-1 h-1.5 rounded-full bg-muted overflow-hidden" />
                  <span className="text-sm text-muted-foreground shrink-0">— / — procedures</span>
                </div>
              </div>
            </div>

          </div>
        </div>
      </div>
    </div>
  );
}
