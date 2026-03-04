import { Square } from 'lucide-react';
import { useWorkflowStore } from '@/stores/workflow-store';
import { Button } from '@/components/ui/button';

export default function HomeSurface() {
  const { workspaceId, appPhase } = useWorkflowStore();
  const isRunning = appPhase === 'running_locked';

  return (
    <div className="flex-1 overflow-auto">
    <div className="px-8 py-6">
    <div className="w-full md:w-[60%] md:min-w-[520px] md:max-w-[960px] md:resize-x overflow-auto flex flex-col gap-5" data-testid="home-dashboard-state">

      {/* Active Migration card */}
      <div>
        <p className="text-sm font-medium text-muted-foreground mb-2">
          Active Migration
        </p>
        <div className="rounded-lg border border-border bg-card p-4 flex flex-col gap-3">
          {/* Title row */}
          <div className="flex items-start justify-between">
            <p className="text-sm font-semibold">{workspaceId ?? '—'}</p>
            {isRunning && (
              <div
                className="flex items-center gap-1.5 text-sm font-medium"
                style={{ color: 'var(--color-pacific)' }}
              >
                <span
                  className="w-1.5 h-1.5 rounded-full animate-pulse shrink-0"
                  style={{ backgroundColor: 'var(--color-pacific)' }}
                  aria-hidden="true"
                />
                Pipeline running
              </div>
            )}
          </div>

          {/* Progress bar + procedure count */}
          <div className="flex items-center gap-3">
            <div className="flex-1 h-1.5 rounded-full bg-muted overflow-hidden">
              <div
                className="h-full rounded-full transition-all duration-200"
                style={{
                  width: isRunning ? '60%' : '0%',
                  backgroundColor: 'var(--color-pacific)',
                }}
              />
            </div>
            <span className="text-sm text-muted-foreground shrink-0">— / — procedures</span>
          </div>

          {/* Status chips (only when running) */}
          {isRunning && (
            <div className="flex items-center gap-4 text-sm font-medium">
              <span className="flex items-center gap-1.5" style={{ color: 'var(--color-seafoam)' }}>
                <span
                  className="w-1.5 h-1.5 rounded-full shrink-0"
                  style={{ backgroundColor: 'var(--color-seafoam)' }}
                  aria-hidden="true"
                />
                — complete
              </span>
              <span className="flex items-center gap-1.5" style={{ color: 'var(--color-pacific)' }}>
                <span
                  className="w-1.5 h-1.5 rounded-full shrink-0"
                  style={{ backgroundColor: 'var(--color-pacific)' }}
                  aria-hidden="true"
                />
                — running
              </span>
              <span className="flex items-center gap-1.5 text-amber-600 dark:text-amber-400">
                <span className="w-1.5 h-1.5 rounded-full shrink-0 bg-amber-500" aria-hidden="true" />
                — blocked
              </span>
            </div>
          )}
        </div>
      </div>

      {/* Quick Actions */}
      <div>
        <p className="text-sm font-medium text-muted-foreground mb-2">
          Quick Actions
        </p>
        <div className="rounded-lg border border-border bg-card p-2 flex flex-col gap-1">
          <Button
            variant="ghost"
            className="w-full justify-start text-sm gap-2 text-destructive hover:text-destructive"
            data-testid="btn-cancel-migration"
          >
            <Square size={13} aria-hidden="true" />
            Cancel migration
          </Button>
        </div>
      </div>
    </div>
    </div>
    </div>
  );
}
