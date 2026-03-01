import { useNavigate } from 'react-router';
import { Settings, Activity, LayoutGrid, CheckCircle2, Square } from 'lucide-react';
import { useWorkflowStore, SCOPE_STEPS, SCOPE_STEP_LABELS } from '@/stores/workflow-store';
import { Button } from '@/components/ui/button';

// ── Setup state (no workspace configured) ────────────────────────────────────

function SetupState() {
  const navigate = useNavigate();
  return (
    <div className="flex-1 overflow-auto px-8 py-6">
      <div className="w-full md:w-[60%] md:min-w-[520px] md:max-w-[960px] md:resize-x overflow-auto flex flex-col items-center justify-center gap-[14px] min-h-full" data-testid="home-setup-state">
        <div
          className="w-14 h-14 rounded-full flex items-center justify-center"
          style={{ backgroundColor: 'color-mix(in oklch, var(--color-pacific), transparent 90%)' }}
        >
          <Settings size={26} style={{ color: 'var(--color-pacific)' }} aria-hidden="true" />
        </div>
        <p className="text-base font-semibold">Setup required</p>
        <p className="text-sm text-muted-foreground text-center leading-relaxed max-w-xs">
          Connect GitHub, add your Anthropic API key, and configure your Fabric workspace before
          starting a migration.
        </p>
        <Button
          data-testid="btn-go-to-settings"
          onClick={() => navigate('/settings')}
          className="mt-2"
        >
          <Settings size={13} aria-hidden="true" />
          Go to Settings
        </Button>
      </div>
    </div>
  );
}

// ── Dashboard state (workspace configured) ───────────────────────────────────

// Placeholder counts until scope data is wired from the backend.
const STEP_COUNTS: Record<string, string> = {
  scope: '— tables',
  config: '— / —',
};

function DashboardState() {
  const navigate = useNavigate();
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

      {/* Two-column: Setup + Quick Actions */}
      <div className="grid grid-cols-2 gap-4">

        {/* Setup summary */}
        <div>
          <p className="text-sm font-medium text-muted-foreground mb-2">
            Setup
          </p>
          <div className="rounded-lg border border-border bg-card divide-y divide-border">
            {SCOPE_STEPS.map((step) => (
              <div key={step} className="flex items-center gap-2.5 px-3 py-2.5">
                <CheckCircle2
                  size={14}
                  style={{ color: 'var(--color-seafoam)' }}
                  aria-hidden="true"
                />
                <span className="text-sm font-medium flex-1">{SCOPE_STEP_LABELS[step]}</span>
                <span className="text-sm text-muted-foreground">{STEP_COUNTS[step]}</span>
              </div>
            ))}
          </div>
        </div>

        {/* Quick Actions */}
        <div>
          <p className="text-sm font-medium text-muted-foreground mb-2">
            Quick Actions
          </p>
          <div className="rounded-lg border border-border bg-card p-2 flex flex-col gap-1">
            <Button
              variant="outline"
              className="w-full justify-start text-sm gap-2"
              data-testid="btn-open-monitor"
              onClick={() => navigate('/monitor')}
            >
              <Activity size={13} aria-hidden="true" />
              Open Monitor
            </Button>
            <Button
              variant="outline"
              className="w-full justify-start text-sm gap-2"
              data-testid="btn-review-scope"
              onClick={() => navigate('/scope')}
            >
              <LayoutGrid size={13} aria-hidden="true" />
              Review Scope
            </Button>
            <Button
              variant="ghost"
              className="w-full justify-start text-sm gap-2 text-destructive hover:text-destructive mt-1"
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
    </div>
  );
}

// ── Root ─────────────────────────────────────────────────────────────────────

export default function HomeSurface() {
  const appPhase = useWorkflowStore((s) => s.appPhase);

  return (
    <div className="h-full flex flex-col">
      {appPhase === 'setup_required' ? <SetupState /> : <DashboardState />}
    </div>
  );
}
