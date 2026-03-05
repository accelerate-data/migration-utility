import { useEffect, useRef, useState } from 'react';
import { CheckCircle2, Loader2, XCircle, AlertTriangle, ChevronDown } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { appStartupSync, listenProjectInitStep } from '@/lib/tauri';
import { GLOBAL_STEPS, PER_PROJECT_STEPS, INIT_STEP_LABEL } from '@/lib/types';
import { logger } from '@/lib/logger';
import type { InitStep, InitStepStatus, Project } from '@/lib/types';

interface SplashProps {
  projects: Project[];
  activeProjectId: string;
  onSuccess: () => void;
  onCancel: () => void;
}

interface StepState {
  step: InitStep;
  status: InitStepStatus | null;
}

function makeSteps(steps: InitStep[]): StepState[] {
  return steps.map((step) => ({ step, status: null }));
}

function projectSummaryStatus(steps: StepState[]): 'pending' | 'running' | 'ok' | 'warning' | 'error' {
  if (steps.some((s) => s.status?.kind === 'error')) return 'error';
  if (steps.some((s) => s.status?.kind === 'warning')) return 'warning';
  if (steps.every((s) => s.status?.kind === 'ok' || s.status?.kind === 'warning')) return 'ok';
  if (steps.some((s) => s.status?.kind === 'running' || s.status === null)) {
    // Only "running" if at least one has started
    if (steps.some((s) => s.status !== null)) return 'running';
  }
  return 'pending';
}

function SummaryIcon({ status }: { status: ReturnType<typeof projectSummaryStatus> }) {
  if (status === 'error') return <XCircle className="h-3.5 w-3.5 shrink-0 text-destructive" />;
  if (status === 'warning') return <AlertTriangle className="h-3.5 w-3.5 shrink-0 text-yellow-500" />;
  if (status === 'ok') return (
    <CheckCircle2 className="h-3.5 w-3.5 shrink-0" style={{ color: 'var(--color-seafoam)' }} />
  );
  if (status === 'running') return (
    <Loader2 className="h-3.5 w-3.5 shrink-0 animate-spin" style={{ color: 'var(--color-pacific)' }} />
  );
  // pending
  return <div className="h-3.5 w-3.5 shrink-0 rounded-full border border-border" />;
}

function StepRow({ step, status }: StepState) {
  const label = INIT_STEP_LABEL[step];
  const isRunning = !status || status.kind === 'running';
  const isOk = status?.kind === 'ok';
  const isWarn = status?.kind === 'warning';
  const isError = status?.kind === 'error';

  return (
    <div className="flex items-start gap-2">
      <div className="mt-0.5 shrink-0">
        {isError && <XCircle className="h-3.5 w-3.5 text-destructive" />}
        {isWarn && <AlertTriangle className="h-3.5 w-3.5 text-yellow-500" />}
        {isOk && <CheckCircle2 className="h-3.5 w-3.5" style={{ color: 'var(--color-seafoam)' }} />}
        {isRunning && <Loader2 className="h-3.5 w-3.5 animate-spin" style={{ color: 'var(--color-pacific)' }} />}
      </div>
      <div className="flex flex-col min-w-0">
        <span className="text-xs text-muted-foreground">{label}</span>
        {isError && status.kind === 'error' && (
          <span className="text-xs text-destructive break-all mt-0.5">{status.message}</span>
        )}
        {isWarn && status.kind === 'warning' && status.warnings.length > 0 && (
          <span className="text-xs text-yellow-600 break-all mt-0.5">{status.warnings[0]}</span>
        )}
      </div>
    </div>
  );
}

function GlobalStepRow({ step, status }: StepState) {
  const label = INIT_STEP_LABEL[step];
  const isRunning = !status || status.kind === 'running';
  const isOk = status?.kind === 'ok';
  const isWarn = status?.kind === 'warning';
  const isError = status?.kind === 'error';

  return (
    <div className="flex items-start gap-2.5">
      <div className="mt-0.5 shrink-0">
        {isError && <XCircle className="h-4 w-4 text-destructive" />}
        {isWarn && <AlertTriangle className="h-4 w-4 text-yellow-500" />}
        {isOk && <CheckCircle2 className="h-4 w-4" style={{ color: 'var(--color-seafoam)' }} />}
        {isRunning && <Loader2 className="h-4 w-4 animate-spin" style={{ color: 'var(--color-pacific)' }} />}
      </div>
      <div className="flex flex-col min-w-0">
        <span className="text-sm">{label}</span>
        {isError && status.kind === 'error' && (
          <span className="text-xs text-destructive break-all mt-0.5">{status.message}</span>
        )}
        {isWarn && status.kind === 'warning' && status.warnings.length > 0 && (
          <span className="text-xs text-yellow-600 break-all mt-0.5">{status.warnings[0]}</span>
        )}
      </div>
    </div>
  );
}

function ProjectRow({
  project,
  steps,
  forceExpanded,
}: {
  project: Project;
  steps: StepState[];
  forceExpanded: boolean;
}) {
  const summary = projectSummaryStatus(steps);
  const [open, setOpen] = useState(false);
  const expanded = open || forceExpanded;

  // Auto-open when a failure is detected.
  const prevSummary = useRef(summary);
  useEffect(() => {
    if (prevSummary.current !== 'error' && summary === 'error') {
      setOpen(true);
    }
    prevSummary.current = summary;
  }, [summary]);

  return (
    <div className="rounded-md border border-border overflow-hidden">
      <button
        type="button"
        className="w-full flex items-center gap-2 px-3 py-2 text-left hover:bg-muted/40 transition-colors"
        onClick={() => setOpen((v) => !v)}
        aria-expanded={expanded}
      >
        <SummaryIcon status={summary} />
        <span className="flex-1 text-sm truncate">{project.name}</span>
        <ChevronDown
          className="h-3.5 w-3.5 text-muted-foreground shrink-0 transition-transform duration-150"
          style={{ transform: expanded ? 'rotate(180deg)' : 'rotate(0deg)' }}
        />
      </button>
      {expanded && (
        <div className="px-3 pb-3 pt-1 flex flex-col gap-2 border-t border-border bg-muted/20">
          {steps.map((s) => (
            <StepRow key={s.step} {...s} />
          ))}
        </div>
      )}
    </div>
  );
}

export default function SplashScreen({ projects, activeProjectId, onSuccess, onCancel }: SplashProps) {
  const [globalSteps, setGlobalSteps] = useState<StepState[]>(makeSteps(GLOBAL_STEPS));
  const [projectSteps, setProjectSteps] = useState<Record<string, StepState[]>>(
    Object.fromEntries(projects.map((p) => [p.id, makeSteps(PER_PROJECT_STEPS)]))
  );
  const [isRunning, setIsRunning] = useState(false);
  const [hasFailed, setHasFailed] = useState(false);

  const unlistenRef = useRef<(() => void) | null>(null);
  const hasStarted = useRef(false);

  useEffect(() => {
    if (hasStarted.current) return;
    hasStarted.current = true;
    void runInit();

    return () => {
      unlistenRef.current?.();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function runInit() {
    setIsRunning(true);
    setHasFailed(false);
    setGlobalSteps(makeSteps(GLOBAL_STEPS));
    setProjectSteps(Object.fromEntries(projects.map((p) => [p.id, makeSteps(PER_PROJECT_STEPS)])));

    try {
      unlistenRef.current = await listenProjectInitStep((ev) => {
        if (ev.projectId) {
          setProjectSteps((prev) => {
            const existing = prev[ev.projectId!] ?? makeSteps(PER_PROJECT_STEPS);
            return {
              ...prev,
              [ev.projectId!]: existing.map((s) =>
                s.step === ev.step ? { ...s, status: ev.status } : s
              ),
            };
          });
        } else {
          setGlobalSteps((prev) =>
            prev.map((s) => (s.step === ev.step ? { ...s, status: ev.status } : s))
          );
        }
      });

      await appStartupSync();
      setIsRunning(false);
      logger.debug('splash: startup sync complete');
      onSuccess();
    } catch (err) {
      logger.error('splash: startup sync failed', err);
      setIsRunning(false);
      setHasFailed(true);
    } finally {
      unlistenRef.current?.();
      unlistenRef.current = null;
    }
  }

  async function handleRetry() {
    hasStarted.current = false;
    unlistenRef.current?.();
    unlistenRef.current = null;
    hasStarted.current = true;
    await runInit();
  }

  const activeProject = projects.find((p) => p.id === activeProjectId);
  const singleProject = projects.length === 1;

  return (
    <div className="flex h-screen items-center justify-center bg-background">
      <div className="w-full max-w-sm flex flex-col gap-5 px-6">
        <div>
          <p className="text-base font-semibold text-foreground tracking-tight">
            {singleProject ? 'Initializing project' : 'Initializing projects'}
          </p>
          <p className="text-sm text-muted-foreground mt-0.5 truncate">
            {activeProject?.name ?? ''}
          </p>
        </div>

        {/* Global steps — always visible */}
        <div className="flex flex-col gap-2.5">
          {globalSteps.map((s) => (
            <GlobalStepRow key={s.step} {...s} />
          ))}
        </div>

        {/* Per-project rows — collapsed by default, auto-open on failure */}
        {projects.length > 0 && (
          <div className="flex flex-col gap-2">
            {projects.map((project) => {
              const steps = projectSteps[project.id] ?? makeSteps(PER_PROJECT_STEPS);
              return (
                <ProjectRow
                  key={project.id}
                  project={project}
                  steps={steps}
                  forceExpanded={singleProject}
                />
              );
            })}
          </div>
        )}

        {hasFailed && !isRunning && (
          <div className="flex gap-2">
            <Button variant="outline" size="sm" onClick={handleRetry} className="flex-1">
              Retry
            </Button>
            <Button variant="ghost" size="sm" onClick={onCancel} className="flex-1">
              Cancel
            </Button>
          </div>
        )}
      </div>
    </div>
  );
}
