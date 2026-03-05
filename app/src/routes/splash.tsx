import { useEffect, useRef, useState } from 'react';
import { CheckCircle2, Loader2, XCircle, AlertTriangle } from 'lucide-react';
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

function StepRow({ step, status }: StepState) {
  const label = INIT_STEP_LABEL[step];
  const isRunning = !status || status.kind === 'running';
  const isOk = status?.kind === 'ok';
  const isWarn = status?.kind === 'warning';
  const isError = status?.kind === 'error';

  return (
    <div className="flex items-start gap-2.5">
      {isError && <XCircle className="h-4 w-4 mt-0.5 shrink-0 text-destructive" />}
      {isWarn && <AlertTriangle className="h-4 w-4 mt-0.5 shrink-0 text-yellow-500" />}
      {(isOk) && (
        <CheckCircle2
          className="h-4 w-4 mt-0.5 shrink-0"
          style={{ color: 'var(--color-seafoam)' }}
        />
      )}
      {isRunning && (
        <Loader2
          className="h-4 w-4 mt-0.5 shrink-0 animate-spin"
          style={{ color: 'var(--color-pacific)' }}
        />
      )}
      <div className="flex flex-col min-w-0">
        <span
          className="text-sm"
          style={isOk || isWarn ? { color: 'var(--color-seafoam)' } : undefined}
        >
          {label}
        </span>
        {isError && status.kind === 'error' && (
          <span className="text-xs text-destructive break-all mt-0.5">{status.message}</span>
        )}
        {isWarn && status.kind === 'warning' && status.warnings.length > 0 && (
          <span className="text-xs text-yellow-600 break-all mt-0.5">
            {status.warnings[0]}
          </span>
        )}
      </div>
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
          // Per-project step event
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
          // Global step event (gitPull, dockerCheck)
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
  const multiProject = projects.length > 1;

  return (
    <div className="flex h-screen items-center justify-center bg-background">
      <div className="w-full max-w-sm flex flex-col gap-6 px-6">
        <div>
          <p className="text-base font-semibold text-foreground tracking-tight">
            {multiProject ? 'Initializing projects' : 'Initializing project'}
          </p>
          <p className="text-sm text-muted-foreground mt-0.5 truncate">
            {activeProject?.name ?? ''}
          </p>
        </div>

        {/* Global steps */}
        <div className="flex flex-col gap-2.5">
          {globalSteps.map((s) => (
            <StepRow key={s.step} {...s} />
          ))}
        </div>

        {/* Per-project steps */}
        {projects.map((project) => {
          const steps = projectSteps[project.id] ?? makeSteps(PER_PROJECT_STEPS);
          return (
            <div key={project.id} className="flex flex-col gap-1.5">
              {multiProject && (
                <p className="text-xs font-medium text-muted-foreground truncate">{project.name}</p>
              )}
              <div className="flex flex-col gap-2.5">
                {steps.map((s) => (
                  <StepRow key={s.step} {...s} />
                ))}
              </div>
            </div>
          );
        })}

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
