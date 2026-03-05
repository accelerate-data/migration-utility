import { useEffect, useRef } from 'react';
import { CheckCircle2, Loader2, XCircle } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { projectInit, listenProjectInitStep } from '@/lib/tauri';
import { INIT_STEP_LABEL } from '@/lib/types';
import { logger } from '@/lib/logger';
import { useProjectStore } from '@/stores/project-store';
import type { InitStep } from '@/lib/types';

interface SplashProps {
  projectId: string;
  projectName: string;
  onSuccess: () => void;
  onCancel: () => void;
}

export default function SplashScreen({ projectId, projectName, onSuccess, onCancel }: SplashProps) {
  const { initSteps, isInitRunning, startInit, finishInit, applyInitStep } = useProjectStore();
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
    startInit();
    try {
      unlistenRef.current = await listenProjectInitStep((ev) => applyInitStep(ev));
      await projectInit(projectId);
      finishInit();
      logger.debug('splash: init complete for', projectId);
      onSuccess();
    } catch (err) {
      logger.error('splash: init failed', err);
      finishInit();
      // Steps remain visible with error state — user can retry or cancel.
    } finally {
      unlistenRef.current?.();
      unlistenRef.current = null;
    }
  }

  async function handleRetry() {
    unlistenRef.current?.();
    unlistenRef.current = null;
    await runInit();
  }

  const hasFailed = initSteps.some((s) => s.status?.kind === 'error');
  const allDone = initSteps.length > 0 && initSteps.every((s) => s.status?.kind === 'ok');

  return (
    <div className="flex h-screen items-center justify-center bg-background">
      <div className="w-full max-w-sm flex flex-col gap-6 px-6">
        <div>
          <p className="text-base font-semibold text-foreground tracking-tight">
            Initializing project
          </p>
          <p className="text-sm text-muted-foreground mt-0.5 truncate">{projectName}</p>
        </div>

        <div className="flex flex-col gap-2.5">
          {initSteps.map(({ step, status }) => {
            const label = INIT_STEP_LABEL[step as InitStep];
            const isRunning = !status || status.kind === 'running';
            const isOk = status?.kind === 'ok';
            const isError = status?.kind === 'error';

            return (
              <div key={step} className="flex items-start gap-2.5">
                {isRunning && !allDone && (
                  <Loader2
                    className="h-4 w-4 mt-0.5 shrink-0 animate-spin"
                    style={{ color: 'var(--color-pacific)' }}
                  />
                )}
                {isOk && (
                  <CheckCircle2
                    className="h-4 w-4 mt-0.5 shrink-0"
                    style={{ color: 'var(--color-seafoam)' }}
                  />
                )}
                {isError && (
                  <XCircle className="h-4 w-4 mt-0.5 shrink-0 text-destructive" />
                )}
                {isRunning && allDone && (
                  <CheckCircle2
                    className="h-4 w-4 mt-0.5 shrink-0"
                    style={{ color: 'var(--color-seafoam)' }}
                  />
                )}
                <div className="flex flex-col min-w-0">
                  <span
                    className="text-sm"
                    style={
                      isOk
                        ? { color: 'var(--color-seafoam)' }
                        : isError
                        ? undefined
                        : undefined
                    }
                  >
                    {label}
                  </span>
                  {isError && status.kind === 'error' && (
                    <span className="text-xs text-destructive break-all mt-0.5">
                      {status.message}
                    </span>
                  )}
                </div>
              </div>
            );
          })}
        </div>

        {hasFailed && !isInitRunning && (
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
