import { AlertTriangle, CheckCircle2, Loader2, X, XCircle } from 'lucide-react';
import { INIT_STEP_LABEL } from '@/lib/types';
import type { InitStep, InitStepStatus } from '@/lib/types';

// ── Types ────────────────────────────────────────────────────────────────────

export interface StepState {
  step: InitStep;
  status: InitStepStatus | null;
}

// ── StepRow ──────────────────────────────────────────────────────────────────

export function StepRow({ step, status }: StepState) {
  const label = INIT_STEP_LABEL[step];
  const pending = !status;
  const isRunning = status?.kind === 'running';
  const isOk = status?.kind === 'ok';
  const isWarn = status?.kind === 'warning';
  const isError = status?.kind === 'error';

  return (
    <div className="flex items-start gap-2">
      <div className="mt-0.5 shrink-0">
        {pending && (
          <span className="h-4 w-4 flex items-center justify-center">
            <span className="h-2 w-2 rounded-full bg-muted-foreground/30" />
          </span>
        )}
        {isRunning && (
          <Loader2 className="h-4 w-4 animate-spin" style={{ color: 'var(--color-pacific)' }} />
        )}
        {isOk && (
          <CheckCircle2 className="h-4 w-4" style={{ color: 'var(--color-seafoam)' }} />
        )}
        {isWarn && (
          <AlertTriangle className="h-4 w-4 text-amber-600 dark:text-amber-400" />
        )}
        {isError && (
          <XCircle className="h-4 w-4 text-destructive" />
        )}
      </div>
      <div className="flex flex-col min-w-0">
        <span className={`text-sm ${pending ? 'text-muted-foreground' : 'text-foreground'}`}>
          {label}
        </span>
        {isError && status.kind === 'error' && (
          <span className="text-xs text-destructive break-all mt-0.5">{status.message}</span>
        )}
        {isWarn && status.kind === 'warning' && status.warnings.length > 0 && (
          <ul className="mt-0.5 flex flex-col gap-0.5">
            {status.warnings.map((w, i) => (
              <li key={i} className="text-xs text-amber-600 dark:text-amber-400 break-all">{w}</li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}

// ── StepProgress ─────────────────────────────────────────────────────────────

interface StepProgressProps {
  steps: StepState[];
  isRunning: boolean;
  onDismiss: () => void;
  title?: string;
}

export default function StepProgress({ steps, isRunning, onDismiss, title = 'Initializing project...' }: StepProgressProps) {
  if (!isRunning && steps.length === 0) return null;

  return (
    <div className="rounded-lg border border-border bg-card p-4 flex flex-col gap-2">
      <div className="flex items-center justify-between">
        <p className="text-sm font-semibold text-foreground">{title}</p>
        {!isRunning && (
          <button
            type="button"
            onClick={onDismiss}
            className="text-muted-foreground hover:text-foreground transition-colors"
            aria-label="Dismiss"
          >
            <X className="h-4 w-4" />
          </button>
        )}
      </div>
      {steps.map((s) => (
        <StepRow key={s.step} {...s} />
      ))}
    </div>
  );
}
