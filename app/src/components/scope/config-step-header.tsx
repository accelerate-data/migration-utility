import { CheckCircle2, Lock, RefreshCw } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';

interface ConfigStepHeaderProps {
  selectedCount: number;
  readyCount: number;
  totalCount: number;
  activeStep: 'select' | 'details';
  isLocked: boolean;
  refreshing: boolean;
  anyAnalyzing: boolean;
  onRefreshSchema: () => void;
  onFinalizeScope: () => void;
  onNavigateToSelect: () => void;
  onNavigateToConfig: () => void;
}

export function ConfigStepHeader({
  selectedCount,
  readyCount,
  totalCount,
  activeStep,
  isLocked,
  refreshing,
  anyAnalyzing,
  onRefreshSchema,
  onFinalizeScope,
  onNavigateToSelect,
  onNavigateToConfig,
}: ConfigStepHeaderProps) {
  const allReady = totalCount > 0 && readyCount === totalCount;

  return (
    <header className="rounded-lg border bg-card shadow-sm">
      <div className="flex flex-wrap items-center justify-between gap-3 px-4 pb-3 pt-4">
        <div className="space-y-1.5">
          <p className="text-[11px] font-semibold uppercase tracking-widest text-muted-foreground">
            Migration Scope
          </p>
          <div className="flex flex-wrap items-center gap-1.5">
            <span className="rounded-full bg-muted px-2 py-0.5 text-xs font-medium text-foreground">
              {selectedCount} selected
            </span>
            {selectedCount > 0 && (
              <span
                className="flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-medium"
                style={{
                  background: allReady
                    ? 'color-mix(in oklch, var(--color-seafoam), transparent 82%)'
                    : 'color-mix(in oklch, var(--color-pacific), transparent 88%)',
                  color: allReady ? 'var(--color-seafoam)' : 'var(--color-pacific)',
                }}
              >
                {allReady && <CheckCircle2 className="size-3" />}
                {readyCount} / {totalCount} ready
              </span>
            )}
          </div>
        </div>

        <div className="flex items-center gap-2">
          <Button
            type="button"
            variant="outline"
            size="sm"
            disabled={isLocked || refreshing || anyAnalyzing}
            onClick={onRefreshSchema}
          >
            <RefreshCw className={cn('size-3.5', refreshing && 'animate-spin')} />
            {refreshing ? 'Refreshing…' : 'Refresh schema'}
          </Button>
          <Button
            type="button"
            size="sm"
            disabled={isLocked || anyAnalyzing}
            onClick={onFinalizeScope}
          >
            {isLocked && <Lock className="size-3.5" />}
            {isLocked ? 'Scope locked' : 'Finalize scope'}
          </Button>
        </div>
      </div>

      <div className="border-t border-border px-4">
        <div className="flex items-center">
          <button
            type="button"
            className={cn(
              'flex items-center gap-1.5 border-b-2 py-2.5 pr-5 text-xs font-medium transition-colors duration-150',
              activeStep === 'select'
                ? 'border-primary text-foreground'
                : 'border-transparent text-muted-foreground hover:text-foreground',
            )}
            disabled={anyAnalyzing}
            onClick={onNavigateToSelect}
          >
            <span
              className={cn(
                'flex size-4 items-center justify-center rounded-full text-[10px] font-semibold leading-none',
                activeStep === 'select'
                  ? 'bg-primary text-primary-foreground'
                  : 'bg-muted text-muted-foreground',
              )}
            >
              1
            </span>{' '}
            Select tables
          </button>
          <button
            type="button"
            className={cn(
              'flex items-center gap-1.5 border-b-2 py-2.5 pl-2 pr-5 text-xs font-medium transition-colors duration-150',
              activeStep === 'details'
                ? 'border-primary text-foreground'
                : 'border-transparent text-muted-foreground hover:text-foreground',
            )}
            disabled={anyAnalyzing}
            onClick={onNavigateToConfig}
          >
            <span
              className={cn(
                'flex size-4 items-center justify-center rounded-full text-[10px] font-semibold leading-none',
                activeStep === 'details'
                  ? 'bg-primary text-primary-foreground'
                  : 'bg-muted text-muted-foreground',
              )}
            >
              2
            </span>{' '}
            Table details
          </button>
        </div>
      </div>
    </header>
  );
}
