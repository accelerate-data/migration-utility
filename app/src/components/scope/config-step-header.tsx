import { Button } from '@/components/ui/button';

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
  return (
    <header className="rounded-md border bg-card p-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="space-y-1">
          <p className="text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">
            Select Tables for migration
          </p>
          <div className="flex flex-wrap items-center gap-2">
            <span className="rounded-full bg-muted px-2 py-0.5 text-xs font-medium text-foreground">
              {selectedCount} selected
            </span>
            <span className="rounded-full bg-primary/20 px-2 py-0.5 text-xs font-medium text-primary">
              {readyCount} / {totalCount} tables ready
            </span>
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
            {refreshing ? 'Refreshing...' : 'Refresh schema'}
          </Button>
          <Button type="button" size="sm" disabled={isLocked || anyAnalyzing} onClick={onFinalizeScope}>
            {isLocked ? 'Scope Finalized' : 'Finalize Scope'}
          </Button>
        </div>
      </div>
      <div className="mt-4 border-b border-border">
        <div className="flex items-center gap-6">
          <button
            type="button"
            className={
              activeStep === 'select'
                ? 'border-b-2 border-primary pb-2 text-sm font-medium text-primary'
                : 'border-b-2 border-transparent pb-2 text-sm font-medium text-muted-foreground'
            }
            disabled={anyAnalyzing}
            onClick={onNavigateToSelect}
          >
            1. Select Tables
          </button>
          <button
            type="button"
            className={
              activeStep === 'details'
                ? 'border-b-2 border-primary pb-2 text-sm font-medium text-primary'
                : 'border-b-2 border-transparent pb-2 text-sm font-medium text-muted-foreground'
            }
            disabled={anyAnalyzing}
            onClick={onNavigateToConfig}
          >
            2. Table Details
          </button>
        </div>
      </div>
    </header>
  );
}
