import { Button } from '@/components/ui/button';

interface ConfigStepHeaderProps {
  readyCount: number;
  totalCount: number;
  needsDetails: number;
  message: string;
  isLocked: boolean;
  refreshing: boolean;
  anyAnalyzing: boolean;
  onRefreshSchema: () => void;
  onFinalizeScope: () => void;
  onNavigateToSelect: () => void;
  onNavigateToDetails: () => void;
}

export function ConfigStepHeader({
  readyCount,
  totalCount,
  needsDetails,
  message,
  isLocked,
  refreshing,
  anyAnalyzing,
  onRefreshSchema,
  onFinalizeScope,
  onNavigateToSelect,
  onNavigateToDetails,
}: ConfigStepHeaderProps) {
  return (
    <header className="rounded-md border bg-card p-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="space-y-1">
          <p className="text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">
            Scope — Table details capture
          </p>
          <div className="flex flex-wrap items-center gap-2">
            <span className="rounded-full bg-primary/20 px-2 py-0.5 text-xs font-medium text-primary">
              {readyCount} / {totalCount} tables ready
            </span>
            <span className="text-xs text-muted-foreground">Needs details for {needsDetails} tables</span>
            <span className="text-xs text-muted-foreground">{message}</span>
            <span className="text-xs text-muted-foreground">
              {isLocked ? 'Scope finalized (read-only)' : 'Scope editable'}
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
            className="border-b-2 border-transparent pb-2 text-sm font-medium text-muted-foreground"
            disabled={anyAnalyzing}
            onClick={onNavigateToSelect}
          >
            1. Select Tables
          </button>
          <button
            type="button"
            className="border-b-2 border-primary pb-2 text-sm font-medium text-primary"
            disabled={anyAnalyzing}
            onClick={onNavigateToDetails}
          >
            2. Table Details
          </button>
        </div>
      </div>
    </header>
  );
}
