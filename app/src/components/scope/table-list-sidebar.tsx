import { Loader2 } from 'lucide-react';

interface TableRow {
  selectedTableId: string;
  schemaName: string;
  tableName: string;
}

interface TableListSidebarProps {
  grouped: Array<[string, TableRow[]]>;
  activeId: string | null;
  loading: boolean;
  anyAnalyzing: boolean;
  approvalStatusById: Record<string, string | null>;
  validationErrorsById?: Record<string, number>;
  onSelectTable: (id: string) => void;
}

export function TableListSidebar({
  grouped,
  activeId,
  loading,
  anyAnalyzing,
  approvalStatusById,
  validationErrorsById = {},
  onSelectTable,
}: TableListSidebarProps) {
  return (
    <div className="rounded-md border bg-card">
      <div className="max-h-[calc(100vh-280px)] overflow-auto">
        {loading && (
          <div className="flex items-center gap-2 p-4 text-sm text-muted-foreground">
            <Loader2 className="h-3.5 w-3.5 animate-spin" />
            Loading tables...
          </div>
        )}
        {!loading && grouped.length === 0 && (
          <p className="p-4 text-sm text-muted-foreground">No selected tables yet.</p>
        )}
        {!loading &&
          grouped.map(([schema, schemaRows]) => (
            <details key={schema} open className="border-b">
              <summary className="flex cursor-pointer items-center justify-between bg-muted/50 px-3 py-2 text-xs">
                <span className="font-medium">{schema}</span>
                <span className="text-muted-foreground">{schemaRows.length} selected</span>
              </summary>
              {schemaRows.map((row) => {
                const approvalStatus = approvalStatusById[row.selectedTableId];
                const isApproved = approvalStatus === 'approved';
                const errorCount = validationErrorsById[row.selectedTableId] || 0;
                const hasErrors = errorCount > 0;

                return (
                  <button
                    key={row.selectedTableId}
                    type="button"
                    className={`w-full border-t px-3 py-2 text-left text-sm transition-colors duration-150 ${
                      row.selectedTableId === activeId ? 'bg-primary/10' : 'hover:bg-muted/50'
                    }`}
                    disabled={anyAnalyzing}
                    onClick={() => onSelectTable(row.selectedTableId)}
                  >
                    <div className="flex items-center justify-between gap-2">
                      <span className="font-mono text-xs">{row.tableName}</span>
                      <div className="flex items-center gap-1.5">
                        {hasErrors && (
                          <span
                            className="inline-flex items-center justify-center rounded-full bg-destructive/15 px-1.5 py-0.5 text-xs font-medium text-destructive"
                            title={`${errorCount} validation error${errorCount > 1 ? 's' : ''}`}
                          >
                            {errorCount}
                          </span>
                        )}
                        {isApproved && (
                          <span
                            className="text-xs font-medium"
                            style={{ color: 'var(--color-seafoam)' }}
                          >
                            ✓
                          </span>
                        )}
                      </div>
                    </div>
                  </button>
                );
              })}
            </details>
          ))}
      </div>
    </div>
  );
}
