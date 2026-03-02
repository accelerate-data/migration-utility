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
  onSelectTable: (id: string) => void;
}

export function TableListSidebar({
  grouped,
  activeId,
  loading,
  anyAnalyzing,
  onSelectTable,
}: TableListSidebarProps) {
  return (
    <div className="rounded-md border bg-card">
      <div className="max-h-[calc(100vh-280px)] overflow-auto">
        {loading && <p className="p-3 text-sm text-muted-foreground">Loading details...</p>}
        {!loading && grouped.length === 0 && (
          <p className="p-3 text-sm text-muted-foreground">No selected tables yet.</p>
        )}
        {!loading &&
          grouped.map(([schema, schemaRows]) => (
            <details key={schema} open className="border-b">
              <summary className="flex cursor-pointer items-center justify-between bg-muted/50 px-3 py-2 text-xs">
                <span className="font-medium">{schema}</span>
                <span className="text-muted-foreground">{schemaRows.length} selected</span>
              </summary>
              {schemaRows.map((row) => (
                <button
                  key={row.selectedTableId}
                  type="button"
                  className={`w-full border-t px-3 py-2 text-left text-sm ${
                    row.selectedTableId === activeId ? 'bg-primary/10' : ''
                  }`}
                  disabled={anyAnalyzing}
                  onClick={() => onSelectTable(row.selectedTableId)}
                >
                  <span className="font-mono">{row.tableName}</span>
                </button>
              ))}
            </details>
          ))}
      </div>
    </div>
  );
}
