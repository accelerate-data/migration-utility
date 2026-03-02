import { MultiSelectColumns } from './multi-select-columns';
import type { ColumnMetadata } from '@/lib/types';

interface PiiSectionProps {
  piiColumns: string | null;
  disabled: boolean;
  availableColumns?: ColumnMetadata[];
  onUpdate: (value: string | null) => void;
}

export function PiiSection({ piiColumns, disabled, availableColumns = [], onUpdate }: PiiSectionProps) {
  let selectedColumns: string[] = [];
  try {
    if (piiColumns) {
      selectedColumns = JSON.parse(piiColumns);
    }
  } catch {
    // Invalid JSON, show raw input
  }

  function handleUpdate(columns: string[]) {
    onUpdate(columns.length > 0 ? JSON.stringify(columns) : null);
  }

  return (
    <div className="space-y-2">
      <span className="text-sm font-medium">PII columns (required for fixture masking)</span>
      
      <MultiSelectColumns
        selectedColumns={selectedColumns}
        availableColumns={availableColumns}
        disabled={disabled}
        placeholder="Type to search and add PII columns..."
        onUpdate={handleUpdate}
      />
    </div>
  );
}
