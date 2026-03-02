import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';

interface CoreFieldsSectionProps {
  tableType: string | null;
  loadStrategy: string | null;
  incrementalColumn: string | null;
  dateColumn: string | null;
  disabled: boolean;
  manualOverrides: string[];
  onUpdate: <K extends 'tableType' | 'loadStrategy' | 'incrementalColumn' | 'dateColumn'>(
    key: K,
    value: string | null,
  ) => void;
}

function FieldChip({ fieldName, manualOverrides }: { fieldName: string; manualOverrides: string[] }) {
  const isManual = manualOverrides.includes(fieldName);
  if (!isManual) {
    return (
      <Badge variant="secondary" className="ml-2 bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400">
        Agent
      </Badge>
    );
  }
  return (
    <Badge variant="secondary" className="ml-2 bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-400">
      Manual
    </Badge>
  );
}

export function CoreFieldsSection({
  tableType,
  loadStrategy,
  incrementalColumn,
  dateColumn,
  disabled,
  manualOverrides,
  onUpdate,
}: CoreFieldsSectionProps) {
  return (
    <div className="space-y-4">
      <label className="block space-y-2">
        <span className="text-sm font-medium">
          Table type
          {tableType && <FieldChip fieldName="tableType" manualOverrides={manualOverrides} />}
        </span>
        <select
          className="h-10 w-full rounded-md border bg-background px-3 text-sm"
          value={tableType ?? ''}
          disabled={disabled}
          onChange={(e) => onUpdate('tableType', e.target.value || null)}
        >
          <option value="">Select...</option>
          <option value="fact">fact</option>
          <option value="dimension">dimension</option>
          <option value="unknown">unknown</option>
        </select>
      </label>
      <label className="block space-y-2">
        <span className="text-sm font-medium">
          Load strategy
          {loadStrategy && <FieldChip fieldName="loadStrategy" manualOverrides={manualOverrides} />}
        </span>
        <select
          className="h-10 w-full rounded-md border bg-background px-3 text-sm"
          value={loadStrategy ?? ''}
          disabled={disabled}
          onChange={(e) => onUpdate('loadStrategy', e.target.value || null)}
        >
          <option value="">Select...</option>
          <option value="incremental">incremental</option>
          <option value="full_refresh">full_refresh</option>
          <option value="snapshot">snapshot</option>
        </select>
      </label>
      <label className="block space-y-2">
        <span className="text-sm font-medium">
          CDC column
          {incrementalColumn && <FieldChip fieldName="incrementalColumn" manualOverrides={manualOverrides} />}
        </span>
        <Input
          value={incrementalColumn ?? ''}
          disabled={disabled}
          onChange={(e) => onUpdate('incrementalColumn', e.target.value || null)}
        />
      </label>
      <label className="block space-y-2">
        <span className="text-sm font-medium">
          Canonical date column
          {dateColumn && <FieldChip fieldName="dateColumn" manualOverrides={manualOverrides} />}
        </span>
        <Input
          value={dateColumn ?? ''}
          disabled={disabled}
          onChange={(e) => onUpdate('dateColumn', e.target.value || null)}
        />
      </label>
    </div>
  );
}
