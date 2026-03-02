import { Input } from '@/components/ui/input';

interface CoreFieldsSectionProps {
  tableType: string | null;
  loadStrategy: string | null;
  incrementalColumn: string | null;
  dateColumn: string | null;
  disabled: boolean;
  onUpdate: <K extends 'tableType' | 'loadStrategy' | 'incrementalColumn' | 'dateColumn'>(
    key: K,
    value: string | null,
  ) => void;
}

export function CoreFieldsSection({
  tableType,
  loadStrategy,
  incrementalColumn,
  dateColumn,
  disabled,
  onUpdate,
}: CoreFieldsSectionProps) {
  return (
    <div className="space-y-4">
      <label className="block space-y-2">
        <span className="text-sm font-medium">Table type</span>
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
        <span className="text-sm font-medium">Load strategy</span>
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
        <span className="text-sm font-medium">CDC column</span>
        <Input
          value={incrementalColumn ?? ''}
          disabled={disabled}
          onChange={(e) => onUpdate('incrementalColumn', e.target.value || null)}
        />
      </label>
      <label className="block space-y-2">
        <span className="text-sm font-medium">Canonical date column</span>
        <Input
          value={dateColumn ?? ''}
          disabled={disabled}
          onChange={(e) => onUpdate('dateColumn', e.target.value || null)}
        />
      </label>
    </div>
  );
}
