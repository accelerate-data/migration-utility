interface ScdSectionProps {
  tableType: string | null;
  snapshotStrategy: string;
  disabled: boolean;
  onUpdate: (value: string) => void;
}

export function ScdSection({ tableType, snapshotStrategy, disabled, onUpdate }: ScdSectionProps) {
  return (
    <label className="space-y-1 text-sm md:col-span-2">
      <span>SCD (dimensions only)</span>
      <select
        className="h-9 w-full rounded-md border bg-background px-3 text-sm"
        value={snapshotStrategy}
        disabled={disabled || tableType !== 'dimension'}
        onChange={(e) => onUpdate(e.target.value)}
      >
        <option value="sample_1day">sample_1day</option>
        <option value="full">full</option>
        <option value="full_flagged">full_flagged</option>
      </select>
    </label>
  );
}
