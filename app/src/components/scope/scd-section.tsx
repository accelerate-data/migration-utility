interface ScdSectionProps {
  tableType: string | null;
  snapshotStrategy: string;
  disabled: boolean;
  onUpdate: (value: string) => void;
}

export function ScdSection({ tableType, snapshotStrategy, disabled, onUpdate }: ScdSectionProps) {
  return (
    <label className="block space-y-2">
      <span className="text-sm font-medium">SCD (dimensions only)</span>
      <select
        className="h-10 w-full rounded-md border bg-background px-3 text-sm"
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
