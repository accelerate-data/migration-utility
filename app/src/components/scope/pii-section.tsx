import { Input } from '@/components/ui/input';

interface PiiSectionProps {
  piiColumns: string | null;
  disabled: boolean;
  onUpdate: (value: string | null) => void;
}

export function PiiSection({ piiColumns, disabled, onUpdate }: PiiSectionProps) {
  return (
    <label className="block space-y-2">
      <span className="text-sm font-medium">PII columns (required for fixture masking)</span>
      <Input
        value={piiColumns ?? ''}
        disabled={disabled}
        onChange={(e) => onUpdate(e.target.value || null)}
      />
    </label>
  );
}
