import { Input } from '@/components/ui/input';

interface PiiSectionProps {
  piiColumns: string | null;
  disabled: boolean;
  onUpdate: (value: string | null) => void;
}

export function PiiSection({ piiColumns, disabled, onUpdate }: PiiSectionProps) {
  return (
    <label className="space-y-1 text-sm md:col-span-2">
      <span>PII columns (required for fixture masking)</span>
      <Input
        value={piiColumns ?? ''}
        disabled={disabled}
        onChange={(e) => onUpdate(e.target.value || null)}
      />
    </label>
  );
}
