import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';

interface PiiSectionProps {
  piiColumns: string | null;
  disabled: boolean;
  onUpdate: (value: string | null) => void;
}

export function PiiSection({ piiColumns, disabled, onUpdate }: PiiSectionProps) {
  let columns: string[] = [];
  try {
    if (piiColumns) {
      columns = JSON.parse(piiColumns);
    }
  } catch {
    // Invalid JSON, show raw input
  }

  return (
    <div className="space-y-2">
      <span className="text-sm font-medium">PII columns (required for fixture masking)</span>
      
      {columns.length > 0 ? (
        <div className="flex flex-wrap gap-2 p-3 border rounded-md bg-muted/50">
          {columns.map((col, idx) => (
            <Badge key={idx} variant="secondary" className="font-mono">
              {col}
            </Badge>
          ))}
        </div>
      ) : (
        <div className="text-sm text-muted-foreground p-3 border rounded-md bg-muted/50">
          No PII columns detected
        </div>
      )}
      
      {/* Raw JSON input for manual editing */}
      <details className="text-xs">
        <summary className="cursor-pointer text-muted-foreground hover:text-foreground">
          Edit raw JSON
        </summary>
        <Input
          className="mt-2 font-mono text-xs"
          value={piiColumns ?? ''}
          disabled={disabled}
          onChange={(e) => onUpdate(e.target.value || null)}
          placeholder='["email", "phone", "ssn"]'
        />
      </details>
    </div>
  );
}
