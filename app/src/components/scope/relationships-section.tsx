import { Input } from '@/components/ui/input';

interface RelationshipsSectionProps {
  relationshipsJson: string | null;
  grainColumns: string | null;
  disabled: boolean;
  onUpdateRelationships: (value: string | null) => void;
  onUpdateGrain: (value: string | null) => void;
}

export function RelationshipsSection({
  relationshipsJson,
  grainColumns,
  disabled,
  onUpdateRelationships,
  onUpdateGrain,
}: RelationshipsSectionProps) {
  return (
    <div className="space-y-4">
      <label className="block space-y-2">
        <span className="text-sm font-medium">Grain columns</span>
        <Input
          value={grainColumns ?? ''}
          disabled={disabled}
          onChange={(e) => onUpdateGrain(e.target.value || null)}
        />
      </label>
      <label className="block space-y-2">
        <span className="text-sm font-medium">Relationships (required for tests)</span>
        <Input
          value={relationshipsJson ?? ''}
          disabled={disabled}
          onChange={(e) => onUpdateRelationships(e.target.value || null)}
        />
      </label>
    </div>
  );
}
