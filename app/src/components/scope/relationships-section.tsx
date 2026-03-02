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
    <>
      <label className="space-y-1 text-sm">
        <span>Grain columns</span>
        <Input
          value={grainColumns ?? ''}
          disabled={disabled}
          onChange={(e) => onUpdateGrain(e.target.value || null)}
        />
      </label>
      <label className="space-y-1 text-sm md:col-span-2">
        <span>Relationships (required for tests)</span>
        <Input
          value={relationshipsJson ?? ''}
          disabled={disabled}
          onChange={(e) => onUpdateRelationships(e.target.value || null)}
        />
      </label>
    </>
  );
}
