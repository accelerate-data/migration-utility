import { Input } from '@/components/ui/input';

interface RelationshipsSectionProps {
  relationshipsJson: string | null;
  grainColumns: string | null;
  disabled: boolean;
  onUpdateRelationships: (value: string | null) => void;
  onUpdateGrain: (value: string | null) => void;
}

interface Relationship {
  child_column: string;
  parent_table: string;
  parent_column: string;
  cardinality: string;
}

export function RelationshipsSection({
  relationshipsJson,
  grainColumns,
  disabled,
  onUpdateRelationships,
  onUpdateGrain,
}: RelationshipsSectionProps) {
  let relationships: Relationship[] = [];
  try {
    if (relationshipsJson) {
      relationships = JSON.parse(relationshipsJson);
    }
  } catch {
    // Invalid JSON, show raw input
  }

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
      
      <div className="space-y-2">
        <span className="text-sm font-medium">Relationships</span>
        {relationships.length > 0 ? (
          <div className="space-y-2">
            {relationships.map((rel, idx) => (
              <div key={idx} className="p-3 border rounded-md bg-muted/50 text-sm">
                <div className="grid grid-cols-2 gap-2">
                  <div>
                    <span className="text-muted-foreground">Child column:</span>{' '}
                    <span className="font-mono">{rel.child_column}</span>
                  </div>
                  <div>
                    <span className="text-muted-foreground">Parent table:</span>{' '}
                    <span className="font-mono">{rel.parent_table}</span>
                  </div>
                  <div>
                    <span className="text-muted-foreground">Parent column:</span>{' '}
                    <span className="font-mono">{rel.parent_column}</span>
                  </div>
                  <div>
                    <span className="text-muted-foreground">Cardinality:</span>{' '}
                    <span>{rel.cardinality.replace(/_/g, ' ')}</span>
                  </div>
                </div>
              </div>
            ))}
          </div>
        ) : (
          <div className="text-sm text-muted-foreground p-3 border rounded-md bg-muted/50">
            No relationships detected
          </div>
        )}
        
        {/* Raw JSON input for manual editing */}
        <details className="text-xs">
          <summary className="cursor-pointer text-muted-foreground hover:text-foreground">
            Edit raw JSON
          </summary>
          <Input
            className="mt-2 font-mono text-xs"
            value={relationshipsJson ?? ''}
            disabled={disabled}
            onChange={(e) => onUpdateRelationships(e.target.value || null)}
            placeholder='[{"child_column":"id","parent_table":"parent","parent_column":"id","cardinality":"many_to_one"}]'
          />
        </details>
      </div>
    </div>
  );
}
