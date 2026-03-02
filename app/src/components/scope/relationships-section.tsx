import { useEffect, useState } from 'react';
import { Input } from '@/components/ui/input';
import { migrationValidateRelationship } from '@/lib/tauri';
import type { RelationshipValidationResult } from '@/lib/types';
import { logger } from '@/lib/logger';

interface RelationshipsSectionProps {
  relationshipsJson: string | null;
  grainColumns: string | null;
  disabled: boolean;
  workspaceId?: string;
  selectedTableId?: string;
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
  workspaceId,
  selectedTableId,
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

  const [validationResults, setValidationResults] = useState<Record<number, RelationshipValidationResult>>({});

  // Validate relationships when they change
  useEffect(() => {
    if (!workspaceId || !selectedTableId || relationships.length === 0) {
      setValidationResults({});
      return;
    }

    async function validateAll() {
      const results: Record<number, RelationshipValidationResult> = {};
      for (let i = 0; i < relationships.length; i++) {
        const rel = relationships[i];
        try {
          // Parse parent_table to extract schema and table name
          const parts = rel.parent_table.split('.');
          const parentSchema = parts.length === 2 ? parts[0] : '';
          const parentTable = parts.length === 2 ? parts[1] : rel.parent_table;

          const result = await migrationValidateRelationship(
            workspaceId!,
            selectedTableId!,
            rel.child_column,
            parentSchema,
            parentTable,
            rel.parent_column
          );
          results[i] = result;
        } catch (err) {
          logger.error('relationship validation failed', err);
          results[i] = {
            childColumn: rel.child_column,
            parentTable: rel.parent_table,
            parentColumn: rel.parent_column,
            parentTableExists: false,
            childColumnExists: false,
            parentColumnExists: false,
            isValid: false,
            errorMessage: err instanceof Error ? err.message : String(err),
          };
        }
      }
      setValidationResults(results);
    }

    void validateAll();
  }, [relationshipsJson, workspaceId, selectedTableId]);

  function getValidationStatusChip(idx: number): React.ReactElement | null {
    const result = validationResults[idx];
    if (!result) {
      return <span className="text-xs text-muted-foreground">Validating...</span>;
    }

    const isValid = result.parentTableExists && result.childColumnExists && result.parentColumnExists;
    
    if (isValid) {
      return (
        <span className="inline-flex items-center rounded-full bg-green-100 px-2 py-0.5 text-xs font-medium text-green-800 dark:bg-green-900/30 dark:text-green-400">
          Valid
        </span>
      );
    }

    return (
      <div className="space-y-1">
        <span className="inline-flex items-center rounded-full bg-red-100 px-2 py-0.5 text-xs font-medium text-red-800 dark:bg-red-900/30 dark:text-red-400">
          Invalid
        </span>
        {result.errorMessage && (
          <p className="text-xs text-destructive">{result.errorMessage}</p>
        )}
        {!result.parentTableExists && (
          <p className="text-xs text-destructive">Parent table not in scope</p>
        )}
        {!result.childColumnExists && (
          <p className="text-xs text-destructive">Child column not found</p>
        )}
        {!result.parentColumnExists && (
          <p className="text-xs text-destructive">Parent column not found</p>
        )}
      </div>
    );
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
                <div className="mb-2 flex items-center justify-between">
                  <span className="text-xs font-medium text-muted-foreground">Relationship {idx + 1}</span>
                  {getValidationStatusChip(idx)}
                </div>
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
