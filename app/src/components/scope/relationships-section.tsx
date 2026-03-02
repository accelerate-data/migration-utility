import { useEffect, useState } from 'react';
import { CheckCircle2, XCircle } from 'lucide-react';
import { MultiSelectColumns } from './multi-select-columns';
import { migrationValidateRelationship } from '@/lib/tauri';
import type { RelationshipValidationResult, ColumnMetadata } from '@/lib/types';
import { logger } from '@/lib/logger';

interface RelationshipsSectionProps {
  relationshipsJson: string | null;
  grainColumns: string | null;
  disabled: boolean;
  workspaceId?: string;
  selectedTableId?: string;
  availableColumns?: ColumnMetadata[];
  onUpdateGrain: (value: string | null) => void;
  onValidationChange?: (errorCount: number) => void;
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
  availableColumns = [],
  onUpdateGrain,
  onValidationChange,
}: RelationshipsSectionProps) {
  let relationships: Relationship[] = [];
  try {
    if (relationshipsJson) {
      relationships = JSON.parse(relationshipsJson);
    }
  } catch {
    // Invalid JSON, show raw input
  }

  // Parse grain columns from comma-separated string
  const grainColumnsList: string[] = grainColumns
    ? grainColumns.split(',').map((c) => c.trim()).filter(Boolean)
    : [];

  function handleGrainColumnsUpdate(columns: string[]) {
    onUpdateGrain(columns.length > 0 ? columns.join(',') : null);
  }

  const [validationResults, setValidationResults] = useState<Record<number, RelationshipValidationResult>>({});

  // Validate relationships when they change
  useEffect(() => {
    if (!workspaceId || !selectedTableId || relationships.length === 0) {
      setValidationResults({});
      onValidationChange?.(0);
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
      
      // Count errors and report to parent
      const errorCount = Object.values(results).filter(r => !r.isValid).length;
      onValidationChange?.(errorCount);
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
        <span
          className="inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-medium"
          style={{
            backgroundColor: 'color-mix(in oklch, var(--color-seafoam), transparent 85%)',
            color: 'var(--color-seafoam)',
          }}
        >
          <CheckCircle2 className="h-3 w-3" />
          Valid
        </span>
      );
    }

    return (
      <div className="space-y-1">
        <span className="inline-flex items-center gap-1 rounded-full bg-destructive/15 px-2 py-0.5 text-xs font-medium text-destructive">
          <XCircle className="h-3 w-3" />
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
      <div className="space-y-2">
        <span className="text-sm font-medium">Grain columns</span>
        <MultiSelectColumns
          selectedColumns={grainColumnsList}
          availableColumns={availableColumns}
          disabled={disabled}
          placeholder="Type to search and add grain columns..."
          onUpdate={handleGrainColumnsUpdate}
        />
      </div>
      
      <div className="space-y-2">
        <span className="text-sm font-medium">Relationships</span>
        {relationships.length > 0 ? (
        <div className="space-y-2">
            {relationships.map((rel, idx) => (
              <div key={idx} className="rounded-md border bg-muted/50 p-4 text-sm">
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
          <div className="rounded-md border bg-muted/50 p-4 text-sm text-muted-foreground">
            No relationships detected by agent analysis.
          </div>
        )}
      </div>
    </div>
  );
}
