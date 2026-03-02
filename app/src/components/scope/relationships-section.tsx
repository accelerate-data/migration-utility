import { useEffect, useState } from 'react';
import { CheckCircle2, XCircle } from 'lucide-react';
import { MultiSelectColumns } from './multi-select-columns';
import { migrationValidateRelationship } from '@/lib/tauri';
import type { Relationship, RelationshipValidationResult, ColumnMetadata } from '@/lib/types';
import { logger } from '@/lib/logger';

interface RelationshipsSectionProps {
  relationshipsJson: Relationship[] | null;
  grainColumns: string[] | null;
  disabled: boolean;
  workspaceId?: string;
  selectedTableId?: string;
  availableColumns?: ColumnMetadata[];
  onUpdateGrain: (value: string[] | null) => void;
  onValidationChange?: (errorCount: number) => void;
}

/** Strip SQL Server bracket notation: [dbo].[Table] → { schema: 'dbo', table: 'Table' } */
function parseTableRef(targetTable: string): { schema: string; table: string } {
  const clean = targetTable.replace(/\[|\]/g, '');
  const dotIdx = clean.indexOf('.');
  if (dotIdx !== -1) {
    return { schema: clean.slice(0, dotIdx), table: clean.slice(dotIdx + 1) };
  }
  return { schema: '', table: clean };
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
  const relationships = relationshipsJson ?? [];
  const grainColumnsList = grainColumns ?? [];

  function handleGrainColumnsUpdate(columns: string[]) {
    onUpdateGrain(columns.length > 0 ? columns : null);
  }

  // Per-relationship array of per-mapping validation results
  const [validationResults, setValidationResults] = useState<Record<number, RelationshipValidationResult[]>>({});

  useEffect(() => {
    if (!workspaceId || !selectedTableId || relationships.length === 0) {
      setValidationResults({});
      onValidationChange?.(0);
      return;
    }

    async function validateAll() {
      const results: Record<number, RelationshipValidationResult[]> = {};
      for (let i = 0; i < relationships.length; i++) {
        const rel = relationships[i];
        const { schema: parentSchema, table: parentTable } = parseTableRef(rel.target_table);
        const mappingResults: RelationshipValidationResult[] = [];
        for (const mapping of rel.mappings) {
          try {
            const result = await migrationValidateRelationship(
              workspaceId!,
              selectedTableId!,
              mapping.source,
              parentSchema,
              parentTable,
              mapping.references
            );
            mappingResults.push(result);
          } catch (err) {
            logger.error('relationship validation failed', err);
            mappingResults.push({
              childColumn: mapping.source,
              parentTable: rel.target_table,
              parentColumn: mapping.references,
              parentTableExists: false,
              childColumnExists: false,
              parentColumnExists: false,
              isValid: false,
              errorMessage: err instanceof Error ? err.message : String(err),
            });
          }
        }
        results[i] = mappingResults;
      }
      setValidationResults(results);

      const errorCount = Object.values(results).filter((mappings) =>
        mappings.some((r) => !r.isValid)
      ).length;
      onValidationChange?.(errorCount);
    }

    void validateAll();
  }, [relationshipsJson, workspaceId, selectedTableId]);

  function getValidationStatusChip(idx: number): React.ReactElement | null {
    const mappings = validationResults[idx];
    if (!mappings) {
      return <span className="text-xs text-muted-foreground">Validating...</span>;
    }

    const allValid = mappings.every(
      (r) => r.parentTableExists && r.childColumnExists && r.parentColumnExists
    );

    if (allValid) {
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

    const invalid = mappings.filter((r) => !r.isValid);
    return (
      <div className="space-y-1">
        <span className="inline-flex items-center gap-1 rounded-full bg-destructive/15 px-2 py-0.5 text-xs font-medium text-destructive">
          <XCircle className="h-3 w-3" />
          Invalid
        </span>
        {invalid.map((r, j) => (
          <div key={j} className="text-xs text-destructive">
            {r.errorMessage && <p>{r.errorMessage}</p>}
            {!r.parentTableExists && <p>Parent table not in scope</p>}
            {!r.childColumnExists && <p>{r.childColumn}: child column not found</p>}
            {!r.parentColumnExists && <p>{r.parentColumn}: parent column not found</p>}
          </div>
        ))}
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
                  <span className="font-mono text-xs font-medium">{rel.target_table}</span>
                  {getValidationStatusChip(idx)}
                </div>
                <div className="space-y-1">
                  {rel.mappings.map((m, j) => (
                    <div key={j} className="flex items-center gap-2 text-xs">
                      <span className="font-mono text-muted-foreground">{m.source}</span>
                      <span className="text-muted-foreground">→</span>
                      <span className="font-mono text-muted-foreground">{m.references}</span>
                    </div>
                  ))}
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
