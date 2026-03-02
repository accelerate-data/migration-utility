import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import { RelationshipsSection } from '@/components/scope/relationships-section';
import type { ColumnMetadata, Relationship } from '@/lib/types';

// Mock the tauri command — validation is async and requires backend
vi.mock('@/lib/tauri', () => ({
  migrationValidateRelationship: vi.fn().mockResolvedValue({
    childColumn: 'customer_id',
    parentTable: 'dbo.dim_customer',
    parentColumn: 'customer_id',
    parentTableExists: true,
    childColumnExists: true,
    parentColumnExists: true,
    isValid: true,
    errorMessage: null,
  }),
}));

const availableColumns: ColumnMetadata[] = [
  { columnName: 'customer_id', dataType: 'int', isNullable: false },
  { columnName: 'order_date', dataType: 'datetime', isNullable: true },
];

const sampleRelationships: Relationship[] = [
  {
    target_table: 'dbo.dim_customer',
    mappings: [{ source: 'customer_id', references: 'customer_id' }],
    confidence: 0.95,
    reasoning: null,
  },
];

describe('RelationshipsSection', () => {
  const onUpdateGrain = vi.fn();
  const onValidationChange = vi.fn();

  beforeEach(() => vi.clearAllMocks());

  it('renders grain columns section', () => {
    render(
      <RelationshipsSection
        relationshipsJson={null}
        grainColumns={null}
        disabled={false}
        availableColumns={availableColumns}
        onUpdateGrain={onUpdateGrain}
      />,
    );
    expect(screen.getByText('Keys and grain')).toBeInTheDocument();
  });

  it('renders relationships section heading', () => {
    render(
      <RelationshipsSection
        relationshipsJson={null}
        grainColumns={null}
        disabled={false}
        availableColumns={availableColumns}
        onUpdateGrain={onUpdateGrain}
      />,
    );
    expect(screen.getByText('Relationships (required for tests)')).toBeInTheDocument();
  });

  it('shows empty state when no relationships', () => {
    render(
      <RelationshipsSection
        relationshipsJson={null}
        grainColumns={null}
        disabled={false}
        availableColumns={availableColumns}
        onUpdateGrain={onUpdateGrain}
      />,
    );
    expect(screen.getByText('No relationships detected by agent analysis.')).toBeInTheDocument();
  });

  it('renders relationship cards when relationships exist', () => {
    render(
      <RelationshipsSection
        relationshipsJson={sampleRelationships}
        grainColumns={null}
        disabled={false}
        workspaceId="ws-1"
        selectedTableId="st-1"
        availableColumns={availableColumns}
        onUpdateGrain={onUpdateGrain}
      />,
    );
    expect(screen.getAllByText('customer_id').length).toBeGreaterThan(0);
    expect(screen.getByText('dbo.dim_customer')).toBeInTheDocument();
  });

  it('renders mapping arrow between source and references columns', () => {
    render(
      <RelationshipsSection
        relationshipsJson={sampleRelationships}
        grainColumns={null}
        disabled={false}
        workspaceId="ws-1"
        selectedTableId="st-1"
        availableColumns={availableColumns}
        onUpdateGrain={onUpdateGrain}
      />,
    );
    expect(screen.getByText('→')).toBeInTheDocument();
  });

  it('renders pre-filled grain columns as pills', () => {
    render(
      <RelationshipsSection
        relationshipsJson={null}
        grainColumns={['customer_id', 'order_date']}
        disabled={false}
        availableColumns={availableColumns}
        onUpdateGrain={onUpdateGrain}
      />,
    );
    expect(screen.getByText('customer_id')).toBeInTheDocument();
    expect(screen.getByText('order_date')).toBeInTheDocument();
  });

  it('shows empty state when relationships is an empty array', () => {
    render(
      <RelationshipsSection
        relationshipsJson={[]}
        grainColumns={null}
        disabled={false}
        availableColumns={availableColumns}
        onUpdateGrain={onUpdateGrain}
      />,
    );
    expect(screen.getByText('No relationships detected by agent analysis.')).toBeInTheDocument();
  });

  it('shows validating state initially when workspaceId provided', () => {
    render(
      <RelationshipsSection
        relationshipsJson={sampleRelationships}
        grainColumns={null}
        disabled={false}
        workspaceId="ws-1"
        selectedTableId="st-1"
        availableColumns={availableColumns}
        onUpdateGrain={onUpdateGrain}
        onValidationChange={onValidationChange}
      />,
    );
    // Initially shows "Validating..." before async resolves
    expect(screen.getByText('Validating...')).toBeInTheDocument();
  });
});
