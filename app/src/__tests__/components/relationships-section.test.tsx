import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import { RelationshipsSection } from '@/components/scope/relationships-section';
import type { ColumnMetadata } from '@/lib/types';

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

const sampleRelationships = JSON.stringify([
  {
    child_column: 'customer_id',
    parent_table: 'dbo.dim_customer',
    parent_column: 'customer_id',
    cardinality: 'many_to_one',
  },
]);

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
    expect(screen.getByText('Grain columns')).toBeInTheDocument();
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
    expect(screen.getByText('Relationships')).toBeInTheDocument();
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
    expect(screen.getByText('Relationship 1')).toBeInTheDocument();
    expect(screen.getAllByText('customer_id').length).toBeGreaterThan(0);
    expect(screen.getByText('dbo.dim_customer')).toBeInTheDocument();
  });

  it('displays cardinality with underscores replaced', () => {
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
    expect(screen.getByText('many to one')).toBeInTheDocument();
  });

  it('renders pre-filled grain columns as pills', () => {
    render(
      <RelationshipsSection
        relationshipsJson={null}
        grainColumns="customer_id,order_date"
        disabled={false}
        availableColumns={availableColumns}
        onUpdateGrain={onUpdateGrain}
      />,
    );
    expect(screen.getByText('customer_id')).toBeInTheDocument();
    expect(screen.getByText('order_date')).toBeInTheDocument();
  });

  it('handles invalid JSON gracefully', () => {
    const { container } = render(
      <RelationshipsSection
        relationshipsJson="not-json"
        grainColumns={null}
        disabled={false}
        availableColumns={availableColumns}
        onUpdateGrain={onUpdateGrain}
      />,
    );
    expect(container).not.toBeEmptyDOMElement();
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
