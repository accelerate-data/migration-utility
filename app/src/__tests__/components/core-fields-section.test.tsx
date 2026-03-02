import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { CoreFieldsSection } from '@/components/scope/core-fields-section';
import type { ColumnMetadata } from '@/lib/types';

const availableColumns: ColumnMetadata[] = [
  { columnName: 'updated_at', dataType: 'datetime', isNullable: false },
  { columnName: 'created_at', dataType: 'datetime', isNullable: false },
  { columnName: 'id', dataType: 'int', isNullable: false },
];

const baseProps = {
  tableType: null as string | null,
  loadStrategy: null as string | null,
  incrementalColumn: null as string | null,
  dateColumn: null as string | null,
  disabled: false,
  manualOverrides: [] as string[],
  availableColumns,
  onUpdate: vi.fn(),
};

describe('CoreFieldsSection', () => {
  beforeEach(() => vi.clearAllMocks());

  it('renders all four field selects', () => {
    render(<CoreFieldsSection {...baseProps} />);
    expect(screen.getByText('Table type')).toBeInTheDocument();
    expect(screen.getByText('Load strategy')).toBeInTheDocument();
    expect(screen.getByText('CDC column')).toBeInTheDocument();
    expect(screen.getByText('Canonical date column')).toBeInTheDocument();
  });

  it('calls onUpdate with correct key when table type changes', () => {
    render(<CoreFieldsSection {...baseProps} />);
    const selects = screen.getAllByRole('combobox');
    fireEvent.change(selects[0], { target: { value: 'fact' } });
    expect(baseProps.onUpdate).toHaveBeenCalledWith('tableType', 'fact');
  });

  it('calls onUpdate with null when empty option selected', () => {
    render(<CoreFieldsSection {...baseProps} tableType="fact" />);
    const selects = screen.getAllByRole('combobox');
    fireEvent.change(selects[0], { target: { value: '' } });
    expect(baseProps.onUpdate).toHaveBeenCalledWith('tableType', null);
  });

  it('disables all selects when disabled is true', () => {
    render(<CoreFieldsSection {...baseProps} disabled={true} />);
    const selects = screen.getAllByRole('combobox');
    selects.forEach((s) => expect(s).toBeDisabled());
  });

  it('populates column dropdowns from availableColumns', () => {
    render(<CoreFieldsSection {...baseProps} />);
    // CDC column dropdown should have available columns
    expect(screen.getAllByRole('option', { name: /updated_at/ }).length).toBeGreaterThan(0);
    expect(screen.getAllByRole('option', { name: /created_at/ }).length).toBeGreaterThan(0);
  });

  it('shows Agent chip when field has value and not in manualOverrides', () => {
    render(<CoreFieldsSection {...baseProps} tableType="fact" manualOverrides={[]} />);
    expect(screen.getByText('Agent')).toBeInTheDocument();
  });

  it('shows Manual chip when field is in manualOverrides', () => {
    render(<CoreFieldsSection {...baseProps} tableType="fact" manualOverrides={['tableType']} />);
    expect(screen.getByText('Manual')).toBeInTheDocument();
  });

  it('shows no chip when field has no value', () => {
    render(<CoreFieldsSection {...baseProps} tableType={null} />);
    expect(screen.queryByText('Agent')).not.toBeInTheDocument();
    expect(screen.queryByText('Manual')).not.toBeInTheDocument();
  });
});
