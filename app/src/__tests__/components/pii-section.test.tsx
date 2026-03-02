import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import { PiiSection } from '@/components/scope/pii-section';
import type { ColumnMetadata } from '@/lib/types';

const availableColumns: ColumnMetadata[] = [
  { columnName: 'customer_email', dataType: 'varchar', isNullable: true },
  { columnName: 'customer_phone', dataType: 'varchar', isNullable: true },
  { columnName: 'id', dataType: 'int', isNullable: false },
];

describe('PiiSection', () => {
  const onUpdate = vi.fn();

  beforeEach(() => vi.clearAllMocks());

  it('renders the section label', () => {
    render(
      <PiiSection piiColumns={null} disabled={false} availableColumns={availableColumns} onUpdate={onUpdate} />,
    );
    expect(screen.getByText(/PII columns/)).toBeInTheDocument();
  });

  it('renders with no selected columns when piiColumns is null', () => {
    render(
      <PiiSection piiColumns={null} disabled={false} availableColumns={availableColumns} onUpdate={onUpdate} />,
    );
    expect(screen.queryByText('customer_email')).not.toBeInTheDocument();
  });

  it('renders selected columns as pills', () => {
    render(
      <PiiSection
        piiColumns={['customer_email', 'customer_phone']}
        disabled={false}
        availableColumns={availableColumns}
        onUpdate={onUpdate}
      />,
    );
    expect(screen.getByText('customer_email')).toBeInTheDocument();
    expect(screen.getByText('customer_phone')).toBeInTheDocument();
  });

  it('renders empty input when piiColumns is an empty array', () => {
    render(
      <PiiSection piiColumns={[]} disabled={false} availableColumns={availableColumns} onUpdate={onUpdate} />,
    );
    expect(screen.queryByText('customer_email')).not.toBeInTheDocument();
    expect(screen.getByPlaceholderText(/search and add PII columns/i)).toBeInTheDocument();
  });

  it('renders placeholder text', () => {
    render(
      <PiiSection piiColumns={null} disabled={false} availableColumns={availableColumns} onUpdate={onUpdate} />,
    );
    expect(screen.getByPlaceholderText(/search and add PII columns/i)).toBeInTheDocument();
  });
});
