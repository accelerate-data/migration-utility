import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { ScdSection } from '@/components/scope/scd-section';

describe('ScdSection', () => {
  const onUpdate = vi.fn();

  beforeEach(() => vi.clearAllMocks());

  it('renders the SCD select', () => {
    render(
      <ScdSection tableType="dimension" snapshotStrategy="sample_1day" disabled={false} onUpdate={onUpdate} />,
    );
    expect(screen.getByRole('combobox')).toBeInTheDocument();
  });

  it('is disabled when tableType is not dimension', () => {
    render(
      <ScdSection tableType="fact" snapshotStrategy="sample_1day" disabled={false} onUpdate={onUpdate} />,
    );
    expect(screen.getByRole('combobox')).toBeDisabled();
  });

  it('is disabled when tableType is null', () => {
    render(
      <ScdSection tableType={null} snapshotStrategy="sample_1day" disabled={false} onUpdate={onUpdate} />,
    );
    expect(screen.getByRole('combobox')).toBeDisabled();
  });

  it('is enabled when tableType is dimension', () => {
    render(
      <ScdSection tableType="dimension" snapshotStrategy="sample_1day" disabled={false} onUpdate={onUpdate} />,
    );
    expect(screen.getByRole('combobox')).not.toBeDisabled();
  });

  it('is disabled when disabled prop is true even for dimension', () => {
    render(
      <ScdSection tableType="dimension" snapshotStrategy="sample_1day" disabled={true} onUpdate={onUpdate} />,
    );
    expect(screen.getByRole('combobox')).toBeDisabled();
  });

  it('shows current snapshotStrategy as selected', () => {
    render(
      <ScdSection tableType="dimension" snapshotStrategy="full" disabled={false} onUpdate={onUpdate} />,
    );
    expect(screen.getByRole('combobox')).toHaveValue('full');
  });

  it('calls onUpdate when selection changes', () => {
    render(
      <ScdSection tableType="dimension" snapshotStrategy="sample_1day" disabled={false} onUpdate={onUpdate} />,
    );
    fireEvent.change(screen.getByRole('combobox'), { target: { value: 'full_flagged' } });
    expect(onUpdate).toHaveBeenCalledWith('full_flagged');
  });

  it('renders all strategy options', () => {
    render(
      <ScdSection tableType="dimension" snapshotStrategy="sample_1day" disabled={false} onUpdate={onUpdate} />,
    );
    expect(screen.getByRole('option', { name: 'sample_1day' })).toBeInTheDocument();
    expect(screen.getByRole('option', { name: 'full' })).toBeInTheDocument();
    expect(screen.getByRole('option', { name: 'full_flagged' })).toBeInTheDocument();
  });
});
