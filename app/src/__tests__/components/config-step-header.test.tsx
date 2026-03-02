import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { ConfigStepHeader } from '@/components/scope/config-step-header';

describe('ConfigStepHeader', () => {
  const defaultProps = {
    readyCount: 5,
    approvedCount: 3,
    totalCount: 10,
    needsDetails: 5,
    message: 'All systems operational',
    isLocked: false,
    refreshing: false,
    anyAnalyzing: false,
    onRefreshSchema: vi.fn(),
    onFinalizeScope: vi.fn(),
    onNavigateToSelect: vi.fn(),
    onNavigateToConfig: vi.fn(),
  };

  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('Progress Display', () => {
    it('renders ready count correctly', () => {
      render(<ConfigStepHeader {...defaultProps} />);
      expect(screen.getByText('5 / 10 tables ready')).toBeInTheDocument();
    });

    it('renders approved count correctly', () => {
      render(<ConfigStepHeader {...defaultProps} />);
      expect(screen.getByText('3 approved')).toBeInTheDocument();
    });

    it('renders needs details count correctly', () => {
      render(<ConfigStepHeader {...defaultProps} />);
      expect(screen.getByText(/5 pending approvals/)).toBeInTheDocument();
    });

    it('updates counts when props change', () => {
      const { rerender } = render(<ConfigStepHeader {...defaultProps} />);
      expect(screen.getByText('5 / 10 tables ready')).toBeInTheDocument();

      rerender(<ConfigStepHeader {...defaultProps} readyCount={8} approvedCount={6} />);
      expect(screen.getByText('8 / 10 tables ready')).toBeInTheDocument();
      expect(screen.getByText('6 approved')).toBeInTheDocument();
    });

    it('displays message text', () => {
      render(<ConfigStepHeader {...defaultProps} />);
      expect(screen.getByText('All systems operational')).toBeInTheDocument();
    });

    it('shows locked status when isLocked is true', () => {
      render(<ConfigStepHeader {...defaultProps} isLocked={true} />);
      expect(screen.getByText('Scope finalized (read-only)')).toBeInTheDocument();
    });

    it('shows editable status when isLocked is false', () => {
      render(<ConfigStepHeader {...defaultProps} isLocked={false} />);
      expect(screen.getByText('Scope editable')).toBeInTheDocument();
    });
  });

  describe('Action Buttons', () => {
    it('renders Refresh Schema button', () => {
      render(<ConfigStepHeader {...defaultProps} />);
      expect(screen.getByRole('button', { name: 'Refresh schema' })).toBeInTheDocument();
    });

    it('renders Finalize Scope button', () => {
      render(<ConfigStepHeader {...defaultProps} />);
      expect(screen.getByRole('button', { name: 'Finalize Scope' })).toBeInTheDocument();
    });

    it('calls onRefreshSchema when Refresh Schema button is clicked', () => {
      render(<ConfigStepHeader {...defaultProps} />);
      fireEvent.click(screen.getByRole('button', { name: 'Refresh schema' }));
      expect(defaultProps.onRefreshSchema).toHaveBeenCalledTimes(1);
    });

    it('calls onFinalizeScope when Finalize Scope button is clicked', () => {
      render(<ConfigStepHeader {...defaultProps} />);
      fireEvent.click(screen.getByRole('button', { name: 'Finalize Scope' }));
      expect(defaultProps.onFinalizeScope).toHaveBeenCalledTimes(1);
    });
  });

  describe('Loading States', () => {
    it('disables Refresh Schema button when refreshing', () => {
      render(<ConfigStepHeader {...defaultProps} refreshing={true} />);
      expect(screen.getByRole('button', { name: 'Refreshing...' })).toBeDisabled();
    });

    it('shows "Refreshing..." text when refreshing', () => {
      render(<ConfigStepHeader {...defaultProps} refreshing={true} />);
      expect(screen.getByText('Refreshing...')).toBeInTheDocument();
    });

    it('disables buttons when anyAnalyzing is true', () => {
      render(<ConfigStepHeader {...defaultProps} anyAnalyzing={true} />);
      expect(screen.getByRole('button', { name: 'Refresh schema' })).toBeDisabled();
      expect(screen.getByRole('button', { name: 'Finalize Scope' })).toBeDisabled();
    });

    it('disables buttons when isLocked is true', () => {
      render(<ConfigStepHeader {...defaultProps} isLocked={true} />);
      expect(screen.getByRole('button', { name: 'Refresh schema' })).toBeDisabled();
      expect(screen.getByRole('button', { name: 'Scope Finalized' })).toBeDisabled();
    });

    it('changes Finalize Scope button text when locked', () => {
      render(<ConfigStepHeader {...defaultProps} isLocked={true} />);
      expect(screen.getByRole('button', { name: 'Scope Finalized' })).toBeInTheDocument();
    });
  });

  describe('Navigation Tabs', () => {
    it('renders Select Tables tab', () => {
      render(<ConfigStepHeader {...defaultProps} />);
      expect(screen.getByRole('button', { name: '1. Select Tables' })).toBeInTheDocument();
    });

    it('renders Table Details tab', () => {
      render(<ConfigStepHeader {...defaultProps} />);
      expect(screen.getByRole('button', { name: '2. Table Details' })).toBeInTheDocument();
    });

    it('calls onNavigateToSelect when Select Tables tab is clicked', () => {
      render(<ConfigStepHeader {...defaultProps} />);
      fireEvent.click(screen.getByRole('button', { name: '1. Select Tables' }));
      expect(defaultProps.onNavigateToSelect).toHaveBeenCalledTimes(1);
    });

    it('calls onNavigateToConfig when Table Details tab is clicked', () => {
      render(<ConfigStepHeader {...defaultProps} />);
      fireEvent.click(screen.getByRole('button', { name: '2. Table Details' }));
      expect(defaultProps.onNavigateToConfig).toHaveBeenCalledTimes(1);
    });

    it('disables navigation tabs when anyAnalyzing is true', () => {
      render(<ConfigStepHeader {...defaultProps} anyAnalyzing={true} />);
      expect(screen.getByRole('button', { name: '1. Select Tables' })).toBeDisabled();
      expect(screen.getByRole('button', { name: '2. Table Details' })).toBeDisabled();
    });
  });

  describe('Edge Cases', () => {
    it('handles zero counts correctly', () => {
      render(
        <ConfigStepHeader
          {...defaultProps}
          readyCount={0}
          approvedCount={0}
          totalCount={0}
          needsDetails={0}
        />,
      );
      expect(screen.getByText('0 / 0 tables ready')).toBeInTheDocument();
      expect(screen.getByText('0 approved')).toBeInTheDocument();
      expect(screen.getByText('No pending approvals')).toBeInTheDocument();
    });

    it('handles all tables ready', () => {
      render(<ConfigStepHeader {...defaultProps} readyCount={10} totalCount={10} needsDetails={0} />);
      expect(screen.getByText('10 / 10 tables ready')).toBeInTheDocument();
      expect(screen.getByText('No pending approvals')).toBeInTheDocument();
    });

    it('handles all tables approved', () => {
      render(<ConfigStepHeader {...defaultProps} approvedCount={10} totalCount={10} />);
      expect(screen.getByText('10 approved')).toBeInTheDocument();
    });

    it('handles empty message', () => {
      render(<ConfigStepHeader {...defaultProps} message="" />);
      // Component should still render without errors
      expect(screen.getByText('5 / 10 tables ready')).toBeInTheDocument();
    });
  });
});
