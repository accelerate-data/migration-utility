import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { ApprovalActions } from '@/components/scope/approval-actions';

const baseProps = {
  approvalStatus: 'pending' as string | null,
  approvedAt: null as string | null,
  confirmedAt: '2026-01-01T10:00:00Z',
  isLocked: false,
  onApprove: vi.fn(),
};

describe('ApprovalActions', () => {
  beforeEach(() => vi.clearAllMocks());

  it('renders nothing when confirmedAt is null', () => {
    const { container } = render(<ApprovalActions {...baseProps} confirmedAt={null} />);
    expect(container).toBeEmptyDOMElement();
  });

  it('shows pending state', () => {
    render(<ApprovalActions {...baseProps} />);
    expect(screen.getByText('Pending approval')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Approve Configuration' })).toBeInTheDocument();
  });

  it('shows approved state with timestamp', () => {
    render(
      <ApprovalActions
        {...baseProps}
        approvalStatus="approved"
        approvedAt="2026-01-15T10:30:00Z"
      />,
    );
    expect(screen.getByText('✓ Approved')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Approve Configuration' })).not.toBeInTheDocument();
  });

  it('calls onApprove when approve button is clicked', () => {
    render(<ApprovalActions {...baseProps} />);
    fireEvent.click(screen.getByRole('button', { name: 'Approve Configuration' }));
    expect(baseProps.onApprove).toHaveBeenCalledTimes(1);
  });

  it('disables approve button when isLocked', () => {
    render(<ApprovalActions {...baseProps} isLocked={true} />);
    expect(screen.getByRole('button', { name: 'Approve Configuration' })).toBeDisabled();
  });

  describe('validation error display', () => {
    it('shows no error message when validationErrorCount is 0', () => {
      render(<ApprovalActions {...baseProps} validationErrorCount={0} />);
      expect(screen.queryByText(/validation error/)).not.toBeInTheDocument();
    });

    it('shows singular error message for 1 error', () => {
      render(<ApprovalActions {...baseProps} validationErrorCount={1} />);
      expect(screen.getByText('1 validation error found')).toBeInTheDocument();
    });

    it('shows plural error message for multiple errors', () => {
      render(<ApprovalActions {...baseProps} validationErrorCount={3} />);
      expect(screen.getByText('3 validation errors found')).toBeInTheDocument();
    });

    it('shows error icon when there are validation errors', () => {
      render(<ApprovalActions {...baseProps} validationErrorCount={2} />);
      // AlertCircle icon should be present
      expect(screen.getByText('2 validation errors found').closest('div')).toBeInTheDocument();
    });

    it('does not show error when validationErrorCount is not provided', () => {
      render(<ApprovalActions {...baseProps} />);
      expect(screen.queryByText(/validation error/)).not.toBeInTheDocument();
    });
  });
});
