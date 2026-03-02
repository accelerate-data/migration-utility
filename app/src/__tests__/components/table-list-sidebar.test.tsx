import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { TableListSidebar } from '@/components/scope/table-list-sidebar';

describe('TableListSidebar', () => {
  const mockOnSelectTable = vi.fn();

  const sampleTables = [
    { selectedTableId: '1', schemaName: 'dbo', tableName: 'customers' },
    { selectedTableId: '2', schemaName: 'dbo', tableName: 'orders' },
    { selectedTableId: '3', schemaName: 'sales', tableName: 'products' },
    { selectedTableId: '4', schemaName: 'sales', tableName: 'invoices' },
  ];

  const defaultProps = {
    grouped: [
      ['dbo', [sampleTables[0], sampleTables[1]]],
      ['sales', [sampleTables[2], sampleTables[3]]],
    ] as [string, typeof sampleTables][],
    activeId: null,
    loading: false,
    anyAnalyzing: false,
    approvalStatusById: {},
    validationErrorsById: {},
    onSelectTable: mockOnSelectTable,
  };

  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('Schema Grouping', () => {
    it('renders schema groups correctly', () => {
      render(<TableListSidebar {...defaultProps} />);
      expect(screen.getByText('dbo')).toBeInTheDocument();
      expect(screen.getByText('sales')).toBeInTheDocument();
    });

    it('displays table count per schema', () => {
      render(<TableListSidebar {...defaultProps} />);
      const counts = screen.getAllByText('2 selected');
      expect(counts).toHaveLength(2);
    });

    it('renders all schema groups when multiple exist', () => {
      render(<TableListSidebar {...defaultProps} />);
      const schemaHeaders = screen.getAllByText(/selected$/);
      expect(schemaHeaders).toHaveLength(2);
    });
  });

  describe('Table List Rendering', () => {
    it('renders all table names', () => {
      render(<TableListSidebar {...defaultProps} />);
      expect(screen.getByText('customers')).toBeInTheDocument();
      expect(screen.getByText('orders')).toBeInTheDocument();
      expect(screen.getByText('products')).toBeInTheDocument();
      expect(screen.getByText('invoices')).toBeInTheDocument();
    });

    it('renders tables under correct schema groups', () => {
      render(<TableListSidebar {...defaultProps} />);
      const dboSummary = screen.getByText('dbo');
      expect(dboSummary.closest('details')).toBeTruthy();
    });
  });

  describe('Approval Status Indicators', () => {
    it('displays green checkmark for approved tables', () => {
      const props = {
        ...defaultProps,
        approvalStatusById: { '1': 'approved' },
      };
      render(<TableListSidebar {...props} />);
      const checkmarks = screen.getAllByText('✓');
      expect(checkmarks).toHaveLength(1);
    });

    it('does not display checkmark for pending tables', () => {
      const props = {
        ...defaultProps,
        approvalStatusById: { '1': 'pending' },
      };
      render(<TableListSidebar {...props} />);
      expect(screen.queryByText('✓')).not.toBeInTheDocument();
    });

    it('displays checkmarks for multiple approved tables', () => {
      const props = {
        ...defaultProps,
        approvalStatusById: {
          '1': 'approved',
          '2': 'approved',
          '3': 'approved',
        },
      };
      render(<TableListSidebar {...props} />);
      const checkmarks = screen.getAllByText('✓');
      expect(checkmarks).toHaveLength(3);
    });

    it('does not display checkmark when approval status is null', () => {
      const props = {
        ...defaultProps,
        approvalStatusById: { '1': null },
      };
      render(<TableListSidebar {...props} />);
      expect(screen.queryByText('✓')).not.toBeInTheDocument();
    });
  });

  describe('Validation Error Indicators', () => {
    it('displays error badge with count for tables with validation errors', () => {
      const props = {
        ...defaultProps,
        validationErrorsById: { '1': 3 },
      };
      render(<TableListSidebar {...props} />);
      expect(screen.getByText('3')).toBeInTheDocument();
    });

    it('displays correct error count for multiple errors', () => {
      const props = {
        ...defaultProps,
        validationErrorsById: { '1': 5 },
      };
      render(<TableListSidebar {...props} />);
      expect(screen.getByText('5')).toBeInTheDocument();
    });

    it('does not display error badge when error count is zero', () => {
      const props = {
        ...defaultProps,
        validationErrorsById: { '1': 0 },
      };
      const { container } = render(<TableListSidebar {...props} />);
      const errorBadges = container.querySelectorAll('.bg-red-100');
      expect(errorBadges).toHaveLength(0);
    });

    it('displays error badges for multiple tables with errors', () => {
      const props = {
        ...defaultProps,
        validationErrorsById: {
          '1': 2,
          '3': 1,
        },
      };
      render(<TableListSidebar {...props} />);
      expect(screen.getByText('2')).toBeInTheDocument();
      expect(screen.getByText('1')).toBeInTheDocument();
    });

    it('displays both error badge and approval checkmark when applicable', () => {
      const props = {
        ...defaultProps,
        approvalStatusById: { '1': 'approved' },
        validationErrorsById: { '1': 2 },
      };
      render(<TableListSidebar {...props} />);
      expect(screen.getByText('2')).toBeInTheDocument();
      expect(screen.getByText('✓')).toBeInTheDocument();
    });
  });

  describe('Table Selection', () => {
    it('calls onSelectTable when table is clicked', () => {
      render(<TableListSidebar {...defaultProps} />);
      fireEvent.click(screen.getByText('customers'));
      expect(mockOnSelectTable).toHaveBeenCalledWith('1');
    });

    it('calls onSelectTable with correct table ID', () => {
      render(<TableListSidebar {...defaultProps} />);
      fireEvent.click(screen.getByText('products'));
      expect(mockOnSelectTable).toHaveBeenCalledWith('3');
    });

    it('highlights active table', () => {
      const props = {
        ...defaultProps,
        activeId: '1',
      };
      const { container } = render(<TableListSidebar {...props} />);
      const activeButton = container.querySelector('.bg-primary\\/10');
      expect(activeButton).toBeTruthy();
      expect(activeButton?.textContent).toContain('customers');
    });

    it('does not highlight non-active tables', () => {
      const props = {
        ...defaultProps,
        activeId: '1',
      };
      const { container } = render(<TableListSidebar {...props} />);
      const allButtons = container.querySelectorAll('button');
      const highlightedButtons = container.querySelectorAll('.bg-primary\\/10');
      expect(highlightedButtons).toHaveLength(1);
      expect(allButtons.length).toBeGreaterThan(1);
    });
  });

  describe('Disabled State', () => {
    it('disables table buttons when anyAnalyzing is true', () => {
      const props = {
        ...defaultProps,
        anyAnalyzing: true,
      };
      render(<TableListSidebar {...props} />);
      const buttons = screen.getAllByRole('button');
      buttons.forEach((button) => {
        expect(button).toBeDisabled();
      });
    });

    it('does not call onSelectTable when disabled', () => {
      const props = {
        ...defaultProps,
        anyAnalyzing: true,
      };
      render(<TableListSidebar {...props} />);
      fireEvent.click(screen.getByText('customers'));
      expect(mockOnSelectTable).not.toHaveBeenCalled();
    });

    it('enables table buttons when anyAnalyzing is false', () => {
      const props = {
        ...defaultProps,
        anyAnalyzing: false,
      };
      render(<TableListSidebar {...props} />);
      const buttons = screen.getAllByRole('button');
      buttons.forEach((button) => {
        expect(button).not.toBeDisabled();
      });
    });
  });

  describe('Empty and Loading States', () => {
    it('displays loading message when loading is true', () => {
      const props = {
        ...defaultProps,
        loading: true,
      };
      render(<TableListSidebar {...props} />);
      expect(screen.getByText('Loading tables...')).toBeInTheDocument();
    });

    it('does not display table list when loading', () => {
      const props = {
        ...defaultProps,
        loading: true,
      };
      render(<TableListSidebar {...props} />);
      expect(screen.queryByText('customers')).not.toBeInTheDocument();
    });

    it('displays empty state message when no tables exist', () => {
      const props = {
        ...defaultProps,
        grouped: [] as [string, typeof sampleTables][],
      };
      render(<TableListSidebar {...props} />);
      expect(screen.getByText('No selected tables yet.')).toBeInTheDocument();
    });

    it('does not display empty state when tables exist', () => {
      render(<TableListSidebar {...defaultProps} />);
      expect(screen.queryByText('No selected tables yet.')).not.toBeInTheDocument();
    });

    it('does not display loading or empty state when tables are loaded', () => {
      render(<TableListSidebar {...defaultProps} />);
      expect(screen.queryByText('Loading details...')).not.toBeInTheDocument();
      expect(screen.queryByText('No selected tables yet.')).not.toBeInTheDocument();
      expect(screen.getByText('customers')).toBeInTheDocument();
    });
  });

  describe('Edge Cases', () => {
    it('handles single schema with single table', () => {
      const props = {
        ...defaultProps,
        grouped: [['dbo', [sampleTables[0]]]] as [string, typeof sampleTables][],
      };
      render(<TableListSidebar {...props} />);
      expect(screen.getByText('dbo')).toBeInTheDocument();
      expect(screen.getByText('1 selected')).toBeInTheDocument();
      expect(screen.getByText('customers')).toBeInTheDocument();
    });

    it('handles schema with many tables', () => {
      const manyTables = Array.from({ length: 10 }, (_, i) => ({
        selectedTableId: `${i}`,
        schemaName: 'dbo',
        tableName: `table_${i}`,
      }));
      const props = {
        ...defaultProps,
        grouped: [['dbo', manyTables]] as [string, typeof manyTables][],
      };
      render(<TableListSidebar {...props} />);
      expect(screen.getByText('10 selected')).toBeInTheDocument();
    });

    it('handles missing validationErrorsById prop', () => {
      const { validationErrorsById: _unused, ...propsWithoutErrors } = defaultProps;
      render(<TableListSidebar {...propsWithoutErrors} />);
      expect(screen.getByText('customers')).toBeInTheDocument();
    });

    it('handles null activeId', () => {
      const props = {
        ...defaultProps,
        activeId: null,
      };
      const { container } = render(<TableListSidebar {...props} />);
      const highlightedButtons = container.querySelectorAll('.bg-primary\\/10');
      expect(highlightedButtons).toHaveLength(0);
    });

    it('handles activeId that does not match any table', () => {
      const props = {
        ...defaultProps,
        activeId: '999',
      };
      const { container } = render(<TableListSidebar {...props} />);
      const highlightedButtons = container.querySelectorAll('.bg-primary\\/10');
      expect(highlightedButtons).toHaveLength(0);
    });
  });
});
