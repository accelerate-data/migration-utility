import { Button } from '@/components/ui/button';
import { AlertCircle } from 'lucide-react';

interface ApprovalActionsProps {
  approvalStatus: string | null;
  approvedAt: string | null;
  confirmedAt: string | null;
  isLocked: boolean;
  validationErrorCount?: number;
  onApprove: () => void;
}

export function ApprovalActions({
  approvalStatus,
  approvedAt,
  confirmedAt,
  isLocked,
  validationErrorCount = 0,
  onApprove,
}: ApprovalActionsProps) {
  // Only show if table has been analyzed
  if (!confirmedAt) return null;

  const hasValidationErrors = validationErrorCount > 0;

  return (
    <div className="flex items-center justify-between rounded-md border border-border bg-muted/30 p-4">
      <div className="space-y-1">
        <p className="text-sm font-semibold">Approval Status</p>
        {approvalStatus === 'approved' && approvedAt && (
          <p className="text-xs text-muted-foreground">
            Approved at {new Date(approvedAt).toLocaleString()}
          </p>
        )}
        {(!approvalStatus || approvalStatus === 'pending') && (
          <p className="text-xs text-muted-foreground">Pending approval</p>
        )}
        {hasValidationErrors && (
          <div className="flex items-center gap-1.5 text-xs text-destructive">
            <AlertCircle className="h-3.5 w-3.5" />
            <span>
              {validationErrorCount} validation error{validationErrorCount > 1 ? 's' : ''} found
            </span>
          </div>
        )}
      </div>
      <div className="flex items-center gap-2">
        {approvalStatus === 'approved' ? (
          <span
            className="rounded-full px-3 py-1 text-xs font-medium"
            style={{
              backgroundColor: 'color-mix(in oklch, var(--color-seafoam), transparent 85%)',
              color: 'var(--color-seafoam)',
            }}
          >
            ✓ Approved
          </span>
        ) : (
          <Button type="button" size="sm" disabled={isLocked} onClick={onApprove}>
            Approve Configuration
          </Button>
        )}
      </div>
    </div>
  );
}
