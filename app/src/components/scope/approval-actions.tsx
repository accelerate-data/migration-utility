import { Button } from '@/components/ui/button';

interface ApprovalActionsProps {
  approvalStatus: string | null;
  approvedAt: string | null;
  confirmedAt: string | null;
  isLocked: boolean;
  onApprove: () => void;
}

export function ApprovalActions({
  approvalStatus,
  approvedAt,
  confirmedAt,
  isLocked,
  onApprove,
}: ApprovalActionsProps) {
  // Only show if table has been analyzed
  if (!confirmedAt) return null;

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
