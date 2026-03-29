import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '@/components/ui/alert-dialog';

interface ProjectResetDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  name: string;
  slug: string;
  onConfirm: () => void;
}

export default function ProjectResetDialog({ open, onOpenChange, name, slug, onConfirm }: ProjectResetDialogProps) {
  return (
    <AlertDialog open={open} onOpenChange={onOpenChange}>
      <AlertDialogContent>
        <AlertDialogHeader>
          <AlertDialogTitle>Reset local state for "{name}"?</AlertDialogTitle>
          <AlertDialogDescription>
            <span className="block font-medium mb-1">Will be removed locally:</span>
            <ul className="list-disc list-inside space-y-1 text-sm">
              <li>Local project directory (DDL files will be re-extracted from source on reinit)</li>
            </ul>
            <span className="block font-medium mt-2 mb-1">Will be kept:</span>
            <ul className="list-disc list-inside space-y-1 text-sm">
              <li>GitHub repository artifacts, source binary, and metadata</li>
              <li>Project record in database</li>
            </ul>
            <span className="block mt-2 text-sm text-muted-foreground">
              The project will be reinitialized immediately after reset.
            </span>
          </AlertDialogDescription>
        </AlertDialogHeader>
        <AlertDialogFooter>
          <AlertDialogCancel>Cancel</AlertDialogCancel>
          <AlertDialogAction
            onClick={onConfirm}
            data-testid={`project-reset-confirm-${slug}`}
          >
            Reset and reinitialize
          </AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  );
}
