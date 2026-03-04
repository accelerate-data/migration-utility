import { cn } from "@/lib/utils";

interface SettingsPanelShellProps {
  children: React.ReactNode;
  groupLabel?: React.ReactNode;
  className?: string;
  outerClassName?: string;
  panelTestId?: string;
  labelTestId?: string;
}

export default function SettingsPanelShell({
  children,
  groupLabel,
  className,
  outerClassName,
  panelTestId = "settings-panel-shell",
  labelTestId = "settings-group-label",
}: SettingsPanelShellProps) {
  return (
    <div
      className={cn("px-8 py-6 h-full overflow-auto", outerClassName)}
      data-testid={`${panelTestId}-outer`}
    >
      <div
        className={cn(
          "w-full md:w-[60%] md:min-w-[520px] md:max-w-[960px]",
          "overflow-auto flex flex-col gap-3",
          className,
        )}
        data-testid={panelTestId}
      >
        {groupLabel ? (
          <p
            className="text-sm font-medium text-muted-foreground"
            data-testid={labelTestId}
          >
            {groupLabel}
          </p>
        ) : null}
        {children}
      </div>
    </div>
  );
}
