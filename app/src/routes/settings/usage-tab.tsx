import SettingsPanelShell from '@/components/settings/settings-panel-shell';

export default function UsageTab() {
  return (
    <SettingsPanelShell panelTestId="settings-panel-usage">
      <div data-testid="settings-usage-tab" className="flex flex-col gap-4">
        <p className="text-sm text-muted-foreground">
          Usage tracking will be available in a future release.
        </p>
      </div>
    </SettingsPanelShell>
  );
}
