import { useEffect, useState } from 'react';
import type { KeyboardEvent } from 'react';
import { Folder, Monitor, Moon, Sun } from 'lucide-react';
import { useTheme } from 'next-themes';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { cn } from '@/lib/utils';
import { logger, LOG_LEVELS, setFrontendLogLevel, type LogLevel } from '@/lib/logger';
import { getSettings, setLogLevel, getLogFilePath, getDataDirPath } from '@/lib/tauri';
import { prettyPath } from '@/lib/path-utils';
import { homeDir } from '@tauri-apps/api/path';
import SettingsPanelShell from '@/components/settings/settings-panel-shell';

// ── Log level descriptions ──────────────────────────────────────────────────

const LEVEL_DESCRIPTION: Record<LogLevel, string> = {
  debug: 'Everything (verbose)',
  info:  'Key events (default)',
  warn:  'Warnings and errors only',
  error: 'Errors only',
};

// ── Theme toggle ────────────────────────────────────────────────────────────

const THEME_OPTIONS = [
  { value: 'system', label: 'System', icon: Monitor },
  { value: 'light',  label: 'Light',  icon: Sun },
  { value: 'dark',   label: 'Dark',   icon: Moon },
] as const;

function ThemeToggle() {
  const { theme, setTheme } = useTheme();
  const currentTheme = theme ?? 'system';

  function focusThemeOption(index: number) {
    const target = document.querySelector<HTMLButtonElement>(`[data-theme-index="${index}"]`);
    target?.focus();
  }

  function handleThemeKeyDown(e: KeyboardEvent<HTMLButtonElement>, index: number) {
    if (e.key === 'ArrowRight' || e.key === 'ArrowDown') {
      e.preventDefault();
      const next = (index + 1) % THEME_OPTIONS.length;
      setTheme(THEME_OPTIONS[next].value);
      focusThemeOption(next);
      return;
    }
    if (e.key === 'ArrowLeft' || e.key === 'ArrowUp') {
      e.preventDefault();
      const prev = (index - 1 + THEME_OPTIONS.length) % THEME_OPTIONS.length;
      setTheme(THEME_OPTIONS[prev].value);
      focusThemeOption(prev);
      return;
    }
    if (e.key === 'Home') {
      e.preventDefault();
      setTheme(THEME_OPTIONS[0].value);
      focusThemeOption(0);
      return;
    }
    if (e.key === 'End') {
      e.preventDefault();
      const last = THEME_OPTIONS.length - 1;
      setTheme(THEME_OPTIONS[last].value);
      focusThemeOption(last);
    }
  }

  return (
    <div
      className="flex items-center gap-1 rounded-md bg-muted p-1 w-fit"
      role="radiogroup"
      aria-label="Theme"
    >
      {THEME_OPTIONS.map(({ value, label, icon: Icon }, index) => {
        const isActive = currentTheme === value;
        return (
          <button
            key={value}
            type="button"
            role="radio"
            aria-checked={isActive}
            tabIndex={isActive ? 0 : -1}
            data-theme-index={index}
            data-testid={`theme-${value}`}
            onClick={() => setTheme(value)}
            onKeyDown={(e) => handleThemeKeyDown(e, index)}
            className={cn(
              'flex items-center gap-1.5 rounded px-2.5 py-1 text-sm font-medium transition-colors duration-150',
              'outline-none focus-visible:ring-2 focus-visible:ring-ring/50',
              isActive
                ? 'bg-background text-foreground shadow-sm'
                : 'text-muted-foreground hover:text-foreground',
            )}
          >
            <Icon size={13} aria-hidden="true" />
            {label}
          </button>
        );
      })}
    </div>
  );
}

// ── Path row ────────────────────────────────────────────────────────────────

function PathRow({ label, path, testId }: { label: string; path: string | null; testId?: string }) {
  return (
    <div className="flex items-start gap-4 py-1.5">
      <span className="text-sm text-muted-foreground shrink-0 w-36 pt-0.5">{label}</span>
      <div className="flex items-center gap-1.5 min-w-0">
        <Folder className="size-3.5 shrink-0 text-muted-foreground" />
        <code className="text-sm font-mono text-muted-foreground truncate" data-testid={testId}>
          {path ?? 'Loading…'}
        </code>
      </div>
    </div>
  );
}

// ── Profile tab ─────────────────────────────────────────────────────────────

export default function ProfileTab() {
  const [level, setLevel] = useState<LogLevel>('info');
  const [logFilePath, setLogFilePath] = useState<string | null>(null);
  const [dataDirPath, setDataDirPath] = useState<string | null>(null);

  useEffect(() => {
    homeDir().then((h) => {
      getLogFilePath().then((p) => setLogFilePath(prettyPath(p, h))).catch(() => setLogFilePath(null));
      getDataDirPath().then((p) => setDataDirPath(prettyPath(p, h))).catch(() => setDataDirPath(null));
    }).catch(() => {
      getLogFilePath().then(setLogFilePath).catch(() => setLogFilePath(null));
      getDataDirPath().then(setDataDirPath).catch(() => setDataDirPath(null));
    });
    // Load log level from DB — DB is the single source of truth.
    getSettings()
      .then((s) => {
        if (s.logLevel) {
          const persisted = s.logLevel as LogLevel;
          setLevel(persisted);
          setFrontendLogLevel(persisted);
        }
      })
      .catch((err) => logger.warn('profile: failed to read settings for log level', err));
  }, []);

  function handleLevelChange(next: string) {
    const nextLevel = next as LogLevel;
    setFrontendLogLevel(nextLevel);
    setLevel(nextLevel);
    setLogLevel(nextLevel).catch((err) => logger.error('profile: failed to set log level', err));
    logger.info('event=log_level_change operation=set_level status=success', { level: nextLevel });
  }

  return (
    <SettingsPanelShell panelTestId="settings-panel-profile" className="gap-4" outerClassName="h-full overflow-auto">
      <div data-testid="settings-profile-tab" className="flex flex-col gap-4">

        {/* Appearance — lightweight row, no card overhead */}
        <div className="flex items-center justify-between py-2 px-0.5" data-testid="settings-profile-appearance">
          <div>
            <p className="text-sm font-medium">Appearance</p>
            <p className="text-xs text-muted-foreground mt-0.5">Preferred colour scheme.</p>
          </div>
          <ThemeToggle />
        </div>

        <div className="border-t border-border" />

        {/* Logging */}
        <Card className="gap-0 py-5" data-testid="settings-profile-logging-card">
          <CardHeader className="pb-3">
            <CardTitle>Logging</CardTitle>
            <CardDescription className="mt-0.5">
              Set verbosity for app and backend logs.
            </CardDescription>
          </CardHeader>
          <CardContent className="pt-0">
            <div className="flex items-center gap-3">
              <Select value={level} onValueChange={handleLevelChange}>
                <SelectTrigger className="w-fit" data-testid="select-log-level">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {LOG_LEVELS.map((l) => (
                    <SelectItem key={l} value={l}>
                      {l.charAt(0).toUpperCase() + l.slice(1)}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <p className="text-sm text-muted-foreground">{LEVEL_DESCRIPTION[level]}</p>
            </div>
          </CardContent>
        </Card>

        {/* Directories */}
        <Card className="gap-0 py-5" data-testid="settings-profile-directories-card">
          <CardHeader className="pb-3">
            <CardTitle>Directories</CardTitle>
            <CardDescription className="mt-0.5">
              Local paths used by the Migration Utility app.
            </CardDescription>
          </CardHeader>
          <CardContent className="pt-0">
            <div className="flex flex-col gap-1">
              <PathRow
                label="Log File"
                path={logFilePath}
                testId="path-log-file"
              />
              <PathRow
                label="Data Directory"
                path={dataDirPath}
                testId="path-data-dir"
              />
            </div>
          </CardContent>
        </Card>

      </div>
    </SettingsPanelShell>
  );
}
