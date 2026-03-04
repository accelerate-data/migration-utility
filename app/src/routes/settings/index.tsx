import type { KeyboardEvent } from 'react';
import { Routes, Route, useLocation, useNavigate } from 'react-router';
import { cn } from '@/lib/utils';
import ProfileTab from './profile-tab';
import UsageTab from './usage-tab';

interface SubTab {
  label: string;
  path: string;
  testId: string;
}

const TABS: SubTab[] = [
  { label: 'Profile', path: '/settings',       testId: 'settings-tab-profile' },
  { label: 'Usage',   path: '/settings/usage', testId: 'settings-tab-usage' },
];

export default function SettingsSurface() {
  const { pathname } = useLocation();
  const navigate = useNavigate();

  function isActive(tab: SubTab): boolean {
    if (tab.path === '/settings') return pathname === '/settings';
    return pathname === tab.path || pathname.startsWith(tab.path + '/');
  }

  const activeIndex = TABS.findIndex((tab) => isActive(tab));

  function focusTab(index: number) {
    const target = document.querySelector<HTMLButtonElement>(`[data-settings-tab-index="${index}"]`);
    target?.focus();
  }

  function handleTabKeyDown(e: KeyboardEvent<HTMLButtonElement>, index: number) {
    if (e.key === 'ArrowRight') {
      e.preventDefault();
      const next = (index + 1) % TABS.length;
      navigate(TABS[next].path);
      focusTab(next);
      return;
    }
    if (e.key === 'ArrowLeft') {
      e.preventDefault();
      const prev = (index - 1 + TABS.length) % TABS.length;
      navigate(TABS[prev].path);
      focusTab(prev);
      return;
    }
    if (e.key === 'Home') {
      e.preventDefault();
      navigate(TABS[0].path);
      focusTab(0);
      return;
    }
    if (e.key === 'End') {
      e.preventDefault();
      const last = TABS.length - 1;
      navigate(TABS[last].path);
      focusTab(last);
    }
  }

  return (
    <div className="h-full flex flex-col overflow-hidden">
      {/* Sub-tab header */}
      <div
        className="shrink-0 border-b border-border flex items-end px-6 gap-0"
        role="tablist"
        aria-label="Settings sections"
      >
        {TABS.map((tab, index) => {
          const active = isActive(tab);
          return (
            <button
              key={tab.path}
              type="button"
              role="tab"
              id={`settings-tab-${index}`}
              aria-selected={active}
              aria-controls="settings-tab-panel"
              tabIndex={active ? 0 : -1}
              data-settings-tab-index={index}
              data-testid={tab.testId}
              onClick={() => navigate(tab.path)}
              onKeyDown={(e) => handleTabKeyDown(e, index)}
              className={cn(
                'px-3 pb-2.5 pt-3 text-sm font-medium border-b transition-colors duration-150 outline-none',
                'focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-1',
                active
                  ? 'border-[var(--color-pacific)] text-foreground'
                  : 'border-transparent text-muted-foreground hover:text-foreground',
              )}
            >
              {tab.label}
            </button>
          );
        })}
      </div>

      {/* Tab content */}
      <div
        className="flex-1 overflow-hidden"
        role="tabpanel"
        id="settings-tab-panel"
        aria-labelledby={activeIndex >= 0 ? `settings-tab-${activeIndex}` : undefined}
      >
        <Routes>
          <Route index element={<ProfileTab />} />
          <Route path="usage" element={<UsageTab />} />
        </Routes>
      </div>
    </div>
  );
}
