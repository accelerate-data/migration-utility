import { useLocation, useNavigate } from 'react-router';
import { House, Settings, type LucideIcon } from 'lucide-react';
import { cn } from '@/lib/utils';
import { BRAND_ASSETS } from '@/lib/branding';

interface NavItem {
  path: string;
  icon: LucideIcon;
  label: string;
  testId: string;
}

const TOP_ITEMS: NavItem[] = [
  { path: '/home', icon: House, label: 'Home', testId: 'nav-home' },
];

const BOTTOM_ITEMS: NavItem[] = [
  { path: '/settings', icon: Settings, label: 'Settings', testId: 'nav-settings' },
];

function NavButton({ item, isActive, onClick }: {
  item: NavItem;
  isActive: boolean;
  onClick: () => void;
}) {
  const Icon = item.icon;

  return (
    <button
      type="button"
      data-testid={item.testId}
      data-active={String(isActive)}
      title={item.label}
      aria-label={item.label}
      aria-current={isActive ? 'page' : undefined}
      onClick={onClick}
      className={cn(
        'group relative w-9 h-9 rounded-lg flex items-center justify-center transition-colors duration-150',
        'outline-none focus-visible:ring-2 focus-visible:ring-white/40',
        isActive ? 'text-white' : 'text-white/40 hover:text-white/70',
      )}
      style={isActive ? { backgroundColor: 'var(--icon-nav-active-bg)' } : undefined}
      onMouseEnter={(e) => {
        if (!isActive) (e.currentTarget as HTMLButtonElement).style.backgroundColor = 'var(--icon-nav-hover-bg)';
      }}
      onMouseLeave={(e) => {
        if (!isActive) (e.currentTarget as HTMLButtonElement).style.backgroundColor = '';
      }}
    >
      {/* Active left accent bar */}
      {isActive && (
        <span
          className="absolute -left-[2px] top-1.5 bottom-1.5 w-[3px] rounded-r-full"
          style={{ backgroundColor: 'var(--color-pacific)' }}
          aria-hidden="true"
        />
      )}
      <span
        data-testid={`${item.testId}-tooltip`}
        className="pointer-events-none absolute left-[calc(100%+10px)] top-1/2 -translate-y-1/2 rounded-md bg-card border border-border px-2 py-1 text-xs font-medium text-foreground whitespace-nowrap opacity-0 shadow-sm transition-opacity duration-150 group-hover:opacity-100 group-focus-visible:opacity-100"
        aria-hidden="true"
      >
        {item.label}
      </span>
      <Icon size={18} aria-hidden="true" />
    </button>
  );
}

export default function IconNav() {
  const navigate = useNavigate();
  const { pathname } = useLocation();

  function isActive(item: NavItem): boolean {
    if (item.path === '/settings') return pathname === '/settings' || pathname.startsWith('/settings/');
    return pathname === item.path;
  }

  function handleClick(item: NavItem) {
    navigate(item.path);
  }

  return (
    <nav
      className="w-[60px] h-full flex flex-col items-center py-3.5 gap-0.5 shrink-0"
      style={{ backgroundColor: 'var(--icon-nav-bg)' }}
      aria-label="Main navigation"
    >
      {/* Logo mark */}
      <div
        data-testid="nav-brand-mark"
        className="w-9 h-9 rounded-[6px] flex items-center justify-center mb-4 shrink-0 border border-white/20"
        style={{ backgroundColor: 'var(--icon-nav-active-bg)' }}
        aria-hidden="true"
      >
        <img
          src={BRAND_ASSETS.icon.light}
          alt=""
          className="size-8"
          data-testid="nav-brand-icon"
        />
      </div>

      {/* Top nav items */}
      {TOP_ITEMS.map((item) => (
        <NavButton
          key={item.path}
          item={item}
          isActive={isActive(item)}
          onClick={() => handleClick(item)}
        />
      ))}

      {/* Spacer */}
      <div className="flex-1" />

      {/* Bottom nav items */}
      {BOTTOM_ITEMS.map((item) => (
        <NavButton
          key={item.path}
          item={item}
          isActive={isActive(item)}
          onClick={() => handleClick(item)}
        />
      ))}
    </nav>
  );
}
