import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { MemoryRouter } from 'react-router';
import IconNav from '@/components/icon-nav';
import { useWorkflowStore } from '@/stores/workflow-store';

const mockNavigate = vi.fn();
vi.mock('react-router', async () => {
  const actual = await vi.importActual<typeof import('react-router')>('react-router');
  return {
    ...actual,
    useNavigate: () => mockNavigate,
    useLocation: () => ({ pathname: '/home' }),
  };
});


describe('IconNav', () => {
  beforeEach(() => {
    mockNavigate.mockReset();
    useWorkflowStore.setState((s) => ({
      ...s,
      currentSurface: 'home',
      appPhase: 'running_locked',
      appPhaseHydrated: true,
    }));
  });

  it('renders all nav items', () => {
    render(
      <MemoryRouter initialEntries={['/home']}>
        <IconNav />
      </MemoryRouter>,
    );
    expect(screen.getByTestId('nav-home')).toBeInTheDocument();
    expect(screen.getByTestId('nav-plan')).toBeInTheDocument();
    expect(screen.getByTestId('nav-monitor')).toBeInTheDocument();
    expect(screen.getByTestId('nav-settings')).toBeInTheDocument();
    expect(screen.getByTestId('nav-brand-mark')).toBeInTheDocument();
    expect(screen.getByTestId('nav-brand-icon')).toHaveAttribute('src', '/branding/icon-light-256.png');
    expect(screen.getByTestId('nav-home-tooltip')).toHaveTextContent('Home');
    expect(screen.getByTestId('nav-plan-tooltip')).toHaveTextContent('Plan');
    expect(screen.getByTestId('nav-monitor-tooltip')).toHaveTextContent('Monitor');
    expect(screen.getByTestId('nav-settings-tooltip')).toHaveTextContent('Settings');
  });

  it('marks /home as active when pathname is /home', () => {
    render(
      <MemoryRouter initialEntries={['/home']}>
        <IconNav />
      </MemoryRouter>,
    );
    expect(screen.getByTestId('nav-home').getAttribute('data-active')).toBe('true');
    expect(screen.getByTestId('nav-settings').getAttribute('data-active')).toBe('false');
  });

  it('navigates to /plan on plan click', () => {
    render(
      <MemoryRouter initialEntries={['/home']}>
        <IconNav />
      </MemoryRouter>,
    );
    fireEvent.click(screen.getByTestId('nav-plan'));
    expect(mockNavigate).toHaveBeenCalledWith('/plan');
  });

  it('navigates to /settings on settings click', () => {
    render(
      <MemoryRouter initialEntries={['/home']}>
        <IconNav />
      </MemoryRouter>,
    );
    fireEvent.click(screen.getByTestId('nav-settings'));
    expect(mockNavigate).toHaveBeenCalledWith('/settings');
  });

  it('updates currentSurface in store on click', () => {
    render(
      <MemoryRouter initialEntries={['/home']}>
        <IconNav />
      </MemoryRouter>,
    );
    fireEvent.click(screen.getByTestId('nav-monitor'));
    expect(useWorkflowStore.getState().currentSurface).toBe('monitor');
  });

  it('exposes accessible nav and icon button names', () => {
    render(
      <MemoryRouter initialEntries={['/home']}>
        <IconNav />
      </MemoryRouter>,
    );
    expect(screen.getByRole('navigation', { name: 'Main navigation' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Home' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Plan' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Monitor' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Settings' })).toBeInTheDocument();
  });

  it('disables non-settings surfaces in setup_required phase', () => {
    useWorkflowStore.setState((s) => ({ ...s, appPhase: 'setup_required' }));
    render(
      <MemoryRouter initialEntries={['/home']}>
        <IconNav />
      </MemoryRouter>,
    );
    // home is always accessible — shows setup prompt when not configured
    expect(screen.getByTestId('nav-home')).not.toBeDisabled();
    expect(screen.getByTestId('nav-plan')).toBeDisabled();
    expect(screen.getByTestId('nav-monitor')).toBeDisabled();
    expect(screen.getByTestId('nav-settings')).not.toBeDisabled();
  });
});
