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
      appPhase: 'scope_editable',
      appPhaseHydrated: true,
    }));
  });

  it('renders home and settings nav items', () => {
    render(
      <MemoryRouter initialEntries={['/home']}>
        <IconNav />
      </MemoryRouter>,
    );
    expect(screen.getByTestId('nav-home')).toBeInTheDocument();
    expect(screen.getByTestId('nav-settings')).toBeInTheDocument();
    expect(screen.getByTestId('nav-brand-mark')).toBeInTheDocument();
    expect(screen.getByTestId('nav-brand-icon')).toHaveAttribute('src', '/branding/icon-light-256.png');
    expect(screen.getByTestId('nav-home-tooltip')).toHaveTextContent('Home');
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

  it('navigates to /home on home click', () => {
    render(
      <MemoryRouter initialEntries={['/home']}>
        <IconNav />
      </MemoryRouter>,
    );
    fireEvent.click(screen.getByTestId('nav-home'));
    expect(mockNavigate).toHaveBeenCalledWith('/home');
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

  it('exposes accessible nav and icon button names', () => {
    render(
      <MemoryRouter initialEntries={['/home']}>
        <IconNav />
      </MemoryRouter>,
    );
    expect(screen.getByRole('navigation', { name: 'Main navigation' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Home' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Settings' })).toBeInTheDocument();
  });
});
