import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import type { AppPhase, AppPhaseState } from '@/lib/types';

// ── Surface types ────────────────────────────────────────────────────────────

export type Surface = 'home' | 'settings';

// ── Constants ───────────────────────────────────────────────────────────────

export const SURFACE_ROUTES: Record<Surface, string> = {
  home: '/home',
  settings: '/settings',
};

export function defaultRouteForPhase(appPhase: AppPhase): string {
  switch (appPhase) {
    case 'setup_required':
    case 'configured':
      return '/home';
  }
}

export function isSurfaceEnabledForPhase(surface: Surface, _appPhase: AppPhase): boolean {
  // All surfaces are accessible once the app is loaded; setup guidance is shown in-surface.
  return surface === 'home' || surface === 'settings';
}

// ── Store shape ─────────────────────────────────────────────────────────────

interface WorkflowState {
  /** Last visited surface — used for root-redirect on app restart. */
  currentSurface: Surface;
  appPhase: AppPhase;
  appPhaseHydrated: boolean;
  phaseFacts: Omit<AppPhaseState, 'appPhase'>;

  // Actions
  setCurrentSurface: (surface: Surface) => void;
  setAppPhaseState: (state: AppPhaseState) => void;
  setAppPhaseHydrated: (hydrated: boolean) => void;
  reset: () => void;
}

// ── Store ───────────────────────────────────────────────────────────────────

const INITIAL_STATE = {
  currentSurface: 'home' as Surface,
  appPhase: 'setup_required' as AppPhase,
  appPhaseHydrated: false,
  phaseFacts: {
    hasGithubAuth: false,
    hasAnthropicKey: false,
    hasProject: false,
  } as Omit<AppPhaseState, 'appPhase'>,
};

export const useWorkflowStore = create<WorkflowState>()(
  persist(
    (set) => ({
      ...INITIAL_STATE,

      setCurrentSurface: (surface) => set({ currentSurface: surface }),

      setAppPhaseState: (state) => set({
        appPhase: state.appPhase,
        phaseFacts: {
          hasGithubAuth: state.hasGithubAuth,
          hasAnthropicKey: state.hasAnthropicKey,
          hasProject: state.hasProject,
        },
        appPhaseHydrated: true,
      }),

      setAppPhaseHydrated: (hydrated) => set({ appPhaseHydrated: hydrated }),

      reset: () => set({ ...INITIAL_STATE }),
    }),
    { name: 'migration-workflow' },
  ),
);
