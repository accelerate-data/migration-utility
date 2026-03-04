import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import type { AppPhase, AppPhaseState } from '@/lib/types';

// ── Surface types ────────────────────────────────────────────────────────────

export type Surface = 'home' | 'plan' | 'monitor' | 'settings';

export type MigrationStatus = 'idle' | 'running' | 'complete';

// ── Constants ───────────────────────────────────────────────────────────────

export const SURFACE_ROUTES: Record<Surface, string> = {
  home: '/home',
  plan: '/plan',
  monitor: '/monitor',
  settings: '/settings',
};

export function defaultRouteForPhase(appPhase: AppPhase): string {
  switch (appPhase) {
    case 'setup_required':
    case 'configured':
      return '/home';
    case 'running_locked':
      return '/monitor';
  }
}

export function isSurfaceEnabledForPhase(surface: Surface, appPhase: AppPhase): boolean {
  if (surface === 'settings') return true;
  if (surface === 'home') return true;
  if (appPhase === 'setup_required') return false;
  if (surface === 'plan') return true;
  if (surface === 'monitor') return appPhase === 'running_locked';
  return false;
}

export function isSurfaceReadOnlyForPhase(surface: Surface, appPhase: AppPhase): boolean {
  if (surface === 'plan') return appPhase === 'running_locked';
  return false;
}

// ── Store shape ─────────────────────────────────────────────────────────────

interface WorkflowState {
  /** Last visited surface — used for root-redirect on app restart. */
  currentSurface: Surface;
  migrationStatus: MigrationStatus;
  appPhase: AppPhase;
  appPhaseHydrated: boolean;
  phaseFacts: Omit<AppPhaseState, 'appPhase'>;

  // Actions
  setCurrentSurface: (surface: Surface) => void;
  setMigrationStatus: (status: MigrationStatus) => void;
  setAppPhaseState: (state: AppPhaseState) => void;
  setAppPhaseHydrated: (hydrated: boolean) => void;
  reset: () => void;
}

// ── Store ───────────────────────────────────────────────────────────────────

const INITIAL_STATE = {
  currentSurface: 'home' as Surface,
  migrationStatus: 'idle' as MigrationStatus,
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

      setMigrationStatus: (status) => set({ migrationStatus: status }),

      setAppPhaseState: (state) => set({
        appPhase: state.appPhase,
        phaseFacts: {
          hasGithubAuth: state.hasGithubAuth,
          hasAnthropicKey: state.hasAnthropicKey,
          hasProject: state.hasProject,
        },
        appPhaseHydrated: true,
        migrationStatus: state.appPhase === 'running_locked' ? 'running' : 'idle',
      }),

      setAppPhaseHydrated: (hydrated) => set({ appPhaseHydrated: hydrated }),

      reset: () => set({ ...INITIAL_STATE }),
    }),
    { name: 'migration-workflow' },
  ),
);
