import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import type { AppPhase, AppPhaseState } from '@/lib/types';

// ── Surface type ─────────────────────────────────────────────────────────────

export type Surface = 'home' | 'settings';

// ── Constants ───────────────────────────────────────────────────────────────

export const SURFACE_ROUTES: Record<Surface, string> = {
  home: '/home',
  settings: '/settings',
};

export function defaultRouteForPhase(_appPhase: AppPhase): string {
  return '/home';
}

export function isSurfaceEnabledForPhase(_surface: Surface, _appPhase: AppPhase): boolean {
  return true;
}

// ── Store shape ─────────────────────────────────────────────────────────────

interface WorkflowState {
  /** Last visited surface — used for root-redirect on app restart. */
  currentSurface: Surface;
  workspaceId: string | null;
  appPhase: AppPhase;
  appPhaseHydrated: boolean;

  // Actions
  setCurrentSurface: (surface: Surface) => void;
  setWorkspaceId: (id: string) => void;
  clearWorkspaceId: () => void;
  setAppPhaseState: (state: AppPhaseState) => void;
  setAppPhaseHydrated: (hydrated: boolean) => void;
  reset: () => void;
}

// ── Store ───────────────────────────────────────────────────────────────────

const INITIAL_STATE = {
  currentSurface: 'home' as Surface,
  workspaceId: null,
  appPhase: 'setup_required' as AppPhase,
  appPhaseHydrated: false,
};

export const useWorkflowStore = create<WorkflowState>()(
  persist(
    (set) => ({
      ...INITIAL_STATE,

      setCurrentSurface: (surface) => set({ currentSurface: surface }),

      setWorkspaceId: (id) => set({ workspaceId: id }),

      clearWorkspaceId: () => set({ workspaceId: null }),

      setAppPhaseState: (state) => set({
        appPhase: state.appPhase,
        appPhaseHydrated: true,
      }),

      setAppPhaseHydrated: (hydrated) => set({ appPhaseHydrated: hydrated }),

      reset: () => set({ ...INITIAL_STATE }),
    }),
    { name: 'migration-workflow' },
  ),
);
