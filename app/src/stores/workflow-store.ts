import { create } from 'zustand';

// ── Surface types ────────────────────────────────────────────────────────────

export type Surface = 'home' | 'settings';

// ── Constants ───────────────────────────────────────────────────────────────

export const SURFACE_ROUTES: Record<Surface, string> = {
  home: '/home',
  settings: '/settings',
};

// ── Store shape ─────────────────────────────────────────────────────────────

interface WorkflowState {
  /** Last visited surface — used for nav state tracking. */
  currentSurface: Surface;

  // Actions
  setCurrentSurface: (surface: Surface) => void;
  reset: () => void;
}

// ── Store ───────────────────────────────────────────────────────────────────

const INITIAL_STATE = {
  currentSurface: 'home' as Surface,
};

export const useWorkflowStore = create<WorkflowState>()((set) => ({
  ...INITIAL_STATE,

  setCurrentSurface: (surface) => set({ currentSurface: surface }),

  reset: () => set({ ...INITIAL_STATE }),
}));
