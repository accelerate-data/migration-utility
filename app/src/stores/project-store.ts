import { create } from 'zustand';
import { projectGetActive, projectList, projectSetActive } from '@/lib/tauri';
import type { InitStep, InitStepEvent, InitStepStatus, Project } from '@/lib/types';
import { INIT_STEPS } from '@/lib/types';
import { logger } from '@/lib/logger';

// ── Types ─────────────────────────────────────────────────────────────────────

export interface StepState {
  step: InitStep;
  status: InitStepStatus | null;
}

// ── Store shape ───────────────────────────────────────────────────────────────

interface ProjectState {
  projects: Project[];
  activeProject: Project | null;
  isLoading: boolean;

  /** Per-step status for the running init. Null until init starts. */
  initSteps: StepState[];
  isInitRunning: boolean;

  // Actions
  loadProjects: () => Promise<void>;
  setActive: (id: string) => Promise<void>;
  clearActive: () => void;
  applyInitStep: (event: InitStepEvent) => void;
  startInit: () => void;
  finishInit: () => void;
  dismissInit: () => void;
}

// ── Store ─────────────────────────────────────────────────────────────────────

const INITIAL_STATE = {
  projects: [] as Project[],
  activeProject: null as Project | null,
  isLoading: false,
  initSteps: [] as StepState[],
  isInitRunning: false,
};

export const useProjectStore = create<ProjectState>()((set, get) => ({
  ...INITIAL_STATE,

  loadProjects: async () => {
    set({ isLoading: true });
    try {
      const [projects, active] = await Promise.all([projectList(), projectGetActive()]);
      set({ projects, activeProject: active, isLoading: false });
    } catch (err) {
      logger.error('project-store: failed to load projects', err);
      set({ isLoading: false });
    }
  },

  setActive: async (id: string) => {
    await projectSetActive(id);
    const activeProject = get().projects.find((p) => p.id === id) ?? null;
    set({ activeProject });
  },

  clearActive: () => set({ activeProject: null }),

  startInit: () =>
    set({
      isInitRunning: true,
      initSteps: INIT_STEPS.map((step) => ({ step, status: null })),
    }),

  finishInit: () => set({ isInitRunning: false }),

  dismissInit: () => set({ isInitRunning: false, initSteps: [] }),

  applyInitStep: (event: InitStepEvent) =>
    set((state) => ({
      initSteps: state.initSteps.map((s) =>
        s.step === event.step ? { ...s, status: event.status } : s
      ),
    })),
}));
