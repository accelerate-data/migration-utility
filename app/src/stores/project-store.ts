import { create } from 'zustand';
import { projectGetActive, projectList, projectSetActive } from '@/lib/tauri';
import type { InitStep, InitStepEvent, InitStepStatus, Project } from '@/lib/types';
import { GLOBAL_STEPS, INIT_STEPS, PER_PROJECT_STEPS } from '@/lib/types';
import { logger } from '@/lib/logger';

// ── Types ─────────────────────────────────────────────────────────────────────

export interface StepState {
  step: InitStep;
  status: InitStepStatus | null;
}

function makeSteps(steps: InitStep[]): StepState[] {
  return steps.map((step) => ({ step, status: null }));
}

// ── Store shape ───────────────────────────────────────────────────────────────

interface ProjectState {
  projects: Project[];
  activeProject: Project | null;
  isLoading: boolean;

  /** Per-step status for the running single-project init. */
  initSteps: StepState[];
  isInitRunning: boolean;

  /** Startup sync state (multi-project, shown on splash screen). */
  startupGlobalSteps: StepState[];
  startupProjectSteps: Record<string, StepState[]>;
  isStartupRunning: boolean;
  startupFailed: boolean;

  // Actions
  loadProjects: () => Promise<void>;
  setActive: (id: string) => Promise<void>;
  clearActive: () => void;
  applyInitStep: (event: InitStepEvent) => void;
  startInit: () => void;
  finishInit: () => void;
  dismissInit: () => void;
  startStartup: (projects: Project[]) => void;
  applyStartupStep: (event: InitStepEvent) => void;
  finishStartup: () => void;
  failStartup: () => void;
  resetStartup: () => void;
}

// ── Store ─────────────────────────────────────────────────────────────────────

const INITIAL_STATE = {
  projects: [] as Project[],
  activeProject: null as Project | null,
  isLoading: false,
  initSteps: [] as StepState[],
  isInitRunning: false,
  startupGlobalSteps: [] as StepState[],
  startupProjectSteps: {} as Record<string, StepState[]>,
  isStartupRunning: false,
  startupFailed: false,
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

  // ── Single-project init ──────────────────────────────────────────────────

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

  // ── Startup sync (multi-project, splash screen) ─────────────────────────

  startStartup: (projects: Project[]) =>
    set({
      isStartupRunning: true,
      startupFailed: false,
      startupGlobalSteps: makeSteps(GLOBAL_STEPS),
      startupProjectSteps: Object.fromEntries(
        projects.map((p) => [p.id, makeSteps(PER_PROJECT_STEPS)])
      ),
    }),

  applyStartupStep: (event: InitStepEvent) =>
    set((state) => {
      if (event.projectId) {
        const existing = state.startupProjectSteps[event.projectId] ?? makeSteps(PER_PROJECT_STEPS);
        return {
          startupProjectSteps: {
            ...state.startupProjectSteps,
            [event.projectId]: existing.map((s) =>
              s.step === event.step ? { ...s, status: event.status } : s
            ),
          },
        };
      }
      return {
        startupGlobalSteps: state.startupGlobalSteps.map((s) =>
          s.step === event.step ? { ...s, status: event.status } : s
        ),
      };
    }),

  finishStartup: () => set({ isStartupRunning: false }),

  failStartup: () => set({ isStartupRunning: false, startupFailed: true }),

  resetStartup: () =>
    set({
      startupGlobalSteps: [],
      startupProjectSteps: {},
      isStartupRunning: false,
      startupFailed: false,
    }),
}));
