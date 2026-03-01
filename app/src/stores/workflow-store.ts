import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import type { AppPhase, AppPhaseState } from '@/lib/types';

// ── Surface & step types ────────────────────────────────────────────────────

export type Surface = 'home' | 'scope' | 'plan' | 'monitor' | 'settings';

export type ScopeStep = 'scope' | 'config';
export type ScopeStepStatus = 'pending' | 'active' | 'done';

export type MigrationStatus = 'idle' | 'running' | 'complete';

// ── Constants ───────────────────────────────────────────────────────────────

export const SCOPE_STEPS: ScopeStep[] = ['scope', 'config'];

export const SCOPE_STEP_LABELS: Record<ScopeStep, string> = {
  scope: 'Scope',
  config: 'Table Config',
};

export const SCOPE_STEP_ROUTES: Record<ScopeStep, string> = {
  scope: '/scope',
  config: '/scope/config',
};

export const SURFACE_ROUTES: Record<Surface, string> = {
  home: '/home',
  scope: '/scope',
  plan: '/plan',
  monitor: '/monitor',
  settings: '/settings',
};

export function defaultRouteForPhase(appPhase: AppPhase): string {
  switch (appPhase) {
    case 'setup_required':
      return '/settings';
    case 'scope_editable':
      return '/scope';
    case 'plan_editable':
      return '/plan';
    case 'ready_to_run':
    case 'running_locked':
      return '/monitor';
  }
}

export function isSurfaceEnabledForPhase(surface: Surface, appPhase: AppPhase): boolean {
  if (surface === 'settings') return true;
  if (appPhase === 'setup_required') return false;
  if (surface === 'home') return true;
  if (surface === 'scope') return true;
  if (surface === 'plan') {
    return appPhase === 'plan_editable' || appPhase === 'ready_to_run' || appPhase === 'running_locked';
  }
  if (surface === 'monitor') {
    return appPhase === 'ready_to_run' || appPhase === 'running_locked';
  }
  return false;
}

export function isSurfaceReadOnlyForPhase(surface: Surface, appPhase: AppPhase): boolean {
  if (appPhase !== 'running_locked') return false;
  return surface === 'scope' || surface === 'plan';
}

// ── Store shape ─────────────────────────────────────────────────────────────

interface WorkflowState {
  /** Last visited surface — used for root-redirect on app restart. */
  currentSurface: Surface;
  /** Last visited scope step — used for scope sub-nav active state. */
  currentScopeStep: ScopeStep;
  scopeStepStatus: Partial<Record<ScopeStep, ScopeStepStatus>>;
  scopeStepSavedAt: Partial<Record<ScopeStep, string>>;
  workspaceId: string | null;
  selectedTableIds: string[];
  migrationStatus: MigrationStatus;
  appPhase: AppPhase;
  appPhaseHydrated: boolean;
  phaseFacts: Omit<AppPhaseState, 'appPhase'>;

  // Actions
  setCurrentSurface: (surface: Surface) => void;
  setCurrentScopeStep: (step: ScopeStep) => void;
  saveScopeStep: (step: ScopeStep) => void;
  setWorkspaceId: (id: string) => void;
  clearWorkspaceId: () => void;
  setSelectedTableIds: (ids: string[]) => void;
  setMigrationStatus: (status: MigrationStatus) => void;
  setAppPhaseState: (state: AppPhaseState) => void;
  setAppPhaseHydrated: (hydrated: boolean) => void;
  reset: () => void;
}

// ── Store ───────────────────────────────────────────────────────────────────

const INITIAL_STATE = {
  currentSurface: 'home' as Surface,
  currentScopeStep: 'scope' as ScopeStep,
  scopeStepStatus: {} as Partial<Record<ScopeStep, ScopeStepStatus>>,
  scopeStepSavedAt: {} as Partial<Record<ScopeStep, string>>,
  workspaceId: null,
  selectedTableIds: [] as string[],
  migrationStatus: 'idle' as MigrationStatus,
  appPhase: 'setup_required' as AppPhase,
  appPhaseHydrated: false,
  phaseFacts: {
    hasGithubAuth: false,
    hasAnthropicKey: false,
    isSourceApplied: false,
    scopeFinalized: false,
    planFinalized: false,
  } as Omit<AppPhaseState, 'appPhase'>,
};

export const useWorkflowStore = create<WorkflowState>()(
  persist(
    (set) => ({
      ...INITIAL_STATE,

      setCurrentSurface: (surface) => set({ currentSurface: surface }),

      setCurrentScopeStep: (step) => set({ currentScopeStep: step }),

      saveScopeStep: (step) => set((s) => ({
        scopeStepStatus: { ...s.scopeStepStatus, [step]: 'done' as ScopeStepStatus },
        scopeStepSavedAt: { ...s.scopeStepSavedAt, [step]: new Date().toISOString() },
      })),

      setWorkspaceId: (id) => set({ workspaceId: id }),

      clearWorkspaceId: () => set({ workspaceId: null }),

      setSelectedTableIds: (ids) => set({ selectedTableIds: ids }),

      setMigrationStatus: (status) => set({ migrationStatus: status }),

      setAppPhaseState: (state) => set({
        appPhase: state.appPhase,
        phaseFacts: {
          hasGithubAuth: state.hasGithubAuth,
          hasAnthropicKey: state.hasAnthropicKey,
          isSourceApplied: state.isSourceApplied,
          scopeFinalized: state.scopeFinalized,
          planFinalized: state.planFinalized,
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
