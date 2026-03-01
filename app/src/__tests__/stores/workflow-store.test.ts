import { describe, it, expect, beforeEach } from 'vitest';
import {
  defaultRouteForPhase,
  isSurfaceEnabledForPhase,
  useWorkflowStore,
} from '@/stores/workflow-store';

describe('useWorkflowStore', () => {
  beforeEach(() => {
    useWorkflowStore.setState((s) => ({
      ...s,
      currentSurface: 'home',
      currentScopeStep: 'scope',
      scopeStepStatus: {},
      scopeStepSavedAt: {},
      workspaceId: null,
      selectedTableIds: [],
      migrationStatus: 'idle',
      appPhase: 'setup_required',
      appPhaseHydrated: false,
      phaseFacts: {
        hasGithubAuth: false,
        hasAnthropicKey: false,
        isSourceApplied: false,
        scopeFinalized: false,
        planFinalized: false,
      },
    }));
  });

  it('has correct initial state', () => {
    const { currentSurface, scopeStepStatus, workspaceId, migrationStatus, appPhase } =
      useWorkflowStore.getState();
    expect(currentSurface).toBe('home');
    expect(scopeStepStatus).toEqual({});
    expect(workspaceId).toBeNull();
    expect(migrationStatus).toBe('idle');
    expect(appPhase).toBe('setup_required');
  });

  it('setCurrentSurface updates currentSurface', () => {
    useWorkflowStore.getState().setCurrentSurface('scope');
    expect(useWorkflowStore.getState().currentSurface).toBe('scope');
  });

  it('setCurrentScopeStep updates currentScopeStep', () => {
    useWorkflowStore.getState().setCurrentScopeStep('config');
    expect(useWorkflowStore.getState().currentScopeStep).toBe('config');
  });

  it('saveScopeStep sets status to done and records timestamp', () => {
    useWorkflowStore.getState().saveScopeStep('scope');
    const { scopeStepStatus, scopeStepSavedAt } = useWorkflowStore.getState();
    expect(scopeStepStatus.scope).toBe('done');
    expect(scopeStepSavedAt.scope).toBeTruthy();
  });

  it('saveScopeStep is idempotent', () => {
    useWorkflowStore.getState().saveScopeStep('config');
    useWorkflowStore.getState().saveScopeStep('config');
    expect(useWorkflowStore.getState().scopeStepStatus.config).toBe('done');
  });

  it('setMigrationStatus updates migrationStatus', () => {
    useWorkflowStore.getState().setMigrationStatus('running');
    expect(useWorkflowStore.getState().migrationStatus).toBe('running');
  });

  it('setWorkspaceId updates workspaceId', () => {
    useWorkflowStore.getState().setWorkspaceId('ws-1');
    expect(useWorkflowStore.getState().workspaceId).toBe('ws-1');
  });

  it('setSelectedTableIds updates selectedTableIds', () => {
    useWorkflowStore.getState().setSelectedTableIds(['t1', 't2']);
    expect(useWorkflowStore.getState().selectedTableIds).toEqual(['t1', 't2']);
  });

  it('reset restores initial state', () => {
    useWorkflowStore.getState().setCurrentSurface('monitor');
    useWorkflowStore.getState().saveScopeStep('scope');
    useWorkflowStore.getState().setWorkspaceId('ws-1');
    useWorkflowStore.getState().setMigrationStatus('running');
    useWorkflowStore.getState().reset();
    const state = useWorkflowStore.getState();
    expect(state.currentSurface).toBe('home');
    expect(state.scopeStepStatus).toEqual({});
    expect(state.scopeStepSavedAt).toEqual({});
    expect(state.workspaceId).toBeNull();
    expect(state.migrationStatus).toBe('idle');
    expect(state.appPhase).toBe('setup_required');
    expect(state.appPhaseHydrated).toBe(false);
  });

  it('setAppPhaseState updates phase and syncs migrationStatus', () => {
    useWorkflowStore.getState().setAppPhaseState({
      appPhase: 'running_locked',
      hasGithubAuth: true,
      hasAnthropicKey: true,
      isSourceApplied: true,
      scopeFinalized: true,
      planFinalized: true,
    });
    const state = useWorkflowStore.getState();
    expect(state.appPhase).toBe('running_locked');
    expect(state.migrationStatus).toBe('running');
    expect(state.appPhaseHydrated).toBe(true);
  });
});

describe('workflow phase guards', () => {
  it('defaults to settings for setup_required phase', () => {
    expect(defaultRouteForPhase('setup_required')).toBe('/settings');
  });

  it('disables monitor until ready_to_run', () => {
    expect(isSurfaceEnabledForPhase('monitor', 'scope_editable')).toBe(false);
    expect(isSurfaceEnabledForPhase('monitor', 'ready_to_run')).toBe(true);
  });

  it('enables plan only in plan/editable-or-later phases', () => {
    expect(isSurfaceEnabledForPhase('plan', 'scope_editable')).toBe(false);
    expect(isSurfaceEnabledForPhase('plan', 'plan_editable')).toBe(true);
    expect(isSurfaceEnabledForPhase('plan', 'running_locked')).toBe(true);
  });
});
