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
      workspaceId: null,
      appPhase: 'setup_required',
      appPhaseHydrated: false,
    }));
  });

  it('has correct initial state', () => {
    const { currentSurface, workspaceId, appPhase } =
      useWorkflowStore.getState();
    expect(currentSurface).toBe('home');
    expect(workspaceId).toBeNull();
    expect(appPhase).toBe('setup_required');
  });

  it('setCurrentSurface updates currentSurface', () => {
    useWorkflowStore.getState().setCurrentSurface('settings');
    expect(useWorkflowStore.getState().currentSurface).toBe('settings');
  });

  it('setWorkspaceId updates workspaceId', () => {
    useWorkflowStore.getState().setWorkspaceId('ws-1');
    expect(useWorkflowStore.getState().workspaceId).toBe('ws-1');
  });

  it('reset restores initial state', () => {
    useWorkflowStore.getState().setCurrentSurface('settings');
    useWorkflowStore.getState().setWorkspaceId('ws-1');
    useWorkflowStore.getState().reset();
    const state = useWorkflowStore.getState();
    expect(state.currentSurface).toBe('home');
    expect(state.workspaceId).toBeNull();
    expect(state.appPhase).toBe('setup_required');
    expect(state.appPhaseHydrated).toBe(false);
  });

  it('setAppPhaseState updates phase', () => {
    useWorkflowStore.getState().setAppPhaseState({
      appPhase: 'running_locked',
      hasGithubAuth: true,
      hasAnthropicKey: true,
      isSourceApplied: true,
    });
    const state = useWorkflowStore.getState();
    expect(state.appPhase).toBe('running_locked');
    expect(state.appPhaseHydrated).toBe(true);
  });
});

describe('workflow phase guards', () => {
  it('always routes to /home', () => {
    expect(defaultRouteForPhase('setup_required')).toBe('/home');
    expect(defaultRouteForPhase('scope_editable')).toBe('/home');
    expect(defaultRouteForPhase('running_locked')).toBe('/home');
  });

  it('all surfaces always enabled', () => {
    expect(isSurfaceEnabledForPhase('home', 'setup_required')).toBe(true);
    expect(isSurfaceEnabledForPhase('settings', 'setup_required')).toBe(true);
  });
});
