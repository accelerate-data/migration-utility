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
      appPhase: 'setup_required',
      appPhaseHydrated: false,
      phaseFacts: {
        hasGithubAuth: false,
        hasAnthropicKey: false,
        hasProject: false,
      },
    }));
  });

  it('has correct initial state', () => {
    const { currentSurface, appPhase } = useWorkflowStore.getState();
    expect(currentSurface).toBe('home');
    expect(appPhase).toBe('setup_required');
  });

  it('setCurrentSurface updates currentSurface', () => {
    useWorkflowStore.getState().setCurrentSurface('settings');
    expect(useWorkflowStore.getState().currentSurface).toBe('settings');
  });

  it('reset restores initial state', () => {
    useWorkflowStore.getState().setCurrentSurface('settings');
    useWorkflowStore.getState().reset();
    const state = useWorkflowStore.getState();
    expect(state.currentSurface).toBe('home');
    expect(state.appPhase).toBe('setup_required');
    expect(state.appPhaseHydrated).toBe(false);
  });

  it('setAppPhaseState updates phase and phaseFacts', () => {
    useWorkflowStore.getState().setAppPhaseState({
      appPhase: 'configured',
      hasGithubAuth: true,
      hasAnthropicKey: true,
      hasProject: true,
    });
    const state = useWorkflowStore.getState();
    expect(state.appPhase).toBe('configured');
    expect(state.appPhaseHydrated).toBe(true);
    expect(state.phaseFacts.hasGithubAuth).toBe(true);
    expect(state.phaseFacts.hasProject).toBe(true);
  });
});

describe('workflow phase guards', () => {
  it('defaults to home for setup_required phase', () => {
    expect(defaultRouteForPhase('setup_required')).toBe('/home');
  });

  it('defaults to home for configured phase', () => {
    expect(defaultRouteForPhase('configured')).toBe('/home');
  });

  it('home and settings are always enabled', () => {
    expect(isSurfaceEnabledForPhase('home', 'setup_required')).toBe(true);
    expect(isSurfaceEnabledForPhase('settings', 'setup_required')).toBe(true);
    expect(isSurfaceEnabledForPhase('home', 'configured')).toBe(true);
    expect(isSurfaceEnabledForPhase('settings', 'configured')).toBe(true);
  });
});
