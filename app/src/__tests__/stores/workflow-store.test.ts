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
      migrationStatus: 'idle',
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
    const { currentSurface, migrationStatus, appPhase } = useWorkflowStore.getState();
    expect(currentSurface).toBe('home');
    expect(migrationStatus).toBe('idle');
    expect(appPhase).toBe('setup_required');
  });

  it('setCurrentSurface updates currentSurface', () => {
    useWorkflowStore.getState().setCurrentSurface('plan');
    expect(useWorkflowStore.getState().currentSurface).toBe('plan');
  });

  it('setMigrationStatus updates migrationStatus', () => {
    useWorkflowStore.getState().setMigrationStatus('running');
    expect(useWorkflowStore.getState().migrationStatus).toBe('running');
  });

  it('reset restores initial state', () => {
    useWorkflowStore.getState().setCurrentSurface('monitor');
    useWorkflowStore.getState().setMigrationStatus('running');
    useWorkflowStore.getState().reset();
    const state = useWorkflowStore.getState();
    expect(state.currentSurface).toBe('home');
    expect(state.migrationStatus).toBe('idle');
    expect(state.appPhase).toBe('setup_required');
    expect(state.appPhaseHydrated).toBe(false);
  });

  it('setAppPhaseState updates phase and syncs migrationStatus', () => {
    useWorkflowStore.getState().setAppPhaseState({
      appPhase: 'running_locked',
      hasGithubAuth: true,
      hasAnthropicKey: true,
      hasProject: true,
    });
    const state = useWorkflowStore.getState();
    expect(state.appPhase).toBe('running_locked');
    expect(state.migrationStatus).toBe('running');
    expect(state.appPhaseHydrated).toBe(true);
  });
});

describe('workflow phase guards', () => {
  it('defaults to home for setup_required phase', () => {
    expect(defaultRouteForPhase('setup_required')).toBe('/home');
  });

  it('defaults to home for configured phase', () => {
    expect(defaultRouteForPhase('configured')).toBe('/home');
  });

  it('defaults to monitor for running_locked phase', () => {
    expect(defaultRouteForPhase('running_locked')).toBe('/monitor');
  });

  it('disables monitor until running_locked', () => {
    expect(isSurfaceEnabledForPhase('monitor', 'configured')).toBe(false);
    expect(isSurfaceEnabledForPhase('monitor', 'running_locked')).toBe(true);
  });

  it('enables plan for configured and running_locked', () => {
    expect(isSurfaceEnabledForPhase('plan', 'setup_required')).toBe(false);
    expect(isSurfaceEnabledForPhase('plan', 'configured')).toBe(true);
    expect(isSurfaceEnabledForPhase('plan', 'running_locked')).toBe(true);
  });
});
