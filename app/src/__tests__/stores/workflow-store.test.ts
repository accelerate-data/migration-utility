import { describe, it, expect, beforeEach } from 'vitest';
import { useWorkflowStore } from '@/stores/workflow-store';

describe('useWorkflowStore', () => {
  beforeEach(() => {
    useWorkflowStore.setState((s) => ({
      ...s,
      currentSurface: 'home',
    }));
  });

  it('has correct initial state', () => {
    const { currentSurface } = useWorkflowStore.getState();
    expect(currentSurface).toBe('home');
  });

  it('setCurrentSurface updates currentSurface', () => {
    useWorkflowStore.getState().setCurrentSurface('settings');
    expect(useWorkflowStore.getState().currentSurface).toBe('settings');
  });

  it('reset restores initial state', () => {
    useWorkflowStore.getState().setCurrentSurface('settings');
    useWorkflowStore.getState().reset();
    expect(useWorkflowStore.getState().currentSurface).toBe('home');
  });
});
