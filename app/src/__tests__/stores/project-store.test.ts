import { describe, it, expect, beforeEach } from 'vitest';
import { useProjectStore } from '@/stores/project-store';
import { mockInvokeCommands, resetTauriMocks } from '../../test/mocks/tauri';
import type { Project } from '@/lib/types';

const P1: Project = { id: 'p1', slug: 'alpha', name: 'Alpha', technology: 'sql_server', createdAt: '2024-01-01' };
const P2: Project = { id: 'p2', slug: 'beta', name: 'Beta', technology: 'fabric_warehouse', createdAt: '2024-01-02' };

beforeEach(() => {
  resetTauriMocks();
  useProjectStore.setState({
    projects: [],
    activeProject: null,
    isLoading: false,
    initSteps: [],
    isInitRunning: false,
  });
});

describe('useProjectStore — loadProjects', () => {
  it('populates projects and activeProject on success', async () => {
    mockInvokeCommands({ project_list: [P1, P2], project_get_active: P1 });
    await useProjectStore.getState().loadProjects();
    const { projects, activeProject, isLoading } = useProjectStore.getState();
    expect(projects).toHaveLength(2);
    expect(activeProject?.id).toBe('p1');
    expect(isLoading).toBe(false);
  });

  it('sets activeProject to null when none is active', async () => {
    mockInvokeCommands({ project_list: [P1], project_get_active: null });
    await useProjectStore.getState().loadProjects();
    expect(useProjectStore.getState().activeProject).toBeNull();
  });

  it('clears isLoading on error', async () => {
    mockInvokeCommands({});
    // project_list is unmocked → rejects
    await useProjectStore.getState().loadProjects();
    expect(useProjectStore.getState().isLoading).toBe(false);
  });
});

describe('useProjectStore — setActive', () => {
  it('updates activeProject after set', async () => {
    mockInvokeCommands({
      project_set_active: undefined,
      project_list: [P1, P2],
    });
    await useProjectStore.getState().setActive('p1');
    expect(useProjectStore.getState().activeProject?.id).toBe('p1');
  });
});

describe('useProjectStore — clearActive', () => {
  it('nulls out activeProject', () => {
    useProjectStore.setState({ activeProject: P1 });
    useProjectStore.getState().clearActive();
    expect(useProjectStore.getState().activeProject).toBeNull();
  });
});

describe('useProjectStore — init lifecycle', () => {
  it('startInit populates all steps as null status', () => {
    useProjectStore.getState().startInit();
    const { initSteps, isInitRunning } = useProjectStore.getState();
    expect(isInitRunning).toBe(true);
    expect(initSteps.length).toBeGreaterThan(0);
    expect(initSteps.every((s) => s.status === null)).toBe(true);
  });

  it('finishInit clears isInitRunning', () => {
    useProjectStore.getState().startInit();
    useProjectStore.getState().finishInit();
    expect(useProjectStore.getState().isInitRunning).toBe(false);
  });

  it('applyInitStep updates matching step status', () => {
    useProjectStore.getState().startInit();
    useProjectStore.getState().applyInitStep({ step: 'ddlCheck', status: { kind: 'ok' } });
    const step = useProjectStore.getState().initSteps.find((s) => s.step === 'ddlCheck');
    expect(step?.status?.kind).toBe('ok');
  });

  it('applyInitStep error stores message', () => {
    useProjectStore.getState().startInit();
    useProjectStore.getState().applyInitStep({
      step: 'ddlExtract',
      status: { kind: 'error', message: 'extraction failed' },
    });
    const step = useProjectStore.getState().initSteps.find((s) => s.step === 'ddlExtract');
    expect(step?.status?.kind).toBe('error');
    if (step?.status?.kind === 'error') {
      expect(step.status.message).toBe('extraction failed');
    }
  });

  it('applyInitStep leaves other steps unchanged', () => {
    useProjectStore.getState().startInit();
    useProjectStore.getState().applyInitStep({ step: 'gitPull', status: { kind: 'ok' } });
    const others = useProjectStore
      .getState()
      .initSteps.filter((s) => s.step !== 'gitPull');
    expect(others.every((s) => s.status === null)).toBe(true);
  });
});
