import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, it, expect, beforeEach } from 'vitest';
import { MemoryRouter, Routes, Route } from 'react-router';
import ProjectsTab from '../../routes/settings/projects-tab';
import { useProjectStore } from '@/stores/project-store';
import {
  mockInvokeCommands,
  mockDialogOpen,
  resetTauriMocks,
} from '../../test/mocks/tauri';
import type { Project } from '@/lib/types';

const PROJECT_A: Project = { id: 'p1', slug: 'alpha', name: 'Alpha', createdAt: '2024-01-01' };
const PROJECT_B: Project = { id: 'p2', slug: 'beta', name: 'Beta', createdAt: '2024-01-02' };

function renderTab() {
  return render(
    <MemoryRouter initialEntries={['/settings/projects']}>
      <Routes>
        <Route path="/settings/projects" element={<ProjectsTab />} />
      </Routes>
    </MemoryRouter>,
  );
}

function stubProjects(projects: Project[], active: Project | null) {
  mockInvokeCommands({
    project_list: projects,
    project_get_active: active,
    project_set_active: undefined,
    project_create_full: PROJECT_A,
    project_detect_databases: ['AlphaDB', 'BetaDB'],
    project_init: undefined,
    project_delete_full: undefined,
    project_reset_local: undefined,
  });
}

beforeEach(() => {
  resetTauriMocks();
  useProjectStore.setState({
    projects: [],
    activeProject: null,
    isLoading: false,
    initSteps: [],
    isInitRunning: false,
  });
  // Radix UI pointer-event polyfills
  window.HTMLElement.prototype.hasPointerCapture = () => false;
  window.HTMLElement.prototype.setPointerCapture = () => {};
  window.HTMLElement.prototype.releasePointerCapture = () => {};
  window.HTMLElement.prototype.scrollIntoView = () => {};
});

// ── Empty state ───────────────────────────────────────────────────────────────

describe('ProjectsTab — empty state', () => {
  it('renders empty state message when no projects exist', async () => {
    stubProjects([], null);
    renderTab();
    await waitFor(() => {
      expect(screen.getByText('No projects yet.')).toBeInTheDocument();
    });
  });

  it('renders "New project" button', async () => {
    stubProjects([], null);
    renderTab();
    await waitFor(() => {
      expect(screen.getByRole('button', { name: /new project/i })).toBeInTheDocument();
    });
  });
});

// ── Project list ──────────────────────────────────────────────────────────────

describe('ProjectsTab — project list', () => {
  it('renders project rows after load', async () => {
    stubProjects([PROJECT_A, PROJECT_B], null);
    renderTab();
    await waitFor(() => {
      expect(screen.getByTestId('project-row-alpha')).toBeInTheDocument();
      expect(screen.getByTestId('project-row-beta')).toBeInTheDocument();
    });
  });

  it('active project row has a Select button absent, reset button present', async () => {
    stubProjects([PROJECT_A], PROJECT_A);
    renderTab();
    await waitFor(() => {
      expect(screen.queryByTestId('project-select-alpha')).not.toBeInTheDocument();
      expect(screen.getByTestId('project-reset-alpha')).toBeInTheDocument();
    });
  });

  it('inactive project row has a Select button', async () => {
    stubProjects([PROJECT_B], PROJECT_A);
    renderTab();
    await waitFor(() => {
      expect(screen.getByTestId('project-select-beta')).toBeInTheDocument();
    });
  });
});

// ── Create project form ───────────────────────────────────────────────────────

describe('ProjectsTab — create project form', () => {
  it('shows form fields after clicking New project', async () => {
    const user = userEvent.setup();
    stubProjects([], null);
    renderTab();
    await waitFor(() => screen.getByRole('button', { name: /new project/i }));
    await user.click(screen.getByRole('button', { name: /new project/i }));
    expect(screen.getByLabelText(/project name/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/sa password/i)).toBeInTheDocument();
    expect(screen.getByPlaceholderText(/select a .dacpac file/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/customer/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/source system/i)).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /detect databases/i })).toBeInTheDocument();
    expect(screen.getByLabelText(/extraction date/i)).toBeInTheDocument();
  });

  it('Create button is disabled when fields are empty', async () => {
    const user = userEvent.setup();
    stubProjects([], null);
    renderTab();
    await waitFor(() => screen.getByRole('button', { name: /new project/i }));
    await user.click(screen.getByRole('button', { name: /new project/i }));
    expect(screen.getByRole('button', { name: /^create$/i })).toBeDisabled();
  });

  it('opens file dialog when Browse button is clicked', async () => {
    const user = userEvent.setup();
    mockDialogOpen.mockResolvedValue('/path/to/schema.dacpac');
    stubProjects([], null);
    renderTab();
    await waitFor(() => screen.getByRole('button', { name: /new project/i }));
    await user.click(screen.getByRole('button', { name: /new project/i }));
    // Click the dacpac input which also triggers pickDacpac
    await user.click(screen.getByPlaceholderText(/select a .dacpac file/i));
    expect(mockDialogOpen).toHaveBeenCalled();
  });

  it('Detect databases button is disabled until name, password, and dacpac are filled', async () => {
    const user = userEvent.setup();
    stubProjects([], null);
    renderTab();
    await waitFor(() => screen.getByRole('button', { name: /new project/i }));
    await user.click(screen.getByRole('button', { name: /new project/i }));
    expect(screen.getByTestId('project-detect-databases')).toBeDisabled();

    // Fill project name only — still disabled
    await user.type(screen.getByLabelText(/project name/i), 'My Project');
    expect(screen.getByTestId('project-detect-databases')).toBeDisabled();
  });

  it('Detect databases populates a Select dropdown', async () => {
    const user = userEvent.setup();
    stubProjects([], null);
    renderTab();
    await waitFor(() => screen.getByRole('button', { name: /new project/i }));
    await user.click(screen.getByRole('button', { name: /new project/i }));

    // Fill required fields
    await user.type(screen.getByLabelText(/project name/i), 'My Project');
    await user.type(screen.getByLabelText(/sa password/i), 'Pass1234!');
    // Simulate dacpac path via mock
    mockDialogOpen.mockResolvedValue('/path/to/schema.dacpac');
    await user.click(screen.getByPlaceholderText(/select a .dacpac file/i));

    await waitFor(() => expect(screen.getByTestId('project-detect-databases')).not.toBeDisabled());
    await user.click(screen.getByTestId('project-detect-databases'));

    await waitFor(() => {
      expect(screen.getByTestId('project-dbname-select')).toBeInTheDocument();
    });
  });

  it('Detect databases shows error on failure', async () => {
    const user = userEvent.setup();
    mockInvokeCommands({
      project_list: [],
      project_get_active: null,
      project_detect_databases: new Error('Docker not running'),
    });
    renderTab();
    await waitFor(() => screen.getByRole('button', { name: /new project/i }));
    await user.click(screen.getByRole('button', { name: /new project/i }));

    await user.type(screen.getByLabelText(/project name/i), 'My Project');
    await user.type(screen.getByLabelText(/sa password/i), 'Pass1234!');
    mockDialogOpen.mockResolvedValue('/path/to/schema.dacpac');
    await user.click(screen.getByPlaceholderText(/select a .dacpac file/i));

    await waitFor(() => expect(screen.getByTestId('project-detect-databases')).not.toBeDisabled());
    await user.click(screen.getByTestId('project-detect-databases'));

    await waitFor(() => {
      expect(screen.getByTestId('project-detect-error')).toBeInTheDocument();
    });
  });

  it('Cancel closes the dialog', async () => {
    const user = userEvent.setup();
    stubProjects([], null);
    renderTab();
    await waitFor(() => screen.getByRole('button', { name: /new project/i }));
    await user.click(screen.getByRole('button', { name: /new project/i }));
    expect(screen.getByLabelText(/project name/i)).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: /^cancel$/i }));
    expect(screen.queryByLabelText(/project name/i)).not.toBeInTheDocument();
  });
});

// ── Delete confirmation ───────────────────────────────────────────────────────

describe('ProjectsTab — delete project', () => {
  it('opens delete dialog when trash icon is clicked', async () => {
    const user = userEvent.setup();
    stubProjects([PROJECT_A], null);
    renderTab();
    await waitFor(() => screen.getByTestId('project-delete-alpha'));
    await user.click(screen.getByTestId('project-delete-alpha'));
    await waitFor(() => {
      expect(screen.getByText(/delete "alpha"\?/i)).toBeInTheDocument();
    });
  });

  it('cancelling delete dialog does not call project_delete_full', async () => {
    const user = userEvent.setup();
    stubProjects([PROJECT_A], null);
    renderTab();
    await waitFor(() => screen.getByTestId('project-delete-alpha'));
    await user.click(screen.getByTestId('project-delete-alpha'));
    await waitFor(() => screen.getByText(/delete "alpha"\?/i));
    await user.click(screen.getByRole('button', { name: /cancel/i }));
    // Dialog should close; project_delete_full should not have been called
    const { mockInvoke } = await import('../../test/mocks/tauri');
    expect(mockInvoke).not.toHaveBeenCalledWith('project_delete_full', expect.anything());
  });

  it('confirms delete calls project_delete_full', async () => {
    const user = userEvent.setup();
    stubProjects([PROJECT_A], null);
    // After delete, return empty list on next load
    mockInvokeCommands({
      project_list: [PROJECT_A],
      project_get_active: null,
      project_delete_full: undefined,
    });
    renderTab();
    await waitFor(() => screen.getByTestId('project-delete-alpha'));
    await user.click(screen.getByTestId('project-delete-alpha'));
    await waitFor(() => screen.getByTestId('project-delete-confirm-alpha'));
    await user.click(screen.getByTestId('project-delete-confirm-alpha'));
    const { mockInvoke } = await import('../../test/mocks/tauri');
    await waitFor(() => {
      expect(mockInvoke).toHaveBeenCalledWith('project_delete_full', { id: 'p1' });
    });
  });
});

// ── Reset confirmation ────────────────────────────────────────────────────────

describe('ProjectsTab — reset project', () => {
  it('opens reset dialog for active project', async () => {
    const user = userEvent.setup();
    stubProjects([PROJECT_A], PROJECT_A);
    renderTab();
    await waitFor(() => screen.getByTestId('project-reset-alpha'));
    await user.click(screen.getByTestId('project-reset-alpha'));
    await waitFor(() => {
      expect(screen.getByText(/reset local state for "alpha"\?/i)).toBeInTheDocument();
    });
  });

  it('reset dialog explains what is removed and what is kept', async () => {
    const user = userEvent.setup();
    stubProjects([PROJECT_A], PROJECT_A);
    renderTab();
    await waitFor(() => screen.getByTestId('project-reset-alpha'));
    await user.click(screen.getByTestId('project-reset-alpha'));
    await waitFor(() => {
      expect(screen.getByText(/will be removed locally/i)).toBeInTheDocument();
      expect(screen.getByText(/will be kept/i)).toBeInTheDocument();
    });
  });

  it('confirms reset calls project_reset_local then project_init', async () => {
    const user = userEvent.setup();
    stubProjects([PROJECT_A], PROJECT_A);
    renderTab();
    await waitFor(() => screen.getByTestId('project-reset-alpha'));
    await user.click(screen.getByTestId('project-reset-alpha'));
    await waitFor(() => screen.getByTestId('project-reset-confirm-alpha'));
    await user.click(screen.getByTestId('project-reset-confirm-alpha'));
    const { mockInvoke } = await import('../../test/mocks/tauri');
    await waitFor(() => {
      expect(mockInvoke).toHaveBeenCalledWith('project_reset_local', { id: 'p1' });
      expect(mockInvoke).toHaveBeenCalledWith('project_init', { id: 'p1' });
    });
  });
});

// ── Init progress ─────────────────────────────────────────────────────────────

describe('ProjectsTab — init progress', () => {
  it('shows init progress section when init is running', async () => {
    stubProjects([], null);
    renderTab();
    await waitFor(() => screen.getByText('No projects yet.'));
    useProjectStore.getState().startInit();
    await waitFor(() => {
      expect(screen.getByText('Initializing project…')).toBeInTheDocument();
    });
  });

  it('hides init progress section when not running and no steps', async () => {
    stubProjects([], null);
    renderTab();
    await waitFor(() => screen.getByText('No projects yet.'));
    expect(screen.queryByText('Initializing project…')).not.toBeInTheDocument();
  });

  it('renders step labels after startInit', async () => {
    stubProjects([], null);
    renderTab();
    await waitFor(() => screen.getByText('No projects yet.'));
    useProjectStore.getState().startInit();
    await waitFor(() => {
      expect(screen.getByText('Sync repository')).toBeInTheDocument();
      expect(screen.getByText('Check Docker')).toBeInTheDocument();
    });
  });

  it('shows error message when a step fails', async () => {
    stubProjects([], null);
    renderTab();
    await waitFor(() => screen.getByText('No projects yet.'));
    useProjectStore.getState().startInit();
    useProjectStore.getState().applyInitStep({
      step: 'dockerCheck',
      status: { kind: 'error', message: 'Docker not running' },
    });
    await waitFor(() => {
      expect(screen.getByText('Docker not running')).toBeInTheDocument();
    });
  });
});
