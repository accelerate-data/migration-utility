import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router';
import { describe, it, expect, beforeEach } from 'vitest';
import HomeSurface from '../../routes/home';
import { useWorkflowStore } from '../../stores/workflow-store';

function renderPage() {
  return render(
    <MemoryRouter initialEntries={['/home']}>
      <HomeSurface />
    </MemoryRouter>,
  );
}

describe('HomeSurface', () => {
  beforeEach(() => {
    useWorkflowStore.setState((s) => ({
      ...s,
      workspaceId: 'ws-1',
      appPhase: 'scope_editable',
    }));
  });

  it('renders dashboard', () => {
    renderPage();
    expect(screen.getByTestId('home-dashboard-state')).toBeInTheDocument();
  });

  it('shows Pipeline running badge when running_locked', () => {
    useWorkflowStore.setState((s) => ({ ...s, workspaceId: 'ws-1', appPhase: 'running_locked' }));
    renderPage();
    expect(screen.getByTestId('home-dashboard-state')).toBeInTheDocument();
    expect(screen.getByText('Pipeline running')).toBeInTheDocument();
  });

  it('does not show Pipeline running badge when idle', () => {
    renderPage();
    expect(screen.queryByText('Pipeline running')).not.toBeInTheDocument();
  });
});
