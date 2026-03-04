import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router';
import { describe, it, expect } from 'vitest';
import HomeSurface from '../../routes/home';

function renderPage() {
  return render(
    <MemoryRouter initialEntries={['/home']}>
      <HomeSurface />
    </MemoryRouter>,
  );
}

describe('HomeSurface', () => {
  it('renders dashboard', () => {
    renderPage();
    expect(screen.getByTestId('home-dashboard')).toBeInTheDocument();
  });

  it('shows Active Migration section', () => {
    renderPage();
    expect(screen.getByText('Active Migration')).toBeInTheDocument();
  });
});
