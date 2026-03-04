import { render, screen } from '@testing-library/react';
import { describe, it, expect } from 'vitest';
import { MemoryRouter, Routes, Route } from 'react-router';
import SettingsSurface from '../../routes/settings';

describe('SettingsSurface tabs', () => {
  it('renders profile and usage tabs', () => {
    render(
      <MemoryRouter initialEntries={['/settings']}>
        <Routes>
          <Route path="/settings/*" element={<SettingsSurface />} />
        </Routes>
      </MemoryRouter>,
    );

    expect(screen.queryByTestId('settings-tab-reset')).not.toBeInTheDocument();
    expect(screen.getByTestId('settings-tab-profile')).toBeInTheDocument();
    expect(screen.getByTestId('settings-tab-usage')).toBeInTheDocument();
  });
});
