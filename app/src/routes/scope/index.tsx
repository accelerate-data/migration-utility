import { Routes, Route } from 'react-router';
import ScopeStep from './scope-step';
import ConfigStep from './config-step';

export default function ScopeSurface() {
  return (
    <div className="h-full min-h-0 overflow-hidden">
      <main className="h-full min-h-0 overflow-hidden px-8 py-6">
        <Routes>
          <Route
            index
            element={
              <div className="h-full min-h-0 w-full px-1">
                <ScopeStep />
              </div>
            }
          />
          <Route
            path="config"
            element={
              <div className="h-full min-h-0 w-full overflow-auto px-1">
                <ConfigStep />
              </div>
            }
          />
        </Routes>
      </main>
    </div>
  );
}
