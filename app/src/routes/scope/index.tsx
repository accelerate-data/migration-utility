import { Routes, Route } from 'react-router';
import ScopeStep from './scope-step';
import ConfigStep from './config-step';

export default function ScopeSurface() {
  return (
    <div className="h-full overflow-hidden">
      <main className="h-full overflow-auto px-8 py-6">
        <div className="w-full overflow-auto px-1">
          <Routes>
            <Route index element={<ScopeStep />} />
            <Route path="config" element={<ConfigStep />} />
          </Routes>
        </div>
      </main>
    </div>
  );
}
