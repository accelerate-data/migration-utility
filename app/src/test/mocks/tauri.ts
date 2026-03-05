import { vi } from "vitest";

// Mock @tauri-apps/plugin-log (used by main.tsx — components don't call it directly)
vi.mock("@tauri-apps/plugin-log", () => ({
  attachConsole: vi.fn(() => Promise.resolve()),
}));

// Mock @tauri-apps/api/core
// Default: resolve undefined for unknown commands
const defaultInvokeImpl = (_cmd: string) => Promise.resolve(undefined);
export const mockInvoke = vi.fn().mockImplementation(defaultInvokeImpl);

vi.mock("@tauri-apps/api/core", () => ({
  invoke: mockInvoke,
}));

// Mock @tauri-apps/api/event (used by listenProjectInitStep)
export const mockListen = vi.fn().mockResolvedValue(() => {});
vi.mock("@tauri-apps/api/event", () => ({
  listen: mockListen,
}));

export const mockDialogOpen = vi.fn();
vi.mock("@tauri-apps/plugin-dialog", () => ({
  open: mockDialogOpen,
}));

// Mock @tauri-apps/plugin-opener
export const mockOpenPath = vi.fn(() => Promise.resolve());
vi.mock("@tauri-apps/plugin-opener", () => ({
  openPath: mockOpenPath,
  openUrl: vi.fn(() => Promise.resolve()),
}));

// Helper to configure invoke return values per command
export function mockInvokeCommand(
  command: string,
  returnValue: unknown
): void {
  mockInvoke.mockImplementation((cmd: string) => {
    if (cmd === command) return Promise.resolve(returnValue);
    return Promise.reject(new Error(`Unmocked command: ${cmd}`));
  });
}

// Helper to configure multiple command responses.
// Pass an Error instance as a value to simulate a rejected command.
export function mockInvokeCommands(
  commands: Record<string, unknown>
): void {
  mockInvoke.mockImplementation((cmd: string) => {
    if (cmd in commands) {
      const val = commands[cmd];
      return val instanceof Error ? Promise.reject(val) : Promise.resolve(val);
    }
    return Promise.reject(new Error(`Unmocked command: ${cmd}`));
  });
}

export function resetTauriMocks(): void {
  mockInvoke.mockReset().mockImplementation(defaultInvokeImpl);
  mockListen.mockReset().mockResolvedValue(() => {});
  mockDialogOpen.mockReset();
  mockOpenPath.mockReset().mockReturnValue(Promise.resolve());
}
