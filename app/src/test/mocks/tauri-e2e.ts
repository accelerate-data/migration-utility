/**
 * E2E mock for Tauri APIs. This file is loaded via vite plugin
 * when TAURI_E2E=true, replacing @tauri-apps/api/core.
 *
 * It provides mock responses for all invoke commands so the frontend
 * can render without the Rust backend.
 */

const mockResponses: Record<string, unknown> = {
  // Workspace
  workspace_apply_start: "job-1",
  workspace_apply_status: {
    jobId: "job-1",
    state: "succeeded",
    isAlive: false,
    stage: "completed",
    percent: 100,
    message: "Apply completed.",
    error: null,
  },
  workspace_test_source_connection: "Connection successful",
  workspace_discover_source_databases: ["master"],
  workspace_reset_state: undefined,
  workspace_create: undefined,
  get_workspaces: [],
  create_workspace: undefined,
  delete_workspace: undefined,
  // Candidacy
  get_candidacies: [],
  update_candidacy: undefined,
  // Scope + table details
  migration_list_scope_inventory: [],
  migration_add_tables_to_selection: 0,
  migration_set_table_selected: undefined,
  migration_reset_selected_tables: 0,
  migration_get_table_config: null,
  migration_analyze_table_details: {
    selectedTableId: "st:ws-1:wh-1:dbo:fact_sales",
    tableType: "unknown",
    loadStrategy: "incremental",
    grainColumns: "[]",
    relationshipsJson: "[]",
    incrementalColumn: "",
    dateColumn: "",
    snapshotStrategy: "sample_1day",
    piiColumns: "[]",
    confirmedAt: null,
  },
  migration_save_table_config: undefined,
  migration_reconcile_scope_state: { kept: 0, invalidated: 0, removed: 0 },
  // Plan
  get_plan_status: { status: "pending", updatedAt: null },
  finalize_plan: undefined,
  // Monitor
  monitor_launch_agent: "mock agent output",
  // GitHub auth
  github_get_user: null,
  github_list_repos: [],
  github_logout: undefined,
  // Settings
  get_settings: { anthropicApiKey: null },
  save_anthropic_api_key: undefined,
  test_api_key: true,
  // App info
  set_log_level: undefined,
  get_log_file_path: null,
  get_data_dir_path: null,
};

const STORE_KEY = "migration-workflow";

interface SeededState {
  workspaceId?: unknown;
  migrationStatus?: unknown;
}

function getSeededState(): SeededState | null {
  try {
    const raw = localStorage.getItem(STORE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as { state?: SeededState };
    return parsed?.state ?? null;
  } catch {
    return null;
  }
}

function getSeededWorkspaceResponse(): Record<string, unknown> | null {
  const state = getSeededState();
  const workspaceId = state?.workspaceId;
  if (typeof workspaceId !== "string" || workspaceId.trim().length === 0) {
    return null;
  }
  return {
    id: workspaceId,
    displayName: workspaceId,
    migrationRepoName: "acme/repo",
    migrationRepoPath: "/tmp/repo",
    sourceServer: "localhost",
    sourceDatabase: "master",
    sourcePort: 1433,
    sourceAuthenticationMode: "sql_password",
    sourceUsername: "sa",
    sourcePassword: "password",
    sourceEncrypt: true,
    sourceTrustServerCertificate: false,
  };
}

/** Resolve workspace — checks overrides first, then falls back to localStorage seed. */
function resolveWorkspace(overrides?: Record<string, unknown>): Record<string, unknown> | null {
  if (overrides && "workspace_get" in overrides) {
    const val = overrides["workspace_get"];
    if (typeof val === "function") {
      return (val as () => Record<string, unknown> | null)();
    }
    return val as Record<string, unknown> | null;
  }
  return getSeededWorkspaceResponse();
}

export async function invoke<T>(cmd: string, args?: Record<string, unknown>): Promise<T> {
  // Allow tests to override via window.__TAURI_MOCK_OVERRIDES__
  const overrides = (window as unknown as Record<string, unknown>).__TAURI_MOCK_OVERRIDES__ as
    | Record<string, unknown>
    | undefined;
  if (overrides && cmd in overrides) {
    const val = overrides[cmd];
    if (typeof val === "function") {
      return (val as (args?: Record<string, unknown>) => T | Promise<T>)(args);
    }
    if (val instanceof Error) throw val;
    return val as T;
  }

  if (cmd === "workspace_get") {
    return getSeededWorkspaceResponse() as T;
  }

  // Derive app phase from workspace availability so navigation works without explicit overrides.
  // app_hydrate_phase checks workspace (including overrides); app_set_phase respects explicit arg.
  if (cmd === "app_hydrate_phase") {
    const workspace = resolveWorkspace(overrides);
    const migrationStatus = getSeededState()?.migrationStatus;
    let appPhase = "setup_required";
    if (workspace) {
      appPhase = migrationStatus === "running" ? "running_locked" : "ready_to_run";
    }
    return {
      appPhase,
      hasGithubAuth: false,
      hasAnthropicKey: false,
      isSourceApplied: workspace !== null,
      scopeFinalized: false,
      planFinalized: false,
    } as T;
  }

  if (cmd === "app_set_phase" || cmd === "app_set_phase_flags") {
    // Respect the explicit phase/flags passed by the caller.
    const requestedPhase = (args?.appPhase as string | undefined) ?? "setup_required";
    const scopeFinalized = (args?.scopeFinalized as boolean | undefined) ?? false;
    const planFinalized = (args?.planFinalized as boolean | undefined) ?? false;
    return {
      appPhase: requestedPhase,
      hasGithubAuth: false,
      hasAnthropicKey: false,
      isSourceApplied: requestedPhase !== "setup_required",
      scopeFinalized,
      planFinalized,
    } as T;
  }

  if (cmd in mockResponses) {
    return mockResponses[cmd] as T;
  }

  console.warn(`[tauri-e2e-mock] Unhandled invoke: ${cmd}`, args);
  return undefined as T;
}
