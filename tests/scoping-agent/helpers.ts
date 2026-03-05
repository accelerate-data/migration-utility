import { spawn } from "node:child_process";
import { mkdtemp, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const REPO_ROOT = fileURLToPath(new URL("../../", import.meta.url));

export interface ScopingOutput {
  schema_version: string;
  run_id: string;
  results: ScopingResult[];
  summary: Record<string, number>;
}

export interface ScopingResult {
  item_id: string;
  status: string;
  selected_writer?: string;
  candidate_writers: CandidateWriter[];
  warnings: string[];
  errors: string[];
  validation: { passed: boolean; issues: string[] };
}

export interface CandidateWriter {
  procedure_name: string;
  write_type: string;
  call_path: string[];
  rationale: string;
  confidence: number;
}

/**
 * Spawns the scoping agent against the given fixture and returns parsed output.
 *
 * @param fixtureName - Base name of the fixture file (e.g. "resolved" resolves to
 *   scripts/sql/test-fixtures/resolved.input.json)
 */
export async function runScopingAgent(
  fixtureName: string
): Promise<ScopingOutput> {
  const inputPath = join(
    REPO_ROOT,
    "scripts/sql/test-fixtures",
    `${fixtureName}.input.json`
  );

  const tmpDir = await mkdtemp(join(tmpdir(), "scoping-agent-"));
  const outputPath = join(tmpDir, "output.json");

  // Pass the shell environment as-is. The MCP server (toolbox) reads
  // MSSQL_HOST, MSSQL_PORT, MSSQL_DB, and SA_PASSWORD from env.
  // Set these in your shell before running tests (see tests/README.md).
  const env: NodeJS.ProcessEnv = { ...process.env };

  await new Promise<void>((resolve, reject) => {
    const proc = spawn(
      "claude",
      [
        "--dangerously-skip-permissions",
        "--plugin-path",
        "plugin/",
        "--agent",
        "scoping-agent",
        inputPath,
        outputPath,
      ],
      { env, cwd: REPO_ROOT, stdio: "pipe" }
    );

    let stderr = "";
    proc.stderr?.on("data", (chunk: Buffer) => {
      stderr += chunk.toString();
    });

    proc.on("close", (code) => {
      if (code !== 0) {
        reject(
          new Error(
            `claude exited with code ${code} for fixture "${fixtureName}".\nstderr: ${stderr}`
          )
        );
      } else {
        resolve();
      }
    });

    proc.on("error", (err) => {
      reject(
        new Error(
          `Failed to spawn claude for fixture "${fixtureName}": ${err.message}`
        )
      );
    });
  });

  const raw = await readFile(outputPath, "utf-8");
  return JSON.parse(raw) as ScopingOutput;
}
