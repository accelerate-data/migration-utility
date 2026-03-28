/**
 * Test helpers for scoping agent integration tests.
 *
 * Each helper runs the scoping agent via the Claude Code CLI against a DDL
 * fixture directory and returns the parsed JSON output.
 */

import { execFileSync } from "child_process";
import { mkdtempSync, writeFileSync, readFileSync, rmSync } from "fs";
import { tmpdir } from "os";
import path from "path";
import { randomUUID } from "crypto";

const REPO_ROOT = path.resolve(import.meta.dirname, "../..");
const PLUGIN_PATH = path.join(REPO_ROOT, "plugin");
const FIXTURES_ROOT = path.join(import.meta.dirname, "fixtures");

export interface ScopingInput {
  item_id: string;
  search_depth?: number;
}

export interface Diagnostic {
  code: string;
  message: string;
  field?: string;
  severity: "error" | "warning";
  details?: Record<string, unknown>;
}

export interface CandidateWriter {
  procedure_name: string;
  write_type: "direct" | "indirect" | "read_only";
  call_path: string[];
  rationale: string;
  confidence: number;
}

export interface ScopingResult {
  item_id: string;
  status:
    | "resolved"
    | "ambiguous_multi_writer"
    | "partial"
    | "no_writer_found"
    | "error";
  selected_writer?: string;
  candidate_writers: CandidateWriter[];
  warnings: Diagnostic[];
  validation: { passed: boolean; issues: Diagnostic[] };
  errors: Diagnostic[];
}

export interface ScopingOutput {
  schema_version: string;
  run_id: string;
  results: ScopingResult[];
  summary: {
    total: number;
    resolved: number;
    ambiguous_multi_writer: number;
    no_writer_found: number;
    partial: number;
    error: number;
  };
}

/**
 * Run the scoping agent against a named fixture directory.
 *
 * @param fixture  Name of a subdirectory under tests/scoping-agent/fixtures/
 * @param items    Items to include in the input payload
 * @returns Parsed scoping agent output
 */
export function runScopingAgent(
  fixture: string,
  items: ScopingInput[]
): ScopingOutput {
  const fixtureDir = path.join(FIXTURES_ROOT, fixture);
  const tmpDir = mkdtempSync(path.join(tmpdir(), "scoping-test-"));

  try {
    const runId = randomUUID();
    const inputFile = path.join(tmpDir, "input.json");
    const outputFile = path.join(tmpDir, "output.json");

    const input = {
      schema_version: "1.0",
      run_id: runId,
      technology: "sql_server",
      ddl_path: fixtureDir,
      items: items.map((item) => ({
        item_id: item.item_id,
        search_depth: item.search_depth ?? 2,
      })),
    };

    writeFileSync(inputFile, JSON.stringify(input, null, 2), "utf8");

    execFileSync(
      "claude",
      [
        "-p",
        "--dangerously-skip-permissions",
        "--plugin-dir",
        PLUGIN_PATH,
        "--agent",
        "scoping-agent",
        `${inputFile} ${outputFile}`,
      ],
      {
        env: { ...process.env },
        input: "",
        timeout: 180_000,
        encoding: "utf8",
      }
    );

    const raw = readFileSync(outputFile, "utf8");
    return JSON.parse(raw) as ScopingOutput;
  } finally {
    rmSync(tmpDir, { recursive: true, force: true });
  }
}
