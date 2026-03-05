import { exec } from "node:child_process";
import { promisify } from "node:util";

const execAsync = promisify(exec);

export async function setup(): Promise<void> {
  // claude reads ANTHROPIC_API_KEY from .claude/settings.local.json (written
  // by `claude auth login`) — no shell env var required for the API key.

  // Verify the aw-sql container is running and MigrationTest exists.
  const saPassword = process.env.SA_PASSWORD ?? "P@ssw0rd123";
  try {
    const { stdout } = await execAsync(
      `docker exec aw-sql /opt/mssql-tools18/bin/sqlcmd` +
        ` -S localhost -U sa -P '${saPassword}' -No -h -1` +
        ` -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM sys.databases WHERE name = 'MigrationTest'"`
    );
    if (parseInt(stdout.trim(), 10) !== 1) {
      throw new Error("MigrationTest database not found");
    }
  } catch (err) {
    throw new Error(
      `SQL Server prerequisite check failed.\n` +
        `Ensure the aw-sql container is running and MigrationTest is loaded:\n` +
        `  docker start aw-sql\n` +
        `  docker exec -i aw-sql sqlcmd -S localhost -U sa -P '${saPassword}' -No \\\n` +
        `    < scripts/sql/create-migration-test-db.sql\n` +
        `Error: ${err}`
    );
  }
}
