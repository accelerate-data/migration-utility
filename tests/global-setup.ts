import { exec } from "node:child_process";
import { promisify } from "node:util";

const execAsync = promisify(exec);

export async function setup(): Promise<void> {
  if (!process.env.ANTHROPIC_API_KEY) {
    throw new Error(
      "ANTHROPIC_API_KEY is not set. Export it before running integration tests."
    );
  }

  const saPassword = process.env.SA_PASSWORD ?? "P@ssw0rd123";

  try {
    const { stdout } = await execAsync(
      `docker exec aw-sql /opt/mssql-tools18/bin/sqlcmd` +
        ` -S localhost -U sa -P '${saPassword}' -No -h -1` +
        ` -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM sys.databases WHERE name = 'MigrationTest'"`
    );
    const count = parseInt(stdout.trim(), 10);
    if (count !== 1) {
      throw new Error("MigrationTest database not found");
    }
  } catch (err) {
    throw new Error(
      `SQL Server prerequisite check failed: ${err}\n` +
        `Ensure the aw-sql Docker container is running and MigrationTest is set up:\n` +
        `  docker start aw-sql\n` +
        `  SA_PASSWORD='P@ssw0rd123' docker exec -i aw-sql /opt/mssql-tools18/bin/sqlcmd \\n` +
        `    -S localhost -U sa -P "\$SA_PASSWORD" -No < scripts/sql/create-migration-test-db.sql`
    );
  }
}
