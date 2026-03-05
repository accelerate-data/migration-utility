import { exec } from "node:child_process";
import { promisify } from "node:util";

const execAsync = promisify(exec);

export async function setup(): Promise<void> {
  const missing = ["ANTHROPIC_API_KEY", "SA_PASSWORD", "MSSQL_DB"].filter(
    (v) => !process.env[v]
  );
  if (missing.length > 0) {
    throw new Error(
      `Missing required environment variables: ${missing.join(", ")}\n` +
        `Export them before running integration tests. Example:\n` +
        `  export ANTHROPIC_API_KEY=your-api-key\n` +
        `  export SA_PASSWORD='P@ssw0rd123'\n` +
        `  export MSSQL_DB=MigrationTest`
    );
  }

  const saPassword = process.env.SA_PASSWORD!;
  const mssqlDb = process.env.MSSQL_DB!;

  try {
    const { stdout } = await execAsync(
      `docker exec aw-sql /opt/mssql-tools18/bin/sqlcmd` +
        ` -S localhost -U sa -P '${saPassword}' -No -h -1` +
        ` -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM sys.databases WHERE name = '${mssqlDb}'"`
    );
    const count = parseInt(stdout.trim(), 10);
    if (count !== 1) {
      throw new Error(`Database '${mssqlDb}' not found in SQL Server`);
    }
  } catch (err) {
    throw new Error(
      `SQL Server prerequisite check failed: ${err}\n` +
        `Ensure the aw-sql Docker container is running and ${mssqlDb} is set up:\n` +
        `  docker start aw-sql\n` +
        `  docker exec -i aw-sql /opt/mssql-tools18/bin/sqlcmd \\n` +
        `    -S localhost -U sa -P "\$SA_PASSWORD" -No < scripts/sql/create-migration-test-db.sql`
    );
  }
}
