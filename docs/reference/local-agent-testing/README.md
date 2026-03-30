# Local Agent Testing

A layered approach to developing and testing the scoping agent pipeline on a Mac without
needing a full GH Actions environment. Each layer is independently testable; bring them
together only in Layer 4.

Design references:

- Overall pipeline: `docs/design/overall-design/README.md` — Agent Execution Model
- Agent contract: `docs/design/agent-contract/scoping-agent.md`

---

## Layers

```text
Layer 1 — MCP server              genai-toolbox + local SQL Server
           ↓ validates               SQL catalog queries return correct data
Layer 2 — Deterministic skills     Python scripts (discover.py, profile.py, migrate.py) + DDL fixtures
           ↓ validates               AST analysis, confidence scoring, output contracts
Layer 3 — GH Action               Workflow YAML mechanics, stub skill step
           ↓ validates               Cache, DB restore, branch/rebase/commit/push
Layer 4 — Integration             Real DacPac project, real skills, real GH Action run
           ↓ validates               Full end-to-end
```

### What each layer does NOT test

| Layer | Explicitly out of scope |
|---|---|
| 1 | Skills, GH Actions |
| 2 | GH Actions, DacPac restore |
| 3 | Skill logic, real SQL Server catalog |
| 4 | Nothing — this is the full system |

---

## Prerequisites (all layers)

- Docker Desktop running
- Local SQL Server container with a project database restored (see [Docker Setup](../setup-docker/README.md))
- `gh` CLI authenticated
- `ANTHROPIC_API_KEY` set in your shell environment (Layer 2+)

---

## Recommended build order

Do not skip ahead. Each layer catches a class of bugs the next layer cannot easily diagnose.

1. **[Layer 1 — MCP Server](layer-1-mcp-server.md)**
   Validate the SQL catalog queries interactively before writing any skill code.
2. **Layer 2 — Deterministic Skills** *(doc coming)*
   Test Python skills (`discover.py`, `discover.py`, etc.) against DDL fixtures. No LLM, no API key needed.
3. **Layer 3 — GH Action** *(doc coming)*
   Wire the workflow once the skill output format is stable; stub the skill step.
4. **Layer 4 — Integration** *(doc coming)*
   Trigger a real run end-to-end.

---

## Stack summary

| Component | Technology | Managed by |
|---|---|---|
| SQL Server | Docker container (local) | Desktop app / manual `docker run` |
| MCP server | googleapis/genai-toolbox | `toolbox` binary (Layer 1–2) / GHCR service container (Layer 3–4) |
| Deterministic skills | Python + sqlglot | `uv run python discover.py ...` (Layer 2) |
| Workflow | GitHub Actions | `act` (Layer 3) / real GH runner (Layer 4) |
