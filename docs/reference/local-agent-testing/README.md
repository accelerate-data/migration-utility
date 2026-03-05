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
Layer 1 — MCP server          genai-toolbox + local SQL Server
           ↓ validates           SQL catalog queries return correct data
Layer 2 — Python agent         Claude Agent SDK + Layer 1 toolbox running locally
           ↓ validates           Agent logic, six-step pipeline, output contract
Layer 3 — GH Action            Workflow YAML mechanics, stub agent step
           ↓ validates           Cache, DB restore, branch/rebase/commit/push
Layer 4 — Integration          Real DacPac project, real agent, real GH Action run
           ↓ validates           Full end-to-end
```

### What each layer does NOT test

| Layer | Explicitly out of scope |
|---|---|
| 1 | Agent reasoning, Python SDK, GH Actions |
| 2 | GH Actions workflow mechanics, DacPac restore |
| 3 | Agent reasoning, API calls, real SQL Server catalog |
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
   Validate the SQL catalog queries interactively before writing any agent code.
2. **Layer 2 — Python Agent** *(doc coming)*
   Implement and iterate on agent logic cheaply against a local toolbox instance.
3. **Layer 3 — GH Action** *(doc coming)*
   Wire the workflow once the agent output format is stable; stub the agent step.
4. **Layer 4 — Integration** *(doc coming)*
   Trigger a real run end-to-end.

---

## Stack summary

| Component | Technology | Managed by |
|---|---|---|
| SQL Server | Docker container (local) | Desktop app / manual `docker run` |
| MCP server | googleapis/genai-toolbox | `toolbox` binary (Layer 1–2) / GHCR service container (Layer 3–4) |
| Agent | Python + Claude Agent SDK | `uv run python -m scoping_agent` |
| Workflow | GitHub Actions | `act` (Layer 3) / real GH runner (Layer 4) |
