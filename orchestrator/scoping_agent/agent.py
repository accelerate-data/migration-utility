"""Scoping agent: identifies writer procedures for a SQL Server table.

Layer 2 entry point:
    uv run python -m scoping_agent.agent --table dbo.fact_sales --depth 2

Output is printed to stdout as JSON. Logs go to stderr.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import uuid
import argparse

import anthropic
from pydantic import ValidationError

from .models import CandidateWritersOutput
from .prompts import SYSTEM_PROMPT, make_analysis_request

logger = logging.getLogger(__name__)

_OUTPUT_RE = re.compile(r"<candidate_writers>\s*(.*?)\s*</candidate_writers>", re.DOTALL)

_DEFAULT_MODEL = "claude-opus-4-6"


def _parse_table_arg(table: str) -> tuple[str, str]:
    """Split 'schema.table' into (schema, table). Defaults to 'dbo' schema."""
    parts = table.split(".", 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    return "dbo", parts[0]


def _extract_json(text: str) -> str:
    """Extract the JSON payload from a <candidate_writers> block."""
    match = _OUTPUT_RE.search(text)
    if not match:
        raise ValueError(
            "Agent response does not contain a <candidate_writers> block. "
            f"Response preview: {text[:300]!r}"
        )
    return match.group(1)


def run_scoping_agent(table: str, depth: int) -> CandidateWritersOutput:
    """Run the scoping agent for a single table.

    Args:
        table: Target table as 'schema.table' (e.g. 'dbo.fact_sales').
        depth: Call-graph traversal depth, 0–5.

    Returns:
        Validated CandidateWritersOutput.

    Raises:
        ValueError: Invalid arguments.
        RuntimeError: Agent returned unparseable or invalid output.
    """
    if not 0 <= depth <= 5:
        raise ValueError(f"search_depth must be in 0–5, got {depth}")

    mcp_url = os.environ["MSSQL_MCP_URL"]
    model = os.environ.get("SCOPING_AGENT_MODEL", _DEFAULT_MODEL)
    batch_id = str(uuid.uuid4())
    schema, table_name = _parse_table_arg(table)

    logger.info(
        "event=agent_start operation=run_scoping_agent "
        "table=%s search_depth=%d model=%s batch_id=%s",
        table,
        depth,
        model,
        batch_id,
    )

    client = anthropic.Anthropic()
    user_message = make_analysis_request(schema, table_name, depth, batch_id)

    response = client.beta.messages.create(
        model=model,
        max_tokens=16384,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_message}],
        mcp_servers=[{"type": "url", "url": mcp_url, "name": "sqlserver"}],
        betas=["mcp-client-2025-04-04"],
    )

    text_blocks = [b.text for b in response.content if hasattr(b, "text") and b.text]
    if not text_blocks:
        raise RuntimeError("Agent returned no text output")

    full_text = "\n".join(text_blocks)

    try:
        raw_json = _extract_json(full_text)
    except ValueError as exc:
        logger.error(
            "event=parse_error operation=run_scoping_agent batch_id=%s error=%s",
            batch_id,
            exc,
        )
        raise RuntimeError(str(exc)) from exc

    try:
        data = json.loads(raw_json)
        output = CandidateWritersOutput.model_validate(data)
    except (json.JSONDecodeError, ValidationError) as exc:
        logger.error(
            "event=validation_error operation=run_scoping_agent batch_id=%s error=%s",
            batch_id,
            exc,
        )
        raise RuntimeError(f"Agent output failed contract validation: {exc}") from exc

    logger.info(
        "event=agent_complete operation=run_scoping_agent "
        "batch_id=%s status=%s",
        batch_id,
        output.results[0].status if output.results else "empty",
    )

    return output


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
    )

    parser = argparse.ArgumentParser(
        description="Scoping agent — identify writer procedures for a SQL Server table",
    )
    parser.add_argument(
        "--table",
        required=True,
        metavar="SCHEMA.TABLE",
        help="Target table (e.g. dbo.fact_sales)",
    )
    parser.add_argument(
        "--depth",
        type=int,
        default=2,
        metavar="N",
        help="Call-graph traversal depth, 0–5 (default: 2)",
    )
    args = parser.parse_args()

    try:
        output = run_scoping_agent(table=args.table, depth=args.depth)
        print(output.model_dump_json(indent=2, exclude_none=True))
        sys.exit(0)
    except Exception as exc:
        logger.error("event=agent_failure operation=main error=%s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
