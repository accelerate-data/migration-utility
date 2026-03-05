"""Tests for agent utility functions that do not require a live SQL Server."""

import json

import pytest

from scoping_agent.agent import _extract_json, _parse_table_arg, run_scoping_agent
from scoping_agent.models import CandidateWritersOutput


# ---------------------------------------------------------------------------
# _parse_table_arg
# ---------------------------------------------------------------------------


def test_parse_table_arg_with_schema():
    assert _parse_table_arg("dbo.fact_sales") == ("dbo", "fact_sales")


def test_parse_table_arg_defaults_to_dbo():
    assert _parse_table_arg("fact_sales") == ("dbo", "fact_sales")


def test_parse_table_arg_preserves_schema_case():
    assert _parse_table_arg("Sales.Orders") == ("Sales", "Orders")


# ---------------------------------------------------------------------------
# _extract_json
# ---------------------------------------------------------------------------

_VALID_OUTPUT = {
    "schema_version": "1.0",
    "batch_id": "test-batch",
    "results": [
        {
            "item_id": "dbo.fact_sales",
            "status": "no_writer_found",
            "candidate_writers": [],
            "warnings": [],
            "validation": {"passed": True, "issues": []},
            "errors": [],
        }
    ],
    "summary": {
        "total": 1,
        "resolved": 0,
        "ambiguous_multi_writer": 0,
        "no_writer_found": 1,
        "partial": 0,
        "error": 0,
    },
}


def test_extract_json_clean_block():
    payload = json.dumps(_VALID_OUTPUT)
    text = f"<candidate_writers>\n{payload}\n</candidate_writers>"
    assert json.loads(_extract_json(text)) == _VALID_OUTPUT


def test_extract_json_with_surrounding_text():
    payload = json.dumps(_VALID_OUTPUT)
    text = (
        "Here is the analysis.\n"
        f"<candidate_writers>{payload}</candidate_writers>\n"
        "Done."
    )
    assert json.loads(_extract_json(text)) == _VALID_OUTPUT


def test_extract_json_missing_block():
    with pytest.raises(ValueError, match="<candidate_writers>"):
        _extract_json("No block here at all.")


# ---------------------------------------------------------------------------
# run_scoping_agent — argument validation (no network required)
# ---------------------------------------------------------------------------


def test_run_scoping_agent_rejects_depth_out_of_range():
    with pytest.raises(ValueError, match="search_depth"):
        run_scoping_agent(table="dbo.fact_sales", depth=6)

    with pytest.raises(ValueError, match="search_depth"):
        run_scoping_agent(table="dbo.fact_sales", depth=-1)
