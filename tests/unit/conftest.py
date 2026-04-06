import json
import sys
from pathlib import Path

import jsonschema
import pytest
from referencing import Registry, Resource
from referencing.jsonschema import DRAFT202012

# Ensure plugin/lib/ is on the path so tests can import shared.* directly.
sys.path.insert(0, str(Path(__file__).parents[2] / "plugin" / "lib"))

SCHEMA_DIR = Path(__file__).parents[2] / "plugin" / "lib" / "shared" / "schemas"


def _build_registry() -> Registry:
    """Load all schemas into a registry for $ref resolution."""
    resources: list[tuple[str, Resource]] = []
    for schema_file in SCHEMA_DIR.glob("*.json"):
        contents = json.loads(schema_file.read_text(encoding="utf-8"))
        resources.append(
            (schema_file.name, Resource.from_contents(contents, default_specification=DRAFT202012))
        )
    return Registry().with_resources(resources)


_registry = _build_registry()


def _assert_valid_schema(data: dict, schema_name: str) -> None:
    """Validate data against a JSON schema in lib/shared/schemas/."""
    schema = json.loads((SCHEMA_DIR / schema_name).read_text(encoding="utf-8"))
    validator = jsonschema.Draft202012Validator(schema, registry=_registry)
    validator.validate(data)


@pytest.fixture()
def assert_valid_schema():
    """Fixture that returns a schema validation callable."""
    return _assert_valid_schema


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "integration: requires Docker SQL Server (MigrationTest database)"
    )
    config.addinivalue_line(
        "markers",
        "oracle: requires Docker Oracle with SH schema loaded "
        "(ORACLE_USER / ORACLE_PASSWORD / ORACLE_DSN)",
    )
