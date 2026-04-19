from __future__ import annotations

from shared.diagnostics.context import CatalogContext
from shared.diagnostics.registry import DiagnosticRegistry, diagnostic


def test_registry_filters_checks_by_object_dialect_and_pass() -> None:
    registry = DiagnosticRegistry()

    def proc_check(_ctx: CatalogContext) -> None:
        return None

    def view_check(_ctx: CatalogContext) -> None:
        return None

    registry.register(proc_check, code="PROC", objects=["procedure"], dialects=("tsql",), severity="warning", pass_number=1)
    registry.register(view_check, code="VIEW", objects=["view"], dialects=("oracle",), severity="warning", pass_number=2)

    assert [spec.code for spec in registry.checks_for("procedure", "tsql", 1)] == ["PROC"]
    assert registry.checks_for("procedure", "oracle", 1) == []
    assert registry.checks_for("view", "oracle", 1) == []


def test_diagnostic_decorator_registers_on_supplied_registry() -> None:
    registry = DiagnosticRegistry()

    @diagnostic("LOCAL", ["procedure"], registry=registry)
    def local_check(_ctx: CatalogContext) -> None:
        return None

    checks = registry.checks_for("procedure", "tsql", 1)
    assert checks[0].fn is local_check
    assert checks[0].code == "LOCAL"
