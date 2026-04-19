"""Diagnostic registry types and decorators."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from shared.diagnostics.context import CatalogContext

ALL_DIALECTS = ("tsql", "oracle")


@dataclass
class DiagnosticResult:
    """One diagnostic finding, written to warnings[] or errors[] in catalog JSON."""

    code: str
    message: str
    severity: str
    details: dict[str, object] | None = None

    def to_dict(self) -> dict[str, object]:
        data: dict[str, object] = {
            "code": self.code,
            "message": self.message,
            "severity": self.severity,
        }
        if self.details:
            data["details"] = self.details
        return data


CheckFn = Callable[[CatalogContext], DiagnosticResult | list[DiagnosticResult] | None]


@dataclass
class _CheckSpec:
    fn: CheckFn
    code: str
    objects: list[str]
    dialects: tuple[str, ...]
    severity: str
    pass_number: int


class DiagnosticRegistry:
    """Global registry of diagnostic check functions."""

    def __init__(self) -> None:
        self._checks: list[_CheckSpec] = []

    def register(
        self,
        fn: CheckFn,
        code: str,
        objects: list[str],
        dialects: tuple[str, ...],
        severity: str,
        pass_number: int,
    ) -> None:
        self._checks.append(
            _CheckSpec(
                fn=fn,
                code=code,
                objects=objects,
                dialects=dialects,
                severity=severity,
                pass_number=pass_number,
            )
        )

    def checks_for(self, object_type: str, dialect: str, pass_number: int) -> list[_CheckSpec]:
        return [
            check
            for check in self._checks
            if object_type in check.objects
            and dialect in check.dialects
            and check.pass_number == pass_number
        ]


_REGISTRY = DiagnosticRegistry()


def diagnostic(
    code: str,
    objects: list[str],
    dialects: tuple[str, ...] = ALL_DIALECTS,
    severity: str = "warning",
    pass_number: int = 1,
    *,
    registry: DiagnosticRegistry | None = None,
) -> Callable[[CheckFn], CheckFn]:
    """Decorator to register a diagnostic check function."""

    def decorator(fn: CheckFn) -> CheckFn:
        target_registry = _REGISTRY if registry is None else registry
        target_registry.register(fn, code, objects, dialects, severity, pass_number)
        return fn

    return decorator
