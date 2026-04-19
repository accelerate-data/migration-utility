# Readiness Error Guard

## Decision

Object-scoped `migrate-util ready` fails when the requested object has unresolved catalog errors. Catalog warnings remain non-blocking.

## Reason

Migration skills use `migrate-util ready <stage> --object <fqn>` as their guard before doing scoped work. `/status` already routes users to diagnostics when unresolved errors exist, but direct stage commands need the same hard stop through the readiness contract.

## Contract

Readiness checks load the requested object's active diagnostics before stage-specific prerequisites. If any diagnostic has `severity: error`, readiness returns `ready: false` with a stable diagnostic-blocked code and reason.

Warnings do not change readiness. They continue to appear in `/status`, object detail, and review workflows without blocking stage execution.

Workflow-exempt objects keep their current behavior. Source, seed, and excluded objects still return their existing not-applicable responses instead of a generic diagnostic-blocked result.

## Scope

The guard applies only to object-scoped readiness. Project-only readiness continues to answer project setup questions such as target, sandbox, dbt project, and profiles availability.

The guard is diagnostic-code agnostic. `TYPE_MAPPING_UNSUPPORTED` from target type translation uses the same path as other active catalog errors.

## Remediation

Users resolve blocked objects through the reviewing-diagnostics workflow. Errors are not accepted through reviewed-warning artifacts; the diagnostic remains active until the underlying catalog error is fixed or removed by the owning workflow.
