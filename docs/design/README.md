# Design Docs

Each design topic gets its own subdirectory with a `README.md`.

## What Goes In `requirements`

Use `docs/requirements/` for discovery and planning artifacts:

- problem framing and scope assumptions
- research notes and evaluation artifacts
- build plans and decision logs

See [Requirements README](../requirements/README.md).

## What Goes In `design`

Use `docs/design/` for architecture and implementation design details.

### Design Index

- [Overall Design](overall-design/README.md) — end-to-end user flow, architecture decisions, and open issues
- [Database Design](database/README.md)
- [Test Scenario Design](test-scenario/README.md)
- [API Docs](api-docs/README.md)
- [UI Patterns](ui-patterns/README.md) — surfaces, user flows, screen-level patterns, and interactive mockup
- [Application State](application-state/README.md)
- [Branding](branding/README.md)
- [Sidecar](sidecar/README.md)
- [Agent Contract](agent-contract/README.md) — structured agent output contracts and FDE review requirements
- [Unit Test Strategy](unit-test-strategy/README.md) — synthetic vs real data for proc-to-dbt behavioral equivalence testing

## What Goes In `references`

Use `docs/reference/` for supporting operational/reference material that is not a
design decision artifact.

See [Reference README](../reference/README.md).
