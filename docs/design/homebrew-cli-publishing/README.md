# Homebrew CLI Publishing

`/init-ad-migration` remains the supported workflow entrypoint, but it must be able to acquire the standalone `ad-migration` CLI through Homebrew on macOS when the binary is missing.

## Decisions

- Publish a custom Homebrew tap at `accelerate-data/homebrew-tap`.
- Scope the first Homebrew release to macOS only.
- Keep Homebrew responsible for machine-wide prerequisites and the public CLI install, not project scaffolding.
- Keep `/init-ad-migration` as the authoritative workflow gate: it installs the brewed CLI when missing, then continues with the existing prerequisite checks and bootstrap flow.
- Build and publish `wheel` and `sdist` artifacts for the public CLI package from tagged releases, and have the tap formula install from those artifacts.
- Split packaging into three Python distributions in this repo: `ad-migration-shared` for reusable logic, `ad-migration-cli` for the public `ad-migration` executable, and `ad-migration-internal` for plugin-only console scripts.

## Required packaging behavior

- The Homebrew formula name remains `ad-migration`.
- The Homebrew-installed package must expose only the public `ad-migration` executable.
- Plugin-only commands such as `discover`, `setup-ddl`, `migrate-util`, and `refactor` stay out of the Homebrew-installed public `bin` surface.
- Shared command logic must live in reusable modules rather than being duplicated between the public and internal packages.
- The formula must install durable non-secret machine dependencies needed for the supported local workflow, including `freetds` and `unixodbc`.

## Required init behavior

- `/init-ad-migration` checks whether `ad-migration` is available on `PATH`.
- If the binary is missing on macOS, `/init-ad-migration` installs the tap if needed and runs `brew install ad-migration`, then resumes the normal init flow.
- `/init-ad-migration` does not treat a successful Homebrew install as proof of readiness; it still runs the existing prerequisite checks and fails on any remaining readiness issue.
- If Homebrew is unavailable or the install command fails, `/init-ad-migration` stops and surfaces a direct remediation message instead of attempting a second installer path.
- Linux support is deferred to a later design and must not be implied by the initial Homebrew flow.

## Release contract

- Tagged releases build the public CLI `wheel` and `sdist` artifacts in CI.
- Release automation updates `accelerate-data/homebrew-tap` with the new formula version and checksums after those artifacts are published.
- The tap is the stable user install surface; `/init-ad-migration` does not install from ad hoc artifact URLs.

## Why this matters

Agents need one stable answer for how the standalone CLI reaches a clean macOS machine without turning Homebrew into a second project bootstrap path or leaking plugin-only commands into the public interface.
