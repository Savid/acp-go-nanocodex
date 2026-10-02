# AGENTS.md

## Purpose

This module exposes the Nanocodex Rust agent library through ACP. The Go
adapter owns ACP and session-store mirroring. A Rust helper in `native/`
owns the agent and communicates over JSONL on dedicated stdio pipes. Each
prompt runs a helper process against the session's native rollout; no helper
runs between prompts. New and restored sessions use short observation runs.

## Project Map

- `cmd/acp-go-nanocodex`: stdio entrypoint, OpenTelemetry, signals, and flags.
- Root `agent*.go`, `options.go`, `request_builders.go`: public ACP methods,
  process options, request validation, and capability negotiation.
- Root `session*.go`: native session state, prompt turns, lifecycle events,
  store mirrors, restore, replay, and configuration.
- `internal/nanocodex`: the helper's JSONL client and native event shapes.
- `native/`: the Rust helper, pinned upstream library, and protocol reference.
- `integration/`: gated tests against the built helper.
- `bin/`: built Go adapter and matching Rust helper; never committed.
- `scripts/release.py`: release tag checks, reproducible builds, archive
  smoke, and the release manifest. `dist/` holds its output; never committed.

## Commands

```sh
make build
make test
make lint
make audit
make native-test
make native-vuln
make test-integration-smoke
make test-integration-live
make release-check TAG=vX.Y.Z
make release-build TAG=vX.Y.Z
make release-smoke TAG=vX.Y.Z RELEASE_TARGET=linux_amd64
```

`make build` stages both executables under `bin/`. The native toolchain is
pinned in `native/rust-toolchain.toml`; Rust build artifacts live under
`.tmp/cargo`. `make test` runs deterministic Rust tests and Go race tests in
shuffled order. `make audit` is the full local gate, including Rust formatting,
clippy, tests, and both executable builds. Integration tests default to both
executables under `bin/`; `ACP_GO_NANOCODEX_AGENT_BINARY` and
`ACP_GO_NANOCODEX_HARNESS_PATH` override their paths. The integration Make
targets build both executables first. Only the live target spends model tokens,
and it requires explicit operator intent.

Release targets require a clean commit whose `HelperVersion` and native
package version equal the tag. `make release-build` installs pinned zig and
cargo-zigbuild under `.tmp/` for Linux targets, builds with a glibc 2.28
floor, and writes reproducible archives under `dist/`. CI runs the release
jobs only for `v*` tags, after audit passes.

`make native-vuln` installs pinned `cargo-audit` under `.tmp/` on first use and
checks `native/Cargo.lock` against the RustSec advisory database. `make audit`
includes that scan. CI runs audit and the real-helper smoke tests on Linux
and macOS.

## Coding Rules

- Follow Go idioms: context first, wrapped errors, and small interfaces at
  the consumer. Keep native protocol details in `internal/nanocodex`.
- Shared behavior comes from `github.com/savid/acp-go-core`; never copy it.
- The helper is an ordinary child process with inherited environment and
  session-scoped overlays. It provides no isolation.
- Keep stdout reserved for protocol frames and diagnostics on stderr.
- Closing or deleting ACP sessions leaves native rollout files in place.
  Recovery preserves complete records and may discard an invalid unfinished
  final record. The session store is the durability boundary.
- Keep the upstream Rust dependency pinned and commit `native/Cargo.lock`.
- Unit tests use deterministic fake processes or local fixture servers and
  never require real credentials, a native installation, or model tokens.
- Comments state what the code does or why a constraint exists.

## Verification

Run `make test` and `make lint` for Go or Rust changes. Run `make audit` once
changes settle. Run the integration smoke target after changing the native
protocol, persistence, or executable packaging. After changing release
packaging, run `make release-build` and `make release-smoke` for the host
target. Tests must verify externally
visible behavior and concrete failure boundaries.

## Boundaries

- Advertise only implemented capabilities. Unsupported input fails clearly.
- Keep provider credentials in environment or native auth files. Never log
  credentials, prompts, tool contents, or raw native event bodies by default.
- Never run live model tests without explicit operator intent.
- Never push, tag, or publish without user authorization.
