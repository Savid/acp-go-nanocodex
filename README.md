# acp-go-nanocodex

An [Agent Client Protocol](https://agentclientprotocol.com/) adapter for the
[Nanocodex](https://github.com/gakonst/nanocodex) Rust agent library. The Go
package implements ACP; the bundled Rust helper runs the native agent and
tools. Each prompt launches a helper, resumes the conversation, runs its tool
loop, flushes native history, and exits before the prompt settles.

## Build and run

Install Go, Rust through rustup, a C toolchain, and Make. The Go module and
`native/rust-toolchain.toml` select the required toolchains; Cargo pins the
upstream source revision and dependency graph.

```sh
make build
install -m 755 bin/acp-go-nanocodex bin/acp-go-nanocodex-native ~/.local/bin/
acp-go-nanocodex
```

The Go command also supports `go install ./cmd/acp-go-nanocodex`; install
the matching built helper on `PATH` alongside it.

Both binaries must be installed from the same release. The adapter checks the
helper release, protocol version, and a fingerprint of the helper build inputs
and Go wire declarations during initialization. Rebuild both binaries after
changing those inputs. Alternatively, run
from the checkout:

```sh
bin/acp-go-nanocodex -path "$PWD/bin/acp-go-nanocodex-native"
```

The helper loads the host's trusted CA certificates during initialization,
even when every endpoint uses `http://`. On Linux, install the system CA bundle
(`ca-certificates`); without it, initialization fails with `invalid_config`.

The command speaks ACP JSON-RPC on stdin/stdout. Diagnostics go to stderr.
Configure authentication through `OPENAI_API_KEY` or native `auth.json` in
`CODEX_HOME` (default `$HOME/.codex`). Rollouts therefore use the caller's real
`~/.codex/sessions` by default; use `-home` or `WithHome` for a separate root.
The helper runs tools with the caller's
identity, environment, and session working directory. There is no ACP approval
or elicitation surface.

For **OpenRouter, OMP auth-gateway, and OpenCode Go**, see [provider configuration](PROVIDERS.md).
Configured API-key HTTPS routes use standard Responses requests with complete
history, `store:false`, and native function tools. All six model IDs accepted by Nanocodex are supported, including the three
omitted from its default picker; `NANOCODEX_MODEL_ID_PREFIX` sets a gateway
namespace for these native IDs. On those routes any other model ID runs as a
[gateway model](PROVIDERS.md#gateway-models): it is sent verbatim and uses the
settings of a native base model.

## Embed

```go
package main

import (
    "context"
    "os"

    nanocodexacp "github.com/savid/acp-go-nanocodex"
)

func main() {
    if err := nanocodexacp.Serve(context.Background(), os.Stdin, os.Stdout,
        nanocodexacp.WithExecutablePath("/opt/bin/acp-go-nanocodex-native"),
        nanocodexacp.WithDefaultModel("gpt-6.1-sol"),
    ); err != nil {
        panic(err)
    }
}
```

`HelperRelease` returns the helper release and fingerprint that this module
version requires. `Serve` closes its agent when the connection or context
ends. It accepts caller-supplied streams and does not take ownership of their
underlying files.
`NewAgent` exposes the ACP methods without an attached notification transport.
Use `Serve` with caller-supplied streams to receive session updates and replay.

### Process options

| Option | Meaning |
|---|---|
| `WithExecutablePath` | Helper path or name on the base `PATH`; default `acp-go-nanocodex-native`. |
| `WithHome` | Absolute native root, passed as `CODEX_HOME`. |
| `WithScratchDir` | Accepted absolute scratch parent; this adapter allocates no scratch state. |
| `WithInputHandoffRoot` | Absolute read root for verified image handoff files. |
| `WithDefaultModel` | Default model ID for new sessions. |
| `WithConfiguredModels` | Explicit model IDs appended to the model selector. Helper validation still applies. |
| `WithEnv` | Environment overlay applied after the inherited environment. |
| `WithSeedFiles` | Relative native-home files written before launch; existing unmanaged files are refused. |
| `WithSessionStore` | Authoritative `acpcore.SessionStore`; defaults to a fresh in-memory store. |
| `WithLogger` | Structured diagnostic logger. |
| `WithAgentName`, `WithAgentTitle`, `WithAgentVersion` | ACP implementation identity. |
| `WithTracerProvider`, `WithMeterProvider`, `WithTextMapPropagator` | OpenTelemetry providers and propagation. |
| `WithConcurrencyLimits` | `MaxActiveSessions` (default 32) and `MaxConcurrentClientCalls` (default 16). The helper needs no client calls. |
| `WithImageLimits` | Decoded input/output byte bounds; each field defaults to 6 MiB. Zero disables that policy bound while transport bounds remain. |

`Options`, `Option`, `ConcurrencyLimits`, and `ImageLimits` configure the
adapter. Invalid construction options fail before native launch.

### Command flags

| Flag | Meaning |
|---|---|
| `-path` | Helper executable. |
| `-home` | Native configuration root. |
| `-scratch-dir` | Absolute scratch parent. |
| `-model` | Default model ID. |
| `-seed-file` | Repeatable `relative/path=/host/file` seed. |
| `-debug` | Enable diagnostic logging to stderr. |
| `-version` | Print the adapter version and exit. |
| `-nanocodex-helper-release` | Print the required helper release and fingerprint as JSON and exit. |

The command configures telemetry through `OTEL_*` environment variables.
Library use does not change global telemetry providers.

## Sessions and configuration

`NanocodexOptions` is the typed `_meta.nanocodex.options` namespace. Construct
it with `NewNanocodexOptions`, then attach it using
`WithSessionNanocodexOptions` and request builders from
`github.com/savid/acp-go-core/wire`.

| Field | Constructor | Meaning |
|---|---|---|
| `model` | `WithNanocodexModel` | Native model ID, or a gateway model ID on a configured gateway route. |
| `baseModel` | `WithNanocodexBaseModel` | Native model whose settings a [gateway model](PROVIDERS.md#gateway-models) uses. |
| `contextWindow` | `WithNanocodexContextWindow` | Token window for usage reporting and context management; see [gateway models](PROVIDERS.md#gateway-models). |
| `thinking` | `WithNanocodexThinking` | Native reasoning effort. |
| `env` | `WithNanocodexEnv` | Session environment overlay. |
| `extraPathDirs` | `WithNanocodexExtraPathDirs` | Ordered absolute directories prepended to `PATH`. |
| `apiBaseUrl` | `WithNanocodexAPIBaseURL` | Responses API base URL. |
| `websocketUrl` | `WithNanocodexWebsocketURL` | Native Responses WebSocket endpoint. |
| `modelIdPrefix` | `WithNanocodexModelIDPrefix` | Provider wire namespace for native model IDs, such as `openai` or `openai-codex`. |
| `transport` | `WithNanocodexTransport` | `https` (default) or `websocket`. |
| `apiKeyEnv` | `WithNanocodexAPIKeyEnv` | Environment variable containing the provider key; defaults to `OPENAI_API_KEY`. |
| `authFile` | `WithNanocodexAuthFile` | Native authentication file. |

`ValidateNanocodexSessionMeta` validates this namespace without launching a
helper. Unknown own-namespace fields are refused. Environment and path values
are copied and stored with the session. Explicit session options override
native environment defaults.

ACP config selectors are `model` (category `model`) and `thought_level`
(category `thought_level`). The model can change before the first committed
turn; later turns retain that model. Effort can change between turns.
`SetModelRequest` builds a model selector request.

Text, resource links, embedded text resources, and raster images are accepted.
Image validation uses `github.com/savid/acp-go-core/image`. Ordered text and
images are preserved. Handoff files are read only when `WithInputHandoffRoot`
is configured. Additional directories, MCP servers, structured output,
session modes, account-usage reads, and slash-command discovery are unsupported.
Slash-prefixed text is ordinary prompt text.

`WithSessionRawEvents(true)` enables `RawEventMethod`
(`_nanocodex/rawEvent`) for the live prompt. Notifications are bounded,
non-authoritative, and omit inline image payloads. They are never replayed.

The negotiated `acp-go.dev/lifecycle` extension reports
`updatesOutsidePrompt:false` and no activity kinds. Each prompt owns a fresh
stream. A second foreground operation receives backpressure. `session/cancel`
cancels the native turn. The prompt response waits for native cleanup, the
store commit, and final lifecycle publication. Cancelling a handler context
does not release turn ownership or cancel native work.

Each completed generation call that reports usage publishes one
`usage_update`. `cost` is the session's cumulative USD sum of the `usage.cost`
charges gateway responses report. Native routes, BYOK calls, and calls without
a reported charge add nothing, so it may understate the session's charge.
`cost` is absent until the first priced call and survives load and resume.
Charges reported after a prompt's last `usage_update` (compaction summaries,
retried or rejected responses, or calls during a cancelled or failed prompt)
first appear on the next one.

## Persistence

`SessionStoreFormat` is `nanocodex-rollout-jsonl-v1`. The main subpath contains
raw native rollout rows; `config` contains the ACP/native identities, relative
rollout path, working directory, provider/model settings, accepted environment,
title, timestamp, and the cumulative cost once a call is priced.
`github.com/savid/acp-go-core` supplies the store types and atomic mirror
operations.

After a prompt, the helper stops the native writer and atomically saves the latest
optional snapshot checkpoint in `<rollout>.acp-checkpoint.json`. The store mirrors
it in `config.checkpoint`. It retains token accounting, the request prefix, and a
pending compaction after a context overflow, so the next helper can compact
before its first model call. Conversation history
remains in native records. Restore uses a checkpoint only when its byte boundary,
history length, identity, and supported shape match; otherwise it rebuilds from
native history. Checkpoint failures are diagnostic and do not fail a committed
turn. Native history corruption still fails restore.

The store is authoritative even when the default in-memory store is used.
An adapter never adopts a native-only session that lacks a store entry.
`session/load` restores state and replays messages, visible reasoning summaries,
and function/custom tool calls with their text results, including new output in
compacted replacement history. Items discarded by native mid-turn compaction
before persistence cannot replay. Multimodal tool results replay only their text
parts. `session/resume` restores without replay.
A native file with additional rows
is adopted only when its shared prefix agrees with the store. Divergence fails
restore. Deletion tombstones the store and leaves native files intact.

Native files live under `CODEX_HOME/sessions/`. The Go adapter holds the native
UUID lock from before hydration until the helper has exited and been reaped.
The helper holds a separate writer lock for its lifetime; hydration and stopped
snapshots refuse an active writer even after an adapter crash. New, load, and
resume responses and list entries expose that UUID in `_meta.nanocodex.nativeSessionId`, distinct
from the ACP `sessionId`. After shutdown, use it as both `sessionId` and `resumeSessionId` in the
[native helper protocol](native/PROTOCOL.md) to continue independently.

An untouched native rollout has no resumable conversation. The next launch
after `session/new`, including a prompt or config change, creates a new native
binding when the existing state is a verified header-only rollout. Each such
launch preserves the old metadata file and commits the new binding before
accepting input or returning configuration success. The ACP session ID
stays stable. Malformed or nonempty native state never takes this path.

An unexpected native session or rollout identity change returns
`nanocodex_session_poisoned` with cause `native_session_identity_drift`.
The session refuses further work with the same cause until explicitly closed
or deleted. After closing, load or resume can attempt store-backed restoration.

A native turn failure returns `nanocodex_turn_failed`. Provider rejections
and failed provider or gateway connections, including a stream that ends
before the response completes, have cause `provider`; invalid provider data
and helper protocol failures have cause `transport`, and a helper exit has
cause `process_exit`. A failed prompt mirror commit returns
`nanocodex_turn_failed` with cause `transport` and message
`session mirror commit failed`. Store failures outside
a prompt return `nanocodex_internal_failure` without a class. Failed restoration returns
`nanocodex_restore_failed` and leaves the store entry intact. Other internal
failures may carry one of these `class` values:

| Class | Condition |
|---|---|
| `native_start` | Helper launch or initialization failed. |
| `native_state` | The helper reported invalid state fields or a rollout path outside its session tree. |
| `lifecycle` | Publishing the prompt's terminal lifecycle update failed. |

## Scope

Configured API-key HTTPS gateways expose native function tools, including
shell execution and file operations through the shell. Freeform patch tools are
excluded on those routes. Code Mode is disabled on every route. Automatic
compaction follows the native or base model's
[compaction policy](PROVIDERS.md#gateway-models) within the session's context
window. On gateway routes the
helper asks the model for a plain-text summary through an ordinary `/responses`
request with `tool_choice:"none"`, so no provider-specific compaction support is
needed; native routes keep remote compaction. The native rollout saves the
compacted context for subsequent prompts and restoration. Failed or cancelled
compaction preserves committed history. After the first committed turn, load
and resume refuse an `apiBaseUrl` or `websocketUrl` change that moves a session
between a custom endpoint and the native route; changing between custom
endpoints remains allowed. A turn that exceeds the provider context window
fails without a retry, and the next prompt, in any later helper, runs native
compaction before its input; if that compaction fails, the prompt fails and the
next one tries again. A gateway summary request that exceeds the context window
is sent again without its oldest remaining turn, at most three times; native
history keeps every turn.

Gateway requests retry connection failures, HTTP 408, 409, 429, and 5xx
responses, and provider failure events whose code is missing, unrecognized, or
a transient HTTP status, up to five total attempts with exponential backoff and
jitter. Authorization, quota, model, invalid-request, context-window, image,
and policy codes fail immediately, as do incomplete responses stopped by
`max_output_tokens` or `content_filter`. Retries stop after assistant or
reasoning output is delivered. Valid `Retry-After`
and `retry-after-ms` delays up to 60 seconds are respected with the normal
backoff as a minimum; longer delays fail without retrying early. Each failed
attempt writes one redacted line to helper stderr. Turn errors carry the
provider's sanitized classification as `providerCode`, even when unrecognized:
a terminal candidate when one is present, else the canonical error type, error
code, error object type, or incomplete reason, in that order. Cancellation interrupts requests and
retry delays. Native default and WebSocket routes retain upstream transport
behavior.
Shell sessions and other in-memory tool state last for one prompt; subsequent
prompts resume the saved conversation in a new helper.
Prompt parameters are limited to 12 MiB of encoded JSON before admission.
Gateway responses require `text/event-stream`; each event and the accumulated
event data for one response are limited to 4 MiB. These limits leave room for
native event envelopes. Compacted native rows can contain an entire history and
are not limited to the helper's 32 MiB protocol frame size.
Image tool outputs are not projected as ACP images.

## Releases

A tag `vX.Y.Z` releases the commit whose `HelperVersion` and
`native/Cargo.toml` version are both `X.Y.Z`; pre-release tags such as
`vX.Y.Z-rc.1` follow the same rule. The tagged commit must be on the default
branch, and the built helper must report the fingerprint the Go command
requires at initialization. CI builds each archive natively on its own
platform and runs integration smoke against its extracted executables. It then
requires the Go module proxy to resolve the tag to that commit, attests the
archives and manifest, verifies the assets of a draft release, and publishes
it. Fix a faulty release with a new version.

| Asset | Contents |
|---|---|
| `<command>_<tag>_linux_amd64.tar.gz`, `<command>_<tag>_linux_arm64.tar.gz` | Go command, helper, and `LICENSE`; requires glibc 2.28 or newer. |
| `<command>_<tag>_darwin_arm64.tar.gz` | Go command, helper, and `LICENSE`; requires macOS 13 or newer; unsigned and not notarized. |
| `release-manifest.json` | Version, commit, helper release and fingerprint, Go and Rust toolchains, and per target the platform floor, native C toolchain, and archive, helper, and command SHA-256 digests. |
| `SHA256SUMS` | SHA-256 digests of the archives and the manifest. |
| `provenance.sigstore.json` | Sigstore bundle of the SLSA build provenance for the archives and the manifest. |

`<command>` is the Go command name. Release executables are reproducible from
the tagged commit: Go, Rust, zig, and cargo-zigbuild are pinned, Cargo runs
with `--locked`, and paths are remapped. Darwin helpers also depend on the
build host's Apple clang and SDK, which the manifest records. Compare rebuilt
executables with the manifest's `helperSha256` and `commandSha256`; archive
bytes also depend on the host's zlib. This script verifies a Linux amd64
archive with `gh` and `jq`:

```sh
#!/usr/bin/env bash
set -euo pipefail
tag=vX.Y.Z
name=acp-go-nanocodex
repo=Savid/acp-go-nanocodex
archive=${name}_${tag}_linux_amd64.tar.gz
gh release download "$tag" --repo "$repo" --pattern "$archive" \
  --pattern SHA256SUMS --pattern release-manifest.json --pattern provenance.sigstore.json
sha256sum --check --ignore-missing SHA256SUMS
commit=$(curl -fsS "https://proxy.golang.org/github.com/savid/acp-go-nanocodex/@v/$tag.info" | jq -er .Origin.Hash)
for subject in "$archive" release-manifest.json; do
  gh attestation verify "$subject" --repo "$repo" --bundle provenance.sigstore.json \
    --signer-workflow "$repo/.github/workflows/check.yml" \
    --source-ref "refs/tags/$tag" --source-digest "$commit" --deny-self-hosted-runners
done
jq -e --arg tag "$tag" --arg commit "$commit" \
  '.version == $tag and .commit == $commit' release-manifest.json
jq -r .helperFingerprint release-manifest.json
```

The commit comes from the Go module proxy, so the attestation binds the
archive to the same source a host's `go.mod` resolves. Compare the manifest
`helperFingerprint` with `HelperRelease` from the module version the host
requires, or with `-nanocodex-helper-release` from the extracted command. Drop
`--bundle` to fetch the attestation from GitHub instead.

To cut a release, set `HelperVersion` and the native package version, refresh
`native/Cargo.lock`, merge, run `make release-check TAG=vX.Y.Z` on the merged
commit, then push the tag. `make release-build TAG=vX.Y.Z` builds the host's
release archives under `dist/` with the same pinned zig and cargo-zigbuild as
CI, and `make release-smoke TAG=vX.Y.Z RELEASE_TARGET=linux_amd64` verifies
one archive and runs integration smoke against its executables;
`RELEASE_IMAGE` additionally probes the helper inside a container image.
`make release-manifest` writes the manifest and checksums for the targets
built under `dist/`.

## Development

```sh
make build
make test
make audit
make test-integration-smoke
```

Unit tests use a scripted helper and require neither provider credentials nor
an installed native binary. Rust tests and integration smoke use real helper
processes with local deterministic provider fixtures. They spend no model
tokens. Missing binaries skip the smoke tests with a prerequisite message.

`make test-integration-live` explicitly enables a token-spending tool call to
resolve a marker executable through session `extraPathDirs`, raw event
delivery, store-backed replay and resume, conversation continuation,
cancellation, and deletion. It uses a
temporary native home. Supply `OPENAI_API_KEY` (or the variable selected by
`NANOCODEX_API_KEY_ENV`), or set `ACP_GO_NANOCODEX_HOME` to a native home whose
`auth.json` can be copied into the temporary home. Provider route environment
variables are inherited. `ACP_GO_NANOCODEX_MODEL` selects the live model;
`ACP_GO_NANOCODEX_HARNESS_PATH` and `ACP_GO_NANOCODEX_AGENT_BINARY` select
prebuilt executables. `ACP_GO_NANOCODEX_GATEWAY_MODEL` additionally runs a
gateway-model journey through a loopback recording proxy in front of the
inherited `OPENAI_BASE_URL` route. It checks a tool call, the wire model and
identity line, store-backed load, refused model and base-model changes, and
compaction at a narrowed context window. Explicitly enabled live tests fail on
missing prerequisites.

`make audit` runs formatting, lint, both builds, race/coverage checks, module
tidiness, vulnerability scanning, and modernization checks. Rust formatting,
Clippy, and native tests participate in those gates. `make native-vuln` scans
Cargo.lock with pinned `cargo-audit`, installed under `.tmp/` on first use.
CI runs audit and the real-helper integration smoke tests on Linux and macOS.

## Context compaction

Reports native compaction starts, completions, failures, and cancellations,
including recovery before prompt acceptance. Starts carry the native
pre-compaction context count; trigger and resulting context counts are
unavailable.

Notifications carry `acp-go.dev/compaction` on the notification’s `_meta`,
with an otherwise empty `session_info_update`. The value is `acp-go-core`
`wire.Compaction`: a required `compactionId` and `status`, and optional
`trigger`, `contextBefore`, and `contextAfter`. A start and its outcome share
an ID. Unknown facts are omitted. These are live notifications; historical
replay emits none. Usage accounting is independent.
