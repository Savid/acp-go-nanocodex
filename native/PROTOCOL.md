# Native helper protocol

`acp-go-nanocodex-native` owns one retained Nanocodex agent. Standard input and
output carry one JSON object per line. Each frame is limited to 32 MiB,
excluding its newline, in both directions. Oversized output fails the
transport before any bytes of that frame are written; frames are never
truncated. Oversized input returns `invalid_request` with a null ID, shuts
down the agent, and exits. Standard error carries diagnostics.
The helper inherits its environment and uses its working directory as the
workspace. This protocol is private to the matching Go and Rust release.

Requests have `id` (positive integer), `method`, and optional `params` (object,
default `{}`). Replies have the same `id` and either `result` or
`error: {code, message, field?, statusCode?, providerCode?}`. `field` identifies
an invalid initialization option; `statusCode` preserves a provider HTTP rejection code.
`providerCode` preserves an error-event code containing at most 128 ASCII
letters, digits, dots, underscores, or hyphens. Known rate-limit, quota,
authorization, model, context-window, and service errors receive specific
safe messages. An SSE error over HTTP 200 does not invent an HTTP status.
IDs must not be reused during a process lifetime. Error messages are fixed
summaries; provider response bodies and credentials are never returned as errors.

Events have `event`, `requestId` (the active prompt's integer ID), and `data`.
`accepted` contains `{turnId}` and precedes every `native` event for its prompt.
A `native` event contains the upstream typed `AgentEvent` JSON object,
including `type`, `seq`, `request_id`, and `payload`. Failure-event messages
are sanitized to exclude provider response bodies. Zero cache-write counts
are omitted because the upstream type loses whether the provider reported
them. Native `run.completed` and
`run.failed` events do not terminate the RPC: the prompt reply is authoritative
and follows event draining and the durable rollout flush.

Configured API-key HTTPS endpoints use standard Responses requests through
an application-owned transport. Its streaming events are `assistant_delta`,
`reasoning_delta`, and `assistant_message`, with `data: {text, itemKey, responseId?}`.
They have the same prompt `requestId` and are delivered after `accepted`.
`itemKey` is the required private presentation identity built from the native
model call index and the provider output index. It stays stable between deltas
and completion, including items without provider IDs, and differs between
items and model calls within its prompt. Standard Responses SSE text, reasoning,
and item-done events must include their `output_index`; completion array positions
identify the same items. `responseId` is included only after the provider exposes
the current response ID; the
adapter places it on ACP text and thought chunks as `messageId`. Earlier
chunks are delivered immediately without that field.
They do not pretend to carry an upstream native event sequence. The Go
adapter coalesces completed messages with any deltas already published.
Only function tools are advertised through this transport; shell and file
operations use native direct tools. Unsupported custom tools are excluded.
Gateway requests omit `prompt_cache_key` and carry the adapter's `User-Agent`.
Only requests to `https://opencode.ai/zen/go/v1` carry the native conversation ID as
`x-opencode-session`, including after a helper restart. Each SSE event is
limited to 32 MiB before parsing; incomplete events never reach native tools.

## Methods

- `initialize`: `{sessionId, model?, thinking?, apiBaseUrl?, websocketUrl?, modelIdPrefix?,
  transport?, apiKeyEnv?, authFile?, resumeSessionId?}`. Model IDs are native
  Nanocodex IDs; an optional prefix changes only the provider wire identifier.
  `sessionId` is a caller-generated UUIDv7 reserved under the native session
  lock. All six upstream model IDs are accepted, including those omitted
  from its default three-model picker. Native aliases such as `sol`, `luna`,
  and `astra` are accepted and returned as canonical model IDs.
  Explicit parameters override inherited `OPENAI_BASE_URL`,
  `NANOCODEX_MODEL_ID_PREFIX`, `NANOCODEX_TRANSPORT`, and
  `NANOCODEX_API_KEY_ENV` for their corresponding fields.
  The default transport is `https`; `websocket` is opt-in. HTTPS always uses
  full history replay and `store:false`. `apiKeyEnv` defaults to
  `OPENAI_API_KEY`. When neither `apiKeyEnv` nor `NANOCODEX_API_KEY_ENV`
  selects a credential variable, a missing default key falls back to native
  ChatGPT authentication from `authFile` or the native home’s `auth.json`.
  Either explicit selection disables that fallback; an unavailable selected
  credential fails authentication. ChatGPT authentication refuses custom HTTP and WebSocket
  endpoints, including an inherited `OPENAI_BASE_URL`. Custom endpoints
  require TLS except for `localhost` and loopback IP addresses. Native home
  resolves from `CODEX_HOME`, otherwise `$HOME/.codex`; relative paths use
  the workspace. The Go adapter resolves home symlinks before launching.
  The result is `{protocolVersion:1, helperVersion, helperFingerprint,
  nativeSessionId, rolloutPath, committedBytes, model, thinking, models,
  textEvents}`. `models` entries contain
  `{id, name, thinking: [string], defaultThinking, contextWindow}`
  from the native catalog, followed by the selected model if it is outside
  the default picker. `helperVersion` and `helperFingerprint` must match the
  Go adapter build. The fingerprint is SHA-256 over sorted repository-relative
  paths, each followed by NUL, file bytes, and NUL. Its inputs are
  `native/src/*.rs`, `native/Cargo.toml`, `native/Cargo.lock`,
  `native/rust-toolchain.toml`, `native/build.rs`,
  `internal/nanocodex/protocol.go`, and `internal/nanocodex/client.go`.
  `textEvents:true` selects gateway text presentation;
  native `assistant.message` then remains raw telemetry only to avoid duplication.
  Resume retains the native UUID and reopens the existing rollout. Its
  returned rollout path is absolute and canonicalized by the upstream loader.
  The caller must restore the complete native rollout tree before initialization.
  A verified single `session_meta` row with no accepted input has no resumable
  native conversation. Only that case creates the supplied candidate UUID
  and a new rollout, then adds
  `replacedNativeSessionId` to the result. The original file is preserved.
  The Go adapter validates and commits this pre-turn binding replacement
  before sending a prompt. Missing, malformed, and nonempty failed rollouts
  never receive this fallback.
- `prompt`: `{content:[{type:"text",text:string}|{type:"image",image_url:string}]}`.
  Only one turn may be active. The reply waits for completion and contains
  `{stopReason:"end_turn"|"cancelled", finalMessage, usage?, nativeSessionId,
  rolloutPath, committedBytes}`. Usage has `inputTokens`, `cachedInputTokens`,
  `outputTokens`, `reasoningOutputTokens`, and `totalTokens`. A failed turn
  returns an error after flushing its native state.
- `cancel`: `{}`. Cancels the active turn and waits for native cancellation
  cleanup. Returns `{cancelled:boolean}`. It remains usable while a prompt RPC
  is open. Cancelling while idle is a successful no-op.
- `state`: `{}`. Flushes state while idle and returns the same session fields
  as initialization. A host can mirror only bytes below `committedBytes`.
- `shutdown`: `{}`. Cancels an active turn, flushes and shuts down the agent,
  then replies `{}` and exits. End of input also shuts down the agent.

Requests that fail envelope decoding return `invalid_request` with a null ID.
Decoded envelopes with zero or reused IDs or nonobject params return
`invalid_request` using the supplied ID. Unsupported methods return
`method_not_found`. Other codes are `not_initialized`, `already_initialized`,
`busy`, `invalid_config`, `authentication`, `restore_failed`, `persistence`,
`native_error`, `connection_error`, and `transport_error`. `restore_failed`
identifies unreadable or invalid restored native state and a mismatched
restored workspace. `connection_error` means the provider or gateway
connection failed, timed out, or ended before the response completed;
`transport_error` covers invalid provider data and helper failures.
Authentication, configuration, and process-start failures remain distinct.

The Go adapter holds an advisory lock per native UUID from before hydration
until the helper exits and is reaped. Empty-session replacement also locks
the candidate UUID. The helper is private to that adapter and requires its
caller to own these locks. Unrelated native consumers must avoid writing
the same rollout concurrently. Native rollout files and complete records
are preserved, including a valid final JSON record without a newline.
Hydration may remove an invalid, unterminated trailing fragment after the
helper exits.
