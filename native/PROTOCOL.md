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
`providerCode` preserves the provider's classification. Its candidates, in
order, are the canonical `error_type` on the response or event, the error
object's `code` (a string, or an integer in decimal), the error object's `type`,
and `incomplete_details.reason`; a candidate longer than 128 bytes or containing
anything other than ASCII letters, digits, dots, underscores, or hyphens is
dropped. The first terminal candidate is reported, else the first candidate, so
the code matches the retry decision. Known rate-limit, quota, authorization, model, context-window, and service
errors receive specific safe messages; other codes keep the generic message.
An SSE error over HTTP 200 does not invent an HTTP status.
IDs must not be reused during a process lifetime. Error messages are fixed
summaries; provider response bodies and credentials are never returned as errors.

Events have `event`, `requestId` (the active prompt's integer ID), and `data`.
`accepted` contains `{turnId}` and precedes every `native` event for its prompt.
A `native` event contains the upstream typed `AgentEvent` JSON object,
including `type`, `seq`, `request_id`, and `payload`. Failure-event messages
are sanitized to exclude provider response bodies. Zero cache-write counts
are omitted because the upstream type loses whether the provider reported
them. In a gateway-model session, a payload `model` naming the base model
reports the gateway model instead, and `estimated_cost`, `cost_usd`, and
`cost_status`, which are priced at the base model's rates, are removed.
Native-model events are unchanged. Native `run.completed` and
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
limited to 4 MiB before parsing; accumulated event data for one response also
has a 4 MiB bound. Successful HTTP responses require `text/event-stream`.
Non-SSE success responses fail without retrying. Incomplete events never reach
native tools.

A gateway model is sent verbatim as the request `model`. The first sentence of
the first developer message must start with `You are ` followed by an agent
name and a comma; generation and summary requests replace it with
``You are <name>, a coding agent running `<model>`.``, keeping that name. A
sentence ends at the first period followed by whitespace or the end of the
text. Initialization refuses a base model whose prompt lacks that sentence as
an invalid `baseModel`, and a request without it fails as `transport_error`
before it is sent. Native state keeps the base model's prompt, and native
models send their prompt unchanged.

Each terminal gateway event, including `response.incomplete` and
`response.failed`, whose `response.usage.cost` is a non-negative number
produces a `call_cost` event with `data: {cost}`: the provider's USD charge for
that response, with `-0.0` sent as `0`. Usage whose `is_byok` is `true`
produces none. Other usage fields, including `estimated_cost`, are ignored.
Generation and compaction summary responses report alike, including attempts
that are retried and summaries the helper rejects. A call's `call_cost`
precedes its native `model.call.completed`, and every `call_cost` precedes the
reply of the prompt whose work produced it. Compaction that runs before
`accepted` delivers its `call_cost` events after `accepted`, or before the
prompt's error reply when the prompt is not accepted. Native routes produce no
`call_cost`.

Gateway compaction uses the native automatic threshold and builds a local
summary; gateway requests never carry `compaction_trigger`. The summary request
repeats the generation request body and history with `tool_choice:"none"` and
`max_output_tokens:16384`, and replaces the trigger with a user message asking
for a plain-text checkpoint under Goal, Progress, State, and Next headings.
Reasoning output is ignored; the trimmed assistant message text is the summary.
The helper returns it to the native agent as a `compaction` item whose
`encrypted_content` is `acp-go-nanocodex:summary:v1` and a newline followed by
the summary, so the native rollout persists and replaces it like provider
compaction. Gateway requests send each marked item as a user message: a fixed
preface saying the earlier conversation was compacted and is background, not a
new request, then the summary inside `<summary>` tags. Unmarked `compaction`
items pass through unchanged. A summary reply that calls a tool, has no text,
or is incomplete fails without retrying as `native_error` with the message
`gateway compaction summary requested a tool`,
`gateway compaction summary was empty`, or
`gateway compaction summary was truncated`. Compaction produces native
lifecycle events without assistant or reasoning presentation events. A summary
request that fails with a `context_length_exceeded` or `context_window_exceeded`
`providerCode` is sent again without its oldest turn, at most three times. A
turn runs from one user input to the next; developer and context messages, the
latest turn, and the trailing instruction stay, and native history is unchanged.
Failed or cancelled compaction leaves committed history intact. Routes without a gateway
keep native remote compaction and send `compaction` items unchanged, including
marked ones.

Gateway generation and compaction retry connection failures, HTTP 408, 409,
429, and 5xx responses without `x-should-retry: false`, and `response.failed`
and error events up to five total attempts; generation also retries
`response.incomplete`. A failure retries unless its `providerCode` is terminal,
so a missing or unrecognized code retries. A three-digit code is an HTTP status
under the same status rule. Other terminal codes cover authorization, quota and
billing, model access, invalid requests, context windows, image input, and
content policy, plus the `max_output_tokens` and `content_filter` incomplete
reasons. Malformed provider data fails immediately. Backoff is exponential with
random jitter. Valid `Retry-After` delays and event `retry_after` values up to
60 seconds are honored, as are `retry-after-ms` values. Backoff remains the
minimum delay, including for zero server delays. Overflowing decimal values and
delays over 60 seconds end the operation without retrying early. Each failed
attempt writes one redacted stderr line with its attempt number, HTTP status or
event type, `providerCode`, and whether it retries or why it stops.
Generation never retries after delivering assistant or reasoning output. A retry
cannot execute tools from an incomplete response. Cancellation interrupts both
requests and retry delays.

## Methods

- `initialize`: `{sessionId, model?, baseModel?, contextWindow?, thinking?,
  apiBaseUrl?, websocketUrl?, modelIdPrefix?, transport?, apiKeyEnv?, authFile?,
  resumeSessionId?}`.
  `sessionId` is a caller-generated UUIDv7 reserved under the native session
  lock. All six upstream model IDs are accepted, including those omitted
  from its default three-model picker. Native aliases such as `sol`, `luna`,
  and `astra` are accepted and returned as canonical model IDs. An optional
  prefix changes only a native model's provider wire identifier. On a
  configured API-key HTTPS gateway route, any other nonempty `model` without
  whitespace or control characters is a gateway model. A gateway model fails
  as `model` when no gateway route is possible: a route without an endpoint or
  with WebSocket transport refuses it before authentication, and ChatGPT
  authentication refuses it before its endpoint checks.
  A gateway model runs with the settings of `baseModel`, a native model ID that
  defaults to `gpt-6-luna`: its efforts, default effort, maximum context window,
  and automatic compaction. A `baseModel` with a native model is refused. Resume
  takes the base model from the rollout and refuses a different explicit
  `baseModel`. `contextWindow` sets the positive token window for context
  accounting and any native automatic compaction threshold, at most the native
  or base model's maximum; without it the window is 272000 or that smaller
  maximum. Resume accepts a changed `contextWindow`.
  Explicit parameters override inherited `OPENAI_BASE_URL`,
  `NANOCODEX_MODEL_ID_PREFIX`, `NANOCODEX_TRANSPORT`, `NANOCODEX_API_KEY_ENV`,
  `NANOCODEX_BASE_MODEL`, and `NANOCODEX_CONTEXT_WINDOW` for their
  corresponding fields. `NANOCODEX_BASE_MODEL` applies whenever no rollout
  supplies the base model and is ignored for native models.
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
  textEvents}`. `model` is the gateway model or the canonical native ID; the
  base model is never reported. `models` entries contain
  `{id, name, thinking: [string], defaultThinking, contextWindow?}`
  from the native catalog, followed by the selected model if it is outside
  the default picker. Native entries report their default `contextWindow`, and
  the selected native entry reports the configured one. A gateway model's entry
  carries its base model's efforts and default effort, and reports
  `contextWindow` only when one is configured. `helperVersion` and
  `helperFingerprint` must match the Go adapter build. The fingerprint is
  SHA-256 over sorted repository-relative paths, each followed by NUL, file
  bytes, and NUL. Its inputs are
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
  returns an error after flushing its native state. A turn that fails with a
  native `ContextWindowExceeded` source or a `context_length_exceeded` or
  `context_window_exceeded` `providerCode` marks the session, and the next
  prompt runs native compaction before `accepted`. A failed compaction returns
  its error, does not submit the input, and keeps the mark.
- `cancel`: `{}`. Cancels the active turn and waits for native cancellation
  cleanup. Returns `{cancelled:boolean}`. It remains usable while a prompt RPC
  is open. Cancelling while idle is a successful no-op.
- `state`: `{}`. Flushes state while idle and returns the same session fields
  as initialization. A host can mirror only bytes below `committedBytes`.
- `shutdown`: `{}`. Cancels an active turn, flushes and shuts down the agent,
  saves an optional snapshot checkpoint if a prompt ran, then replies `{}`
  and exits. End of input performs the same shutdown. A caller can mirror the
  complete stopped file.

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
the candidate UUID. The helper holds `<uuid>.writer.lock` under
`CODEX_HOME/nanocodex/acp-locks/` for every initialized and resumed UUID until
shutdown. Hydration and stopped snapshots acquire that writer lock before
accessing native files. Unrelated native consumers must honor writer exclusion.
Native rollout files and complete records are preserved, including a valid final JSON record without a newline.
Hydration may remove an invalid, unterminated trailing fragment after the
helper exits.

The helper atomically replaces `<rollout>.acp-checkpoint.json` after a prompted
helper shuts down its native writer. This optional file retains the native
snapshot head and request prefix, including context token accounting, without
duplicating history. It carries `compact_next:true` while the compaction mark
is set. Go mirrors the latest file in `config.checkpoint`, alongside
the native rows in the same store generation. Its byte boundary, history length,
identity, version, and supported prefix shape must match restoration. Missing,
stale, unreadable, or undecodable checkpoints fall back to native history and
produce a redacted stderr diagnostic when present but unusable. Failed checkpoint
writes also produce a diagnostic without failing shutdown. Observations preserve
the checkpoint; header-only sessions have none. The file is limited to 1 MiB.

Prompt parameters are limited to 12 MiB of encoded JSON before native admission.
Native rollout rows, including complete compacted histories, have no protocol
frame-size bound. The 32 MiB bound applies to helper transport frames and invalid
unterminated recovery tails.

Gateway context-overflow and invalid-image failures carry sanitized native
`ResponsesError` sources for native repair.
