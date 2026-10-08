# Providers

The helper embeds Nanocodex's OpenAI agent runtime. It supports native OpenAI
authentication and explicitly configured endpoints implementing the HTTP
Responses API. Credentials come from the helper's environment or native auth
file. The `apiKeyEnv` option selects the environment variable containing the key.

## OpenRouter

Set `OPENROUTER_API_KEY` through your normal credential configuration, then
launch the ACP command with:

```sh
export OPENAI_BASE_URL=https://openrouter.ai/api/v1
export NANOCODEX_API_KEY_ENV=OPENROUTER_API_KEY
export NANOCODEX_MODEL_ID_PREFIX=openai
export NANOCODEX_TRANSPORT=https

./bin/acp-go-nanocodex \
  -path ./bin/acp-go-nanocodex-native \
  -model gpt-6.1-sol
```

This sends the provider model ID `openai/gpt-6.1-sol`. The selected model must
exist in OpenRouter's catalog and be available to your account. Other
OpenRouter models run as [gateway models](#gateway-models).

OpenRouter requires complete conversation history and rejects stored
continuation requests. The gateway transport sends `store:false`, the full
history, and no `previous_response_id` on every model call. See the
[OpenRouter Responses documentation](https://openrouter.ai/docs/api_reference/responses/overview).

OpenRouter responses carry their charge as `usage.cost`, which counts toward
the session's [cost](README.md#session-options), except on BYOK
calls (`is_byok: true`).

## OMP auth-gateway

Configure and start the broker and gateway using
[OMP's auth gateway instructions](https://github.com/can1357/oh-my-pi/blob/main/docs/auth-broker-gateway.md).
The ACP command needs the **gateway bearer token**, and the gateway holds the
provider credentials through its broker.

For a gateway running on the same machine:

```sh
export OMP_AUTH_GATEWAY_TOKEN="$(omp auth-gateway token)"
export OPENAI_BASE_URL=http://127.0.0.1:4000/v1
export NANOCODEX_API_KEY_ENV=OMP_AUTH_GATEWAY_TOKEN
export NANOCODEX_MODEL_ID_PREFIX=openai-codex
export NANOCODEX_TRANSPORT=https

./bin/acp-go-nanocodex \
  -path ./bin/acp-go-nanocodex-native \
  -model gpt-6-astra
```

Use the URL and token supplied by the gateway operator for a remote gateway.
The prefix must match the provider namespace returned by its authenticated
`GET /v1/models` endpoint. The example selects
`openai-codex/gpt-6-astra`, a ChatGPT-subscription model; a different gateway
account may expose a different namespace or model roster. OMP serves API-key
OpenAI models under `openai/` and OpenRouter models under `openrouter/openai/`.

For a gateway exposing `openrouter/openai/gpt-6-luna`, use
`NANOCODEX_MODEL_ID_PREFIX=openrouter/openai` and `-model gpt-6-luna`.

An OMP auth gateway whose responses carry `usage.cost` contributes it to the
session's [cost](README.md#session-options) for non-BYOK calls.
Calls OMP prices from its own catalog carry no `usage.cost`.

## OpenCode Go

Set `OPENCODE_GO_API_KEY` through your credential configuration, then launch:

```sh
export OPENAI_BASE_URL=https://opencode.ai/zen/go/v1
export NANOCODEX_API_KEY_ENV=OPENCODE_GO_API_KEY
unset NANOCODEX_MODEL_ID_PREFIX
export NANOCODEX_TRANSPORT=https

./bin/acp-go-nanocodex \
  -path ./bin/acp-go-nanocodex-native \
  -model gpt-6-luna
```

A native model must be supported by both the provider and the native runtime;
other provider models run as [gateway models](#gateway-models). Requests
to the OpenCode Go endpoint identify this adapter in `User-Agent` and send the native conversation ID in
`x-opencode-session`, as required by [OpenCode Go](https://opencode.ai/docs/go/#where-can-i-use-it).
The session header stays stable across tool calls and restored prompts.

## Gateway models

On a configured API-key HTTPS route (`apiBaseUrl` or `OPENAI_BASE_URL`), a
`model` that is not a native ID is a gateway model:

```sh
export OPENAI_BASE_URL=http://127.0.0.1:4000/v1
export NANOCODEX_API_KEY_ENV=OMP_AUTH_GATEWAY_TOKEN
export NANOCODEX_TRANSPORT=https
export NANOCODEX_CONTEXT_WINDOW=262144

./bin/acp-go-nanocodex \
  -path ./bin/acp-go-nanocodex-native \
  -model opencode-go/qwen3.8-flash
```

The native aliases `sol`, `luna`, `astra`, `glm-5.3`, `glm53`, `kimi`, and
`mimo` are native IDs, not gateway models. Load and the model selector compare
`model` as spelled, so a different spelling of the same model, such as `luna`
against a stored `gpt-6-luna`, counts as a model change. The helper sends a
gateway model verbatim as the Responses `model` for generation and compaction
summaries; `modelIdPrefix` applies only to native IDs. A non-native `model`
fails with a `model` configuration error when no gateway route is possible:
without an endpoint or with WebSocket transport before authentication, and
under ChatGPT authentication. On a gateway route, a misspelt native ID is sent
as a gateway model, and the gateway's `model_not_found` fails the turn.

A gateway model runs with the settings of a native base model: its efforts and
default effort, maximum context window, and compaction behavior. `baseModel`
(`WithNanocodexBaseModel`) selects it, falling back to `NANOCODEX_BASE_MODEL`
and then `gpt-6-luna`. Luna accepts every effort from `none` to `max` and
defaults to `medium`; the gateway and its model must still accept the selected
effort. A `baseModel` that is not a native ID, or that accompanies a native
`model`, is a configuration error. `NANOCODEX_BASE_MODEL` applies whenever no
native rollout supplies the base model and is ignored for native models. Load
and resume take the base model from the rollout and refuse an explicit
`baseModel` that differs from it.

`contextWindow` (`WithNanocodexContextWindow`, env `NANOCODEX_CONTEXT_WINDOW`)
sets the session's context window for native and gateway models alike. It must
be a positive integer no greater than the native or base model's maximum.
Without it the session uses 272000, or the model's smaller maximum. Load and
resume accept a changed `contextWindow`. A request that changes `model` before
the first turn drops the stored `baseModel`, `contextWindow`, and `thinking`
unless it supplies them again.

The GPT models compact automatically when the context reaches 90% of the
window. GLM, Kimi, and MiMo, as native models or as bases, never compact
automatically: for them `contextWindow` affects only usage `size` and
summary-request trimming, and compaction runs before the prompt that follows a
context overflow. A model whose real window is smaller than the session's
window overflows the same way: the turn fails, and the next prompt compacts
first.

The ACP model selector and stored session options report the gateway model,
and its selector row carries the base model's efforts. Usage `size` is the
configured window, else a native model's default window. Without a configured
window, a gateway model's usage `size` is 0 and its selector row has no
`contextWindow`, because its real window is unknown. The
base model is never reported; raw events name the gateway model and omit the
base model's cost estimates.

The first sentence of the native system prompt, `You are <name>, ...`, names
the native model. Gateway-model requests replace it with
``You are <name>, a coding agent running `<model>`.``, keeping the agent name
(`Codex` for the GPT models, `Nanocodex` for GLM, Kimi, and MiMo) and the rest
of the prompt. A base model whose prompt lacks that sentence fails session
setup with a `baseModel` configuration error. Native rollouts and checkpoints
keep the base model and its prompt, and native-model requests are unchanged.

## Go embedding

Provider options can be selected independently for each session:

```go
import (
    "github.com/savid/acp-go-core/wire"
    nanocodexacp "github.com/savid/acp-go-nanocodex"
)

options := nanocodexacp.NewNanocodexOptions(
    nanocodexacp.WithNanocodexModel("gpt-6.1-sol"),
    nanocodexacp.WithNanocodexAPIBaseURL("https://openrouter.ai/api/v1"),
    nanocodexacp.WithNanocodexModelIDPrefix("openai"),
    nanocodexacp.WithNanocodexAPIKeyEnv("OPENROUTER_API_KEY"),
    nanocodexacp.WithNanocodexTransport("https"),
)
request := wire.NewSessionRequest(workspace,
    nanocodexacp.WithSessionNanocodexOptions(options),
)
```

The selected credential variable must be present in the helper environment.
Session options take precedence over environment defaults. A missing variable
selected through `apiKeyEnv` fails authentication instead of using a different
account.

## Scope

- Native model IDs are Nanocodex's `gpt-6-astra`, `gpt-6.1-sol`,
  `gpt-6-luna`, `@cf/zai-org/glm-5.3`, `kimi-k3`, and `mimo-v2.6-pro`.
  The default picker lists the three GPT models; `WithConfiguredModels`
  can add the other native IDs and gateway models. Provider support for a
  model and its reasoning settings is required independently.
- Custom API-key HTTP endpoints receive standard Responses requests with
  direct `exec_command`, `write_stdin`, `update_plan`, and `view_image`
  function tools. Shell commands can read and edit workspace files.
- Gateway compaction is a local summary requested through an ordinary
  `/responses` call with the conversation's own prefix and `tool_choice:"none"`,
  so it needs no provider compaction support. It works through OpenRouter and
  the OMP auth-gateway, neither of which forwards OpenAI's `compaction_trigger`.
  A started session cannot move between a custom endpoint and the native
  route; see [route pinning](README.md#scope).
- Gateways cache the shared request prefix without `prompt_cache_key`. A
  `thought_level` change starts a separate provider cache, so the next call is
  uncached.
- Gateway failures without a terminal code retry up to five attempts before
  output is delivered.
  See [retry and persistence behavior](README.md#scope).
- Code Mode is disabled on every route. The freeform `apply_patch` tool is
  excluded on gateways; provider web search and image generation are disabled.
  Unsupported gateway input or output fails the turn.
- Native OpenAI/ChatGPT routes retain Nanocodex's own transport. Custom gateway
  routing requires API-key authentication and HTTPS mode. Plain HTTP and
  WebSocket endpoints are accepted only on `localhost` or loopback IPs.
  ChatGPT authentication refuses custom endpoints, including inherited
  `OPENAI_BASE_URL`, so its credentials stay on the native provider route.
- Deterministic tests cover the actual helper, HTTP/SSE streaming, a native
  shell tool round trip, cancellation and continuation, and restoration after
  native state loss. These checks do not call paid model endpoints.
