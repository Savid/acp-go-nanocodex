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
exist in OpenRouter's catalog and be available to your account. The adapter
does not expand Nanocodex's model catalog to every OpenRouter model.

OpenRouter requires complete conversation history and rejects stored
continuation requests. The gateway transport sends `store:false`, the full
history, and no `previous_response_id` on every model call. See the
[OpenRouter Responses documentation](https://openrouter.ai/docs/api_reference/responses/overview).

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
  -model gpt-6.1-sol
```

Use the URL and token supplied by the gateway operator for a remote gateway.
The prefix must match the provider namespace returned by its authenticated
`GET /v1/models` endpoint. The example selects
`openai-codex/gpt-6.1-sol`; a different gateway account may expose a different
namespace or model roster.

For a gateway exposing `openrouter/openai/gpt-6-luna`, use
`NANOCODEX_MODEL_ID_PREFIX=openrouter/openai` and `-model gpt-6-luna`.

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

The model must be supported by both the provider and native runtime. Requests
to the OpenCode Go endpoint identify this adapter in `User-Agent` and send the native conversation ID in
`x-opencode-session`, as required by [OpenCode Go](https://opencode.ai/docs/go/#where-can-i-use-it).
The session header stays stable across tool calls and restored prompts.

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

- Model IDs remain Nanocodex's native `gpt-6-astra`, `gpt-6.1-sol`,
  `gpt-6-luna`, `@cf/zai-org/glm-5.3`, `kimi-k3`, and `mimo-v2.6-pro`.
  The default picker lists the three GPT models; `WithConfiguredModels`
  can add the other supported native IDs. Prefixes change provider routing only. Provider support for a
  model and its reasoning settings is required independently.
- Custom API-key HTTP endpoints receive standard Responses requests with
  direct `exec_command`, `write_stdin`, `update_plan`, and `view_image`
  function tools. Shell commands can read and edit workspace files.
- Gateway compaction requires `/responses` to accept `compaction_trigger` and
  return one encrypted compaction item. This applies independently to the
  selected OpenRouter provider, OMP backend, and OpenCode Go route. Response
  streaming alone does not establish compaction support. A refused trigger
  preserves history but prevents continuation past the automatic threshold;
  there is no local summary fallback. Encrypted content is opaque to the adapter.
- Transient gateway failures retry up to five attempts before output is delivered.
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
