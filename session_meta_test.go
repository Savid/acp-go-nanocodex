package nanocodexacp

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-nanocodex/internal/nanocodex"
	"github.com/stretchr/testify/require"
)

func TestNanocodexOptionsMetaRoundTrip(t *testing.T) {
	t.Parallel()

	options := NewNanocodexOptions(WithNanocodexModel("opencode-go/qwen3.8-flash"), WithNanocodexBaseModel("kimi-k3"), WithNanocodexContextWindow(262144), WithNanocodexShellEnv("EXAMPLE_API_TOKEN", "_ALT_KEY2"))
	encoded, err := json.Marshal(options.Meta())
	require.NoError(t, err)
	require.JSONEq(t, `{"nanocodex":{"options":{"model":"opencode-go/qwen3.8-flash","baseModel":"kimi-k3","contextWindow":262144,"shellEnv":["EXAMPLE_API_TOKEN","_ALT_KEY2"]}}}`, string(encoded))

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	for _, meta := range []map[string]any{options.Meta(), decoded} {
		parsed, parseErr := parseSessionMeta(meta)
		require.NoError(t, parseErr)
		require.Equal(t, options, parsed.options)
	}

	require.Equal(t, nanocodex.Initialize{
		SessionID: "session", ResumeSessionID: "native",
		Model: "opencode-go/qwen3.8-flash", BaseModel: "kimi-k3", ContextWindow: 262144,
		ShellEnv: []string{"EXAMPLE_API_TOKEN", "_ALT_KEY2"},
	}, options.initialize("session", "native"))
	require.Nil(t, NewNanocodexOptions().initialize("session", "").ShellEnv)
}

func TestValidateNanocodexSessionMeta(t *testing.T) {
	t.Parallel()

	options := func(name string, value any) map[string]any {
		return map[string]any{vendor: map[string]any{metaOptionsKey: map[string]any{name: value}}}
	}
	require.NoError(t, ValidateNanocodexSessionMeta(nil))
	require.NoError(t, ValidateNanocodexSessionMeta(options(metaContextWindow, 262144)))
	require.NoError(t, ValidateNanocodexSessionMeta(options(metaContextWindow, 262144.0)))
	require.NoError(t, ValidateNanocodexSessionMeta(options(metaShellEnv, []any{"EXAMPLE_API_TOKEN", "_X1"})))
	require.NoError(t, ValidateNanocodexSessionMeta(options(metaShellEnv, []any{})))

	for _, test := range []struct {
		name   string
		option string
		value  any
	}{
		{"zero window", metaContextWindow, 0.0},
		{"negative window", metaContextWindow, -1.0},
		{"fractional window", metaContextWindow, 1.5},
		{"window beyond int64", metaContextWindow, math.Pow(2, 63)},
		{"string window", metaContextWindow, "262144"},
		{"boolean window", metaContextWindow, true},
		{"null window", metaContextWindow, nil},
		{"zero integer window", metaContextWindow, int64(0)},
		{"negative integer window", metaContextWindow, -3},
		{"empty base model", metaBaseModel, ""},
		{"padded base model", metaBaseModel, " kimi-k3"},
		{"spaced base model", metaBaseModel, "kimi k3"},
		{"numeric base model", metaBaseModel, 5},
		{"string shell env", metaShellEnv, "EXAMPLE_API_TOKEN"},
		{"empty shell env name", metaShellEnv, []any{""}},
		{"digit-led shell env name", metaShellEnv, []any{"1TOKEN"}},
		{"spaced shell env name", metaShellEnv, []any{"EXAMPLE API_TOKEN"}},
		{"assignment shell env name", metaShellEnv, []any{"A=B"}},
		{"hyphenated shell env name", metaShellEnv, []any{"EXAMPLE-API-TOKEN"}},
		{"non-ASCII shell env name", metaShellEnv, []any{"TOKÉN"}},
		{"NUL shell env name", metaShellEnv, []any{"TOKEN\x00"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateNanocodexSessionMeta(options(test.option, test.value))
			require.Equal(t, wire.Unsupported(wire.MetaOptionPath(vendor, test.option)), err)
		})
	}
}
