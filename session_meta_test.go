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
		require.Nil(t, parseErr)
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
	require.NoError(t, ValidateNanocodexSessionMeta(options(metaContextWindowKey, 262144)))
	require.NoError(t, ValidateNanocodexSessionMeta(options(metaContextWindowKey, 262144.0)))
	require.NoError(t, ValidateNanocodexSessionMeta(options(metaShellEnvKey, []any{"EXAMPLE_API_TOKEN", "_X1"})))
	require.NoError(t, ValidateNanocodexSessionMeta(options(metaShellEnvKey, []any{})))

	for _, test := range []struct {
		name   string
		option string
		value  any
	}{
		{"zero window", metaContextWindowKey, 0.0},
		{"negative window", metaContextWindowKey, -1.0},
		{"fractional window", metaContextWindowKey, 1.5},
		{"window beyond int64", metaContextWindowKey, math.Pow(2, 63)},
		{"string window", metaContextWindowKey, "262144"},
		{"boolean window", metaContextWindowKey, true},
		{"null window", metaContextWindowKey, nil},
		{"zero integer window", metaContextWindowKey, int64(0)},
		{"negative integer window", metaContextWindowKey, -3},
		{"empty base model", metaBaseModelKey, ""},
		{"padded base model", metaBaseModelKey, " kimi-k3"},
		{"spaced base model", metaBaseModelKey, "kimi k3"},
		{"numeric base model", metaBaseModelKey, 5},
		{"string shell env", metaShellEnvKey, "EXAMPLE_API_TOKEN"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateNanocodexSessionMeta(options(test.option, test.value))
			require.Equal(t, wire.Unsupported(wire.MetaOptionPath(vendor, test.option)), err)
		})
	}

	for _, name := range []string{"", "1TOKEN", "EXAMPLE API_TOKEN", "A=B", "EXAMPLE-API-TOKEN", "TOKÉN", "TOKEN\x00"} {
		err := ValidateNanocodexSessionMeta(options(metaShellEnvKey, []any{"EXAMPLE_API_TOKEN", name}))
		require.Equal(t, wire.Unsupported(wire.MetaOptionPath(vendor, metaShellEnvKey)+"[1]"), err, "%q", name)
	}
}

func TestShellEnvInheritanceAndExplicitEmptyList(t *testing.T) {
	t.Parallel()

	stored := sessionRecord{Options: NewNanocodexOptions(WithNanocodexShellEnv("STORED_TOKEN"))}
	request := func(names ...any) sessionMeta {
		fields := map[string]any{}
		if names != nil {
			fields[metaShellEnvKey] = names
		}

		meta, err := parseSessionMeta(map[string]any{vendor: map[string]any{metaOptionsKey: fields}})
		require.Nil(t, err)

		return meta
	}
	require.Equal(t, []string{"STORED_TOKEN"}, inheritCarrier(request(), stored).ShellEnv)
	require.Equal(t, []string{"REQUESTED_TOKEN"}, inheritCarrier(request("REQUESTED_TOKEN"), stored).ShellEnv)

	cleared := inheritCarrier(request([]any{}...), stored)
	require.NotNil(t, cleared.ShellEnv)
	require.Empty(t, cleared.ShellEnv)

	for _, options := range []NanocodexOptions{cleared, NewNanocodexOptions(WithNanocodexShellEnv())} {
		encoded, err := json.Marshal(options.initialize("session", ""))
		require.NoError(t, err)
		require.Contains(t, string(encoded), `"shellEnv":[]`)

		record, err := json.Marshal(sessionRecord{Options: options})
		require.NoError(t, err)

		var decoded sessionRecord
		require.NoError(t, json.Unmarshal(record, &decoded))
		require.Equal(t, []string{}, decoded.Options.ShellEnv)
	}
}
