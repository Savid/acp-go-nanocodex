package nanocodexacp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/coder/acp-go-sdk"
	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/sessionlog"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-nanocodex/internal/nanocodex"
	"github.com/stretchr/testify/require"
)

func TestFailedConfigObservationClearsStagedBinding(t *testing.T) {
	t.Parallel()

	store := acpcore.NewInMemorySessionStore()
	agent, _, _, workspace := fixtureAgent(t, WithSessionStore(store))
	options := NewNanocodexOptions(WithNanocodexEnv(map[string]string{"NANOCODEX_TEST_SHUTDOWN_FAILURE_MODEL": "rejected-change"}))
	created := fixtureSession(t, agent, workspace, WithSessionNanocodexOptions(options))
	s, err := agent.lookup(created.SessionId)
	require.NoError(t, err)
	before := configSnapshot(t, s)
	generation, err := store.Load(t.Context(), string(created.SessionId))
	require.NoError(t, err)
	_, err = agent.SetSessionConfigOption(t.Context(), SetModelRequest(created.SessionId, "rejected-change"))
	require.Equal(t, wire.InternalFailure(vendor, "native_start"), err)
	require.Equal(t, before, configSnapshot(t, s))
	raw, err := os.ReadFile(s.state.RolloutPath)
	require.NoError(t, err)
	conflicting := bytes.Replace(raw, []byte(`"timestamp":"2026-10-02T01:00:00Z"`), []byte(`"timestamp":"2026-10-02T02:00:00Z"`), 1)
	require.NotEqual(t, raw, conflicting)
	require.NoError(t, os.WriteFile(s.state.RolloutPath, conflicting, 0o600))
	_, err = agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: created.SessionId})
	require.Equal(t, wire.InternalFailure(vendor, ""), err)
	retained, err := store.Load(t.Context(), string(created.SessionId))
	require.NoError(t, err)
	require.Equal(t, generation, retained)
}

type configFailureStore struct {
	acpcore.SessionStore
	fail atomic.Bool
}

func (s *configFailureStore) Replace(ctx context.Context, key acpcore.SessionKey, replacements []acpcore.SessionStoreReplacement) error {
	if s.fail.Load() {
		return errors.New("fixture store unavailable")
	}

	return s.SessionStore.Replace(ctx, key, replacements)
}

func configSnapshot(t *testing.T, s *session) []byte {
	t.Helper()

	s.mu.Lock()
	data, err := json.Marshal(map[string]any{"options": s.options, "state": s.state, "rows": s.rows})
	s.mu.Unlock()
	require.NoError(t, err)

	return data
}

func TestConfigSelectionUsesNativeValidation(t *testing.T) {
	t.Parallel()

	agent, client, _, workspace := fixtureAgent(t)
	created := fixtureSession(t, agent, workspace)
	response, err := agent.SetSessionConfigOption(t.Context(), SetModelRequest(created.SessionId, "unlisted-model"))
	require.NoError(t, err)
	require.Equal(t, acp.SessionConfigValueId("unlisted-model"), response.ConfigOptions[0].Select.CurrentValue)
	_, err = agent.SetSessionConfigOption(t.Context(), wire.SetConfigOptionRequest(created.SessionId, configThinking, "unlisted-effort"))
	require.NoError(t, err)
	s, err := agent.lookup(created.SessionId)
	require.NoError(t, err)
	s.mu.Lock()
	thinking := s.options.Thinking
	s.mu.Unlock()
	require.Equal(t, "unlisted-effort", thinking)
	notifications, _ := client.snapshot()
	var updates int
	for _, notification := range notifications {
		if notification.Update.ConfigOptionUpdate != nil {
			updates++
		}
	}
	require.Equal(t, 2, updates)
}

func TestConfigSelectionRollsBackFailedChanges(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name      string
		model     string
		failStore bool
		want      error
	}{
		{name: "native refusal", model: "refuse-model", want: wire.Unsupported("value")},
		{name: "store refusal", model: "unlisted-model", failStore: true, want: wire.InternalFailure(vendor, "")},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			store := &configFailureStore{SessionStore: acpcore.NewInMemorySessionStore()}
			agent, client, _, workspace := fixtureAgent(t, WithSessionStore(store))
			created := fixtureSession(t, agent, workspace)
			s, err := agent.lookup(created.SessionId)
			require.NoError(t, err)
			before := configSnapshot(t, s)
			store.fail.Store(test.failStore)
			_, err = agent.SetSessionConfigOption(t.Context(), SetModelRequest(created.SessionId, test.model))
			store.fail.Store(false)
			require.Equal(t, test.want, err)
			require.Equal(t, before, configSnapshot(t, s))
			notifications, _ := client.snapshot()
			for _, notification := range notifications {
				require.Nil(t, notification.Update.ConfigOptionUpdate)
			}
		})
	}
}

func TestModelMetadataPublishesOnlyNativeReportedFacts(t *testing.T) {
	t.Parallel()

	s := &session{
		agent:   NewAgent(WithConfiguredModels([]string{"configured", "reported"}), WithDefaultModel("default")),
		options: NanocodexOptions{Model: "reported"},
		state: nanocodex.State{Models: []nanocodex.Model{
			{ID: "unknown"},
			{ID: "reported", ContextWindow: 128000, Thinking: []string{"medium", "high"}},
		}},
	}
	options := s.configOptions()
	models := *options[0].Select.Options.Ungrouped
	require.Len(t, models, 4)
	require.Equal(t, map[string]any{vendor: map[string]any{"modelId": "unknown"}}, models[0].Meta)
	require.Equal(t, map[string]any{vendor: map[string]any{
		"modelId": "reported", "contextWindow": int64(128000), "supportedEffortLevels": []string{"medium", "high"},
	}}, models[1].Meta)
	require.Equal(t, acp.SessionConfigValueId("configured"), models[2].Value)
	require.Empty(t, models[2].Meta)
	require.Equal(t, acp.SessionConfigValueId("default"), models[3].Value)
	require.Empty(t, models[3].Meta)
}

const gatewayModel = "opencode-go/qwen3.8-flash"

// gatewayOptions selects a gateway model with explicit model settings on a
// gateway route; the fixture helper records each initialization in capture.
func gatewayOptions(capture string) NanocodexOptions {
	return NewNanocodexOptions(WithNanocodexModel(gatewayModel), WithNanocodexBaseModel("kimi-k3"), WithNanocodexContextWindow(262144),
		WithNanocodexAPIBaseURL("http://127.0.0.1:1/v1"), WithNanocodexEnv(map[string]string{"NANOCODEX_TEST_CAPTURE": capture}))
}

func storedOptions(t *testing.T, store acpcore.SessionStore, id acp.SessionId) NanocodexOptions {
	t.Helper()

	var record sessionRecord
	_, found, err := sessionlog.Load(t.Context(), store, string(id), &record)
	require.NoError(t, err)
	require.True(t, found)

	return record.Options
}

func lastInitialize(t *testing.T, capture string) nanocodex.Initialize {
	t.Helper()

	raw, err := os.ReadFile(capture)
	require.NoError(t, err)
	lines := bytes.Split(bytes.TrimSpace(raw), []byte{'\n'})
	var captured struct {
		Initialize nanocodex.Initialize `json:"initialize"`
	}
	require.NoError(t, json.Unmarshal(lines[len(lines)-1], &captured))

	return captured.Initialize
}

// selectedRow returns the model selector's current value and that row's metadata.
func selectedRow(t *testing.T, configOptions []acp.SessionConfigOption) (acp.SessionConfigValueId, map[string]any) {
	t.Helper()

	selector := configOptions[0].Select
	for _, row := range *selector.Options.Ungrouped {
		if row.Value == selector.CurrentValue {
			metadata, ok := row.Meta[vendor].(map[string]any)
			require.True(t, ok)

			return selector.CurrentValue, metadata
		}
	}
	t.Fatal("selector omits its current value")

	return "", nil
}

func usageSizes(client *recordingClient) []int {
	notifications, _ := client.snapshot()
	var sizes []int
	for _, notification := range notifications {
		if usage := notification.Update.UsageUpdate; usage != nil {
			sizes = append(sizes, usage.Size)
		}
	}

	return sizes
}

func TestGatewayModelReportsItsIdentityAndConfiguredWindow(t *testing.T) {
	t.Parallel()

	store := acpcore.NewInMemorySessionStore()
	agent, client, _, workspace := fixtureAgent(t, WithSessionStore(store))
	// Without a configured window the gateway model's row has none, and usage
	// reports size 0.
	unsized := fixtureSession(t, agent, workspace, WithSessionNanocodexOptions(NewNanocodexOptions(WithNanocodexModel(gatewayModel), WithNanocodexAPIBaseURL("http://127.0.0.1:1/v1"))))
	selected, metadata := selectedRow(t, unsized.ConfigOptions)
	require.Equal(t, acp.SessionConfigValueId(gatewayModel), selected)
	require.NotContains(t, metadata, "contextWindow")
	fixturePrompt(t, agent, unsized.SessionId, "report usage")

	created := fixtureSession(t, agent, workspace, WithSessionNanocodexOptions(gatewayOptions(filepath.Join(t.TempDir(), "initialize.jsonl"))))
	selected, metadata = selectedRow(t, created.ConfigOptions)
	require.Equal(t, acp.SessionConfigValueId(gatewayModel), selected)
	require.Equal(t, int64(262144), metadata["contextWindow"])

	fixturePrompt(t, agent, created.SessionId, "report usage")
	require.Equal(t, []int{0, 262144}, usageSizes(client))
	stored := storedOptions(t, store, created.SessionId)
	require.Equal(t, []any{gatewayModel, "kimi-k3", int64(262144)}, []any{stored.Model, stored.BaseModel, stored.ContextWindow})

	load := func(options ...NanocodexOption) (*acp.LoadSessionResponse, error) {
		_, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: created.SessionId})
		require.NoError(t, err)
		meta := []wire.SessionRequestOption{}
		if len(options) > 0 {
			meta = append(meta, WithSessionNanocodexOptions(NewNanocodexOptions(options...)))
		}
		response, err := agent.LoadSession(t.Context(), wire.LoadSessionRequest(created.SessionId, workspace, meta...))
		if err != nil {
			return nil, err
		}

		return &response, nil
	}
	for _, options := range [][]NanocodexOption{{WithNanocodexContextWindow(524288)}, nil} {
		loaded, err := load(options...)
		require.NoError(t, err)
		_, metadata = selectedRow(t, loaded.ConfigOptions)
		require.Equal(t, int64(524288), metadata["contextWindow"])
		require.Equal(t, int64(524288), storedOptions(t, store, created.SessionId).ContextWindow)
	}
	_, err := load(WithNanocodexModel("opencode-go/another-model"))
	require.Equal(t, wire.Unsupported(wire.MetaOptionPath(vendor, "model")), err)
}

func TestModelChangeDropsInheritedModelSettings(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name          string
		change        func(context.Context, *Agent, acp.SessionId, string) error
		model         string
		baseModel     string
		contextWindow int64
	}{
		{name: "picker to a native model", model: "gpt-6.1-sol", change: func(ctx context.Context, agent *Agent, id acp.SessionId, _ string) error {
			_, err := agent.SetSessionConfigOption(ctx, SetModelRequest(id, "gpt-6.1-sol"))

			return err
		}},
		{name: "load to a native model", model: "gpt-6.1-sol", change: loadWith(WithNanocodexModel("gpt-6.1-sol"))},
		{name: "load to another gateway model", model: "opencode-go/another-model", change: loadWith(WithNanocodexModel("opencode-go/another-model"))},
		{
			name: "load that supplies the settings again", model: "opencode-go/another-model", baseModel: "glm-5.3", contextWindow: 131072,
			change: loadWith(WithNanocodexModel("opencode-go/another-model"), WithNanocodexBaseModel("glm-5.3"), WithNanocodexContextWindow(131072)),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			store := acpcore.NewInMemorySessionStore()
			agent, _, _, workspace := fixtureAgent(t, WithSessionStore(store))
			capture := filepath.Join(t.TempDir(), "initialize.jsonl")
			created := fixtureSession(t, agent, workspace, WithSessionNanocodexOptions(gatewayOptions(capture)))
			require.NoError(t, test.change(t.Context(), agent, created.SessionId, workspace))

			initialized := lastInitialize(t, capture)
			require.Equal(t, []any{test.model, test.baseModel, test.contextWindow}, []any{initialized.Model, initialized.BaseModel, initialized.ContextWindow})
			stored := storedOptions(t, store, created.SessionId)
			require.Equal(t, []any{test.model, test.baseModel, test.contextWindow}, []any{stored.Model, stored.BaseModel, stored.ContextWindow})
		})
	}
}

func TestModelChangeDropsTheStoredThinkingLevel(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name     string
		load     []NanocodexOption
		thinking string
	}{
		{name: "load without a thinking level", load: []NanocodexOption{WithNanocodexModel("kimi-k3")}, thinking: "low"},
		{name: "load that supplies a thinking level", load: []NanocodexOption{WithNanocodexModel("kimi-k3"), WithNanocodexThinking("high")}, thinking: "high"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			store := acpcore.NewInMemorySessionStore()
			agent, _, _, workspace := fixtureAgent(t, WithSessionStore(store))
			options := NewNanocodexOptions(WithNanocodexModel(gatewayModel), WithNanocodexAPIBaseURL("http://127.0.0.1:1/v1"))
			created := fixtureSession(t, agent, workspace, WithSessionNanocodexOptions(options))
			// The helper reports the Luna base model's default effort.
			require.Equal(t, "medium", storedOptions(t, store, created.SessionId).Thinking)

			require.NoError(t, loadWith(test.load...)(t.Context(), agent, created.SessionId, workspace))
			stored := storedOptions(t, store, created.SessionId)
			require.Equal(t, []string{"kimi-k3", test.thinking}, []string{stored.Model, stored.Thinking})
		})
	}
}

func TestLoadWithTheStoredModelKeepsItsSettings(t *testing.T) {
	t.Parallel()

	store := acpcore.NewInMemorySessionStore()
	agent, _, _, workspace := fixtureAgent(t, WithSessionStore(store))
	capture := filepath.Join(t.TempDir(), "initialize.jsonl")
	options := gatewayOptions(capture)
	options.Thinking = "high"
	created := fixtureSession(t, agent, workspace, WithSessionNanocodexOptions(options))
	require.NoError(t, loadWith(WithNanocodexModel(gatewayModel))(t.Context(), agent, created.SessionId, workspace))

	initialized := lastInitialize(t, capture)
	require.Equal(t, []any{gatewayModel, "kimi-k3", int64(262144), "high"}, []any{initialized.Model, initialized.BaseModel, initialized.ContextWindow, initialized.Thinking})
	stored := storedOptions(t, store, created.SessionId)
	require.Equal(t, []any{gatewayModel, "kimi-k3", int64(262144), "high"}, []any{stored.Model, stored.BaseModel, stored.ContextWindow, stored.Thinking})
}

// loadWith closes a session and loads it with the given options.
func loadWith(options ...NanocodexOption) func(context.Context, *Agent, acp.SessionId, string) error {
	return func(ctx context.Context, agent *Agent, id acp.SessionId, workspace string) error {
		if _, err := agent.CloseSession(ctx, acp.CloseSessionRequest{SessionId: id}); err != nil {
			return err
		}
		_, err := agent.LoadSession(ctx, wire.LoadSessionRequest(id, workspace, WithSessionNanocodexOptions(NewNanocodexOptions(options...))))

		return err
	}
}

func TestConfigModelRefusalOfAnEnvironmentDefaultIsInternal(t *testing.T) {
	t.Parallel()

	agent, _, _, workspace := fixtureAgent(t)
	created := fixtureSession(t, agent, workspace)
	for _, field := range []string{metaBaseModelKey, metaContextWindowKey} {
		_, err := agent.SetSessionConfigOption(t.Context(), SetModelRequest(created.SessionId, "refuse-"+field))
		require.Equal(t, wire.InternalFailure(vendor, "native_start"), err)
	}
}
