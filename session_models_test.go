package nanocodexacp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync/atomic"
	"testing"

	"github.com/coder/acp-go-sdk"
	acpcore "github.com/savid/acp-go-core"
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
		{name: "native refusal", model: "invalid-model", want: wire.Unsupported("value")},
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
