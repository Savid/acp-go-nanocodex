package nanocodexacp

import (
	"bytes"
	"errors"
	"os"
	"testing"

	"github.com/coder/acp-go-sdk"
	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/wire"
	"github.com/stretchr/testify/require"
)

func TestConformanceStoreRestoreSurvivesNativeHomeDeletion(t *testing.T) {
	t.Parallel()
	store := acpcore.NewInMemorySessionStore()
	a, _, home, workspace := fixtureAgent(t, WithSessionStore(store))
	session := fixtureSession(t, a, workspace)
	first := nativeID(t, session.Meta)
	response := fixturePrompt(t, a, session.SessionId, "remember this")
	started := nativeID(t, response.Meta)
	require.NotEqual(t, first, started)
	require.FileExists(t, rolloutPath(home, first))
	original, err := os.ReadFile(rolloutPath(home, started))
	require.NoError(t, err)
	_, err = a.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
	require.NoError(t, os.RemoveAll(home))
	require.NoError(t, a.Close())
	a, client, _, _ := fixtureAgent(t, WithSessionStore(store), WithHome(home))
	loaded, err := a.LoadSession(t.Context(), acp.LoadSessionRequest{SessionId: session.SessionId, Cwd: workspace})
	require.NoError(t, err)
	require.Equal(t, started, nativeID(t, loaded.Meta))
	restored, err := os.ReadFile(rolloutPath(home, started))
	require.NoError(t, err)
	require.Equal(t, original, restored)
	require.Equal(t, "Hello world.Second answer.", client.text())
	notifications, _ := client.snapshot()
	require.NotNil(t, notifications[0].Update.UserMessageChunk)
	require.Equal(t, "remember this", notifications[0].Update.UserMessageChunk.Content.Text.Text)
	_, err = a.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
	client.reset()
	resumed, err := a.ResumeSession(t.Context(), acp.ResumeSessionRequest{SessionId: session.SessionId, Cwd: workspace})
	require.NoError(t, err)
	require.Equal(t, started, nativeID(t, resumed.Meta))
	notifications, _ = client.snapshot()
	require.Empty(t, notifications)
	fixturePrompt(t, a, session.SessionId, "continue")
	listed, err := a.ListSessions(t.Context(), acp.ListSessionsRequest{})
	require.NoError(t, err)
	require.Len(t, listed.Sessions, 1)
	require.Equal(t, session.SessionId, listed.Sessions[0].SessionId)
}

func TestConformanceNativeLongerHistoryAdoptedAndDivergenceRefused(t *testing.T) {
	t.Parallel()
	a, client, home, workspace := fixtureAgent(t)
	session := fixtureSession(t, a, workspace)
	response := fixturePrompt(t, a, session.SessionId, "base")
	id := nativeID(t, response.Meta)
	_, err := a.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
	path := rolloutPath(home, id)
	original, err := os.ReadFile(path)
	require.NoError(t, err)
	extra := []byte(`{"timestamp":"2026-10-02T02:00:00Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"external continuation"}]}}` + "\n")
	require.NoError(t, os.WriteFile(path, append(bytes.Clone(original), extra...), 0o600))
	client.reset()
	_, err = a.LoadSession(t.Context(), acp.LoadSessionRequest{SessionId: session.SessionId, Cwd: workspace})
	require.NoError(t, err)
	require.Contains(t, client.text(), "external continuation")
	_, err = a.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
	adopted, err := os.ReadFile(path)
	require.NoError(t, err)
	conflict := bytes.Replace(adopted, []byte("external continuation"), []byte("conflicting history"), 1)
	require.NoError(t, os.WriteFile(path, conflict, 0o600))
	_, err = a.ResumeSession(t.Context(), acp.ResumeSessionRequest{SessionId: session.SessionId, Cwd: workspace})
	require.Equal(t, wire.RestoreFailed("nanocodex"), err)
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, conflict, after)
	require.NoError(t, os.WriteFile(path, adopted, 0o600))
	_, err = a.ResumeSession(t.Context(), acp.ResumeSessionRequest{SessionId: session.SessionId, Cwd: workspace})
	require.NoError(t, err)
}

func TestConformanceEphemeralNeverWritesStoreAndDeleteRetainsNativeState(t *testing.T) {
	t.Parallel()
	store := &countingStore{SessionStore: acpcore.NewInMemorySessionStore()}
	a, _, home, workspace := fixtureAgent(t, WithSessionStore(store))
	request := wire.NewSessionRequest(workspace)
	request.Meta = wire.SessionMeta{Ephemeral: true}.Apply(nil)
	session, err := a.NewSession(t.Context(), request)
	require.NoError(t, err)
	response := fixturePrompt(t, a, session.SessionId, "temporary")
	_, err = a.UnstableDeleteSession(t.Context(), acp.UnstableDeleteSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
	store.mu.Lock()
	require.Zero(t, store.writes)
	require.Zero(t, store.deletes)
	store.mu.Unlock()
	require.FileExists(t, rolloutPath(home, nativeID(t, response.Meta)))
	_, err = a.ResumeSession(t.Context(), acp.ResumeSessionRequest{SessionId: session.SessionId, Cwd: workspace})
	require.Equal(t, wire.UnknownSession(), err)
	listed, err := a.ListSessions(t.Context(), acp.ListSessionsRequest{})
	require.NoError(t, err)
	require.Empty(t, listed.Sessions)
}

func TestConformanceDeleteFailureKeepsSessionUsableAndSuccessTombstonesIt(t *testing.T) {
	t.Parallel()
	store := &countingStore{SessionStore: acpcore.NewInMemorySessionStore(), deleteErr: errors.New("fixture store unavailable")}
	a, _, home, workspace := fixtureAgent(t, WithSessionStore(store))
	session := fixtureSession(t, a, workspace)
	_, err := a.UnstableDeleteSession(t.Context(), acp.UnstableDeleteSessionRequest{SessionId: session.SessionId})
	require.Equal(t, wire.InternalFailure(vendor, ""), err)
	response := fixturePrompt(t, a, session.SessionId, "still usable")
	store.mu.Lock()
	store.deleteErr = nil
	store.mu.Unlock()
	_, err = a.UnstableDeleteSession(t.Context(), acp.UnstableDeleteSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
	require.FileExists(t, rolloutPath(home, nativeID(t, response.Meta)))
	_, err = a.Prompt(t.Context(), wire.PromptRequest(session.SessionId, acp.TextBlock("gone")))
	require.Equal(t, wire.UnknownSession(), err)
	generation, err := store.Load(t.Context(), string(session.SessionId))
	require.NoError(t, err)
	require.Empty(t, generation)
	_, err = a.LoadSession(t.Context(), acp.LoadSessionRequest{SessionId: session.SessionId, Cwd: workspace})
	require.Equal(t, wire.UnknownSession(), err)
}
