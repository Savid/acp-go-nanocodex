package nanocodexacp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/process"
	"github.com/savid/acp-go-core/sessionlog"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-nanocodex/internal/nanocodex"
	"github.com/stretchr/testify/require"
)

func TestFailedEmptyReplacementCommitRetriesFromDurableBinding(t *testing.T) {
	t.Parallel()

	store := &configFailureStore{SessionStore: acpcore.NewInMemorySessionStore()}
	agent, _, home, workspace := fixtureAgent(t, WithSessionStore(store))
	capture := filepath.Join(t.TempDir(), "initialize.jsonl")
	options := NewNanocodexOptions(WithNanocodexEnv(map[string]string{"NANOCODEX_TEST_CAPTURE": capture}))
	created := fixtureSession(t, agent, workspace, WithSessionNanocodexOptions(options))
	s, err := agent.lookup(created.SessionId)
	require.NoError(t, err)
	before := configSnapshot(t, s)
	generation, err := store.Load(t.Context(), string(created.SessionId))
	require.NoError(t, err)
	store.fail.Store(true)
	_, err = agent.Prompt(t.Context(), wire.TextPromptRequest(created.SessionId, "uncommitted prompt"))
	store.fail.Store(false)
	require.Equal(t, wire.TurnFailed(vendor, wire.TurnFailure{Cause: wire.CauseTransport, Message: "session mirror commit failed"}), err)
	require.Equal(t, before, configSnapshot(t, s))
	retained, err := store.Load(t.Context(), string(created.SessionId))
	require.NoError(t, err)
	require.Equal(t, generation, retained)
	fixturePrompt(t, agent, created.SessionId, "retry from durable binding")
	raw, err := os.ReadFile(capture)
	require.NoError(t, err)
	lines := bytes.Split(bytes.TrimSpace(raw), []byte{'\n'})
	require.Len(t, lines, 3)
	initializations := make([]nanocodex.Initialize, 3)
	for i, line := range lines {
		var captured struct {
			Initialize nanocodex.Initialize `json:"initialize"`
		}
		require.NoError(t, json.Unmarshal(line, &captured))
		initializations[i] = captured.Initialize
	}
	require.Equal(t, initializations[0].SessionID, initializations[1].ResumeSessionID)
	require.Equal(t, initializations[0].SessionID, initializations[2].ResumeSessionID)
	require.NotEqual(t, initializations[1].SessionID, initializations[2].SessionID)
	files, err := filepath.Glob(filepath.Join(home, "sessions", "*.jsonl"))
	require.NoError(t, err)
	require.Len(t, files, 3)
	for _, path := range files {
		rows, readErr := nanocodex.ReadRows(home, path, -1)
		require.NoError(t, readErr)
		var headers int
		for _, row := range rows {
			var envelope nanocodex.Row
			require.NoError(t, json.Unmarshal(row, &envelope))
			if envelope.Type == "session_meta" {
				headers++
			}
		}
		require.Equal(t, 1, headers)
	}
}

func TestNativeInitializationRefusesSelfReplacement(t *testing.T) {
	t.Parallel()

	agent, _, _, workspace := fixtureAgent(t)
	options := NewNanocodexOptions(WithNanocodexEnv(map[string]string{"NANOCODEX_TEST_SELF_REPLACEMENT": "1"}))
	_, err := agent.NewSession(t.Context(), wire.NewSessionRequest(workspace, WithSessionNanocodexOptions(options)))
	require.Equal(t, wire.InternalFailure(vendor, "native_state"), err)
	listed, err := agent.ListSessions(t.Context(), acp.ListSessionsRequest{})
	require.NoError(t, err)
	require.Empty(t, listed.Sessions)
}

func TestDeadRuntimeRetainsSnapshotLockUntilStopped(t *testing.T) {
	t.Parallel()

	agent, _, _, workspace := fixtureAgent(t)
	created := fixtureSession(t, agent, workspace)
	s, err := agent.lookup(created.SessionId)
	require.NoError(t, err)
	rt, err := s.launch(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = rt.stop() })
	require.NoError(t, rt.proc.Kill())
	_, err = rt.proc.Wait(t.Context())
	require.NoError(t, err)
	require.NoError(t, s.commitMirror(t.Context()))
	lockPath := filepath.Join(s.home, "nanocodex", "acp-locks", s.state.NativeSessionID+".lock")
	contender, err := process.LockFile(lockPath)
	if contender != nil {
		_ = contender.Close()
	}
	require.Error(t, err)
	_ = rt.stop()
	s.mu.Lock()
	s.rt = nil
	s.mu.Unlock()
	contender, err = process.LockFile(lockPath)
	require.NoError(t, err)
	require.NoError(t, contender.Close())
	fixturePrompt(t, agent, created.SessionId, "continue after startup process exit")
}

func TestPromptReportsProcessExitAfterResumeInitialization(t *testing.T) {
	t.Parallel()

	agent, _, _, workspace := fixtureAgent(t)
	options := NewNanocodexOptions(WithNanocodexEnv(map[string]string{"NANOCODEX_TEST_EXIT_AFTER_RESUME": "1"}))
	created := fixtureSession(t, agent, workspace, WithSessionNanocodexOptions(options))
	for range 5 {
		_, err := agent.Prompt(t.Context(), wire.TextPromptRequest(created.SessionId, "helper exits before native admission"))
		requestErr, ok := errors.AsType[*acp.RequestError](err)
		require.True(t, ok)
		data, ok := requestErr.Data.(map[string]any)
		require.True(t, ok)
		require.Equal(t, "nanocodex_turn_failed", data["error"])
		require.Equal(t, "process_exit", data["cause"])
		require.Contains(t, data["message"], "code 17")
	}
}

func TestSessionRestoresWithSymlinkedHome(t *testing.T) {
	t.Parallel()

	actual := t.TempDir()
	link := filepath.Join(t.TempDir(), "home")
	require.NoError(t, os.Symlink(actual, link))
	agent, _, _, workspace := fixtureAgent(t, WithHome(filepath.Join(link, "codex")))
	created := fixtureSession(t, agent, workspace)
	fixturePrompt(t, agent, created.SessionId, "retain this conversation")
	s, err := agent.lookup(created.SessionId)
	require.NoError(t, err)
	expectedHome, err := filepath.EvalSymlinks(filepath.Join(actual, "codex"))
	require.NoError(t, err)
	require.Equal(t, expectedHome, s.home)
	require.True(t, strings.HasPrefix(s.state.RolloutPath, s.home+string(filepath.Separator)))
	_, err = agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: created.SessionId})
	require.NoError(t, err)
	_, err = agent.LoadSession(t.Context(), wire.LoadSessionRequest(created.SessionId, workspace))
	require.NoError(t, err)
	fixturePrompt(t, agent, created.SessionId, "continue")
}

func TestSessionRecoversUnterminatedNativeTail(t *testing.T) {
	t.Parallel()

	agent, _, _, workspace := fixtureAgent(t)
	created := fixtureSession(t, agent, workspace)
	fixturePrompt(t, agent, created.SessionId, "completed turn")
	s, err := agent.lookup(created.SessionId)
	require.NoError(t, err)
	path := s.state.RolloutPath
	for range 2 {
		file, openErr := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
		require.NoError(t, openErr)
		_, writeErr := file.WriteString(`{"timestamp":"interrupted`)
		require.NoError(t, writeErr)
		require.NoError(t, file.Close())
		_, err = agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: created.SessionId})
		require.NoError(t, err)
		_, err = agent.LoadSession(t.Context(), wire.LoadSessionRequest(created.SessionId, workspace))
		require.NoError(t, err)
		raw, readErr := os.ReadFile(path)
		require.NoError(t, readErr)
		require.True(t, bytes.HasSuffix(raw, []byte{'\n'}))
		require.NotContains(t, string(raw), `{"timestamp":"interrupted`)
		fixturePrompt(t, agent, created.SessionId, "continue after interrupted write")
	}
}

func TestSessionPreservesCompleteFinalRecordWithoutNewline(t *testing.T) {
	t.Parallel()

	agent, client, _, workspace := fixtureAgent(t)
	created := fixtureSession(t, agent, workspace)
	fixturePrompt(t, agent, created.SessionId, "completed history")
	s, err := agent.lookup(created.SessionId)
	require.NoError(t, err)
	raw, err := os.ReadFile(s.state.RolloutPath)
	require.NoError(t, err)
	require.True(t, bytes.HasSuffix(raw, []byte{'\n'}))
	require.NoError(t, os.WriteFile(s.state.RolloutPath, raw[:len(raw)-1], 0o600))
	_, err = agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: created.SessionId})
	require.NoError(t, err)
	client.reset()
	_, err = agent.LoadSession(t.Context(), wire.LoadSessionRequest(created.SessionId, workspace))
	require.NoError(t, err)
	require.Equal(t, "Hello world.Second answer.", client.text())
	repaired, err := os.ReadFile(s.state.RolloutPath)
	require.NoError(t, err)
	require.Equal(t, raw, repaired)
	fixturePrompt(t, agent, created.SessionId, "continue after complete final record")
}

func TestNativeLockCoversHydrationAndRunningHelper(t *testing.T) {
	t.Parallel()

	agent, client, _, workspace := fixtureAgent(t)
	created := fixtureSession(t, agent, workspace)
	fixturePrompt(t, agent, created.SessionId, "completed history")
	s, err := agent.lookup(created.SessionId)
	require.NoError(t, err)
	lockPath := filepath.Join(s.home, "nanocodex", "acp-locks", s.state.NativeSessionID+".lock")
	lock, err := process.LockFile(lockPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = lock.Close() })
	header := append(bytes.Clone(s.rows[0]), '\n')
	require.NoError(t, os.WriteFile(s.state.RolloutPath, header, 0o600))
	_, err = agent.Prompt(t.Context(), wire.TextPromptRequest(created.SessionId, "cannot hydrate while locked"))
	require.Equal(t, wire.RestoreFailed(vendor), err)
	raw, err := os.ReadFile(s.state.RolloutPath)
	require.NoError(t, err)
	require.Equal(t, header, raw)
	require.NoError(t, lock.Close())

	result := make(chan error, 1)
	go func() {
		_, promptErr := agent.Prompt(t.Context(), wire.TextPromptRequest(created.SessionId, "wait"))
		result <- promptErr
	}()
	waitForText(t, client, "waiting")
	contender, err := process.LockFile(lockPath)
	if contender != nil {
		_ = contender.Close()
	}
	require.Error(t, err)
	require.NoError(t, agent.Cancel(t.Context(), acp.CancelNotification{SessionId: created.SessionId}))
	require.NoError(t, awaitAgentResult(t, result))
	contender, err = process.LockFile(lockPath)
	require.NoError(t, err)
	require.NoError(t, contender.Close())
}

func TestStoppedSnapshotRefusesCompetingWriterWithoutChangingStore(t *testing.T) {
	t.Parallel()

	store := acpcore.NewInMemorySessionStore()
	agent, _, _, workspace := fixtureAgent(t, WithSessionStore(store))
	created := fixtureSession(t, agent, workspace)
	fixturePrompt(t, agent, created.SessionId, "durable history")
	s, err := agent.lookup(created.SessionId)
	require.NoError(t, err)
	before, err := store.Load(t.Context(), string(created.SessionId))
	require.NoError(t, err)
	lock, err := process.LockFile(filepath.Join(s.home, "nanocodex", "acp-locks", s.state.NativeSessionID+".lock"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = lock.Close() })
	_, err = agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: created.SessionId})
	require.Equal(t, wire.InternalFailure(vendor, ""), err)
	after, err := store.Load(t.Context(), string(created.SessionId))
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.NoError(t, lock.Close())
	_, err = agent.ResumeSession(t.Context(), acp.ResumeSessionRequest{SessionId: created.SessionId, Cwd: workspace})
	require.NoError(t, err)
	fixturePrompt(t, agent, created.SessionId, "continue after the writer releases ownership")
}

type listLoadFailureStore struct {
	acpcore.SessionStore
	fail atomic.Bool
}

func (s *listLoadFailureStore) Load(ctx context.Context, id string) (map[string][]acpcore.SessionStoreEntry, error) {
	if s.fail.Load() {
		return nil, errors.New("private backend failure")
	}

	return s.SessionStore.Load(ctx, id)
}

func TestListSkipsCorruptRecordsAndReportsStoreFailure(t *testing.T) {
	t.Parallel()

	store := &listLoadFailureStore{SessionStore: acpcore.NewInMemorySessionStore()}
	agent, _, _, workspace := fixtureAgent(t, WithSessionStore(store))
	created := fixtureSession(t, agent, workspace)
	badID := "corrupt-session"
	main := acpcore.SessionKey{SessionID: badID}
	require.NoError(t, store.Replace(t.Context(), main, []acpcore.SessionStoreReplacement{
		{Key: main, Entries: []acpcore.SessionStoreEntry{[]byte(`{"broken":true}`)}},
		{Key: acpcore.SessionKey{SessionID: badID, Subpath: sessionlog.ConfigSubpath}, Entries: []acpcore.SessionStoreEntry{[]byte(`{"broken":true}`)}},
	}))
	listed, err := agent.ListSessions(t.Context(), acp.ListSessionsRequest{})
	require.NoError(t, err)
	require.Len(t, listed.Sessions, 1)
	require.Equal(t, created.SessionId, listed.Sessions[0].SessionId)
	store.fail.Store(true)
	_, err = agent.ListSessions(t.Context(), acp.ListSessionsRequest{})
	require.Equal(t, wire.InternalFailure(vendor, ""), err)
	store.fail.Store(false)
}

type blockingReplayClient struct {
	recordingClient
	once    sync.Once
	entered chan struct{}
	release chan struct{}
	err     error
}

func (c *blockingReplayClient) SessionUpdate(ctx context.Context, notification acp.SessionNotification) error {
	c.once.Do(func() { close(c.entered); <-c.release })
	if c.err != nil {
		return c.err
	}

	return c.recordingClient.SessionUpdate(ctx, notification)
}

func TestLoadOwnsForegroundUntilReplayCompletes(t *testing.T) {
	t.Parallel()

	for _, replayFails := range []bool{false, true} {
		name := "completed"
		if replayFails {
			name = "failed"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			agent, _, _, workspace := fixtureAgent(t, WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 2}))
			created := fixtureSession(t, agent, workspace)
			fixturePrompt(t, agent, created.SessionId, "saved conversation")
			_, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: created.SessionId})
			require.NoError(t, err)

			client := &blockingReplayClient{entered: make(chan struct{}), release: make(chan struct{})}
			if replayFails {
				client.err = errors.New("replay consumer disconnected")
			}
			var unblock sync.Once
			t.Cleanup(func() { unblock.Do(func() { close(client.release) }) })
			agent.attach(client, nil)
			loaded := make(chan error, 1)
			go func() {
				_, loadErr := agent.LoadSession(t.Context(), wire.LoadSessionRequest(created.SessionId, workspace))
				loaded <- loadErr
			}()
			awaitAgentResult(t, client.entered)
			fixtureSession(t, agent, workspace)

			_, err = agent.Prompt(t.Context(), wire.TextPromptRequest(created.SessionId, "must wait for replay"))
			require.Equal(t, wire.Backpressure("session_prompt"), err)
			session, err := agent.lookup(created.SessionId)
			require.NoError(t, err)
			closed := make(chan error, 1)
			go func() {
				_, closeErr := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: created.SessionId})
				closed <- closeErr
			}()
			require.Eventually(t, func() bool {
				session.mu.Lock()
				defer session.mu.Unlock()

				return session.closing
			}, 5*time.Second, time.Millisecond)
			select {
			case <-closed:
				t.Fatal("close completed while replay was blocked")
			default:
			}

			unblock.Do(func() { close(client.release) })
			loadErr := awaitAgentResult(t, loaded)
			if replayFails {
				require.Equal(t, wire.InternalFailure(vendor, ""), loadErr)
			} else {
				require.NoError(t, loadErr)
				require.Equal(t, "Hello world.Second answer.", client.text())
			}
			require.NoError(t, awaitAgentResult(t, closed))
			_, err = agent.lookup(created.SessionId)
			require.Equal(t, wire.UnknownSession(), err)
		})
	}
}

func TestRestoreRefusesAnActiveNativeWriterBeforeHydration(t *testing.T) {
	t.Parallel()
	a, _, home, workspace := fixtureAgent(t)
	created := fixtureSession(t, a, workspace)
	completed := fixturePrompt(t, a, created.SessionId, "committed history")
	_, err := a.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: created.SessionId})
	require.NoError(t, err)
	id := nativeID(t, completed.Meta)
	path := rolloutPath(home, id)
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	lock, err := process.LockFile(filepath.Join(home, "nanocodex", "acp-locks", id+".writer.lock"))
	require.NoError(t, err)
	defer lock.Close()
	_, err = a.LoadSession(t.Context(), wire.LoadSessionRequest(created.SessionId, workspace))
	require.Equal(t, wire.RestoreFailed(vendor), err)
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.NoError(t, lock.Close())
	_, err = a.LoadSession(t.Context(), wire.LoadSessionRequest(created.SessionId, workspace))
	require.NoError(t, err)
}

func TestCheckpointMetadataMirrorsAndRehydratesWithoutAddingNativeRows(t *testing.T) {
	t.Parallel()
	store := acpcore.NewInMemorySessionStore()
	a, _, home, workspace := fixtureAgent(t, WithSessionStore(store))
	created := fixtureSession(t, a, workspace)
	completed := fixturePrompt(t, a, created.SessionId, "saved history")
	path := rolloutPath(home, nativeID(t, completed.Meta))
	checkpoint := json.RawMessage(`{"preceding_bytes":123,"history_items":2,"head":{},"prefix":[]}`)
	require.NoError(t, nanocodex.WriteCheckpoint(home, path, checkpoint))
	_, err := a.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: created.SessionId})
	require.NoError(t, err)
	var record sessionRecord
	rows, found, err := sessionlog.Load(t.Context(), store, string(created.SessionId), &record)
	require.NoError(t, err)
	require.True(t, found)
	require.JSONEq(t, string(checkpoint), string(record.Checkpoint))
	for _, row := range rows {
		require.NotContains(t, string(row), "preceding_bytes")
	}
	require.NoError(t, os.Remove(path+".acp-checkpoint.json"))
	_, err = a.LoadSession(t.Context(), wire.LoadSessionRequest(created.SessionId, workspace))
	require.NoError(t, err)
	hydrated, err := nanocodex.ReadCheckpoint(home, path)
	require.NoError(t, err)
	require.JSONEq(t, string(checkpoint), string(hydrated))
}
