package nanocodexacp

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/wire"
	"github.com/stretchr/testify/require"
)

type establishmentStore struct {
	acpcore.SessionStore
	load      atomic.Bool
	replace   atomic.Bool
	entered   chan struct{}
	cancelled chan struct{}
	release   chan struct{}
}

func (s *establishmentStore) block(ctx context.Context) error {
	close(s.entered)
	<-ctx.Done()
	close(s.cancelled)
	<-s.release

	return ctx.Err()
}

func (s *establishmentStore) Load(ctx context.Context, id string) (map[string][]acpcore.SessionStoreEntry, error) {
	if s.load.Load() {
		return nil, s.block(ctx)
	}

	return s.SessionStore.Load(ctx, id)
}

func (s *establishmentStore) Replace(ctx context.Context, key acpcore.SessionKey, replacements []acpcore.SessionStoreReplacement) error {
	if s.replace.Load() {
		return s.block(ctx)
	}

	return s.SessionStore.Replace(ctx, key, replacements)
}

func awaitAgentResult[T any](t *testing.T, result <-chan T) T {
	t.Helper()

	select {
	case value := <-result:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for agent cleanup")

		var zero T

		return zero
	}
}

func TestAgentCloseCancelsAndJoinsEstablishment(t *testing.T) {
	t.Parallel()

	for _, operation := range []string{"new", "resume"} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()

			store := &establishmentStore{
				SessionStore: acpcore.NewInMemorySessionStore(), entered: make(chan struct{}),
				cancelled: make(chan struct{}), release: make(chan struct{}),
			}
			agent, _, _, workspace := fixtureAgent(t, WithSessionStore(store))
			var release sync.Once
			t.Cleanup(func() { release.Do(func() { close(store.release) }) })
			var id acp.SessionId
			if operation == "resume" {
				created := fixtureSession(t, agent, workspace)
				id = created.SessionId
				_, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: id})
				require.NoError(t, err)
				store.load.Store(true)
			} else {
				store.replace.Store(true)
			}
			started := make(chan error, 1)
			go func() {
				if operation == "new" {
					_, err := agent.NewSession(t.Context(), wire.NewSessionRequest(workspace))
					started <- err
				} else {
					_, err := agent.ResumeSession(t.Context(), acp.ResumeSessionRequest{SessionId: id, Cwd: workspace})
					started <- err
				}
			}()
			awaitAgentResult(t, store.entered)
			closed := make(chan error, 1)
			go func() { closed <- agent.Close() }()
			awaitAgentResult(t, store.cancelled)
			select {
			case <-closed:
				t.Fatal("Close returned while an establishment callback was still running")
			default:
			}
			release.Do(func() { close(store.release) })
			require.Error(t, awaitAgentResult(t, started))
			require.NoError(t, awaitAgentResult(t, closed))
			agent.mu.Lock()
			pending, active, reserved := len(agent.starts), len(agent.sessions), agent.starting
			agent.mu.Unlock()
			require.Zero(t, pending)
			require.Zero(t, active)
			require.Zero(t, reserved)
		})
	}
}

func closingFixtureAgent(t *testing.T, store acpcore.SessionStore) (*Agent, *recordingClient, string) {
	t.Helper()

	executable, err := os.Executable()
	require.NoError(t, err)
	agent := NewAgent(WithExecutablePath(executable), WithHome(t.TempDir()), WithSessionStore(store),
		WithEnv(map[string]string{fakeHelperEnv: "1", "GORACE": "atexit_sleep_ms=0"}))
	client := &recordingClient{wake: make(chan struct{}, 32)}
	agent.attach(client, nil)
	t.Cleanup(func() { _ = agent.Close() })

	return agent, client, t.TempDir()
}

func TestAgentConcurrentClosePreservesFailure(t *testing.T) {
	t.Parallel()

	store := &configFailureStore{SessionStore: acpcore.NewInMemorySessionStore()}
	agent, _, workspace := closingFixtureAgent(t, store)
	fixtureSession(t, agent, workspace)
	store.fail.Store(true)
	start := make(chan struct{})
	results := make(chan error, 8)
	for range cap(results) {
		go func() {
			<-start
			results <- agent.Close()
		}()
	}
	close(start)
	first := awaitAgentResult(t, results)
	require.Error(t, first)
	for range cap(results) - 1 {
		require.Equal(t, first, awaitAgentResult(t, results))
	}
	require.Equal(t, first, agent.Close())
}

func TestAgentCloseReapsHelperThatIgnoresCancel(t *testing.T) {
	t.Parallel()

	agent, client, workspace := closingFixtureAgent(t, nil)
	created := fixtureSession(t, agent, workspace)
	type outcome struct {
		response acp.PromptResponse
		err      error
	}
	prompted := make(chan outcome, 1)
	go func() {
		response, err := agent.Prompt(t.Context(), wire.PromptRequest(created.SessionId, acp.TextBlock("ignore-cancel")))
		prompted <- outcome{response, err}
	}()
	waitForText(t, client, "waiting")
	s, err := agent.lookup(created.SessionId)
	require.NoError(t, err)
	s.mu.Lock()
	rt := s.rt
	s.mu.Unlock()
	require.NotNil(t, rt)
	closed := make(chan error, 1)
	go func() { closed <- agent.Close() }()
	select {
	case closeErr := <-closed:
		require.NoError(t, closeErr)
	case <-time.After(nativeTimeout + 10*time.Second):
		t.Fatal("Close did not reap the unresponsive helper")
	}
	select {
	case <-rt.proc.Done():
	default:
		t.Fatal("Close returned before the helper was reaped")
	}
	result, err := rt.proc.Wait(t.Context())
	require.NoError(t, err)
	require.Equal(t, int(syscall.SIGKILL), result.Signal)
	resultPrompt := awaitAgentResult(t, prompted)
	require.NoError(t, resultPrompt.err)
	require.Equal(t, acp.StopReasonCancelled, resultPrompt.response.StopReason)
}
