package nanocodexacp

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/savid/acp-go-core/process"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-nanocodex/internal/nanocodex"
	"github.com/stretchr/testify/require"
)

const stopHelperMode = "ACP_GO_NANOCODEX_INTERNAL_STOP_TEST"

func TestHelperIdentityMismatchFailsBeforeSessionBinding(t *testing.T) {
	t.Parallel()
	for _, field := range []string{"NANOCODEX_TEST_HELPER_VERSION", "NANOCODEX_TEST_HELPER_FINGERPRINT"} {
		t.Run(field, func(t *testing.T) {
			t.Parallel()
			a, _, _, workspace := fixtureAgent(t, WithAgentVersion("custom-host-version"))
			options := NewNanocodexOptions(WithNanocodexEnv(map[string]string{field: "mismatched-build"}))
			_, err := a.NewSession(t.Context(), wire.NewSessionRequest(workspace, WithSessionNanocodexOptions(options)))
			require.Equal(t, wire.InternalFailure(vendor, "native_state"), err)
			a.mu.Lock()
			defer a.mu.Unlock()
			require.Empty(t, a.sessions)
		})
	}
}

func TestRuntimeStopHelper(t *testing.T) {
	mode := os.Getenv(stopHelperMode)
	if mode == "" {
		return
	}

	signal.Ignore(syscall.SIGTERM)
	decoder := json.NewDecoder(os.Stdin)
	encoder := json.NewEncoder(os.Stdout)
	for {
		var request struct {
			ID     uint64 `json:"id"`
			Method string `json:"method"`
		}
		if err := decoder.Decode(&request); err != nil {
			os.Exit(2)
		}
		switch request.Method {
		case "ready":
			if err := encoder.Encode(nanocodex.Frame{ID: request.ID, Result: json.RawMessage(`{}`)}); err != nil {
				os.Exit(3)
			}
			if mode == "clean_exit" {
				os.Exit(0)
			}
		case "shutdown":
			if mode == "persistence" {
				if err := encoder.Encode(nanocodex.Frame{
					ID: request.ID,
					Error: &nanocodex.Error{
						Code: "persistence", Message: "native rollout could not be flushed",
					},
				}); err != nil {
					os.Exit(4)
				}
				os.Exit(0)
			}
			if mode != "unresponsive" {
				os.Exit(5)
			}
		default:
			os.Exit(6)
		}
	}
}

func newStopRuntime(t *testing.T, mode string) *runtime {
	t.Helper()

	executable, err := os.Executable()
	require.NoError(t, err)
	env, err := (process.Environment{
		Process: os.Environ(), Owned: map[string]string{stopHelperMode: mode},
	}).Build()
	require.NoError(t, err)
	proc, err := process.Start(t.Context(), process.Request{
		Executable: executable, Args: []string{"-test.run=^TestRuntimeStopHelper$"},
		Env: env, Dir: t.TempDir(),
	})
	require.NoError(t, err)
	rt := &runtime{proc: proc, client: nanocodex.NewClient(proc.Stdin(), proc.Stdout())}
	t.Cleanup(func() {
		_ = proc.Kill()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, waitErr := proc.Wait(ctx)
		require.NoError(t, waitErr)
		require.NoError(t, proc.Close())
		select {
		case <-rt.client.Done():
		case <-ctx.Done():
			t.Fatal("helper client did not stop")
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	require.NoError(t, rt.client.Call(ctx, "ready", struct{}{}, nil, nil))

	return rt
}

func TestRuntimeStopKillsUnresponsiveProcessAfterRPCTimeout(t *testing.T) {
	t.Parallel()

	rt := newStopRuntime(t, "unresponsive")
	stopped := make(chan error, 1)
	go func() { stopped <- rt.stop() }()
	select {
	case err := <-stopped:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(nativeTimeout + 10*time.Second):
		t.Fatal("shutdown did not finish after the RPC timeout")
	}
	select {
	case <-rt.proc.Done():
	default:
		t.Fatal("shutdown returned with an unreaped helper process")
	}
	result, err := rt.proc.Wait(t.Context())
	require.NoError(t, err)
	require.Equal(t, int(syscall.SIGKILL), result.Signal)
}

func TestRuntimeStopPreservesNativePersistenceFailure(t *testing.T) {
	t.Parallel()

	rt := newStopRuntime(t, "persistence")
	err := rt.stop()
	nativeErr, ok := errors.AsType[*nanocodex.Error](err)
	require.True(t, ok, "shutdown lost the native failure: %v", err)
	require.Equal(t, "persistence", nativeErr.Code)
}

func TestRuntimeStopAcceptsAlreadyReapedCleanExit(t *testing.T) {
	t.Parallel()

	rt := newStopRuntime(t, "clean_exit")
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	result, err := rt.proc.Wait(ctx)
	require.NoError(t, err)
	require.Zero(t, result.ExitCode)
	select {
	case <-rt.client.Done():
	case <-ctx.Done():
		t.Fatal("helper stream remained open after clean exit")
	}
	require.NoError(t, rt.stop())
}
