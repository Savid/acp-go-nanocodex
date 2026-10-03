package nanocodexacp

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/lifecycle"
	"github.com/savid/acp-go-core/wire"
	"github.com/stretchr/testify/require"
)

func TestConformancePromptStreamsToolsUsageAndDeduplicatedMessages(t *testing.T) {
	t.Parallel()
	a, client, _, workspace := fixtureAgent(t)
	session := fixtureSession(t, a, workspace, WithSessionRawEvents(true))
	response := fixturePrompt(t, a, session.SessionId, "hello")
	require.Equal(t, acp.StopReasonEndTurn, response.StopReason)
	require.Equal(t, "Hello world.Second answer.", client.text())
	require.NotNil(t, response.Usage)
	require.EqualValues(t, 20, response.Usage.TotalTokens)
	require.EqualValues(t, 2, *response.Usage.CachedReadTokens)
	notifications, _ := client.snapshot()
	var starts, completions, usages, thoughts int
	for _, notification := range notifications {
		update := notification.Update
		if update.ToolCall != nil {
			starts++
			require.Equal(t, acp.ToolCallId("call-one"), update.ToolCall.ToolCallId)
			require.Equal(t, acp.ToolCallStatusInProgress, update.ToolCall.Status)
		}
		if update.ToolCallUpdate != nil {
			completions++
			require.Equal(t, acp.ToolCallStatusCompleted, *update.ToolCallUpdate.Status)
		}
		if update.UsageUpdate != nil {
			usages++
			require.EqualValues(t, 20, update.UsageUpdate.Used)
			require.EqualValues(t, 128000, update.UsageUpdate.Size)
		}
		if update.AgentThoughtChunk != nil {
			thoughts++
		}
	}
	require.Equal(t, 1, starts)
	require.Equal(t, 1, completions)
	require.Equal(t, 1, usages)
	require.Equal(t, 1, thoughts)
	client.mu.Lock()
	defer client.mu.Unlock()
	require.Len(t, client.extensions, 5)
	for _, event := range client.extensions {
		require.Contains(t, string(event), RawEventMethod)
	}
}

func TestConformanceCancellationAndNativeFailureRemainRestorable(t *testing.T) {
	t.Parallel()
	for _, command := range []string{"wait", "fail", "rate-limit", "provider-failed", "connection-lost", "invalid-stream"} {
		t.Run(command, func(t *testing.T) {
			t.Parallel()
			a, client, _, workspace := fixtureAgent(t)
			session := fixtureSession(t, a, workspace)
			type outcome struct {
				response acp.PromptResponse
				err      error
			}
			finished := make(chan outcome, 1)
			go func() {
				response, err := a.Prompt(t.Context(), wire.PromptRequest(session.SessionId, acp.TextBlock(command)))
				finished <- outcome{response, err}
			}()
			if command == "wait" {
				waitForText(t, client, "waiting")
				require.NoError(t, a.Cancel(t.Context(), acp.CancelNotification{SessionId: session.SessionId}))
			}
			select {
			case got := <-finished:
				if command == "wait" {
					require.NoError(t, got.err)
					require.Equal(t, acp.StopReasonCancelled, got.response.StopReason)
				} else {
					var requestErr *acp.RequestError
					require.True(t, errors.As(got.err, &requestErr))
					data, ok := requestErr.Data.(map[string]any)
					require.True(t, ok)
					require.Equal(t, "nanocodex_turn_failed", data["error"])
					switch command {
					case "connection-lost":
						require.Equal(t, "gateway connection ended before completion", data["message"])
					case "invalid-stream":
						require.Equal(t, "invalid gateway event stream", data["message"])
					}
					if command == "invalid-stream" {
						require.Equal(t, "transport", data["cause"])
					} else {
						require.Equal(t, "provider", data["cause"])
					}
					if command == "rate-limit" {
						require.Equal(t, "rate_limit_exceeded", data["providerCode"])
						require.Equal(t, "provider rate limit exceeded", data["message"])
						require.NotContains(t, data, "statusCode")
					}
					if command == "provider-failed" {
						require.Equal(t, "upstream_error", data["providerCode"])
						require.Equal(t, "provider response did not complete", data["message"])
						require.NotContains(t, data, "statusCode")
					}
				}
			case <-time.After(5 * time.Second):
				t.Fatal("prompt did not finish")
			}
			_, err := a.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.SessionId})
			require.NoError(t, err)
			client.reset()
			_, err = a.LoadSession(t.Context(), acp.LoadSessionRequest{SessionId: session.SessionId, Cwd: workspace})
			require.NoError(t, err)
			notifications, _ := client.snapshot()
			require.Len(t, notifications, 1)
			require.NotNil(t, notifications[0].Update.UserMessageChunk)
			fixturePrompt(t, a, session.SessionId, "recover")
		})
	}
}

func TestConformanceCancellationBeforeAcceptanceReachesNativeTurn(t *testing.T) {
	t.Parallel()
	for _, command := range []string{"wait", "fail"} {
		t.Run(command, func(t *testing.T) {
			t.Parallel()
			a, client, _, workspace := fixtureAgent(t)
			barrier := filepath.Join(t.TempDir(), "accept")
			options := NewNanocodexOptions(WithNanocodexEnv(map[string]string{"NANOCODEX_TEST_ACCEPT_BARRIER": barrier}))
			session := fixtureSession(t, a, workspace, WithSessionNanocodexOptions(options))
			type outcome struct {
				response acp.PromptResponse
				err      error
			}
			finished := make(chan outcome, 1)
			go func() {
				response, err := a.Prompt(t.Context(), wire.PromptRequest(session.SessionId, acp.TextBlock(command)))
				finished <- outcome{response, err}
			}()
			require.Eventually(t, func() bool {
				_, err := os.Stat(barrier + ".ready")

				return err == nil
			}, 5*time.Second, 5*time.Millisecond)
			require.NoError(t, a.Cancel(t.Context(), acp.CancelNotification{SessionId: session.SessionId}))
			require.NoError(t, os.WriteFile(barrier, nil, 0o600))
			select {
			case got := <-finished:
				require.NoError(t, got.err)
				require.Equal(t, acp.StopReasonCancelled, got.response.StopReason)
			case <-time.After(5 * time.Second):
				t.Fatal("cancellation before acceptance was lost")
			}
			require.Empty(t, client.text())
		})
	}
}

func TestConformanceNativeResultWithoutAcceptanceFailsAndRemainsRestorable(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"end_turn", "native_cancelled", "local_cancelled"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			a, client, home, workspace := fixtureAgent(t)
			_, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber, Meta: map[string]any{lifecycle.MetaKey: map[string]any{"version": 1}}})
			require.NoError(t, err)
			env := map[string]string{"NANOCODEX_TEST_OMIT_EVENTS": "1"}
			barrier := filepath.Join(t.TempDir(), "accept")
			if name == "local_cancelled" {
				env["NANOCODEX_TEST_ACCEPT_BARRIER"] = barrier
			}
			if name == "native_cancelled" {
				env["NANOCODEX_TEST_CANCELLED_RESULT"] = "1"
			}
			options := NewNanocodexOptions(WithNanocodexEnv(env))
			session := fixtureSession(t, a, workspace, WithSessionNanocodexOptions(options))
			request := wire.PromptRequest(session.SessionId, acp.TextBlock("unacknowledged prompt"))
			request.Meta = map[string]any{lifecycle.MetaKey: map[string]any{"version": 1, "submission": map[string]any{"submissionId": "missing-acceptance", "clientNonce": "nonce-missing"}}}
			finished := make(chan error, 1)
			go func() {
				_, promptErr := a.Prompt(t.Context(), request)
				finished <- promptErr
			}()
			if name == "local_cancelled" {
				require.Eventually(t, func() bool {
					_, statErr := os.Stat(barrier + ".ready")

					return statErr == nil
				}, 5*time.Second, 5*time.Millisecond)
				require.NoError(t, a.Cancel(t.Context(), acp.CancelNotification{SessionId: session.SessionId}))
				require.NoError(t, os.WriteFile(barrier, nil, 0o600))
			}
			select {
			case err = <-finished:
			case <-time.After(5 * time.Second):
				t.Fatal("prompt did not finish")
			}
			var requestErr *acp.RequestError
			require.ErrorAs(t, err, &requestErr)
			data, ok := requestErr.Data.(map[string]any)
			require.True(t, ok)
			require.Equal(t, "nanocodex_turn_failed", data["error"])
			require.Equal(t, "transport", data["cause"])
			notifications, frames := client.snapshot()
			require.Empty(t, notifications)
			require.Empty(t, frames)
			require.Empty(t, client.text())

			_, err = a.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.SessionId})
			require.NoError(t, err)
			require.NoError(t, os.Remove(rolloutPath(home, nativeID(t, session.Meta))))
			options = NewNanocodexOptions(WithNanocodexEnv(map[string]string{"NANOCODEX_TEST_OMIT_EVENTS": "0", "NANOCODEX_TEST_CANCELLED_RESULT": "0"}))
			_, err = a.LoadSession(t.Context(), wire.LoadSessionRequest(session.SessionId, workspace, WithSessionNanocodexOptions(options)))
			require.NoError(t, err)
			notifications, _ = client.snapshot()
			require.Len(t, notifications, 3)
			require.NotNil(t, notifications[0].Update.UserMessageChunk)
			require.Equal(t, "unacknowledged prompt", notifications[0].Update.UserMessageChunk.Content.Text.Text)
			require.Equal(t, "Hello world.Second answer.", client.text())

			client.reset()
			request = wire.PromptRequest(session.SessionId, acp.TextBlock("recovered prompt"))
			request.Meta = map[string]any{lifecycle.MetaKey: map[string]any{"version": 1, "submission": map[string]any{"submissionId": "recovered", "clientNonce": "nonce-recovered"}}}
			response, err := a.Prompt(t.Context(), request)
			require.NoError(t, err)
			require.Equal(t, acp.StopReasonEndTurn, response.StopReason)
			require.Equal(t, "Hello world.Second answer.", client.text())
			_, frames = client.snapshot()
			negotiated := lifecycle.Negotiated{Version: 1, UpdatesOutsidePrompt: false, ActivityKinds: []lifecycle.ActivityKind{}}
			require.NoError(t, lifecycle.CheckAttribution(negotiated, frames))
		})
	}
}

func TestConformanceNativeIdentityDriftStaysPoisonedThroughCancellation(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"end_turn", "native_cancelled", "local_cancelled"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			a, client, _, workspace := fixtureAgent(t)
			_, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber, Meta: map[string]any{lifecycle.MetaKey: map[string]any{"version": 1}}})
			require.NoError(t, err)
			env := map[string]string{"NANOCODEX_TEST_CHANGED_RESULT_ID": "1"}
			if name == "native_cancelled" {
				env["NANOCODEX_TEST_CANCELLED_RESULT"] = "1"
			}
			options := NewNanocodexOptions(WithNanocodexEnv(env))
			session := fixtureSession(t, a, workspace, WithSessionNanocodexOptions(options))
			input := "hello"
			if name == "local_cancelled" {
				input = "wait"
			}
			request := wire.PromptRequest(session.SessionId, acp.TextBlock(input))
			request.Meta = map[string]any{lifecycle.MetaKey: map[string]any{"version": 1, "submission": map[string]any{"submissionId": "changed-identity", "clientNonce": "nonce-changed"}}}
			finished := make(chan error, 1)
			go func() {
				_, promptErr := a.Prompt(t.Context(), request)
				finished <- promptErr
			}()
			if name == "local_cancelled" {
				waitForText(t, client, "waiting")
				require.NoError(t, a.Cancel(t.Context(), acp.CancelNotification{SessionId: session.SessionId}))
			}
			select {
			case err = <-finished:
			case <-time.After(5 * time.Second):
				t.Fatal("prompt did not finish")
			}
			require.Equal(t, wire.SessionPoisoned(vendor, "native_session_identity_drift"), err)
			_, frames := client.snapshot()
			negotiated := lifecycle.Negotiated{Version: 1, UpdatesOutsidePrompt: false, ActivityKinds: []lifecycle.ActivityKind{}}
			require.NoError(t, lifecycle.CheckAttribution(negotiated, frames))
			terminal, err := lifecycle.DecodeSessionUpdate(frames[len(frames)-1], negotiated)
			require.NoError(t, err)
			require.NotNil(t, terminal.Event.State)
			require.Equal(t, lifecycle.ForegroundIdle, terminal.Event.State.State)
			require.Equal(t, lifecycle.OutcomeFailed, terminal.Event.State.Outcome)

			_, err = a.Prompt(t.Context(), request)
			require.Equal(t, wire.SessionPoisoned(vendor, "native_session_identity_drift"), err)
			options = NewNanocodexOptions(WithNanocodexEnv(map[string]string{"NANOCODEX_TEST_CHANGED_RESULT_ID": "0", "NANOCODEX_TEST_CANCELLED_RESULT": "0"}))
			load := wire.LoadSessionRequest(session.SessionId, workspace, WithSessionNanocodexOptions(options))
			_, err = a.LoadSession(t.Context(), load)
			require.Equal(t, wire.SessionPoisoned(vendor, "native_session_identity_drift"), err)
			_, err = a.ResumeSession(t.Context(), wire.ResumeSessionRequest(session.SessionId, workspace, WithSessionNanocodexOptions(options)))
			require.Equal(t, wire.SessionPoisoned(vendor, "native_session_identity_drift"), err)
			_, err = a.Prompt(t.Context(), request)
			require.Equal(t, wire.SessionPoisoned(vendor, "native_session_identity_drift"), err)

			_, err = a.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.SessionId})
			require.NoError(t, err)
			_, err = a.LoadSession(t.Context(), load)
			require.NoError(t, err)
			client.reset()
			request = wire.PromptRequest(session.SessionId, acp.TextBlock("recovered prompt"))
			request.Meta = map[string]any{lifecycle.MetaKey: map[string]any{"version": 1, "submission": map[string]any{"submissionId": "recovered", "clientNonce": "nonce-recovered"}}}
			response, err := a.Prompt(t.Context(), request)
			require.NoError(t, err)
			require.Equal(t, acp.StopReasonEndTurn, response.StopReason)
			require.Equal(t, "Hello world.Second answer.", client.text())
		})
	}
}

type terminalCommitStore struct {
	acpcore.SessionStore
	writes  atomic.Int32
	entered chan struct{}
	release chan struct{}
}

func (s *terminalCommitStore) Replace(ctx context.Context, key acpcore.SessionKey, rows []acpcore.SessionStoreReplacement) error {
	if s.writes.Add(1) == 3 {
		close(s.entered)
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return s.SessionStore.Replace(ctx, key, rows)
}

func TestConformanceTerminalMirrorFailurePreservesGenerationAndFences(t *testing.T) {
	t.Parallel()

	for _, input := range []string{"hello", "wait", "http-rate-limit", "exit"} {
		t.Run(input, func(t *testing.T) {
			t.Parallel()

			failure := &configFailureStore{SessionStore: acpcore.NewInMemorySessionStore()}
			store := &terminalCommitStore{SessionStore: failure, entered: make(chan struct{}), release: make(chan struct{})}
			a, client, _, workspace := fixtureAgent(t, WithSessionStore(store))
			var release sync.Once
			t.Cleanup(func() {
				failure.fail.Store(false)
				release.Do(func() { close(store.release) })
			})
			_, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber, Meta: map[string]any{lifecycle.MetaKey: map[string]any{"version": 1}}})
			require.NoError(t, err)
			created := fixtureSession(t, a, workspace)
			request := wire.TextPromptRequest(created.SessionId, input)
			request.Meta = map[string]any{lifecycle.MetaKey: map[string]any{"version": 1, "submission": map[string]any{"submissionId": "commit-failure", "clientNonce": "failed-nonce"}}}
			type outcome struct {
				response acp.PromptResponse
				err      error
			}
			finished := make(chan outcome, 1)
			go func() {
				response, promptErr := a.Prompt(t.Context(), request)
				finished <- outcome{response, promptErr}
			}()
			if input == "wait" {
				waitForText(t, client, "waiting")
				require.NoError(t, a.Cancel(t.Context(), acp.CancelNotification{SessionId: created.SessionId}))
			}
			awaitAgentResult(t, store.entered)
			before, err := store.Load(t.Context(), string(created.SessionId))
			require.NoError(t, err)
			failure.fail.Store(true)
			release.Do(func() { close(store.release) })
			result := awaitAgentResult(t, finished)
			failure.fail.Store(false)
			require.Empty(t, result.response.StopReason)
			switch input {
			case "http-rate-limit":
				require.Equal(t, wire.TurnFailed(vendor, wire.TurnFailure{Cause: wire.CauseProvider, Message: "provider rate limit exceeded", StatusCode: 429, ProviderCode: "rate_limit_exceeded"}), result.err)
			case "exit":
				requestErr, ok := errors.AsType[*acp.RequestError](result.err)
				require.True(t, ok)
				data, ok := requestErr.Data.(map[string]any)
				require.True(t, ok)
				require.Equal(t, "nanocodex_turn_failed", data["error"])
				require.Equal(t, "process_exit", data["cause"])
				require.Contains(t, data["message"], "code 37")
				require.Contains(t, data["message"], "fixture native crash")
			default:
				require.Equal(t, wire.TurnFailed(vendor, wire.TurnFailure{Cause: wire.CauseTransport, Message: "session mirror commit failed"}), result.err)
			}
			after, err := store.Load(t.Context(), string(created.SessionId))
			require.NoError(t, err)
			require.Equal(t, before, after)

			_, frames := client.snapshot()
			negotiated := lifecycle.Negotiated{Version: 1, UpdatesOutsidePrompt: false, ActivityKinds: []lifecycle.ActivityKind{}}
			var streamID string
			accepted := false
			for _, frame := range frames {
				delivery, decodeErr := lifecycle.DecodeSessionUpdate(frame, negotiated)
				if errors.Is(decodeErr, lifecycle.ErrNoEnvelope) {
					continue
				}
				require.NoError(t, decodeErr)
				streamID = delivery.StreamID
				accepted = accepted || delivery.Event.PromptAccepted != nil
				if state := delivery.Event.State; state != nil {
					require.NotEqual(t, lifecycle.ForegroundIdle, state.State, "failed commit must not publish terminal idle")
				}
			}
			require.True(t, accepted)
			require.NotEmpty(t, streamID)
			client.reset()
			request = wire.TextPromptRequest(created.SessionId, "retry")
			request.Meta = map[string]any{lifecycle.MetaKey: map[string]any{"version": 1, "submission": map[string]any{"submissionId": "commit-retry", "clientNonce": "retry-nonce"}}}
			response, err := a.Prompt(t.Context(), request)
			require.NoError(t, err)
			require.Equal(t, acp.StopReasonEndTurn, response.StopReason)
			_, frames = client.snapshot()
			require.NotEmpty(t, frames)
			opening, err := lifecycle.DecodeSessionUpdate(frames[0], negotiated)
			require.NoError(t, err)
			require.NotNil(t, opening.Event.Snapshot)
			require.NotEqual(t, streamID, opening.StreamID)
		})
	}
}

func TestConformanceSDKCancellationWaitsForCommitAndIdle(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"cancel", "request_cancel", "refused_second_prompt"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			store := &terminalCommitStore{SessionStore: acpcore.NewInMemorySessionStore(), entered: make(chan struct{}), release: make(chan struct{})}
			a, _, _, workspace := fixtureAgent(t, WithSessionStore(store))
			var release sync.Once
			t.Cleanup(func() { release.Do(func() { close(store.release) }) })
			_, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber, Meta: map[string]any{lifecycle.MetaKey: map[string]any{"version": 1}}})
			require.NoError(t, err)
			created := fixtureSession(t, a, workspace)
			host, server := net.Pipe()
			t.Cleanup(func() { _ = host.Close(); _ = server.Close() })
			connection := acp.NewAgentSideConnection(a, server, server)
			a.attach(connection, nil)
			frames := make(chan map[string]any, 32)
			go func() {
				defer close(frames)
				decoder := json.NewDecoder(host)
				for {
					var frame map[string]any
					if decoder.Decode(&frame) != nil {
						return
					}
					frames <- frame
				}
			}()
			encoder := json.NewEncoder(host)
			prompt := wire.PromptRequest(created.SessionId, acp.TextBlock("wait"))
			prompt.Meta = map[string]any{lifecycle.MetaKey: map[string]any{"version": 1, "submission": map[string]any{"submissionId": "cancel-me", "clientNonce": "nonce"}}}
			require.NoError(t, encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "session/prompt", "params": prompt}))
			var notices []json.RawMessage
			for {
				frame := awaitAgentResult(t, frames)
				require.Nil(t, frame["id"], "prompt finished before cancellation")
				if frame["method"] == "session/update" {
					raw, marshalErr := json.Marshal(frame["params"])
					require.NoError(t, marshalErr)
					notices = append(notices, raw)
					var notification acp.SessionNotification
					require.NoError(t, json.Unmarshal(raw, &notification))
					if chunk := notification.Update.AgentMessageChunk; chunk != nil && chunk.Content.Text != nil && chunk.Content.Text.Text == "waiting" {
						break
					}
				}
			}
			if mode != "cancel" {
				method := "session/prompt"
				var params any = prompt
				if mode == "request_cancel" {
					require.NoError(t, encoder.Encode(map[string]any{"jsonrpc": "2.0", "method": "$/cancel_request", "params": map[string]any{"requestId": 1}}))
					method = "session/set_config_option"
					params = SetModelRequest(created.SessionId, "gpt-6.1-sol")
				}
				require.NoError(t, encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": 2, "method": method, "params": params}))
				frame := awaitAgentResult(t, frames)
				require.Equal(t, float64(2), frame["id"])
				requestErr, ok := frame["error"].(map[string]any)
				require.True(t, ok)
				require.Equal(t, float64(wire.Backpressure("session_prompt").Code), requestErr["code"])
			}
			require.NoError(t, encoder.Encode(map[string]any{"jsonrpc": "2.0", "method": "session/cancel", "params": acp.CancelNotification{SessionId: created.SessionId, Meta: map[string]any{vendor: map[string]any{"unknown": true}}}}))
			awaitAgentResult(t, store.entered)
			select {
			case frame := <-frames:
				t.Fatalf("published before terminal commit: %v", frame)
			case <-time.After(25 * time.Millisecond):
			}
			release.Do(func() { close(store.release) })
			for {
				frame := awaitAgentResult(t, frames)
				if frame["id"] != nil {
					require.Equal(t, float64(1), frame["id"])
					require.Nil(t, frame["error"])
					result, ok := frame["result"].(map[string]any)
					require.True(t, ok)
					require.Equal(t, "cancelled", result["stopReason"])

					break
				}
				if frame["method"] == "session/update" {
					raw, marshalErr := json.Marshal(frame["params"])
					require.NoError(t, marshalErr)
					notices = append(notices, raw)
				}
			}
			negotiated := lifecycle.Negotiated{Version: 1, UpdatesOutsidePrompt: false, ActivityKinds: []lifecycle.ActivityKind{}}
			require.NoError(t, lifecycle.CheckAttribution(negotiated, notices))
			last, err := lifecycle.DecodeSessionUpdate(notices[len(notices)-1], negotiated)
			require.NoError(t, err)
			require.NotNil(t, last.Event.State)
			require.Equal(t, lifecycle.ForegroundIdle, last.Event.State.State)
			require.Equal(t, lifecycle.OutcomeCancelled, last.Event.State.Outcome)
		})
	}
}
