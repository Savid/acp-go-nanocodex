//go:build integration

package integration

import (
	"bytes"
	"cmp"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/google/uuid"
	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/sessionlog"
	"github.com/savid/acp-go-core/wire"
	nanocodexacp "github.com/savid/acp-go-nanocodex"
	"github.com/stretchr/testify/require"
)

func initializeACP(t *testing.T, h *rpcHarness) {
	t.Helper()

	initialized := h.call(t, "initialize", acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	capabilities, ok := initialized["agentCapabilities"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, true, capabilities["loadSession"])
}

func sessionID(t *testing.T, result map[string]any) acp.SessionId {
	t.Helper()

	id, ok := result["sessionId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, id)

	return acp.SessionId(id)
}

func TestSmokeBinaryUsesGatewayAndNativeTools(t *testing.T) {
	requireIntegration(t)

	provider := newProvider(t, true)
	nativeHome, workspace := t.TempDir(), t.TempDir()
	h := startBinary(t, nativeHome, provider.server.URL+"/v1")
	initializeACP(t, h)
	session := h.call(t, "session/new", wire.NewSessionRequest(workspace))
	id := sessionID(t, session)
	result := h.call(t, "session/prompt", wire.TextPromptRequest(id, "create the fixture file"))
	require.Equal(t, "end_turn", result["stopReason"])
	require.Equal(t, "answer-2", h.text(t), "streamed chunks and complete messages must not duplicate text")
	content, err := os.ReadFile(filepath.Join(workspace, "integration-proof.txt"))
	require.NoError(t, err)
	require.Equal(t, "native-file-value", string(content))
	require.Contains(t, string(mustJSON(t, h.notices)), `"sessionUpdate":"tool_call"`)
	h.call(t, "session/close", acp.CloseSessionRequest{SessionId: id})
	h.stop()

	requests := provider.history(t)
	require.Len(t, requests, 2)
	require.Equal(t, "openai-codex/gpt-6-astra", requests[0]["model"])
	require.Contains(t, string(mustJSON(t, requests[1])), "tool-proof")
}

func TestSmokeShellEnvForwardsNamedSensitiveVariablesRedacted(t *testing.T) {
	requireIntegration(t)

	provider := newProviderWithResponseMode(t, false, "shell-env")
	h := startBinary(t, t.TempDir(), provider.server.URL+"/v1")
	initializeACP(t, h)
	options := nanocodexacp.NewNanocodexOptions(
		nanocodexacp.WithNanocodexEnv(map[string]string{"FIXTURE_ALLOWED_TOKEN": "fixture-allowed-secret", "FIXTURE_DENIED_TOKEN": "fixture-denied-secret"}),
		nanocodexacp.WithNanocodexShellEnv("FIXTURE_ALLOWED_TOKEN"),
	)
	created := h.call(t, "session/new", wire.NewSessionRequest(t.TempDir(), nanocodexacp.WithSessionNanocodexOptions(options), nanocodexacp.WithSessionRawEvents(true)))
	id := sessionID(t, created)
	result := h.call(t, "session/prompt", wire.TextPromptRequest(id, "probe the shell environment"))
	require.Equal(t, "end_turn", result["stopReason"])
	h.call(t, "session/close", acp.CloseSessionRequest{SessionId: id})
	h.stop()

	requests := provider.history(t)
	require.Len(t, requests, 2)
	continuation := string(mustJSON(t, requests[1]))
	require.Contains(t, continuation, "allowed-present [REDACTED]|denied-absent")

	for _, secret := range []string{"fixture-allowed-secret", "fixture-denied-secret"} {
		require.NotContains(t, continuation, secret)
		require.NotContains(t, string(mustJSON(t, h.notices)), secret)
	}
}

func TestSmokeNativeViewImageDoesNotPublishBinaryData(t *testing.T) {
	requireIntegration(t)
	const png = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aD1sAAAAASUVORK5CYII="
	image, err := base64.StdEncoding.DecodeString(png)
	require.NoError(t, err)
	workspace := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(workspace, "fixture.png"), image, 0o600))
	provider := newProviderWithResponseMode(t, false, "view-image")
	h := startBinary(t, t.TempDir(), provider.server.URL+"/v1")
	initializeACP(t, h)
	created := h.call(t, "session/new", wire.NewSessionRequest(workspace, nanocodexacp.WithSessionRawEvents(true)))
	id := sessionID(t, created)
	result := h.call(t, "session/prompt", wire.TextPromptRequest(id, "view fixture.png"))
	require.Equal(t, "end_turn", result["stopReason"])
	require.Equal(t, "answer-2", h.text(t))
	serialized := string(mustJSON(t, h.notices))
	require.NotContains(t, serialized, png)
	require.Contains(t, serialized, `"sessionUpdate":"tool_call_update"`)
	h.call(t, "session/close", acp.CloseSessionRequest{SessionId: id})
	h.stop()
}

func TestSmokeResponseIDsAndUnknownCacheAccounting(t *testing.T) {
	requireIntegration(t)

	for _, mode := range []string{"created", "late", "completion"} {
		t.Run(mode, func(t *testing.T) {
			provider := newProviderWithResponseMode(t, false, mode)
			h := startBinary(t, t.TempDir(), provider.server.URL+"/v1")
			initializeACP(t, h)
			created := h.call(t, "session/new", wire.NewSessionRequest(t.TempDir()))
			id := sessionID(t, created)
			h.call(t, "session/prompt", wire.TextPromptRequest(id, "report attribution"))
			require.Equal(t, "answer-1", h.text(t))
			var texts, thoughts, usages int
			for _, notice := range h.notices {
				if notice["method"] != "session/update" {
					continue
				}
				var notification acp.SessionNotification
				require.NoError(t, json.Unmarshal(mustJSON(t, notice["params"]), &notification))
				update := notification.Update
				checkID := func(responseID *string) {
					if mode == "late" {
						require.Nil(t, responseID)
					} else {
						require.NotNil(t, responseID)
						require.Equal(t, "response-1", *responseID)
					}
				}
				if update.AgentMessageChunk != nil {
					texts++
					checkID(update.AgentMessageChunk.MessageId)
				}
				if update.AgentThoughtChunk != nil {
					thoughts++
					checkID(update.AgentThoughtChunk.MessageId)
				}
				if usage := update.UsageUpdate; usage != nil {
					usages++
					require.Equal(t, 15, usage.Used)
					require.Positive(t, usage.Size)
					call, ok := usage.Meta[wire.CallUsageKey].(map[string]any)
					require.True(t, ok)
					require.Equal(t, "response-1", call["responseId"])
					require.Equal(t, float64(3), call["outputTokens"])
					require.NotContains(t, call, "inputTokens")
					require.NotContains(t, call, "cachedReadTokens")
					require.NotContains(t, call, "cachedWriteTokens")
				}
			}
			require.Equal(t, 1, texts)
			require.Equal(t, 1, usages)
			if mode == "completion" {
				require.Zero(t, thoughts)
			} else {
				require.Equal(t, 1, thoughts)
			}
			h.call(t, "session/close", acp.CloseSessionRequest{SessionId: id})
			h.stop()
		})
	}
}

func TestSmokeIdlessItemsPreserveDistinctMessagesAndStreaming(t *testing.T) {
	requireIntegration(t)

	for _, test := range []struct {
		mode   string
		chunks []string
		calls  int
	}{
		{mode: "idless-calls", chunks: []string{"Checking", "Checking complete"}, calls: 2},
		{mode: "idless-items", chunks: []string{"Checking", "Checking complete", "Checking"}, calls: 1},
		{mode: "idless-stream", chunks: []string{"Checking", " complete", "Checking complete"}, calls: 1},
	} {
		t.Run(test.mode, func(t *testing.T) {
			provider := newProviderWithResponseMode(t, false, test.mode)
			h := startBinary(t, t.TempDir(), provider.server.URL+"/v1")
			initializeACP(t, h)
			created := h.call(t, "session/new", wire.NewSessionRequest(t.TempDir()))
			id := sessionID(t, created)
			h.call(t, "session/prompt", wire.TextPromptRequest(id, "check and report"))
			var chunks []string
			for _, notice := range h.notices {
				if notice["method"] != "session/update" {
					continue
				}
				var notification acp.SessionNotification
				require.NoError(t, json.Unmarshal(mustJSON(t, notice["params"]), &notification))
				if chunk := notification.Update.AgentMessageChunk; chunk != nil {
					require.NotNil(t, chunk.Content.Text)
					chunks = append(chunks, chunk.Content.Text.Text)
				}
			}
			require.Equal(t, test.chunks, chunks)
			require.Len(t, provider.history(t), test.calls)
			h.call(t, "session/close", acp.CloseSessionRequest{SessionId: id})
			h.stop()
		})
	}
}

func TestSmokeStoreRestoresAfterNativeStateLoss(t *testing.T) {
	requireIntegration(t)

	provider := newProvider(t, true)
	store := acpcore.NewInMemorySessionStore()
	nativeHome, workspace := t.TempDir(), t.TempDir()
	first := startEmbedded(t, nativeHome, store)
	initializeACP(t, first)
	options := nanocodexacp.NewNanocodexOptions(
		nanocodexacp.WithNanocodexModel("gpt-6.1-sol"), nanocodexacp.WithNanocodexThinking("high"),
		nanocodexacp.WithNanocodexAPIBaseURL(provider.server.URL+"/v1"), nanocodexacp.WithNanocodexModelIDPrefix("openai"),
	)
	session := first.call(t, "session/new", wire.NewSessionRequest(workspace, nanocodexacp.WithSessionNanocodexOptions(options)))
	id := sessionID(t, session)
	first.call(t, "session/prompt", wire.TextPromptRequest(id, "remember the fixture file"))
	first.call(t, "session/close", acp.CloseSessionRequest{SessionId: id})
	first.stop()

	records, err := store.Load(t.Context(), string(id))
	require.NoError(t, err)
	require.NotEmpty(t, records)
	require.NoError(t, os.RemoveAll(nativeHome))

	restored := startEmbedded(t, nativeHome, store)
	initializeACP(t, restored)
	restored.call(t, "session/load", wire.LoadSessionRequest(id, workspace))
	require.Contains(t, restored.text(t), "answer-2")
	require.Contains(t, string(mustJSON(t, restored.notices)), "tool-proof", "tool result replays from native rows")
	restored.notices = nil
	response := restored.call(t, "session/prompt", wire.TextPromptRequest(id, "continue from the previous tool output"))
	require.Equal(t, "end_turn", response["stopReason"])
	require.Equal(t, "answer-3", restored.text(t))
	restored.call(t, "session/close", acp.CloseSessionRequest{SessionId: id})
	restored.stop()

	requests := provider.history(t)
	require.Len(t, requests, 3)
	last := string(mustJSON(t, requests[2]))
	require.Contains(t, last, "remember the fixture file")
	require.Contains(t, last, "tool-proof")
	require.Contains(t, last, "answer-2")
	require.Equal(t, "openai/gpt-6.1-sol", requests[2]["model"])
}

func TestSmokeEmptyStoreRestoresBeforeFirstPrompt(t *testing.T) {
	requireIntegration(t)

	provider := newProvider(t, false)
	store := acpcore.NewInMemorySessionStore()
	nativeHome, workspace := t.TempDir(), t.TempDir()
	first := startEmbedded(t, nativeHome, store)
	initializeACP(t, first)
	options := nanocodexacp.NewNanocodexOptions(nanocodexacp.WithNanocodexAPIBaseURL(provider.server.URL + "/v1"))
	created := first.call(t, "session/new", wire.NewSessionRequest(workspace, nanocodexacp.WithSessionNanocodexOptions(options)))
	id := sessionID(t, created)
	first.call(t, "session/close", acp.CloseSessionRequest{SessionId: id})
	first.stop()
	require.Empty(t, provider.history(t), "opening an agent must not spend model tokens")
	require.NoError(t, os.RemoveAll(nativeHome))

	restored := startEmbedded(t, nativeHome, store)
	initializeACP(t, restored)
	restored.call(t, "session/load", wire.LoadSessionRequest(id, workspace))
	response := restored.call(t, "session/prompt", wire.TextPromptRequest(id, "first accepted prompt"))
	require.Equal(t, "end_turn", response["stopReason"])
	require.Equal(t, "answer-1", restored.text(t))
	restored.call(t, "session/close", acp.CloseSessionRequest{SessionId: id})
	restored.stop()
	require.Len(t, provider.history(t), 1)
}

func TestSmokeNativeContinuationReconcilesIntoStore(t *testing.T) {
	requireIntegration(t)

	provider := newProvider(t, false)
	store := acpcore.NewInMemorySessionStore()
	nativeHome, workspace := t.TempDir(), t.TempDir()
	first := startEmbedded(t, nativeHome, store)
	initializeACP(t, first)
	options := nanocodexacp.NewNanocodexOptions(nanocodexacp.WithNanocodexAPIBaseURL(provider.server.URL + "/v1"))
	created := first.call(t, "session/new", wire.NewSessionRequest(workspace, nanocodexacp.WithSessionNanocodexOptions(options)))
	id := sessionID(t, created)
	completed := first.call(t, "session/prompt", wire.TextPromptRequest(id, "original ACP prompt"))
	meta, ok := completed["_meta"].(map[string]any)
	require.True(t, ok)
	binding, ok := meta["nanocodex"].(map[string]any)
	require.True(t, ok)
	nativeID, ok := binding["nativeSessionId"].(string)
	require.True(t, ok)
	first.call(t, "session/close", acp.CloseSessionRequest{SessionId: id})
	first.stop()

	native := startNative(t, nativeHome, workspace)
	native.call(t, "initialize", map[string]any{"sessionId": nativeID, "resumeSessionId": nativeID, "apiBaseUrl": provider.server.URL + "/v1"})
	native.call(t, "prompt", map[string]any{"content": []map[string]any{{"type": "text", "text": "independent native continuation"}}})
	native.call(t, "shutdown", map[string]any{})
	native.stop()

	restored := startEmbedded(t, nativeHome, store)
	initializeACP(t, restored)
	restored.call(t, "session/load", wire.LoadSessionRequest(id, workspace))
	require.Equal(t, "answer-1answer-2", restored.text(t))
	restored.notices = nil
	restored.call(t, "session/prompt", wire.TextPromptRequest(id, "continue after native work"))
	require.Equal(t, "answer-3", restored.text(t))
	restored.call(t, "session/close", acp.CloseSessionRequest{SessionId: id})
	restored.stop()
	requests := provider.history(t)
	require.Len(t, requests, 3)
	require.Contains(t, string(mustJSON(t, requests[2])), "independent native continuation")
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()

	encoded, err := json.Marshal(value)
	require.NoError(t, err)

	return encoded
}

func TestLiveSentinelPromptAndReplay(t *testing.T) {
	requireIntegration(t)

	if os.Getenv("ACP_GO_NANOCODEX_RUN_LIVE_TOKENS") != "1" {
		t.Skip("set ACP_GO_NANOCODEX_RUN_LIVE_TOKENS=1 to spend model tokens")
	}

	nativeHome := t.TempDir()
	keyEnv := os.Getenv("NANOCODEX_API_KEY_ENV")
	if keyEnv == "" {
		keyEnv = "OPENAI_API_KEY"
	}

	if os.Getenv(keyEnv) == "" {
		require.Empty(t, os.Getenv("NANOCODEX_API_KEY_ENV"), "the explicitly selected credential environment variable is empty")
		source := os.Getenv("ACP_GO_NANOCODEX_HOME")
		require.NotEmpty(t, source, "live requires an explicit API key or ACP_GO_NANOCODEX_HOME containing native auth.json")
		auth, err := os.ReadFile(filepath.Join(source, "auth.json"))
		require.NoError(t, err, "read explicitly selected native authentication")
		require.NoError(t, os.WriteFile(filepath.Join(nativeHome, "auth.json"), auth, 0o600))
	}

	args := []string{"--path", binaryPath(t, "ACP_GO_NANOCODEX_HARNESS_PATH", "acp-go-nanocodex-native", true), "--home", nativeHome}
	if model := os.Getenv("ACP_GO_NANOCODEX_MODEL"); model != "" {
		args = append(args, "--model", model)
	}

	command := exec.CommandContext(t.Context(), binaryPath(t, "ACP_GO_NANOCODEX_AGENT_BINARY", "acp-go-nanocodex", true), args...)
	command.Env = append(os.Environ(), "OTEL_SDK_DISABLED=true")
	h := startCommand(t, command)
	h.timeout = 2 * time.Minute
	initializeACP(t, h)
	workspace := t.TempDir()
	sentinel := "ACP_NANOCODEX_LIVE_" + uuid.NewString()
	firstPath, secondPath := t.TempDir(), t.TempDir()
	writeMarker := func(directory, forbidden, value string) {
		script := "#!/bin/sh\ncase \"$PATH\" in '" + directory + "':*) ;; *) exit 31;; esac\n"
		if forbidden != "" {
			script += "case :\"$PATH\": in *:'" + forbidden + "':*) exit 32;; esac\n"
		}
		script += "printf '%s' '" + value + "'\n"
		require.NoError(t, os.WriteFile(filepath.Join(directory, "acp-native-marker"), []byte(script), 0o700))
	}
	writeMarker(firstPath, "", sentinel)
	rotated := "ROTATED_" + uuid.NewString()
	writeMarker(secondPath, firstPath, rotated)
	options := nanocodexacp.NewNanocodexOptions(nanocodexacp.WithNanocodexExtraPathDirs(firstPath))
	created := h.call(t, "session/new", wire.NewSessionRequest(workspace, nanocodexacp.WithSessionRawEvents(true), nanocodexacp.WithSessionNanocodexOptions(options)))
	id := sessionID(t, created)
	response := h.call(t, "session/prompt", wire.TextPromptRequest(id, "Use exec_command with login:false to run acp-native-marker by name using PATH. Reply with exactly its complete stdout and nothing else. Do not write commentary or modify files."))
	require.Equal(t, "end_turn", response["stopReason"])
	require.Equal(t, sentinel, h.text(t))
	require.Contains(t, string(mustJSON(t, h.notices)), nanocodexacp.RawEventMethod)
	require.Contains(t, string(mustJSON(t, h.notices)), `"sessionUpdate":"tool_call"`)
	t.Logf("tool turn usage: %s", mustJSON(t, response["usage"]))
	h.call(t, "session/close", acp.CloseSessionRequest{SessionId: id})
	h.notices = nil
	rotatedOptions := nanocodexacp.NewNanocodexOptions(nanocodexacp.WithNanocodexExtraPathDirs(secondPath))
	h.call(t, "session/load", wire.LoadSessionRequest(id, workspace, nanocodexacp.WithSessionNanocodexOptions(rotatedOptions)))
	require.Equal(t, sentinel, h.text(t), "the store-backed load must replay the completed turn")
	h.notices = nil
	continued := h.call(t, "session/prompt", wire.TextPromptRequest(id, "Use exec_command with login:false to run acp-native-marker by name using PATH again. Reply with exactly your previous final answer, then a vertical bar, then the new complete stdout. No whitespace or commentary."))
	require.Equal(t, "end_turn", continued["stopReason"])
	require.Equal(t, sentinel+"|"+rotated, h.text(t), "a new helper must retain history and use only the rotated extraPathDirs")
	t.Logf("continuation usage: %s", mustJSON(t, continued["usage"]))
	h.call(t, "session/close", acp.CloseSessionRequest{SessionId: id})
	h.notices = nil
	h.call(t, "session/resume", acp.ResumeSessionRequest{SessionId: id, Cwd: workspace})
	require.Empty(t, h.text(t))
	h.nextID++
	require.NoError(t, h.encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": h.nextID, "method": "session/prompt", "params": wire.TextPromptRequest(id, "Use exec_command with login:false to run sleep 60. Wait for it to finish before answering. Do not write commentary.")}))
	deadline := time.NewTimer(h.timeout)
	defer deadline.Stop()
	cancelled := false
	for {
		select {
		case frame := <-h.frames:
			require.NotNil(t, frame, "ACP transport ended before cancellation completed")
			if frame["id"] != nil {
				require.Equal(t, float64(h.nextID), frame["id"])
				require.Nil(t, frame["error"])
				result, ok := frame["result"].(map[string]any)
				require.True(t, ok)
				require.True(t, cancelled, "native turn completed without starting its long-running tool")
				require.Equal(t, "cancelled", result["stopReason"])
				goto completed
			}
			if !cancelled && frame["method"] == "session/update" && strings.Contains(string(mustJSON(t, frame["params"])), `"sessionUpdate":"tool_call"`) {
				require.NoError(t, h.encoder.Encode(map[string]any{"jsonrpc": "2.0", "method": "session/cancel", "params": acp.CancelNotification{SessionId: id}}))
				cancelled = true
			}
		case <-deadline.C:
			t.Fatal("live native cancellation did not complete")
		}
	}
completed:
	h.call(t, "session/close", acp.CloseSessionRequest{SessionId: id})
	h.call(t, "session/delete", wire.DeleteSessionRequest(id))
	h.stop()
}

func TestSmokeSymlinkedHomeRecoversInterruptedRollout(t *testing.T) {
	requireIntegration(t)

	provider := newProvider(t, false)
	store := acpcore.NewInMemorySessionStore()
	link := filepath.Join(t.TempDir(), "home")
	require.NoError(t, os.Symlink(t.TempDir(), link))
	home, workspace := filepath.Join(link, "codex"), t.TempDir()
	first := startEmbedded(t, home, store)
	initializeACP(t, first)
	options := nanocodexacp.NewNanocodexOptions(nanocodexacp.WithNanocodexAPIBaseURL(provider.server.URL + "/v1"))
	created := first.call(t, "session/new", wire.NewSessionRequest(workspace, nanocodexacp.WithSessionNanocodexOptions(options)))
	id := sessionID(t, created)
	first.call(t, "session/prompt", wire.TextPromptRequest(id, "retain completed history"))
	first.call(t, "session/close", acp.CloseSessionRequest{SessionId: id})
	first.stop()

	generation, err := store.Load(t.Context(), string(id))
	require.NoError(t, err)
	var record struct {
		RolloutRelative string `json:"rolloutRelative"`
	}
	require.NoError(t, json.Unmarshal(generation[sessionlog.ConfigSubpath][0], &record))
	path := filepath.Join(home, record.RolloutRelative)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	require.NoError(t, err)
	_, err = file.WriteString(`{"timestamp":"interrupted`)
	require.NoError(t, err)
	require.NoError(t, file.Close())

	restored := startEmbedded(t, home, store)
	initializeACP(t, restored)
	restored.call(t, "session/load", wire.LoadSessionRequest(id, workspace))
	require.Equal(t, "answer-1", restored.text(t))
	restored.notices = nil
	response := restored.call(t, "session/prompt", wire.TextPromptRequest(id, "continue after interrupted write"))
	require.Equal(t, "end_turn", response["stopReason"])
	require.Equal(t, "answer-2", restored.text(t))
	restored.call(t, "session/close", acp.CloseSessionRequest{SessionId: id})
	restored.notices = nil
	restored.call(t, "session/resume", acp.ResumeSessionRequest{SessionId: id, Cwd: workspace})
	require.Empty(t, restored.text(t), "resume must not replay history")
	restored.call(t, "session/delete", acp.UnstableDeleteSessionRequest{SessionId: id})
	listed := restored.call(t, "session/list", acp.ListSessionsRequest{})
	require.Empty(t, listed["sessions"])
	restored.stop()
	remaining, err := store.Load(t.Context(), string(id))
	require.NoError(t, err)
	require.Nil(t, remaining)
	_, err = os.Stat(path)
	require.NoError(t, err, "deleting the store entry must preserve native state")
	require.Len(t, provider.history(t), 2)
}

func TestSmokeChatGPTEndpointRefusalLogsConfigurationReason(t *testing.T) {
	requireIntegration(t)

	home := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(home, "auth.json"), []byte(`{"personal_access_token":"at-fixture-token"}`), 0o600))
	var logs bytes.Buffer

	const endpoint = "https://gateway.fixture.invalid/v1"
	agent := nanocodexacp.NewAgent(
		nanocodexacp.WithHome(home),
		nanocodexacp.WithExecutablePath(binaryPath(t, "ACP_GO_NANOCODEX_HARNESS_PATH", "acp-go-nanocodex-native", false)),
		nanocodexacp.WithLogger(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelError}))),
		nanocodexacp.WithEnv(map[string]string{
			"OPENAI_API_KEY": "", "OPENAI_BASE_URL": endpoint,
			"NANOCODEX_API_KEY_ENV": "", "NANOCODEX_MODEL_ID_PREFIX": "", "NANOCODEX_TRANSPORT": "https",
		}),
	)
	t.Cleanup(func() { require.NoError(t, agent.Close()) })
	_, err := agent.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.Equal(t, wire.InternalFailure("nanocodex", "native_start"), err)
	var record map[string]any
	require.NoError(t, json.Unmarshal(logs.Bytes(), &record))
	require.Equal(t, "ERROR", record["level"])
	require.Equal(t, "native initialization refused", record["msg"])
	require.Equal(t, "invalid_config", record["code"])
	require.Equal(t, "apiBaseUrl", record["field"])
	require.Equal(t, "ChatGPT authentication refuses custom API endpoints; remove apiBaseUrl and OPENAI_BASE_URL or configure an API key", record["reason"])
	require.NotContains(t, logs.String(), "at-fixture-token")
	require.NotContains(t, logs.String(), endpoint)
}

func TestSmokeCheckpointStoreRestoreAndCompactedReplay(t *testing.T) {
	requireIntegration(t)
	provider := newProviderWithResponseMode(t, false, "compaction")
	store := acpcore.NewInMemorySessionStore()
	home, workspace := t.TempDir(), t.TempDir()
	first := startEmbedded(t, home, store)
	initializeACP(t, first)
	options := nanocodexacp.NewNanocodexOptions(nanocodexacp.WithNanocodexAPIBaseURL(provider.server.URL + "/v1"))
	created := first.call(t, "session/new", wire.NewSessionRequest(workspace, nanocodexacp.WithSessionNanocodexOptions(options)))
	id := sessionID(t, created)
	first.call(t, "session/prompt", wire.TextPromptRequest(id, "first user input"))
	require.Equal(t, []*acp.Cost{{Amount: 0.125, Currency: "USD"}}, usageCosts(t, first))
	first.call(t, "session/close", acp.CloseSessionRequest{SessionId: id})
	first.stop()
	var config map[string]any
	rows, found, err := sessionlog.Load(t.Context(), store, string(id), &config)
	require.NoError(t, err)
	require.True(t, found)
	require.NotNil(t, config["checkpoint"])
	for _, row := range rows {
		require.NotContains(t, string(row), "acp_checkpoint")
	}
	require.NoError(t, os.RemoveAll(home))
	restored := startEmbedded(t, home, store)
	initializeACP(t, restored)
	restored.call(t, "session/load", wire.LoadSessionRequest(id, workspace))
	require.Equal(t, "answer-1", restored.text(t))
	restored.notices = nil
	restored.call(t, "session/prompt", wire.TextPromptRequest(id, "second user input"))
	require.Equal(t, "answer-3", restored.text(t))
	require.Equal(t, []*acp.Cost{{Amount: 0.75, Currency: "USD"}}, usageCosts(t, restored), "generation and summary charges accumulate after restore")
	for _, notice := range restored.notices {
		if notice["method"] != "session/update" {
			continue
		}
		var notification acp.SessionNotification
		require.NoError(t, json.Unmarshal(mustJSON(t, notice["params"]), &notification))
		if usage := notification.Update.UsageUpdate; usage != nil {
			require.Equal(t, 15, usage.Used, "compaction must not restate old context usage")
		}
	}
	requests := provider.history(t)
	require.Len(t, requests, 3)
	require.Equal(t, "none", requests[1]["tool_choice"])
	require.NotContains(t, string(mustJSON(t, requests[1])), "compaction_trigger")
	compacted := string(mustJSON(t, requests[2]))
	require.Contains(t, compacted, "fixture-summary")
	require.NotContains(t, compacted, `"type":"compaction"`)
	restored.call(t, "session/close", acp.CloseSessionRequest{SessionId: id})
	restored.notices = nil
	restored.call(t, "session/load", wire.LoadSessionRequest(id, workspace))
	require.Equal(t, "answer-1answer-3", restored.text(t))
	replay := string(mustJSON(t, restored.notices))
	require.Contains(t, replay, "first user input")
	require.Contains(t, replay, "second user input")
	require.NotContains(t, replay, "fixture-summary")
	restored.call(t, "session/close", acp.CloseSessionRequest{SessionId: id})
	restored.stop()
}

func TestSmokeGatewayModelJourney(t *testing.T) {
	requireIntegration(t)

	const model = "fixture-gateway/model-1"
	identity := "You are Codex, a coding agent running `" + model + "`. "
	provider := newProvider(t, true)
	store := acpcore.NewInMemorySessionStore()
	home, workspace := t.TempDir(), t.TempDir()
	first := startEmbedded(t, home, store)
	initializeACP(t, first)
	options := nanocodexacp.NewNanocodexOptions(
		nanocodexacp.WithNanocodexModel(model), nanocodexacp.WithNanocodexContextWindow(262144),
		nanocodexacp.WithNanocodexAPIBaseURL(provider.server.URL+"/v1"), nanocodexacp.WithNanocodexModelIDPrefix("openai"),
	)
	created := first.call(t, "session/new", wire.NewSessionRequest(workspace, nanocodexacp.WithSessionRawEvents(true), nanocodexacp.WithSessionNanocodexOptions(options)))
	id := sessionID(t, created)
	requireSelectedModel(t, created, model, 262144)
	first.call(t, "session/prompt", wire.TextPromptRequest(id, "create the fixture file"))
	require.Equal(t, "answer-2", first.text(t))
	content, err := os.ReadFile(filepath.Join(workspace, "integration-proof.txt"))
	require.NoError(t, err)
	require.Equal(t, "native-file-value", string(content))
	var sizes []int
	for _, notice := range first.notices {
		if notice["method"] != "session/update" {
			continue
		}
		var notification acp.SessionNotification
		require.NoError(t, json.Unmarshal(mustJSON(t, notice["params"]), &notification))
		if usage := notification.Update.UsageUpdate; usage != nil {
			sizes = append(sizes, usage.Size)
		}
	}
	require.Equal(t, []int{262144, 262144}, sizes)
	// Raw events name the gateway model, never the default base model or its prices.
	var raw []string
	for _, notice := range first.notices {
		if notice["method"] == nanocodexacp.RawEventMethod {
			raw = append(raw, string(mustJSON(t, notice)))
		}
	}
	require.NotEmpty(t, raw)
	require.Contains(t, strings.Join(raw, "\n"), `"model":"`+model+`"`)
	for _, event := range raw {
		require.NotContains(t, event, "gpt-6-luna")
		require.NotContains(t, event, "cost_status")
	}
	first.call(t, "session/close", acp.CloseSessionRequest{SessionId: id})
	first.stop()

	restored := startEmbedded(t, home, store)
	initializeACP(t, restored)
	for option, changed := range map[string]nanocodexacp.NanocodexOption{
		"model":     nanocodexacp.WithNanocodexModel("fixture-gateway/model-2"),
		"baseModel": nanocodexacp.WithNanocodexBaseModel("gpt-6.1-sol"),
	} {
		request := wire.LoadSessionRequest(id, workspace, nanocodexacp.WithSessionNanocodexOptions(nanocodexacp.NewNanocodexOptions(changed)))
		requireRefusedField(t, restored.refusal(t, "session/load", request), option)
	}
	widened := nanocodexacp.NewNanocodexOptions(nanocodexacp.WithNanocodexContextWindow(524288))
	loaded := restored.call(t, "session/load", wire.LoadSessionRequest(id, workspace, nanocodexacp.WithSessionNanocodexOptions(widened)))
	requireSelectedModel(t, loaded, model, 524288)
	restored.notices = nil
	restored.call(t, "session/prompt", wire.TextPromptRequest(id, "continue from the previous tool output"))
	require.Equal(t, "answer-3", restored.text(t))
	restored.call(t, "session/close", acp.CloseSessionRequest{SessionId: id})
	resumed := restored.call(t, "session/resume", acp.ResumeSessionRequest{SessionId: id, Cwd: workspace})
	requireSelectedModel(t, resumed, model, 524288)
	restored.call(t, "session/close", acp.CloseSessionRequest{SessionId: id})
	restored.stop()

	requests := provider.history(t)
	require.Len(t, requests, 3)
	for _, request := range requests {
		require.Equal(t, model, request["model"])
		require.True(t, strings.HasPrefix(developerPrompt(t, request), identity))
	}
	require.Contains(t, string(mustJSON(t, requests[2])), "tool-proof")
}

func TestLiveGatewayModelJourney(t *testing.T) {
	requireIntegration(t)

	if os.Getenv("ACP_GO_NANOCODEX_RUN_LIVE_TOKENS") != "1" {
		t.Skip("set ACP_GO_NANOCODEX_RUN_LIVE_TOKENS=1 to spend model tokens")
	}

	model := os.Getenv("ACP_GO_NANOCODEX_GATEWAY_MODEL")
	if model == "" {
		t.Skip("set ACP_GO_NANOCODEX_GATEWAY_MODEL to run the gateway-model journey")
	}

	upstream, err := url.Parse(strings.TrimSuffix(os.Getenv("OPENAI_BASE_URL"), "/"))
	require.NoError(t, err)
	require.NotEmpty(t, upstream.Host, "the gateway-model journey requires OPENAI_BASE_URL")
	keyEnv := cmp.Or(os.Getenv("NANOCODEX_API_KEY_ENV"), "OPENAI_API_KEY")
	require.NotEmpty(t, os.Getenv(keyEnv), "the gateway-model journey requires the selected API key")

	// A loopback recording proxy exposes the wire requests the helper sends to the gateway.
	var mu sync.Mutex
	var requests []map[string]any
	forward := &httputil.ReverseProxy{Rewrite: func(request *httputil.ProxyRequest) { request.SetURL(upstream) }, FlushInterval: -1}
	recorder := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, readErr := io.ReadAll(request.Body)
		if readErr != nil {
			http.Error(writer, "unreadable request", http.StatusBadRequest)

			return
		}
		var decoded map[string]any
		if json.Unmarshal(body, &decoded) == nil {
			mu.Lock()
			requests = append(requests, decoded)
			mu.Unlock()
		}
		request.Body = io.NopCloser(bytes.NewReader(body))
		forward.ServeHTTP(writer, request)
	}))
	t.Cleanup(recorder.Close)
	recorded := func() []map[string]any {
		mu.Lock()
		defer mu.Unlock()

		return append([]map[string]any(nil), requests...)
	}

	command := exec.CommandContext(t.Context(), binaryPath(t, "ACP_GO_NANOCODEX_AGENT_BINARY", "acp-go-nanocodex", true),
		"--path", binaryPath(t, "ACP_GO_NANOCODEX_HARNESS_PATH", "acp-go-nanocodex-native", true), "--home", t.TempDir(), "--model", model)
	command.Env = append(os.Environ(), "OTEL_SDK_DISABLED=true", "OPENAI_BASE_URL="+recorder.URL, "NANOCODEX_TRANSPORT=https",
		"NANOCODEX_BASE_MODEL=", "NANOCODEX_CONTEXT_WINDOW=", "NANOCODEX_SHELL_ENV=")
	h := startCommand(t, command)
	h.timeout = 2 * time.Minute
	initializeACP(t, h)
	workspace, markerDir := t.TempDir(), t.TempDir()
	sentinel := "ACP_NANOCODEX_GATEWAY_" + uuid.NewString()
	require.NoError(t, os.WriteFile(filepath.Join(markerDir, "acp-native-marker"), []byte("#!/bin/sh\nprintf '%s' '"+sentinel+"'\n"), 0o700))
	options := nanocodexacp.NewNanocodexOptions(nanocodexacp.WithNanocodexExtraPathDirs(markerDir))
	created := h.call(t, "session/new", wire.NewSessionRequest(workspace, nanocodexacp.WithSessionNanocodexOptions(options)))
	id := sessionID(t, created)
	requireSelectedModel(t, created, model, 0)
	response := h.call(t, "session/prompt", wire.TextPromptRequest(id, "Use exec_command with login:false to run acp-native-marker by name using PATH. Reply with exactly its complete stdout and nothing else."))
	require.Equal(t, "end_turn", response["stopReason"])
	require.Equal(t, sentinel, h.text(t))
	require.Contains(t, string(mustJSON(t, h.notices)), `"sessionUpdate":"tool_call"`)
	used := 0
	for _, notice := range h.notices {
		if notice["method"] != "session/update" {
			continue
		}
		var notification acp.SessionNotification
		require.NoError(t, json.Unmarshal(mustJSON(t, notice["params"]), &notification))
		if usage := notification.Update.UsageUpdate; usage != nil {
			used = usage.Used
		}
	}
	require.Positive(t, used)
	identity := "You are Codex, a coding agent running `" + model + "`. "
	toolTurn := recorded()
	require.GreaterOrEqual(t, len(toolTurn), 2)
	for _, request := range toolTurn {
		require.Equal(t, model, request["model"])
		require.True(t, strings.HasPrefix(developerPrompt(t, request), identity))
	}

	h.call(t, "session/close", acp.CloseSessionRequest{SessionId: id})
	h.notices = nil
	h.call(t, "session/load", wire.LoadSessionRequest(id, workspace))
	require.Equal(t, sentinel, h.text(t), "an unchanged load must replay the tool turn")
	h.call(t, "session/close", acp.CloseSessionRequest{SessionId: id})
	for option, changed := range map[string]nanocodexacp.NanocodexOption{
		"model":     nanocodexacp.WithNanocodexModel(model + "-changed"),
		"baseModel": nanocodexacp.WithNanocodexBaseModel("gpt-6.1-sol"),
	} {
		request := wire.LoadSessionRequest(id, workspace, nanocodexacp.WithSessionNanocodexOptions(nanocodexacp.NewNanocodexOptions(changed)))
		requireRefusedField(t, h.refusal(t, "session/load", request), option)
	}

	// A window at the last reported usage puts the restored context above the
	// automatic compaction threshold.
	narrowed := nanocodexacp.NewNanocodexOptions(nanocodexacp.WithNanocodexContextWindow(int64(used)))
	loaded := h.call(t, "session/load", wire.LoadSessionRequest(id, workspace, nanocodexacp.WithSessionNanocodexOptions(narrowed)))
	requireSelectedModel(t, loaded, model, used)
	h.notices = nil
	continued := h.call(t, "session/prompt", wire.TextPromptRequest(id, "Reply with exactly the stdout you reported before and nothing else."))
	require.Equal(t, "end_turn", continued["stopReason"])
	require.Equal(t, sentinel, h.text(t), "the compacted context must keep the tool output")
	compacted := recorded()[len(toolTurn):]
	require.GreaterOrEqual(t, len(compacted), 2)
	require.Equal(t, "none", compacted[0]["tool_choice"], "the first restored request must be the compaction summary")
	for _, request := range compacted {
		require.Equal(t, model, request["model"])
		require.True(t, strings.HasPrefix(developerPrompt(t, request), identity))
	}
	h.call(t, "session/close", acp.CloseSessionRequest{SessionId: id})
	h.call(t, "session/delete", wire.DeleteSessionRequest(id))
	h.stop()
}
