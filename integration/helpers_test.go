//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	acpcore "github.com/savid/acp-go-core"
	nanocodexacp "github.com/savid/acp-go-nanocodex"
	"github.com/stretchr/testify/require"
)

const integrationTimeout = 30 * time.Second

func requireIntegration(t *testing.T) {
	t.Helper()

	if os.Getenv("ACP_GO_NANOCODEX_RUN_INTEGRATION") != "1" {
		t.Skip("set ACP_GO_NANOCODEX_RUN_INTEGRATION=1 to run native integration tests")
	}
}

func binaryPath(t *testing.T, envName, name string, live bool) string {
	t.Helper()

	path := os.Getenv(envName)
	if path == "" {
		var err error
		path, err = filepath.Abs(filepath.Join("..", "bin", name))
		require.NoError(t, err)
	}

	resolved, err := exec.LookPath(path)
	if err != nil {
		if live {
			t.Fatalf("live prerequisite %s is unavailable: %v", name, err)
		}

		t.Skipf("smoke prerequisite %s is unavailable; run make build or set %s: %v", name, envName, err)
	}

	return resolved
}

type rpcHarness struct {
	timeout time.Duration
	native  bool
	encoder *json.Encoder
	frames  <-chan map[string]any
	nextID  int
	notices []map[string]any
	stop    func()
}

func newRPC(t *testing.T, reader io.Reader, writer io.Writer, stop func()) *rpcHarness {
	t.Helper()

	frames := make(chan map[string]any, 128)
	go func() {
		defer close(frames)
		decoder := json.NewDecoder(reader)

		for {
			var frame map[string]any
			if err := decoder.Decode(&frame); err != nil {
				return
			}

			frames <- frame
		}
	}()

	var once sync.Once
	h := &rpcHarness{timeout: integrationTimeout, encoder: json.NewEncoder(writer), frames: frames, stop: func() { once.Do(stop) }}
	t.Cleanup(h.stop)

	return h
}

func (h *rpcHarness) call(t *testing.T, method string, params any) map[string]any {
	t.Helper()

	frame := h.exchange(t, method, params)
	require.Nil(t, frame["error"], "ACP %s failed: %v", method, frame["error"])
	result, valid := frame["result"].(map[string]any)
	require.True(t, valid, "ACP result must be an object")

	return result
}

func (h *rpcHarness) refusal(t *testing.T, method string, params any) map[string]any {
	t.Helper()

	frame := h.exchange(t, method, params)
	refused, ok := frame["error"].(map[string]any)
	require.True(t, ok, "ACP %s unexpectedly succeeded", method)

	return refused
}

func (h *rpcHarness) exchange(t *testing.T, method string, params any) map[string]any {
	t.Helper()

	h.nextID++
	frame := map[string]any{"id": h.nextID, "method": method, "params": params}
	if !h.native {
		frame["jsonrpc"] = "2.0"
	}

	require.NoError(t, h.encoder.Encode(frame))

	ctx, cancel := context.WithTimeout(t.Context(), h.timeout)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			t.Fatal("ACP reply deadline exceeded: " + method)
		case frame, ok := <-h.frames:
			require.True(t, ok, "ACP transport closed before reply: %s", method)

			_, notification := frame["method"]
			_, nativeEvent := frame["event"]
			if notification || nativeEvent {
				h.notices = append(h.notices, frame)

				continue
			}

			require.Equal(t, float64(h.nextID), frame["id"])

			return frame
		}
	}
}

func (h *rpcHarness) text(t *testing.T) string {
	t.Helper()

	var text strings.Builder

	for _, notice := range h.notices {
		if notice["method"] != "session/update" {
			continue
		}

		params, ok := notice["params"].(map[string]any)
		require.True(t, ok)
		update, ok := params["update"].(map[string]any)
		require.True(t, ok)

		if update["sessionUpdate"] != "agent_message_chunk" {
			continue
		}

		content, ok := update["content"].(map[string]any)
		require.True(t, ok)

		if content["type"] == "text" {
			value, ok := content["text"].(string)
			require.True(t, ok)
			text.WriteString(value)
		}
	}

	return text.String()
}

func usageCosts(t *testing.T, h *rpcHarness) []*acp.Cost {
	t.Helper()

	var costs []*acp.Cost

	for _, notice := range h.notices {
		if notice["method"] != "session/update" {
			continue
		}

		var notification acp.SessionNotification
		require.NoError(t, json.Unmarshal(mustJSON(t, notice["params"]), &notification))

		if usage := notification.Update.UsageUpdate; usage != nil {
			costs = append(costs, usage.Cost)
		}
	}

	return costs
}

func startBinary(t *testing.T, nativeHome, endpoint string) *rpcHarness {
	t.Helper()

	command := exec.CommandContext(t.Context(), binaryPath(t, "ACP_GO_NANOCODEX_AGENT_BINARY", "acp-go-nanocodex", false),
		"--path", binaryPath(t, "ACP_GO_NANOCODEX_HARNESS_PATH", "acp-go-nanocodex-native", false), "--home", nativeHome)
	command.Env = append(os.Environ(),
		"OPENAI_API_KEY=fixture-bearer-token", "OPENAI_BASE_URL="+endpoint,
		"NANOCODEX_MODEL_ID_PREFIX=openai-codex", "NANOCODEX_TRANSPORT=https", "NANOCODEX_API_KEY_ENV=OPENAI_API_KEY",
		"NANOCODEX_BASE_MODEL=", "NANOCODEX_CONTEXT_WINDOW=", "NANOCODEX_SHELL_ENV=", "OTEL_SDK_DISABLED=true")

	return startCommand(t, command)
}

func startNative(t *testing.T, nativeHome, workspace string) *rpcHarness {
	t.Helper()

	command := exec.CommandContext(t.Context(), binaryPath(t, "ACP_GO_NANOCODEX_HARNESS_PATH", "acp-go-nanocodex-native", false))
	command.Dir = workspace
	command.Env = append(os.Environ(), "CODEX_HOME="+nativeHome, "OPENAI_API_KEY=fixture-bearer-token",
		"OPENAI_BASE_URL=", "NANOCODEX_MODEL_ID_PREFIX=", "NANOCODEX_TRANSPORT=https", "NANOCODEX_API_KEY_ENV=OPENAI_API_KEY",
		"NANOCODEX_BASE_MODEL=", "NANOCODEX_CONTEXT_WINDOW=", "NANOCODEX_SHELL_ENV=")
	h := startCommand(t, command)
	h.native = true

	return h
}

func startCommand(t *testing.T, command *exec.Cmd) *rpcHarness {
	t.Helper()

	input, err := command.StdinPipe()
	require.NoError(t, err)
	output, err := command.StdoutPipe()
	require.NoError(t, err)
	stderr, err := os.Create(filepath.Join(t.TempDir(), "stderr.log"))
	require.NoError(t, err)
	command.Stderr = stderr
	require.NoError(t, command.Start())

	return newRPC(t, output, input, func() {
		_ = input.Close()
		done := make(chan error, 1)
		go func() { done <- command.Wait() }()

		select {
		case err := <-done:
			if err != nil && !t.Failed() {
				t.Errorf("adapter exited: %v", err)
			}
		case <-time.After(integrationTimeout):
			_ = command.Process.Kill()
			<-done
			t.Error("adapter failed to stop after stdin closed")
		}

		_ = stderr.Close()
		if t.Failed() {
			data, _ := os.ReadFile(stderr.Name())
			t.Logf("adapter stderr: %s", data)
		}
	})
}

func startEmbedded(t *testing.T, nativeHome string, store acpcore.SessionStore) *rpcHarness {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	helper := binaryPath(t, "ACP_GO_NANOCODEX_HARNESS_PATH", "acp-go-nanocodex-native", false)
	reader, serverOutput := io.Pipe()
	serverInput, writer := io.Pipe()
	done := make(chan error, 1)

	go func() {
		done <- nanocodexacp.Serve(ctx, serverInput, serverOutput,
			nanocodexacp.WithExecutablePath(helper),
			nanocodexacp.WithHome(nativeHome), nanocodexacp.WithSessionStore(store),
			nanocodexacp.WithLogger(slog.New(slog.DiscardHandler)),
			nanocodexacp.WithEnv(map[string]string{"OPENAI_API_KEY": "fixture-bearer-token", "OPENAI_BASE_URL": "", "NANOCODEX_MODEL_ID_PREFIX": "", "NANOCODEX_TRANSPORT": "https", "NANOCODEX_API_KEY_ENV": "OPENAI_API_KEY",
				"NANOCODEX_BASE_MODEL": "", "NANOCODEX_CONTEXT_WINDOW": "", "NANOCODEX_SHELL_ENV": ""}))
	}()

	return newRPC(t, reader, writer, func() {
		cancel()
		_ = writer.Close()
		_ = serverInput.Close()
		_ = serverOutput.Close()
		_ = reader.Close()

		select {
		case <-done:
		case <-time.After(integrationTimeout):
			t.Error("embedded ACP server did not stop")
		}
	})
}

type providerFixture struct {
	server   *httptest.Server
	mu       sync.Mutex
	requests []map[string]any
}

func newProvider(t *testing.T, shell bool) *providerFixture {
	t.Helper()

	return newProviderWithResponseMode(t, shell, "created")
}

func newProviderWithResponseMode(t *testing.T, shell bool, mode string) *providerFixture {
	t.Helper()

	provider := &providerFixture{}
	provider.server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/responses" || request.Header.Get("Authorization") != "Bearer fixture-bearer-token" {
			http.Error(writer, "invalid fixture route or credentials", http.StatusBadRequest)

			return
		}

		var body map[string]any
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			http.Error(writer, "invalid JSON", http.StatusBadRequest)

			return
		}

		if body["store"] != false || body["previous_response_id"] != nil || body["tools"] == nil {
			http.Error(writer, "expected stateless standard Responses request", http.StatusBadRequest)

			return
		}

		provider.mu.Lock()
		provider.requests = append(provider.requests, body)
		number := len(provider.requests)
		provider.mu.Unlock()
		writer.Header().Set("Content-Type", "text/event-stream")

		writeEvent := func(event any) {
			encoded, err := json.Marshal(event)
			if err == nil {
				_, _ = fmt.Fprintf(writer, "data: %s\n\n", encoded)
			}
		}

		id := fmt.Sprintf("response-%d", number)
		text := fmt.Sprintf("answer-%d", number)
		var output []map[string]any

		if mode == "created" {
			writeEvent(map[string]any{"type": "response.created", "response": map[string]any{"id": id, "status": "in_progress"}})
		}

		compacting := mode == "compaction" && body["tool_choice"] == "none"
		switch {
		case compacting:
			output = []map[string]any{providerMessage("fixture-summary")}
		case mode == "view-image" && number == 1:
			output = []map[string]any{{"type": "function_call", "call_id": "image-fixture", "name": "view_image", "arguments": `{"path":"fixture.png"}`}}
		case mode == "idless-calls" && number == 1:
			output = []map[string]any{
				providerMessage("Checking"),
				{"type": "function_call", "call_id": "idless-tool", "name": "exec_command", "arguments": `{"cmd":"printf fixture-idless-tool","max_output_tokens":100}`},
			}
		case mode == "idless-calls":
			output = []map[string]any{providerMessage("Checking complete")}
		case mode == "idless-items":
			output = []map[string]any{providerMessage("Checking"), providerMessage("Checking complete"), providerMessage("Checking")}
		case mode == "idless-stream":
			item := providerMessage("Checking complete")
			writeEvent(map[string]any{"type": "response.output_text.delta", "output_index": 0, "content_index": 0, "delta": "Checking"})
			writeEvent(map[string]any{"type": "response.output_item.done", "output_index": 0, "item": item})
			output = []map[string]any{item, providerMessage("Checking complete")}
		case mode == "shell-env" && number == 1:
			output = []map[string]any{{"type": "function_call", "call_id": "shell-env-fixture", "name": "exec_command", "arguments": mustString(t, map[string]any{"cmd": shellEnvProbe, "login": false})}}
		case shell && number == 1:
			output = []map[string]any{{"type": "function_call", "call_id": "shell-fixture", "name": "exec_command", "arguments": `{"cmd":"printf native-file-value > integration-proof.txt; printf tool-proof","max_output_tokens":100}`}}
		default:
			itemID := fmt.Sprintf("message-%d", number)
			if mode != "completion" {
				reasoningID := fmt.Sprintf("reasoning-%d", number)
				writeEvent(map[string]any{"type": "response.reasoning_summary_text.delta", "item_id": reasoningID, "output_index": 0, "summary_index": 0, "delta": "visible reasoning"})
				writeEvent(map[string]any{"type": "response.output_text.delta", "item_id": itemID, "output_index": 1, "content_index": 0, "delta": text})
				output = append(output, map[string]any{"id": reasoningID, "type": "reasoning", "summary": []map[string]any{{"type": "summary_text", "text": "visible reasoning"}}})
			}
			message := providerMessage(text)
			message["id"] = itemID
			output = append(output, message)
		}

		inputTokens, totalTokens := 12, 15
		if mode == "compaction" && number == 1 {
			inputTokens, totalTokens = 1_000_000, 1_000_003
		}
		usage := map[string]any{"input_tokens": inputTokens, "output_tokens": 3, "total_tokens": totalTokens}
		if mode == "compaction" {
			usage["cost"] = float64(number) / 8
		}
		writeEvent(map[string]any{"type": "response.completed", "response": map[string]any{
			"id": id, "status": "completed", "output": output, "usage": usage,
		}})
	}))
	t.Cleanup(provider.server.Close)

	return provider
}

// shellEnvProbe reports whether the allowed sensitive variable reached the
// tool shell without naming its value, prints it for redaction, and reports
// whether the withheld one was stripped.
const shellEnvProbe = `test "${#FIXTURE_ALLOWED_TOKEN}" = 22 && printf 'allowed-present '; printf '%s|%s' "$FIXTURE_ALLOWED_TOKEN" "${FIXTURE_DENIED_TOKEN:-denied-absent}"`

func mustString(t *testing.T, value any) string {
	t.Helper()

	encoded, err := json.Marshal(value)
	require.NoError(t, err)

	return string(encoded)
}

func providerMessage(text string) map[string]any {
	return map[string]any{"type": "message", "role": "assistant", "content": []map[string]any{{"type": "output_text", "text": text}}}
}

func (p *providerFixture) history(t *testing.T) []map[string]any {
	t.Helper()

	p.mu.Lock()
	defer p.mu.Unlock()

	return append([]map[string]any(nil), p.requests...)
}

// developerPrompt returns the first developer message text of a Responses request.
func developerPrompt(t *testing.T, request map[string]any) string {
	t.Helper()

	input, ok := request["input"].([]any)
	require.True(t, ok, "request input must be an array")

	for _, raw := range input {
		item, ok := raw.(map[string]any)
		require.True(t, ok)

		if item["type"] != "message" || item["role"] != "developer" {
			continue
		}

		content, ok := item["content"].([]any)
		require.True(t, ok)
		require.NotEmpty(t, content)
		part, ok := content[0].(map[string]any)
		require.True(t, ok)
		text, ok := part["text"].(string)
		require.True(t, ok)

		return text
	}

	t.Fatal("request has no developer prompt")

	return ""
}

// requireSelectedModel checks the ACP model selector and the selected row's
// context window; zero requires the row to report none.
func requireSelectedModel(t *testing.T, result map[string]any, model string, window int) {
	t.Helper()

	var response struct {
		ConfigOptions []acp.SessionConfigOption `json:"configOptions"`
	}
	require.NoError(t, json.Unmarshal(mustJSON(t, result), &response))
	require.NotEmpty(t, response.ConfigOptions)
	selector := response.ConfigOptions[0].Select
	require.NotNil(t, selector)
	require.Equal(t, acp.SessionConfigValueId(model), selector.CurrentValue)

	for _, row := range *selector.Options.Ungrouped {
		if row.Value == acp.SessionConfigValueId(model) {
			metadata, ok := row.Meta["nanocodex"].(map[string]any)
			require.True(t, ok)
			if window == 0 {
				require.NotContains(t, metadata, "contextWindow")
			} else {
				require.Equal(t, float64(window), metadata["contextWindow"])
			}

			return
		}
	}

	t.Fatalf("model selector omits %s", model)
}

// requireRefusedField checks that an ACP error refuses the named session option.
func requireRefusedField(t *testing.T, refused map[string]any, option string) {
	t.Helper()

	data, ok := refused["data"].(map[string]any)
	require.True(t, ok, "refusal must carry data: %v", refused)
	require.Equal(t, "_meta.nanocodex.options."+option, data["field"])
}
