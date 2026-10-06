package nanocodexacp

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/sessionlog"
	"github.com/savid/acp-go-core/wire"
	"github.com/stretchr/testify/require"
)

type recordingClient struct {
	mu            sync.Mutex
	notifications []acp.SessionNotification
	raw           []json.RawMessage
	extensions    []json.RawMessage
	wake          chan struct{}
}

func (c *recordingClient) SessionUpdate(_ context.Context, n acp.SessionNotification) error {
	raw, err := json.Marshal(n)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.notifications = append(c.notifications, n)
	c.raw = append(c.raw, raw)
	c.mu.Unlock()
	select {
	case c.wake <- struct{}{}:
	default:
	}

	return nil
}

func (c *recordingClient) NotifyExtension(_ context.Context, method string, value any) error {
	raw, err := json.Marshal(map[string]any{"method": method, "params": value})
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.extensions = append(c.extensions, raw)
	c.mu.Unlock()

	return nil
}

func (c *recordingClient) snapshot() ([]acp.SessionNotification, []json.RawMessage) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]acp.SessionNotification(nil), c.notifications...), append([]json.RawMessage(nil), c.raw...)
}

func (c *recordingClient) text() string {
	rows, _ := c.snapshot()
	var text strings.Builder
	for _, row := range rows {
		if chunk := row.Update.AgentMessageChunk; chunk != nil && chunk.Content.Text != nil {
			text.WriteString(chunk.Content.Text.Text)
		}
	}

	return text.String()
}

func (c *recordingClient) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.notifications = nil
	c.raw = nil
	c.extensions = nil
}

func usageCosts(c *recordingClient) []*acp.Cost {
	rows, _ := c.snapshot()
	var costs []*acp.Cost
	for _, row := range rows {
		if usage := row.Update.UsageUpdate; usage != nil {
			costs = append(costs, usage.Cost)
		}
	}

	return costs
}

func usd(amount float64) *acp.Cost { return &acp.Cost{Amount: amount, Currency: costCurrency} }

func storedCost(t *testing.T, store acpcore.SessionStore, id acp.SessionId) *float64 {
	t.Helper()
	var record sessionRecord
	_, found, err := sessionlog.Load(t.Context(), store, string(id), &record)
	require.NoError(t, err)
	require.True(t, found)

	return record.CostUSD
}

// logBuffer collects text log output from concurrent goroutines.
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p)
}

func (b *logBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}

func captureLogs() (*slog.Logger, *logBuffer) {
	logs := &logBuffer{}

	return slog.New(slog.NewTextHandler(logs, nil)), logs
}

func waitForText(t *testing.T, c *recordingClient, text string) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for !strings.Contains(c.text(), text) {
		select {
		case <-c.wake:
		case <-timer.C:
			t.Fatal("helper did not deliver expected text")
		}
	}
}

func fixtureAgent(t *testing.T, opts ...Option) (*Agent, *recordingClient, string, string) {
	t.Helper()
	exe, err := os.Executable()
	require.NoError(t, err)
	home, workspace := t.TempDir(), t.TempDir()
	base := make([]Option, 0, 3+len(opts))
	base = append(base, WithExecutablePath(exe), WithHome(home), WithEnv(map[string]string{fakeHelperEnv: "1", "GORACE": "atexit_sleep_ms=0"}))
	agent := NewAgent(append(base, opts...)...)
	client := &recordingClient{wake: make(chan struct{}, 32)}
	agent.attach(client, nil)
	t.Cleanup(func() { require.NoError(t, agent.Close()) })
	_, err = agent.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err)

	return agent, client, home, workspace
}

func fixtureSession(t *testing.T, a *Agent, workspace string, opts ...wire.SessionRequestOption) acp.NewSessionResponse {
	t.Helper()
	response, err := a.NewSession(t.Context(), wire.NewSessionRequest(workspace, opts...))
	require.NoError(t, err)

	return response
}

func fixturePrompt(t *testing.T, a *Agent, id acp.SessionId, text string) acp.PromptResponse {
	t.Helper()
	response, err := a.Prompt(t.Context(), wire.PromptRequest(id, acp.TextBlock(text)))
	require.NoError(t, err)

	return response
}

func nativeID(t *testing.T, meta map[string]any) string {
	t.Helper()
	value, ok := meta["nanocodex"].(map[string]any)
	require.True(t, ok)
	id, ok := value["nativeSessionId"].(string)
	require.True(t, ok)

	return id
}

func rolloutPath(home, id string) string { return filepath.Join(home, "sessions", id+".jsonl") }

type countingStore struct {
	acpcore.SessionStore
	mu        sync.Mutex
	writes    int
	deletes   int
	deleteErr error
}

func (s *countingStore) Replace(ctx context.Context, key acpcore.SessionKey, rows []acpcore.SessionStoreReplacement) error {
	s.mu.Lock()
	s.writes++
	s.mu.Unlock()

	return s.SessionStore.Replace(ctx, key, rows)
}

func (s *countingStore) Delete(ctx context.Context, key acpcore.SessionKey) error {
	s.mu.Lock()
	s.deletes++
	err := s.deleteErr
	s.mu.Unlock()
	if err != nil {
		return err
	}

	return s.SessionStore.Delete(ctx, key)
}
