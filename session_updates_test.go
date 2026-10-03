package nanocodexacp

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-nanocodex/internal/nanocodex"
	"github.com/stretchr/testify/require"
)

func TestGatewayResponseIDsAttributeTextAndThoughtChunks(t *testing.T) {
	t.Parallel()
	a, client, _, workspace := fixtureAgent(t)
	created := fixtureSession(t, a, workspace)
	fixturePrompt(t, a, created.SessionId, "attribute these chunks")
	notifications, _ := client.snapshot()
	var texts, thoughts int
	for _, notification := range notifications {
		if chunk := notification.Update.AgentMessageChunk; chunk != nil {
			texts++
			require.NotNil(t, chunk.MessageId)
			require.Equal(t, "response-one", *chunk.MessageId)
		}
		if chunk := notification.Update.AgentThoughtChunk; chunk != nil {
			thoughts++
			require.NotNil(t, chunk.MessageId)
			require.Equal(t, "response-one", *chunk.MessageId)
		}
	}
	require.Equal(t, 3, texts)
	require.Equal(t, 1, thoughts)
	require.Equal(t, "Hello world.Second answer.", client.text())
}

func TestNativeTextDoesNotInheritAPreviousProviderResponseID(t *testing.T) {
	t.Parallel()
	a, client, _, workspace := fixtureAgent(t)
	options := NewNanocodexOptions(WithNanocodexEnv(map[string]string{"NANOCODEX_TEST_NATIVE_TEXT": "1"}))
	created := fixtureSession(t, a, workspace, WithSessionNanocodexOptions(options))
	fixturePrompt(t, a, created.SessionId, "first call")
	fixturePrompt(t, a, created.SessionId, "second call")
	notifications, _ := client.snapshot()
	var texts, usages int
	for _, notification := range notifications {
		if chunk := notification.Update.AgentMessageChunk; chunk != nil {
			texts++
			require.Nil(t, chunk.MessageId)
		}
		if usage := notification.Update.UsageUpdate; usage != nil {
			usages++
			call, ok := usage.Meta[wire.CallUsageKey].(wire.CallUsage)
			require.True(t, ok)
			require.Equal(t, "response-one", call.ResponseID)
		}
	}
	require.Equal(t, 2, texts)
	require.Equal(t, 2, usages)
}

func TestGatewayTextRejectsMissingPresentationIdentity(t *testing.T) {
	t.Parallel()
	a, client, _, _ := fixtureAgent(t)
	s := session{agent: a}
	active := turn{texts: make(map[string]string)}
	err := s.textUpdate(t.Context(), &active, "assistant_message", json.RawMessage(`{"text":"unidentified output","responseId":"actual-provider-response"}`))
	require.ErrorContains(t, err, "missing item key")
	notifications, _ := client.snapshot()
	require.Empty(t, notifications)
}

func TestCallUsageRequiresReportedCacheAccountingForUncachedInput(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		details json.RawMessage
		input   *int
		read    *int
		write   *int
	}{
		{name: "missing"},
		{name: "null", details: json.RawMessage(`null`)},
		{name: "unknown writes", details: json.RawMessage(`{"cached_tokens":4}`), read: new(4)},
		{name: "reported zero", details: json.RawMessage(`{"cached_tokens":0,"cache_write_tokens":0}`), input: new(12), read: new(0), write: new(0)},
		{name: "cache split", details: json.RawMessage(`{"cached_tokens":4,"cache_write_tokens":3}`), input: new(5), read: new(4), write: new(3)},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			a, client, _, _ := fixtureAgent(t)
			s := session{agent: a, options: NanocodexOptions{Model: "gpt-6.1-sol"}, state: nanocodex.State{Models: []nanocodex.Model{{ID: "gpt-6.1-sol", ContextWindow: 128000}}}}
			usage := map[string]any{"input_tokens": 12, "output_tokens": 3, "total_tokens": 15}
			if test.details != nil {
				usage["input_tokens_details"] = test.details
			}
			payload, err := json.Marshal(map[string]any{"response_id": "response-usage", "usage": usage})
			require.NoError(t, err)
			require.NoError(t, s.callUsage(t.Context(), payload))
			notifications, _ := client.snapshot()
			require.Len(t, notifications, 1)
			got := notifications[0].Update.UsageUpdate
			require.Equal(t, 15, got.Used)
			require.Equal(t, 128000, got.Size)
			call, ok := got.Meta[wire.CallUsageKey].(wire.CallUsage)
			require.True(t, ok)
			require.Equal(t, wire.CallUsage{ResponseID: "response-usage", InputTokens: test.input, CachedReadTokens: test.read, CachedWriteTokens: test.write, OutputTokens: new(3)}, call)
		})
	}
}

func TestCallUsagePublishesWithUnknownContextWindow(t *testing.T) {
	t.Parallel()
	a, client, _, _ := fixtureAgent(t)
	s := session{agent: a}
	require.NoError(t, s.callUsage(t.Context(), json.RawMessage(`{"usage":{"input_tokens":2,"output_tokens":1,"total_tokens":3}}`)))
	updates, _ := client.snapshot()
	require.Len(t, updates, 1)
	require.Equal(t, 3, updates[0].Update.UsageUpdate.Used)
	require.Zero(t, updates[0].Update.UsageUpdate.Size)
}

func TestPromptRawEventsOmitTypedImageBytes(t *testing.T) {
	t.Parallel()
	a, client, _, workspace := fixtureAgent(t)
	created := fixtureSession(t, a, workspace, WithSessionRawEvents(true))
	s, err := a.lookup(created.SessionId)
	require.NoError(t, err)
	turn := &turn{}
	turn.accepted.Store(true)
	frame := nanocodex.Frame{Event: "native", Data: json.RawMessage(`{"type":"input.accepted","seq":1,"payload":{"input":[{"type":"image","image_url":"data:image/png;base64,secret-image-bytes"}]}}`)}
	require.NoError(t, s.promptEvent(t.Context(), turn, frame))
	client.mu.Lock()
	defer client.mu.Unlock()
	require.Len(t, client.extensions, 1)
	require.NotContains(t, string(client.extensions[0]), "secret-image-bytes")
	require.Contains(t, string(client.extensions[0]), "[inline image omitted]")
}

func TestNativeViewImageCompletesWithoutPublishingBinaryPayloads(t *testing.T) {
	t.Parallel()
	a, client, _, workspace := fixtureAgent(t)
	created := fixtureSession(t, a, workspace, WithSessionRawEvents(true))
	s, err := a.lookup(created.SessionId)
	require.NoError(t, err)
	turn := &turn{}
	turn.accepted.Store(true)
	frame := nanocodex.Frame{Event: "native", Data: json.RawMessage(`{"type":"tool.result","seq":6,"payload":{"call_id":"call_image","tool":"view_image","status":"completed","result":[{"type":"input_text","text":"image viewed"},{"type":"input_image","image_url":"data:application/octet-stream;base64,secret-image-bytes"}],"structured_result":{"image_url":"data:application/octet-stream;base64,secret-image-bytes","detail":"original"}}}`)}
	require.NoError(t, s.promptEvent(t.Context(), turn, frame))
	updates, raw := client.snapshot()
	require.Len(t, updates, 1)
	update := updates[0].Update.ToolCallUpdate
	require.Equal(t, acp.ToolCallStatusCompleted, *update.Status)
	require.Equal(t, []acp.ToolCallContent{acp.ToolContent(acp.TextBlock("image viewed"))}, update.Content)
	require.NotContains(t, string(raw[0]), "secret-image-bytes")
	client.mu.Lock()
	defer client.mu.Unlock()
	require.Len(t, client.extensions, 1)
	require.NotContains(t, string(client.extensions[0]), "secret-image-bytes")
}

func TestOversizedPromptIsRejectedBeforeAdmissionAndSessionRemainsUsable(t *testing.T) {
	t.Parallel()
	a, client, _, workspace := fixtureAgent(t)
	created := fixtureSession(t, a, workspace)
	client.reset()
	_, err := a.Prompt(t.Context(), wire.TextPromptRequest(created.SessionId, strings.Repeat("x", nanocodex.MaxPromptBytes)))
	require.Error(t, err)
	notices, _ := client.snapshot()
	require.Empty(t, notices)
	fixturePrompt(t, a, created.SessionId, "small valid prompt")
	require.NotEmpty(t, client.text())
}
