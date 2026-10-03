package nanocodexacp

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-core/wire"
	"github.com/stretchr/testify/require"
)

func TestReplayCurrentNativeToolsAndVisibleReasoning(t *testing.T) {
	t.Parallel()
	a, client, home, workspace := fixtureAgent(t)
	session := fixtureSession(t, a, workspace)
	completed := fixturePrompt(t, a, session.SessionId, "remember tool results")
	_, err := a.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
	appendReplayItems(t, rolloutPath(home, nativeID(t, completed.Meta)), []any{
		map[string]any{"type": "reasoning", "summary": []any{
			map[string]any{"type": "summary_text", "text": "Checked the workspace."},
			map[string]any{"type": "summary_text", "text": "Prepared the command."},
		}, "content": []any{map[string]any{"type": "reasoning_text", "text": "unprojected-reasoning-content"}}, "encrypted_content": "opaque-reasoning-payload"},
		map[string]any{"type": "custom_tool_call", "call_id": "custom-one", "name": "exec", "input": "return tools.exec_command({cmd: 'false'});", "status": "completed"},
		map[string]any{"type": "custom_tool_call_output", "call_id": "custom-one", "output": "command failed", "status": "failed"},
		map[string]any{"type": "function_call", "call_id": "function-one", "name": "view_image", "arguments": `{"path":"fixture.png"}`, "status": "completed"},
		map[string]any{"type": "function_call_output", "call_id": "function-one", "status": "completed", "output": []any{
			map[string]any{"type": "input_text", "text": "First visible part."},
			map[string]any{"type": "input_image", "image_url": "data:image/png;base64,opaque-image-payload"},
			map[string]any{"type": "input_audio", "audio_url": "data:audio/wav;base64,opaque-audio-payload"},
			map[string]any{"type": "encrypted_content", "encrypted_content": "opaque-tool-payload"},
			map[string]any{"type": "input_text", "text": "Second visible part."},
		}},
		map[string]any{"type": "custom_tool_call", "call_id": "custom-two", "name": "exec", "input": "return tools.view_image({path: 'fixture.png'});"},
		map[string]any{"type": "custom_tool_call_output", "call_id": "custom-two", "output": []any{
			map[string]any{"type": "input_image", "image_url": "data:image/png;base64,opaque-image-only"},
		}},
	})
	client.reset()
	_, err = a.LoadSession(t.Context(), acp.LoadSessionRequest{SessionId: session.SessionId, Cwd: workspace})
	require.NoError(t, err)
	notifications, frames := client.snapshot()
	var thoughts []string
	var starts []*acp.SessionUpdateToolCall
	var results []*acp.SessionToolCallUpdate
	for _, notification := range notifications {
		update := notification.Update
		if update.AgentThoughtChunk != nil {
			thoughts = append(thoughts, update.AgentThoughtChunk.Content.Text.Text)
		}
		if update.ToolCall != nil {
			starts = append(starts, update.ToolCall)
		}
		if update.ToolCallUpdate != nil {
			results = append(results, update.ToolCallUpdate)
		}
	}
	require.Equal(t, []string{"Checked the workspace.", "Prepared the command."}, thoughts)
	require.Len(t, starts, 3)
	require.Equal(t, "exec", starts[0].Title)
	require.Equal(t, "return tools.exec_command({cmd: 'false'});", starts[0].RawInput)
	require.Equal(t, map[string]any{"path": "fixture.png"}, starts[1].RawInput)
	require.Len(t, results, 3)
	require.Equal(t, acp.ToolCallId("custom-one"), results[0].ToolCallId)
	require.Equal(t, acp.ToolCallStatusFailed, *results[0].Status)
	require.Equal(t, []acp.ToolCallContent{acp.ToolContent(acp.TextBlock("command failed"))}, results[0].Content)
	require.Equal(t, acp.ToolCallStatusCompleted, *results[1].Status)
	require.Equal(t, []acp.ToolCallContent{acp.ToolContent(acp.TextBlock("First visible part.")), acp.ToolContent(acp.TextBlock("Second visible part."))}, results[1].Content)
	require.Equal(t, acp.ToolCallStatusCompleted, *results[2].Status)
	require.Empty(t, results[2].Content)
	for _, frame := range frames {
		require.NotContains(t, string(frame), "opaque-")
		require.NotContains(t, string(frame), "unprojected-reasoning-content")
	}
}

func TestReplayMalformedRecordsUseRestoreFailure(t *testing.T) {
	t.Parallel()
	for _, row := range []string{
		`{"type":"event_msg","payload":`,
		`{"type":"event_msg","payload":{"type":"input_accepted","input":"invalid"}}`,
		`{"type":"response_item","payload":{"type":"message","content":"invalid"}}`,
	} {
		a, _, _, _ := fixtureAgent(t)
		s := session{agent: a, rows: [][]byte{[]byte(row)}}
		require.Equal(t, wire.RestoreFailed(vendor), s.replay(t.Context()))
	}
}

func TestReplayMalformedToolOutputRefusesRestoreWithoutRawFallback(t *testing.T) {
	t.Parallel()
	for _, output := range []any{nil, map[string]any{"unexpected": "opaque-payload"}} {
		name := "object"
		if output == nil {
			name = "null"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			a, client, home, workspace := fixtureAgent(t)
			session := fixtureSession(t, a, workspace)
			completed := fixturePrompt(t, a, session.SessionId, "saved history")
			_, err := a.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.SessionId})
			require.NoError(t, err)
			appendReplayItems(t, rolloutPath(home, nativeID(t, completed.Meta)), []any{
				map[string]any{"type": "function_call_output", "call_id": "bad-output", "output": output},
			})
			client.reset()
			_, err = a.LoadSession(t.Context(), acp.LoadSessionRequest{SessionId: session.SessionId, Cwd: workspace})
			require.Equal(t, wire.RestoreFailed("nanocodex"), err)
			_, frames := client.snapshot()
			for _, frame := range frames {
				require.NotContains(t, string(frame), "opaque-payload")
			}
		})
	}
}

func appendReplayItems(t *testing.T, path string, items []any) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	require.NoError(t, err)
	defer file.Close()
	encoder := json.NewEncoder(file)
	for _, item := range items {
		require.NoError(t, encoder.Encode(map[string]any{"timestamp": "2026-10-02T03:00:00Z", "type": "response_item", "payload": item}))
	}
	require.NoError(t, file.Sync())
}

func TestReplayCompactedTurnProjectsOnlyNewVisibleItems(t *testing.T) {
	t.Parallel()
	a, client, _, _ := fixtureAgent(t)
	data, err := os.ReadFile("testdata/compacted-replay.jsonl")
	require.NoError(t, err)
	s := session{agent: a, rows: bytes.Split(bytes.TrimSpace(data), []byte{'\n'})}
	require.NoError(t, s.replay(t.Context()))
	notifications, frames := client.snapshot()
	var users, thoughts []string
	var tools, results int
	for _, notification := range notifications {
		update := notification.Update
		if update.UserMessageChunk != nil {
			users = append(users, update.UserMessageChunk.Content.Text.Text)
		}
		if update.AgentThoughtChunk != nil {
			thoughts = append(thoughts, update.AgentThoughtChunk.Content.Text.Text)
		}
		if update.ToolCall != nil {
			tools++
		}
		if update.ToolCallUpdate != nil {
			results++
		}
	}
	require.Equal(t, []string{"first user", "second user"}, users)
	require.Equal(t, "first answersecond answer", client.text())
	require.Equal(t, []string{"visible thought"}, thoughts)
	require.Equal(t, 1, tools)
	require.Equal(t, 1, results)
	for _, frame := range frames {
		require.NotContains(t, string(frame), "private instructions")
		require.NotContains(t, string(frame), "opaque-summary")
	}
}
