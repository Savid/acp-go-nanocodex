package nanocodexacp

import (
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/savid/acp-go-nanocodex/internal/nanocodex"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-core/wire"
	"github.com/stretchr/testify/require"
)

func compactionReports(t *testing.T, notifications []acp.SessionNotification) []wire.Compaction {
	t.Helper()
	var reports []wire.Compaction
	for _, notification := range notifications {
		value, exists := notification.Meta[wire.CompactionKey]
		if !exists {
			continue
		}
		carrier, err := json.Marshal(notification.Update)
		require.NoError(t, err)
		require.JSONEq(t, `{"sessionUpdate":"session_info_update"}`, string(carrier))
		require.Len(t, notification.Meta, 1)
		encoded, err := json.Marshal(value)
		require.NoError(t, err)
		var report wire.Compaction
		require.NoError(t, json.Unmarshal(encoded, &report))
		require.NotEmpty(t, report.CompactionID)
		reports = append(reports, report)
	}

	return reports
}

func TestRecoveryCompactionBeforeAcceptanceAndAfterCancel(t *testing.T) {
	t.Parallel()
	a, client, _, _ := fixtureAgent(t)
	s := session{agent: a, id: "session"}
	active := &turn{}
	send := func(seq int, kind string, payload string) {
		t.Helper()
		data, err := json.Marshal(nativeEvent(seq, kind, json.RawMessage(payload)))
		require.NoError(t, err)
		require.NoError(t, s.promptEvent(t.Context(), active, nanocodex.Frame{Event: "native", Data: data}))
	}
	send(1, "model.compaction.started", `{"after_model_call_index":0,"active_context_tokens":120}`)
	send(1, "model.compaction.started", `{"after_model_call_index":0,"active_context_tokens":120}`)
	send(2, "model.compaction.failed", `{"after_model_call_index":0}`)
	send(3, "model.compaction.started", `{"after_model_call_index":0,"active_context_tokens":120}`)
	send(4, "model.compaction.completed", `{"after_model_call_index":0}`)
	active.cancelled.Store(true)
	send(5, "model.compaction.started", `{"after_model_call_index":1,"active_context_tokens":0}`)
	send(6, "model.compaction.failed", `{"after_model_call_index":1,"cancelled":true}`)
	send(6, "model.compaction.failed", `{"after_model_call_index":1,"cancelled":true}`)
	require.False(t, active.accepted.Load())
	notifications, _ := client.snapshot()
	reports := compactionReports(t, notifications)
	require.Len(t, reports, 6)
	for i, status := range []string{wire.CompactionFailed, wire.CompactionCompleted, wire.CompactionCancelled} {
		require.Equal(t, reports[2*i].CompactionID, reports[2*i+1].CompactionID)
		require.Equal(t, status, reports[2*i+1].Status)
		if i > 0 {
			require.NotEqual(t, reports[2*i-1].CompactionID, reports[2*i].CompactionID)
		}
		require.Nil(t, reports[2*i+1].ContextAfter)
	}
	require.Equal(t, new(120), reports[1].ContextBefore)
	require.Equal(t, new(0), reports[5].ContextBefore)
}

func TestRecoveryCompactionTransport(t *testing.T) {
	t.Parallel()
	a, _, _, workspace := fixtureAgent(t)
	created := fixtureSession(t, a, workspace)
	host, server := net.Pipe()
	require.NoError(t, host.SetDeadline(time.Now().Add(10*time.Second)))
	t.Cleanup(func() { _ = host.Close(); _ = server.Close() })
	a.attach(acp.NewAgentSideConnection(a, server, server), nil)
	require.NoError(t, json.NewEncoder(host).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "session/prompt", "params": wire.PromptRequest(created.SessionId, acp.TextBlock("COMPACT"))}))
	decoder := json.NewDecoder(host)
	var notifications []acp.SessionNotification
	for {
		var frame struct {
			ID     int               `json:"id"`
			Method string            `json:"method"`
			Params json.RawMessage   `json:"params"`
			Error  *acp.RequestError `json:"error"`
		}
		require.NoError(t, decoder.Decode(&frame))
		if frame.ID != 0 {
			require.Nil(t, frame.Error)

			break
		}
		if frame.Method == "session/update" {
			var notification acp.SessionNotification
			require.NoError(t, json.Unmarshal(frame.Params, &notification))
			notifications = append(notifications, notification)
		}
	}
	reports := compactionReports(t, notifications)
	require.Len(t, reports, 2)
	require.Equal(t, wire.CompactionInProgress, reports[0].Status)
	require.Equal(t, wire.CompactionCompleted, reports[1].Status)
	require.Equal(t, reports[0].CompactionID, reports[1].CompactionID)
	require.Equal(t, new(120), reports[1].ContextBefore)
}
