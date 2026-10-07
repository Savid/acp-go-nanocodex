package nanocodexacp

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-nanocodex/internal/nanocodex"
)

func (s *session) projectCompaction(ctx context.Context, t *turn, event nanocodex.Event) error {
	var payload struct {
		AfterModelCallIndex int  `json:"after_model_call_index"` //nolint:tagliatelle // Native compaction fields use snake_case.
		ActiveContextTokens *int `json:"active_context_tokens"`  //nolint:tagliatelle // Native compaction fields use snake_case.
		Cancelled           bool `json:"cancelled"`
	}
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return err
	}

	value := wire.Compaction{}

	switch event.Type {
	case "model.compaction.started":
		value.Status = wire.CompactionInProgress
		value.ContextBefore = payload.ActiveContextTokens
	case "model.compaction.completed":
		value.Status = wire.CompactionCompleted
	case "model.compaction.failed":
		value.Status = wire.CompactionFailed
		if payload.Cancelled {
			value.Status = wire.CompactionCancelled
		}
	default:
		return nil
	}

	nativeKey := event.RequestID + ":" + strconv.Itoa(payload.AfterModelCallIndex)

	if t.compactionKeys == nil {
		t.compactionKeys = make(map[string]string)
	}

	key := t.compactionKeys[nativeKey]
	if key == "" || value.Status == wire.CompactionInProgress {
		key = strconv.FormatUint(event.Seq, 10)
		t.compactionKeys[nativeKey] = key
	}

	return t.compactions.Publish(ctx, s.agent.connection(), s.id, key, value)
}
