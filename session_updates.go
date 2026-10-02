package nanocodexacp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-core/observer"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-nanocodex/internal/nanocodex"
)

func observerPromptResult(response acp.PromptResponse, err error, model string) observer.PromptResult {
	return observer.PromptResultFrom(response, err, model, "openai")
}

// omitRawImages keeps raw telemetry from copying admitted inline image bytes.
func omitRawImages(value any) {
	switch value := value.(type) {
	case map[string]any:
		for key, item := range value {
			if text, ok := item.(string); ok && (strings.HasPrefix(text, "data:image/") || key == "image_url" && strings.HasPrefix(text, "data:")) {
				value[key] = "[inline image omitted]"

				continue
			}

			omitRawImages(item)
		}
	case []any:
		for _, item := range value {
			omitRawImages(item)
		}
	}
}

func promptUsage(usage nanocodex.Usage) *acp.Usage {
	if usage.TotalTokens <= 0 {
		return nil
	}

	return &acp.Usage{InputTokens: int(usage.InputTokens), OutputTokens: int(usage.OutputTokens), TotalTokens: int(usage.TotalTokens), CachedReadTokens: new(int(usage.CachedInputTokens)), ThoughtTokens: new(int(usage.ReasoningOutputTokens))}
}

func (s *session) textUpdate(ctx context.Context, t *turn, kind string, data json.RawMessage) error {
	var payload struct {
		Text       string `json:"text"`
		ItemKey    string `json:"itemKey"`
		ResponseID string `json:"responseId"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return err
	}

	if payload.ItemKey == "" {
		return fmt.Errorf("helper text event missing item key")
	}

	if payload.Text == "" {
		return nil
	}

	if kind == "reasoning_delta" {
		update := acp.UpdateAgentThoughtText(payload.Text)
		if payload.ResponseID != "" {
			update.AgentThoughtChunk.MessageId = new(payload.ResponseID)
		}

		return s.emit(ctx, update)
	}

	if kind != "assistant_delta" && kind != "assistant_message" {
		return fmt.Errorf("unknown helper event %q", kind)
	}

	key := payload.ItemKey

	text := payload.Text
	if kind == "assistant_message" {
		text = wire.UnstreamedSuffix(t.texts[key], text)
	}

	if text == "" {
		return nil
	}

	t.texts[key] += text
	t.hasText = true

	update := acp.UpdateAgentMessageText(text)
	if payload.ResponseID != "" {
		update.AgentMessageChunk.MessageId = new(payload.ResponseID)
	}

	return s.emit(ctx, update)
}

func (s *session) nativeUpdate(ctx context.Context, t *turn, event nanocodex.Event) error {
	switch event.Type {
	case "assistant.message":
		if s.state.TextEvents {
			return nil
		}

		var payload struct {
			Text   string `json:"text"`
			ItemID string `json:"item_id"` //nolint:tagliatelle // Native protocol fields use snake_case.
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return err
		}

		text := wire.UnstreamedSuffix(t.texts[payload.ItemID], payload.Text)
		if text == "" {
			return nil
		}

		t.texts[payload.ItemID] += text
		t.hasText = true

		return s.emit(ctx, acp.UpdateAgentMessageText(text))
	case "tool.call":
		var payload struct {
			CallID    string `json:"call_id"` //nolint:tagliatelle // Native protocol fields use snake_case.
			Tool      string `json:"tool"`
			Arguments any    `json:"arguments"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return err
		}

		if payload.CallID == "" {
			return fmt.Errorf("native tool call missing id")
		}

		return s.emit(ctx, acp.StartToolCall(acp.ToolCallId(payload.CallID), payload.Tool, acp.WithStartKind(acp.ToolKindExecute), acp.WithStartStatus(acp.ToolCallStatusInProgress), acp.WithStartRawInput(payload.Arguments)))
	case "tool.result":
		var payload struct {
			CallID           string          `json:"call_id"` //nolint:tagliatelle // Native protocol fields use snake_case.
			Result           json.RawMessage `json:"result"`
			Status           string          `json:"status"`
			StructuredResult any             `json:"structured_result"` //nolint:tagliatelle // Native protocol fields use snake_case.
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return err
		}

		status := acp.ToolCallStatusCompleted
		if payload.Status != "completed" {
			status = acp.ToolCallStatusFailed
		}

		content, err := nativeToolContent(payload.Result)
		if err != nil {
			return err
		}

		omitRawImages(payload.StructuredResult)

		return s.emit(ctx, acp.UpdateToolCall(acp.ToolCallId(payload.CallID), acp.WithUpdateStatus(status), acp.WithUpdateRawOutput(payload.StructuredResult), acp.WithUpdateContent(content)))
	case "model.call.completed":
		return s.callUsage(ctx, event.Payload)
	default:
		return nil
	}
}

func (s *session) callUsage(ctx context.Context, data json.RawMessage) error {
	var payload struct {
		ResponseID string `json:"response_id"` //nolint:tagliatelle // Native protocol fields use snake_case.
		Usage      *struct {
			Input   int `json:"input_tokens"`  //nolint:tagliatelle // Native protocol fields use snake_case.
			Output  int `json:"output_tokens"` //nolint:tagliatelle // Native protocol fields use snake_case.
			Total   int `json:"total_tokens"`  //nolint:tagliatelle // Native protocol fields use snake_case.
			Details *struct {
				Cached  *int `json:"cached_tokens"`      //nolint:tagliatelle // Native protocol fields use snake_case.
				Written *int `json:"cache_write_tokens"` //nolint:tagliatelle // Native protocol fields use snake_case.
			} `json:"input_tokens_details"` //nolint:tagliatelle // Native protocol fields use snake_case.
		} `json:"usage"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return err
	}

	if payload.Usage == nil || payload.Usage.Total <= 0 {
		return nil
	}

	size := 0

	for _, model := range s.state.Models {
		if model.ID == s.options.Model {
			size = int(model.ContextWindow)
		}
	}

	call := wire.CallUsage{ResponseID: payload.ResponseID, OutputTokens: new(payload.Usage.Output)}

	if details := payload.Usage.Details; details != nil {
		call.CachedReadTokens = details.Cached
		call.CachedWriteTokens = details.Written

		if details.Cached != nil && details.Written != nil {
			uncached := payload.Usage.Input - *details.Cached - *details.Written
			if uncached >= 0 {
				call.InputTokens = new(uncached)
			}
		}
	}

	return s.emit(ctx, acp.SessionUpdate{UsageUpdate: &acp.SessionUsageUpdate{SessionUpdate: "usage_update", Used: payload.Usage.Total, Size: size, Meta: call.Apply(nil)}})
}

func nativeToolContent(raw json.RawMessage) ([]acp.ToolCallContent, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return nil, errors.New("native tool output must be text or content parts")
	}

	if raw[0] == '"' {
		var text string
		if err := json.Unmarshal(raw, &text); err != nil {
			return nil, errors.New("native tool output must be text or content parts")
		}

		return []acp.ToolCallContent{acp.ToolContent(acp.TextBlock(text))}, nil
	}

	if raw[0] != '[' {
		return nil, errors.New("native tool output must be text or content parts")
	}

	var parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &parts); err != nil {
		return nil, errors.New("native tool output must be text or content parts")
	}

	content := make([]acp.ToolCallContent, 0, len(parts))
	for _, part := range parts {
		if part.Type == "input_text" {
			content = append(content, acp.ToolContent(acp.TextBlock(part.Text)))
		}
	}

	return content, nil
}
