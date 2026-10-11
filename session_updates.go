package nanocodexacp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"strings"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-core/observer"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-nanocodex/internal/nanocodex"
)

// costCurrency is the currency of the gateways' usage.cost.
const costCurrency = "USD"

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
			t.recordMessage(payload.ItemKey, attemptMessage{MessageID: payload.ResponseID, Kind: thoughtKind})
		}

		return s.emit(ctx, update)
	}

	if kind != "assistant_delta" && kind != "assistant_message" {
		return fmt.Errorf("unknown helper event %q", kind)
	}

	text := payload.Text
	if kind == "assistant_message" {
		text = t.texts.Suffix(payload.ItemKey, text)
	} else {
		t.texts.Add(payload.ItemKey, text)
	}

	if text == "" {
		return nil
	}

	t.hasText = true

	update := acp.UpdateAgentMessageText(text)
	if payload.ResponseID != "" {
		update.AgentMessageChunk.MessageId = new(payload.ResponseID)
		t.recordMessage(payload.ItemKey, attemptMessage{MessageID: payload.ResponseID, Kind: messageKind})
	}

	return s.emit(ctx, update)
}

// Kinds of the ACP chunks an attemptMessage carried.
const (
	messageKind = "message"
	thoughtKind = "thought"
)

// attemptMessage is an ACP message whose chunks carried a helper item's text,
// with kind messageKind for agent message chunks or thoughtKind for thought
// chunks.
type attemptMessage struct {
	MessageID string `json:"messageId"`
	Kind      string `json:"kind"`
}

// recordMessage remembers that a chunk of the item's text carried the message.
func (t *turn) recordMessage(itemKey string, message attemptMessage) {
	if slices.Contains(t.itemMessages[itemKey], message) {
		return
	}

	if t.itemMessages == nil {
		t.itemMessages = map[string][]attemptMessage{}
	}

	t.itemMessages[itemKey] = append(t.itemMessages[itemKey], message)
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

		text := t.texts.Suffix(payload.ItemID, payload.Text)
		if text == "" {
			return nil
		}

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

	update := &acp.SessionUsageUpdate{SessionUpdate: "usage_update", Used: payload.Usage.Total, Size: size, Meta: call.Apply(nil)}

	s.mu.Lock()
	if s.cost != nil {
		update.Cost = &acp.Cost{Amount: *s.cost, Currency: costCurrency}
	}
	s.mu.Unlock()

	return s.emit(ctx, acp.SessionUpdate{UsageUpdate: update})
}

// callCost adds one provider-priced gateway call to the session's cumulative
// cost. A charge that would make the total non-finite is dropped so the
// reported total stays representable.
func (s *session) callCost(ctx context.Context, data json.RawMessage) error {
	var payload struct {
		Cost *float64 `json:"cost"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return err
	}

	if payload.Cost == nil || *payload.Cost < 0 {
		return errors.New("helper call cost requires a non-negative cost")
	}

	s.mu.Lock()

	total := *payload.Cost
	if s.cost != nil {
		total += *s.cost
	}

	overflow := math.IsInf(total, 0)
	if !overflow {
		s.cost = &total
	}
	s.mu.Unlock()

	if overflow {
		s.agent.log.WarnContext(ctx, "native call cost ignored; session total would overflow", slog.Float64("cost", *payload.Cost))
	}

	return nil
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

// discardedAttempt is a gateway attempt the helper retried, discarding the
// output it had delivered.
type discardedAttempt struct {
	Kind           string   `json:"kind"`
	ModelCallIndex int      `json:"modelCallIndex"`
	Attempt        int      `json:"attempt"`
	ItemKeys       []string `json:"itemKeys"`
	StreamedBytes  int64    `json:"streamedBytes"`
	Code           string   `json:"code,omitempty"`
	Message        string   `json:"message,omitempty"`
	StatusCode     int      `json:"statusCode,omitempty"`
	ProviderCode   string   `json:"providerCode,omitempty"`
	Usage          *struct {
		Input   int `json:"input_tokens"`  //nolint:tagliatelle // Provider usage fields use snake_case.
		Output  int `json:"output_tokens"` //nolint:tagliatelle // Provider usage fields use snake_case.
		Total   int `json:"total_tokens"`  //nolint:tagliatelle // Provider usage fields use snake_case.
		Details *struct {
			Cached *int `json:"cached_tokens"` //nolint:tagliatelle // Provider usage fields use snake_case.
		} `json:"input_tokens_details"` //nolint:tagliatelle // Provider usage fields use snake_case.
		OutputDetails *struct {
			Reasoning *int `json:"reasoning_tokens"` //nolint:tagliatelle // Provider usage fields use snake_case.
		} `json:"output_tokens_details"` //nolint:tagliatelle // Provider usage fields use snake_case.
	} `json:"usage,omitempty"`
}

// attemptDiscarded publishes a retried gateway attempt as
// _meta.nanocodex.attemptDiscarded on a session_info_update with the ACP
// messages its chunks carried, releases the text its items streamed, and adds
// its reported usage to the turn's.
func (s *session) attemptDiscarded(ctx context.Context, t *turn, data json.RawMessage) error {
	var attempt discardedAttempt
	if err := json.Unmarshal(data, &attempt); err != nil {
		return err
	}

	messages := []attemptMessage{}

	for _, key := range attempt.ItemKeys {
		t.texts.Forget(key)

		for _, message := range t.itemMessages[key] {
			if !slices.Contains(messages, message) {
				messages = append(messages, message)
			}
		}

		delete(t.itemMessages, key)
	}

	published := map[string]any{
		"kind": attempt.Kind, "modelCallIndex": attempt.ModelCallIndex, "attempt": attempt.Attempt,
		"itemKeys": attempt.ItemKeys, "messages": messages, "streamedBytes": attempt.StreamedBytes,
	}

	for name, value := range map[string]string{"code": attempt.Code, wire.FieldMessage: attempt.Message, "providerCode": attempt.ProviderCode} {
		if value != "" {
			published[name] = value
		}
	}

	if attempt.StatusCode > 0 {
		published["statusCode"] = attempt.StatusCode
	}

	if usage := attempt.Usage; usage != nil && usage.Total > 0 {
		discarded := acp.Usage{InputTokens: usage.Input, OutputTokens: usage.Output, TotalTokens: usage.Total}
		if usage.Details != nil {
			discarded.CachedReadTokens = usage.Details.Cached
		}

		if usage.OutputDetails != nil {
			discarded.ThoughtTokens = usage.OutputDetails.Reasoning
		}

		published["usage"] = discarded
		t.discardedUsage = addUsage(t.discardedUsage, &discarded)
	}

	conn := s.agent.connection()
	if conn == nil {
		return nil
	}

	return conn.SessionUpdate(ctx, acp.SessionNotification{
		SessionId: s.id,
		Update:    acp.SessionUpdate{SessionInfoUpdate: &acp.SessionSessionInfoUpdate{}},
		Meta:      map[string]any{vendor: map[string]any{"attemptDiscarded": published}},
	})
}

// addUsage sums two turn usages; a nil usage adds nothing.
func addUsage(total, usage *acp.Usage) *acp.Usage {
	if usage == nil {
		return total
	}

	if total == nil {
		return &acp.Usage{InputTokens: usage.InputTokens, OutputTokens: usage.OutputTokens, TotalTokens: usage.TotalTokens, CachedReadTokens: addCount(nil, usage.CachedReadTokens), CachedWriteTokens: addCount(nil, usage.CachedWriteTokens), ThoughtTokens: addCount(nil, usage.ThoughtTokens)}
	}

	return &acp.Usage{
		InputTokens: total.InputTokens + usage.InputTokens, OutputTokens: total.OutputTokens + usage.OutputTokens, TotalTokens: total.TotalTokens + usage.TotalTokens,
		CachedReadTokens: addCount(total.CachedReadTokens, usage.CachedReadTokens), ThoughtTokens: addCount(total.ThoughtTokens, usage.ThoughtTokens),
		CachedWriteTokens: addCount(total.CachedWriteTokens, usage.CachedWriteTokens),
	}
}

func addCount(total, count *int) *int {
	switch {
	case count == nil:
		return total
	case total == nil:
		return new(*count)
	default:
		return new(*total + *count)
	}
}
