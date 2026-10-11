package nanocodexacp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-core/image"
	"github.com/savid/acp-go-nanocodex/internal/nanocodex"
)

type replayTextPart struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// replay streams the committed native rows to the client as history.
func (s *session) replay(ctx context.Context) error {
	s.mu.Lock()
	path := s.state.RolloutPath
	s.mu.Unlock()

	var replayErr error

	err := nanocodex.EachRow(s.home, path, func(raw []byte) error {
		replayErr = s.replayRow(ctx, raw)

		return replayErr
	})

	if replayErr != nil {
		return replayErr
	}

	if err != nil {
		return s.restoreRefused(ctx, fmt.Errorf("read native rollout: %w", err))
	}

	return nil
}

func (s *session) replayRow(ctx context.Context, raw []byte) error {
	var row nanocodex.Row
	if err := json.Unmarshal(raw, &row); err != nil {
		return s.restoreRefused(ctx, fmt.Errorf("decode native row: %w", err))
	}

	switch row.Type {
	case "event_msg":
		var payload struct {
			Type  string              `json:"type"`
			Input []nanocodex.Content `json:"input"`
		}
		if err := json.Unmarshal(row.Payload, &payload); err != nil {
			return s.restoreRefused(ctx, fmt.Errorf("decode native event: %w", err))
		}

		if payload.Type == "input_accepted" {
			for _, part := range payload.Input {
				if err := s.replayUserContent(ctx, part); err != nil {
					return err
				}
			}
		}
	case "compacted":
		var payload struct {
			History []json.RawMessage `json:"replacement_history"` //nolint:tagliatelle // Native protocol field.
		}
		if err := json.Unmarshal(row.Payload, &payload); err != nil {
			return s.restoreRefused(ctx, fmt.Errorf("decode compacted history: %w", err))
		}

		start := len(payload.History)
		for index, rawItem := range slices.Backward(payload.History) {
			var item struct {
				Type string `json:"type"`
				Role string `json:"role"`
			}
			if err := json.Unmarshal(rawItem, &item); err != nil {
				return s.restoreRefused(ctx, fmt.Errorf("decode compacted history item: %w", err))
			}

			if item.Type == "message" && item.Role == "user" {
				start = index + 1

				break
			}
		}

		for _, item := range payload.History[start:] {
			if err := s.replayResponseItem(ctx, item); err != nil {
				return err
			}
		}
	case "response_item":
		if err := s.replayResponseItem(ctx, row.Payload); err != nil {
			return err
		}
	}

	return nil
}

func (s *session) replayUserContent(ctx context.Context, content nanocodex.Content) error {
	switch content.Type {
	case "text":
		return s.emit(ctx, acp.UpdateUserMessageText(content.Text))
	case "image":
		prefix, data, ok := strings.Cut(content.ImageURL, ",")
		if !ok || !strings.HasPrefix(prefix, "data:") || !strings.HasSuffix(prefix, ";base64") {
			return s.restoreRefused(ctx, errors.New("replayed image is not a base64 data URL"))
		}

		mime := strings.TrimSuffix(strings.TrimPrefix(prefix, "data:"), ";base64")

		output, refusal := image.DecodeOutput(data, mime, s.agent.options.ImageLimits.core().EffectiveOutputPerImage())
		if refusal != nil {
			return s.restoreRefused(ctx, fmt.Errorf("replayed image is invalid: %w", refusal))
		}

		return s.emit(ctx, acp.UpdateUserMessage(acp.ImageBlock(output.Data, output.MIME)))
	default:
		return nil
	}
}

func (s *session) replayResponseItem(ctx context.Context, raw json.RawMessage) error {
	var item struct {
		Type      string           `json:"type"`
		Role      string           `json:"role"`
		CallID    string           `json:"call_id"` //nolint:tagliatelle // Native protocol fields use snake_case.
		Name      string           `json:"name"`
		Arguments string           `json:"arguments"`
		Input     string           `json:"input"`
		Status    string           `json:"status"`
		Output    json.RawMessage  `json:"output"`
		Content   []replayTextPart `json:"content"`
		Summary   []replayTextPart `json:"summary"`
	}
	if err := json.Unmarshal(raw, &item); err != nil {
		return s.restoreRefused(ctx, fmt.Errorf("decode native response item: %w", err))
	}

	switch item.Type {
	case "message":
		if item.Role != "assistant" {
			return nil
		}

		for _, part := range item.Content {
			if part.Type == "output_text" && part.Text != "" {
				if err := s.emit(ctx, acp.UpdateAgentMessageText(part.Text)); err != nil {
					return err
				}
			}
		}
	case "reasoning":
		for _, part := range item.Summary {
			if part.Type == "summary_text" && part.Text != "" {
				if err := s.emit(ctx, acp.UpdateAgentThoughtText(part.Text)); err != nil {
					return err
				}
			}
		}
	case "function_call", "custom_tool_call":
		if item.CallID == "" || item.Name == "" {
			return s.restoreRefused(ctx, errors.New("replayed tool call lacks its call ID or name"))
		}

		var input any
		if item.Type == "custom_tool_call" {
			input = item.Input
		} else {
			if err := json.Unmarshal([]byte(item.Arguments), &input); err != nil {
				input = item.Arguments
			}
		}

		return s.emit(ctx, acp.StartToolCall(acp.ToolCallId(item.CallID), item.Name, acp.WithStartKind(acp.ToolKindExecute), acp.WithStartStatus(acp.ToolCallStatusInProgress), acp.WithStartRawInput(input)))
	case "function_call_output", "custom_tool_call_output":
		return s.replayToolOutput(ctx, item.CallID, item.Status, item.Output)
	}

	return nil
}

func (s *session) replayToolOutput(ctx context.Context, callID, status string, raw json.RawMessage) error {
	if callID == "" {
		return s.restoreRefused(ctx, errors.New("replayed tool output lacks its call ID"))
	}

	content, err := nativeToolContent(raw)
	if err != nil {
		return s.restoreRefused(ctx, fmt.Errorf("decode replayed tool output: %w", err))
	}

	var toolStatus acp.ToolCallStatus

	switch status {
	case "", "completed":
		toolStatus = acp.ToolCallStatusCompleted
	case "failed", "incomplete":
		toolStatus = acp.ToolCallStatusFailed
	case "in_progress":
		toolStatus = acp.ToolCallStatusInProgress
	default:
		return s.restoreRefused(ctx, fmt.Errorf("replayed tool output has unknown status %q", status))
	}

	return s.emit(ctx, acp.UpdateToolCall(acp.ToolCallId(callID), acp.WithUpdateStatus(toolStatus), acp.WithUpdateContent(content)))
}
