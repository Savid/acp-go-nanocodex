package nanocodexacp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/savid/acp-go-nanocodex/internal/nanocodex"
)

const fakeHelperEnv = "NANOCODEX_TEST_HELPER"

func TestMain(m *testing.M) {
	if os.Getenv(fakeHelperEnv) == "1" {
		if err := runFakeNanocodex(); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

type fakeRequest struct {
	ID     uint64          `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

type fakeNative struct {
	writer       *json.Encoder
	state        nanocodex.State
	waiting      uint64
	ignoreCancel bool
}

func runFakeNanocodex() error {
	f := fakeNative{writer: json.NewEncoder(os.Stdout)}
	decoder := json.NewDecoder(os.Stdin)
	for {
		var req fakeRequest
		if err := decoder.Decode(&req); err != nil {
			if err == io.EOF {
				return nil
			}

			return err
		}
		done, err := f.handle(req)
		if err != nil || done {
			return err
		}
	}
}

func (f *fakeNative) reply(id uint64, result any) error {
	return f.writer.Encode(map[string]any{"id": id, "result": result})
}

func (f *fakeNative) failure(id uint64, code string) error {
	return f.writer.Encode(map[string]any{"id": id, "error": map[string]any{"code": code, "message": "fixture native failure"}})
}

func (f *fakeNative) event(id uint64, kind string, data any) error {
	if os.Getenv("NANOCODEX_TEST_OMIT_EVENTS") == "1" {
		return nil
	}

	return f.writer.Encode(map[string]any{"requestId": id, "event": kind, "data": data})
}

func (f *fakeNative) handle(req fakeRequest) (bool, error) {
	switch req.Method {
	case "initialize":
		return false, f.initialize(req)
	case "prompt":
		return false, f.prompt(req)
	case "cancel":
		if f.ignoreCancel {
			return false, nil
		}
		cancelled := f.waiting != 0
		if cancelled {
			if err := f.reply(f.waiting, f.result("cancelled")); err != nil {
				return false, err
			}
			f.waiting = 0
		}

		return false, f.reply(req.ID, map[string]any{"cancelled": cancelled})
	case "state":
		return false, f.reply(req.ID, f.state)
	case "shutdown":
		if model := os.Getenv("NANOCODEX_TEST_SHUTDOWN_FAILURE_MODEL"); model != "" && model == f.state.Model {
			return true, f.failure(req.ID, "persistence")
		}

		return true, f.reply(req.ID, struct{}{})
	default:
		return false, f.failure(req.ID, "method_not_found")
	}
}

func (f *fakeNative) initialize(req fakeRequest) error {
	var params nanocodex.Initialize
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return err
	}
	if params.Model == "invalid-model" || params.Thinking == "invalid-effort" {
		return f.writer.Encode(map[string]any{"id": req.ID, "error": map[string]any{"code": "invalid_config", "field": "model", "message": "fixture model rejected"}})
	}
	home := os.Getenv("CODEX_HOME")
	if home == "" {
		home = filepath.Join(os.Getenv("HOME"), ".codex")
	}
	if !filepath.IsAbs(home) {
		cwd, err := os.Getwd()
		if err != nil {
			return err
		}
		home = filepath.Join(cwd, home)
	}
	id := params.ResumeSessionID
	var replaced string
	if id != "" {
		raw, err := os.ReadFile(filepath.Join(home, "sessions", id+".jsonl"))
		if err != nil {
			return f.failure(req.ID, "native_error")
		}
		if bytes.Count(raw, []byte{'\n'}) == 1 {
			replaced = id
			id = ""
		}
	}
	if id == "" {
		id = params.SessionID
		if err := os.MkdirAll(filepath.Join(home, "sessions"), 0o700); err != nil {
			return err
		}
		f.state.RolloutPath = filepath.Join(home, "sessions", id+".jsonl")
		cwd, err := os.Getwd()
		if err != nil {
			return err
		}
		if err := f.appendRow("session_meta", map[string]any{"id": id, "cwd": cwd}); err != nil {
			return err
		}
	}
	model := params.Model
	if model == "" {
		model = "gpt-6.1-sol"
	}
	thinking := params.Thinking
	if thinking == "" {
		thinking = "medium"
	}
	f.state = nanocodex.State{ProtocolVersion: 1, HelperVersion: nanocodex.HelperVersion, HelperFingerprint: nativeHelperFingerprint, NativeSessionID: id, ReplacedNativeSessionID: replaced, RolloutPath: filepath.Join(home, "sessions", id+".jsonl"), Model: model, Thinking: thinking, TextEvents: os.Getenv("NANOCODEX_TEST_NATIVE_TEXT") != "1", Models: []nanocodex.Model{{ID: "gpt-6.1-sol", Name: "GPT 6.1 Sol", Thinking: []string{"low", "medium", "high"}, DefaultThinking: "medium", ContextWindow: 128000}}}
	if os.Getenv("NANOCODEX_TEST_SELF_REPLACEMENT") == "1" {
		f.state.ReplacedNativeSessionID = id
	}
	if fingerprint := os.Getenv("NANOCODEX_TEST_HELPER_FINGERPRINT"); fingerprint != "" {
		f.state.HelperFingerprint = fingerprint
	}
	if version := os.Getenv("NANOCODEX_TEST_HELPER_VERSION"); version != "" {
		f.state.HelperVersion = version
	}
	if err := f.refresh(); err != nil {
		return err
	}
	if path := os.Getenv("NANOCODEX_TEST_CAPTURE"); path != "" {
		file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			return err
		}
		capture := map[string]any{"initialize": params, "path": os.Getenv("PATH"), "home": home, "inherited": os.Getenv("NANOCODEX_TEST_INHERITED"), "scope": os.Getenv("NANOCODEX_TEST_SCOPE"), "internal": os.Getenv("ACP_GO_NANOCODEX_INTERNAL_SECRET")}
		err = json.NewEncoder(file).Encode(capture)
		closeErr := file.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
	}

	if err := f.reply(req.ID, f.state); err != nil {
		return err
	}

	if os.Getenv("NANOCODEX_TEST_EXIT_AFTER_RESUME") == "1" && params.ResumeSessionID != "" {
		os.Exit(17)
	}

	return nil
}

func (f *fakeNative) refresh() error {
	info, err := os.Stat(f.state.RolloutPath)
	if err != nil {
		return err
	}
	f.state.CommittedBytes = info.Size()

	return nil
}

func (f *fakeNative) appendRow(kind string, payload any) error {
	file, err := os.OpenFile(f.state.RolloutPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	err = json.NewEncoder(file).Encode(map[string]any{"timestamp": "2026-10-02T01:00:00Z", "type": kind, "payload": payload})
	closeErr := file.Close()
	if err != nil {
		return err
	}

	return closeErr
}

func (f *fakeNative) result(stop string) nanocodex.Result {
	if os.Getenv("NANOCODEX_TEST_CANCELLED_RESULT") == "1" {
		stop = "cancelled"
	}

	nativeID := f.state.NativeSessionID
	if os.Getenv("NANOCODEX_TEST_CHANGED_RESULT_ID") == "1" {
		nativeID = "99999999-9999-4999-8999-999999999999"
	}

	return nanocodex.Result{StopReason: stop, FinalMessage: "Second answer.", NativeSessionID: nativeID, RolloutPath: f.state.RolloutPath, CommittedBytes: f.state.CommittedBytes, Usage: &nanocodex.Usage{InputTokens: 12, CachedInputTokens: 2, OutputTokens: 8, ReasoningOutputTokens: 3, TotalTokens: 20}}
}

func (f *fakeNative) prompt(req fakeRequest) error {
	var params struct {
		Content []nanocodex.Content `json:"content"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return err
	}
	if err := f.appendRow("event_msg", map[string]any{"type": "input_accepted", "input": params.Content}); err != nil {
		return err
	}
	if err := f.refresh(); err != nil {
		return err
	}
	if barrier := os.Getenv("NANOCODEX_TEST_ACCEPT_BARRIER"); barrier != "" {
		if err := os.WriteFile(barrier+".ready", nil, 0o600); err != nil {
			return err
		}
		deadline := time.Now().Add(10 * time.Second)
		for {
			if _, err := os.Stat(barrier); err == nil {
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("acceptance barrier timed out")
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	if err := f.event(req.ID, "accepted", map[string]any{"turnId": "fixture-turn"}); err != nil {
		return err
	}
	command := ""
	if len(params.Content) > 0 {
		command = params.Content[0].Text
	}
	if command == "fail" {
		return f.failure(req.ID, "native_error")
	}
	if command == "rate-limit" {
		return f.writer.Encode(map[string]any{"id": req.ID, "error": map[string]any{"code": "native_error", "message": "provider rate limit exceeded", "providerCode": "rate_limit_exceeded"}})
	}
	if command == "http-rate-limit" {
		return f.writer.Encode(map[string]any{"id": req.ID, "error": map[string]any{"code": "native_error", "message": "provider rate limit exceeded", "statusCode": 429, "providerCode": "rate_limit_exceeded"}})
	}
	if command == "exit" {
		_, _ = fmt.Fprintln(os.Stderr, "fixture native crash")
		os.Exit(37)
	}
	if command == "wait" || command == "ignore-cancel" {
		if command == "ignore-cancel" {
			f.ignoreCancel = true
			signal.Ignore(syscall.SIGTERM)
		}
		f.waiting = req.ID

		return f.event(req.ID, "assistant_delta", map[string]any{"text": "waiting", "itemKey": "0:0"})
	}
	events := []struct {
		kind string
		data any
	}{
		{"reasoning_delta", map[string]any{"text": "thinking", "itemKey": "0:0", "responseId": "response-one"}},
		{"assistant_delta", map[string]any{"text": "Hello ", "itemKey": "0:1", "responseId": "response-one"}},
		{"assistant_delta", map[string]any{"text": "world.", "itemKey": "0:1", "responseId": "response-one"}},
		{"assistant_message", map[string]any{"text": "Hello world.", "itemKey": "0:1", "responseId": "response-one"}},
		{"assistant_message", map[string]any{"text": "Second answer.", "itemKey": "0:2", "responseId": "response-one"}},
	}
	for _, event := range events {
		if !f.state.TextEvents {
			continue
		}
		if err := f.event(req.ID, event.kind, event.data); err != nil {
			return err
		}
	}
	native := []struct {
		kind    string
		payload any
	}{
		{"tool.call", map[string]any{"call_id": "call-one", "tool": "exec_command", "arguments": map[string]any{"cmd": "printf fixture"}}},
		{"tool.result", map[string]any{"call_id": "call-one", "status": "completed", "result": "fixture", "structured_result": map[string]any{"exit_code": 0}}},
		{"assistant.message", map[string]any{"text": "Second answer.", "item_id": "two"}},
		{"model.call.completed", map[string]any{"response_id": "response-one", "usage": map[string]any{"input_tokens": 12, "output_tokens": 8, "total_tokens": 20, "input_tokens_details": map[string]any{"cached_tokens": 2, "cache_write_tokens": 0}}}},
		{"run.completed", map[string]any{}},
	}
	for index, event := range native {
		if err := f.event(req.ID, "native", map[string]any{"type": event.kind, "seq": index + 1, "request_id": "fixture-turn", "payload": event.payload}); err != nil {
			return err
		}
	}
	for _, message := range []string{"Hello world.", "Second answer."} {
		if err := f.appendRow("response_item", map[string]any{"type": "message", "role": "assistant", "content": []any{map[string]any{"type": "output_text", "text": message}}}); err != nil {
			return err
		}
	}
	if err := f.refresh(); err != nil {
		return err
	}

	return f.reply(req.ID, f.result("end_turn"))
}
