package nanocodex

import "encoding/json"

// HelperVersion identifies the helper release required by this adapter.
const HelperVersion = "0.2.0"

// Initialize selects native configuration and an optional existing rollout.
type Initialize struct {
	SessionID       string `json:"sessionId"`
	Model           string `json:"model,omitempty"`
	Thinking        string `json:"thinking,omitempty"`
	APIBaseURL      string `json:"apiBaseUrl,omitempty"`
	WebsocketURL    string `json:"websocketUrl,omitempty"`
	ModelIDPrefix   string `json:"modelIdPrefix,omitempty"`
	Transport       string `json:"transport,omitempty"`
	APIKeyEnv       string `json:"apiKeyEnv,omitempty"`
	AuthFile        string `json:"authFile,omitempty"`
	ResumeSessionID string `json:"resumeSessionId,omitempty"`
}

// State identifies a flushed native rollout and its model catalog.
type State struct {
	HelperFingerprint       string  `json:"helperFingerprint"`
	ReplacedNativeSessionID string  `json:"replacedNativeSessionId,omitempty"`
	TextEvents              bool    `json:"textEvents"`
	ProtocolVersion         int     `json:"protocolVersion"`
	HelperVersion           string  `json:"helperVersion"`
	NativeSessionID         string  `json:"nativeSessionId"`
	RolloutPath             string  `json:"rolloutPath"`
	CommittedBytes          int64   `json:"committedBytes"`
	Model                   string  `json:"model"`
	Thinking                string  `json:"thinking"`
	Models                  []Model `json:"models"`
}

// Model is one invokable native model and its supported thinking levels.
type Model struct {
	ContextWindow   int64    `json:"contextWindow"`
	ID              string   `json:"id"`
	Name            string   `json:"name"`
	Thinking        []string `json:"thinking"`
	DefaultThinking string   `json:"defaultThinking"`
}

// Content preserves text/image ordering at the native boundary.
type Content struct {
	Type     string `json:"type"`
	Text     string `json:"text,omitempty"`
	ImageURL string `json:"image_url,omitempty"` //nolint:tagliatelle // Native content uses image_url.
}

// Result is the authoritative, durably flushed terminal turn result.
type Result struct {
	StopReason      string `json:"stopReason"`
	FinalMessage    string `json:"finalMessage"`
	Usage           *Usage `json:"usage,omitempty"`
	NativeSessionID string `json:"nativeSessionId"`
	RolloutPath     string `json:"rolloutPath"`
	CommittedBytes  int64  `json:"committedBytes"`
}

// Usage is the consumption of one native model call or a complete turn.
type Usage struct {
	InputTokens           int64 `json:"inputTokens"`
	CachedInputTokens     int64 `json:"cachedInputTokens"`
	OutputTokens          int64 `json:"outputTokens"`
	ReasoningOutputTokens int64 `json:"reasoningOutputTokens"`
	TotalTokens           int64 `json:"totalTokens"`
}

// Event preserves an upstream event envelope for mapping and raw delivery.
type Event struct {
	Type      string          `json:"type"`
	Seq       uint64          `json:"seq"`
	RequestID string          `json:"request_id"` //nolint:tagliatelle // Upstream event field.
	Payload   json.RawMessage `json:"payload"`
}
