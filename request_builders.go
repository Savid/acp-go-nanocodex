package nanocodexacp

import (
	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-core/wire"
)

// WithSessionNanocodexOptions merges Nanocodex-specific options into _meta.nanocodex.options.
func WithSessionNanocodexOptions(options NanocodexOptions) wire.SessionRequestOption {
	return wire.WithSessionMetaValue(options.Meta())
}

// WithSessionRawEvents toggles raw Nanocodex event emission for the session.
func WithSessionRawEvents(enabled bool) wire.SessionRequestOption {
	return wire.WithSessionMetaValue(map[string]any{
		vendor: map[string]any{metaRawEventKey: map[string]any{metaEnabledKey: enabled}},
	})
}

// SetModelRequest constructs a model selector update.
func SetModelRequest(sessionID acp.SessionId, model string) acp.SetSessionConfigOptionRequest {
	return wire.SetConfigOptionRequest(sessionID, configModel, acp.SessionConfigValueId(model))
}
