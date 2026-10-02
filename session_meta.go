package nanocodexacp

import (
	"maps"
	"net/netip"
	"net/url"
	"slices"
	"strings"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-nanocodex/internal/nanocodex"
)

const (
	metaThinking                         = "thinking"
	metaAPIBaseURL                       = "apiBaseUrl"
	metaWebsocketURL                     = "websocketUrl"
	metaAPIKeyEnv                        = "apiKeyEnv"
	metaOptionsKey                       = "options"
	metaRawEventKey                      = "rawEvent"
	metaEnabledKey                       = "enabled"
	configModel      acp.SessionConfigId = "model"
	configThinking   acp.SessionConfigId = "thought_level"
)

// NanocodexOptions selects one native agent's model, route, and environment.
type NanocodexOptions struct {
	Model         string            `json:"model,omitempty"`
	Env           map[string]string `json:"env,omitempty"`
	ExtraPathDirs []string          `json:"extraPathDirs,omitempty"`
	Thinking      string            `json:"thinking,omitempty"`
	APIBaseURL    string            `json:"apiBaseUrl,omitempty"`
	WebsocketURL  string            `json:"websocketUrl,omitempty"`
	ModelIDPrefix string            `json:"modelIdPrefix,omitempty"`
	Transport     string            `json:"transport,omitempty"`
	APIKeyEnv     string            `json:"apiKeyEnv,omitempty"`
	AuthFile      string            `json:"authFile,omitempty"`
}

// NanocodexOption configures per-session native options.
type NanocodexOption func(*NanocodexOptions)

// NewNanocodexOptions constructs independently owned session options.
func NewNanocodexOptions(opts ...NanocodexOption) NanocodexOptions {
	var options NanocodexOptions
	for _, opt := range opts {
		opt(&options)
	}

	return options.clone()
}

// WithNanocodexModel selects a native model ID.
func WithNanocodexModel(model string) NanocodexOption {
	return func(o *NanocodexOptions) { o.Model = model }
}

// WithNanocodexEnv sets the session environment overlay.
func WithNanocodexEnv(env map[string]string) NanocodexOption {
	copied := maps.Clone(env)

	return func(o *NanocodexOptions) { o.Env = maps.Clone(copied) }
}

// WithNanocodexExtraPathDirs prepends the ordered directories to the session PATH.
func WithNanocodexExtraPathDirs(dirs ...string) NanocodexOption {
	copied := slices.Clone(dirs)

	return func(o *NanocodexOptions) { o.ExtraPathDirs = slices.Clone(copied) }
}

// WithNanocodexThinking selects a native reasoning effort.
func WithNanocodexThinking(thinking string) NanocodexOption {
	return func(o *NanocodexOptions) { o.Thinking = thinking }
}

// WithNanocodexAPIBaseURL selects the provider's API base URL.
func WithNanocodexAPIBaseURL(baseURL string) NanocodexOption {
	return func(o *NanocodexOptions) { o.APIBaseURL = baseURL }
}

// WithNanocodexWebsocketURL overrides the Responses WebSocket endpoint.
func WithNanocodexWebsocketURL(endpoint string) NanocodexOption {
	return func(o *NanocodexOptions) { o.WebsocketURL = endpoint }
}

// WithNanocodexModelIDPrefix selects a gateway namespace for native model IDs.
func WithNanocodexModelIDPrefix(prefix string) NanocodexOption {
	return func(o *NanocodexOptions) { o.ModelIDPrefix = prefix }
}

// WithNanocodexTransport selects https or websocket transport.
func WithNanocodexTransport(transport string) NanocodexOption {
	return func(o *NanocodexOptions) { o.Transport = transport }
}

// WithNanocodexAPIKeyEnv selects the inherited environment variable containing a key.
func WithNanocodexAPIKeyEnv(name string) NanocodexOption {
	return func(o *NanocodexOptions) { o.APIKeyEnv = name }
}

// WithNanocodexAuthFile selects a native authentication file.
func WithNanocodexAuthFile(path string) NanocodexOption {
	return func(o *NanocodexOptions) { o.AuthFile = path }
}

func (o NanocodexOptions) clone() NanocodexOptions {
	o.Env = maps.Clone(o.Env)
	o.ExtraPathDirs = slices.Clone(o.ExtraPathDirs)

	return o
}

// Meta returns a fresh owned nanocodex options namespace.
func (o NanocodexOptions) Meta() map[string]any {
	return map[string]any{vendor: map[string]any{metaOptionsKey: o.values()}}
}

func (o NanocodexOptions) values() map[string]any {
	values := make(map[string]any)

	for key, value := range o.strings() {
		if value != "" {
			values[key] = value
		}
	}

	if o.Env != nil {
		values["env"] = maps.Clone(o.Env)
	}

	if o.ExtraPathDirs != nil {
		values["extraPathDirs"] = slices.Clone(o.ExtraPathDirs)
	}

	return values
}

func (o NanocodexOptions) strings() map[string]string {
	return map[string]string{
		"model": o.Model, metaThinking: o.Thinking, metaAPIBaseURL: o.APIBaseURL, metaWebsocketURL: o.WebsocketURL,
		"modelIdPrefix": o.ModelIDPrefix, "transport": o.Transport, metaAPIKeyEnv: o.APIKeyEnv, "authFile": o.AuthFile,
	}
}

func (o NanocodexOptions) initialize(sessionID, nativeID string) nanocodex.Initialize {
	return nanocodex.Initialize{
		Model: o.Model, Thinking: o.Thinking, APIBaseURL: o.APIBaseURL, WebsocketURL: o.WebsocketURL, ModelIDPrefix: o.ModelIDPrefix,
		Transport: o.Transport, APIKeyEnv: o.APIKeyEnv, AuthFile: o.AuthFile, SessionID: sessionID, ResumeSessionID: nativeID,
	}
}

type sessionMeta struct {
	options   NanocodexOptions
	rawEvents bool
	present   map[string]bool
}

// ValidateNanocodexSessionMeta validates session metadata without launching an agent.
func ValidateNanocodexSessionMeta(meta map[string]any) error {
	_, err := parseSessionMeta(meta)

	return err
}

func parseSessionMeta(meta map[string]any) (sessionMeta, error) {
	parsed := sessionMeta{present: make(map[string]bool)}
	if err := rejectLifecycle(meta); err != nil {
		return parsed, err
	}

	raw, exists := meta[vendor]
	if !exists {
		return parsed, nil
	}

	values, ok := raw.(map[string]any)
	if !ok {
		return parsed, wire.Unsupported("_meta." + vendor)
	}

	for key, item := range values {
		switch key {
		case metaRawEventKey:
			fields, ok := item.(map[string]any)
			if !ok {
				return parsed, wire.Unsupported("_meta." + vendor + ".rawEvent")
			}

			for name, value := range fields {
				enabled, ok := value.(bool)
				if name != metaEnabledKey || !ok {
					return parsed, wire.Unsupported("_meta." + vendor + ".rawEvent." + name)
				}

				parsed.rawEvents = enabled
			}
		case metaOptionsKey:
			fields, ok := item.(map[string]any)
			if !ok {
				return parsed, wire.Unsupported(wire.MetaOptionPath(vendor, ""))
			}

			for name, value := range fields {
				path := wire.MetaOptionPath(vendor, name)

				parsed.present[name] = true
				switch name {
				case "env":
					env, err := wire.StringMapOption(value, path)
					if err != nil {
						return parsed, err
					}

					parsed.options.Env = env
				case "extraPathDirs":
					dirs, err := wire.StringSliceOption(value, path)
					if err != nil {
						return parsed, err
					}

					parsed.options.ExtraPathDirs = dirs
				default:
					text, ok := value.(string)
					if !ok || text == "" {
						return parsed, wire.Unsupported(path)
					}

					switch name {
					case "model":
						parsed.options.Model = text
					case metaThinking:
						parsed.options.Thinking = text
					case metaAPIBaseURL:
						parsed.options.APIBaseURL = text
					case metaWebsocketURL:
						parsed.options.WebsocketURL = text
					case "modelIdPrefix":
						parsed.options.ModelIDPrefix = text
					case "transport":
						parsed.options.Transport = text
					case metaAPIKeyEnv:
						parsed.options.APIKeyEnv = text
					case "authFile":
						parsed.options.AuthFile = text
					default:
						return parsed, wire.Unsupported(path)
					}
				}
			}
		default:
			return parsed, wire.Unsupported("_meta." + vendor + "." + key)
		}
	}

	return parsed, validateNativeOptions(parsed.options)
}

func validateNativeOptions(o NanocodexOptions) error {
	if err := wire.ValidateSessionEnvironment(o.Env, o.ExtraPathDirs, wire.MetaOptionPath(vendor, "")); err != nil {
		return err
	}

	for name, value := range o.strings() {
		if strings.ContainsRune(value, 0) {
			return wire.Unsupported(wire.MetaOptionPath(vendor, name))
		}
	}

	if o.Model != "" && !validModel(o.Model) {
		return wire.Unsupported(wire.MetaOptionPath(vendor, "model"))
	}

	if o.Transport != "" && o.Transport != "https" && o.Transport != "websocket" {
		return wire.Unsupported(wire.MetaOptionPath(vendor, "transport"))
	}

	for name, value := range map[string]string{metaAPIBaseURL: o.APIBaseURL, metaWebsocketURL: o.WebsocketURL} {
		if value == "" {
			continue
		}

		parsed, err := url.Parse(value)
		if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
			return wire.Unsupported(wire.MetaOptionPath(vendor, name))
		}

		valid := parsed.Scheme == "http" || parsed.Scheme == "https"
		if name == metaWebsocketURL {
			valid = parsed.Scheme == "ws" || parsed.Scheme == "wss"
		}

		if !valid {
			return wire.Unsupported(wire.MetaOptionPath(vendor, name))
		}

		if parsed.Scheme == "http" || parsed.Scheme == "ws" {
			address, _ := netip.ParseAddr(parsed.Hostname())
			if !strings.EqualFold(parsed.Hostname(), "localhost") && !address.IsLoopback() {
				return wire.Unsupported(wire.MetaOptionPath(vendor, name))
			}
		}
	}

	if o.APIKeyEnv != "" && strings.ContainsAny(o.APIKeyEnv, "=\x00") {
		return wire.Unsupported(wire.MetaOptionPath(vendor, metaAPIKeyEnv))
	}

	return nil
}

func (m sessionMeta) inherit(options NanocodexOptions) NanocodexOptions {
	base := options.values()
	maps.Copy(base, m.options.values())

	if m.present["env"] {
		base["env"] = maps.Clone(m.options.Env)
	}

	if m.present["extraPathDirs"] {
		base["extraPathDirs"] = slices.Clone(m.options.ExtraPathDirs)
	}

	parsed, _ := parseSessionMeta(map[string]any{vendor: map[string]any{metaOptionsKey: base}})

	return parsed.options
}
