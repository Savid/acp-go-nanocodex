package nanocodexacp

import (
	"fmt"
	"maps"
	"math"
	"net/netip"
	"net/url"
	"slices"
	"strings"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-core/lifecycle"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-nanocodex/internal/nanocodex"
)

const (
	metaOptionsKey       = "options"
	metaRawEventKey      = "rawEvent"
	metaModelKey         = "model"
	metaEnvKey           = "env"
	metaExtraPathDirsKey = "extraPathDirs"
	metaBaseModelKey     = "baseModel"
	metaContextWindowKey = "contextWindow"
	metaThinkingKey      = "thinking"
	metaAPIBaseURLKey    = "apiBaseUrl"
	metaWebsocketURLKey  = "websocketUrl"
	metaModelIDPrefixKey = "modelIdPrefix"
	metaTransportKey     = "transport"
	metaAPIKeyEnvKey     = "apiKeyEnv"
	metaAuthFileKey      = "authFile"
	metaShellEnvKey      = "shellEnv"
	metaEnabledKey       = "enabled"
)

const (
	configModel    acp.SessionConfigId = "model"
	configThinking acp.SessionConfigId = "thought_level"
)

// NanocodexOptions selects one native agent's model, route, and environment.
type NanocodexOptions struct {
	Model         string            `json:"model,omitempty"`
	Env           map[string]string `json:"env,omitempty"`
	ExtraPathDirs []string          `json:"extraPathDirs,omitempty"`
	// BaseModel is the native model whose settings a gateway model uses.
	BaseModel string `json:"baseModel,omitempty"`
	// ContextWindow is the token window used for context accounting, usage
	// size, and compaction; zero leaves the helper's environment value or the
	// model default.
	ContextWindow int64  `json:"contextWindow,omitempty"`
	Thinking      string `json:"thinking,omitempty"`
	APIBaseURL    string `json:"apiBaseUrl,omitempty"`
	WebsocketURL  string `json:"websocketUrl,omitempty"`
	ModelIDPrefix string `json:"modelIdPrefix,omitempty"`
	Transport     string `json:"transport,omitempty"`
	APIKeyEnv     string `json:"apiKeyEnv,omitempty"`
	AuthFile      string `json:"authFile,omitempty"`
	// ShellEnv names variables of the helper's environment that the tool
	// shell receives even though their names look sensitive. Nil leaves
	// NANOCODEX_SHELL_ENV in effect; an empty list withholds every name.
	ShellEnv []string `json:"shellEnv,omitzero"`
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

// WithNanocodexModel selects a native model ID, or a gateway model ID on a
// configured gateway route.
func WithNanocodexModel(model string) NanocodexOption {
	return func(o *NanocodexOptions) { o.Model = model }
}

// WithNanocodexBaseModel selects the native model whose settings a gateway model uses.
func WithNanocodexBaseModel(model string) NanocodexOption {
	return func(o *NanocodexOptions) { o.BaseModel = model }
}

// WithNanocodexContextWindow sets the token window used for context accounting,
// usage size, and compaction.
func WithNanocodexContextWindow(tokens int64) NanocodexOption {
	return func(o *NanocodexOptions) { o.ContextWindow = tokens }
}

// WithNanocodexEnv sets the session environment overlay.
func WithNanocodexEnv(env map[string]string) NanocodexOption {
	cloned := maps.Clone(env)

	return func(o *NanocodexOptions) { o.Env = maps.Clone(cloned) }
}

// WithNanocodexExtraPathDirs prepends the ordered directories to the session PATH.
func WithNanocodexExtraPathDirs(dirs ...string) NanocodexOption {
	cloned := slices.Clone(dirs)

	return func(o *NanocodexOptions) { o.ExtraPathDirs = slices.Clone(cloned) }
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

// WithNanocodexShellEnv names variables of the helper's environment that the
// tool shell receives even though the shell's sensitive-name filter would
// strip them. Each helper launch reads the values and skips unset names.
// Without names it withholds every sensitive variable and ignores
// NANOCODEX_SHELL_ENV.
func WithNanocodexShellEnv(names ...string) NanocodexOption {
	cloned := append([]string{}, names...)

	return func(o *NanocodexOptions) { o.ShellEnv = slices.Clone(cloned) }
}

func (options NanocodexOptions) clone() NanocodexOptions {
	options.Env = maps.Clone(options.Env)
	options.ExtraPathDirs = slices.Clone(options.ExtraPathDirs)
	options.ShellEnv = slices.Clone(options.ShellEnv)

	return options
}

// Meta returns a fresh owned nanocodex options namespace.
func (options NanocodexOptions) Meta() map[string]any {
	return map[string]any{vendor: map[string]any{metaOptionsKey: options.values()}}
}

func (options NanocodexOptions) values() map[string]any {
	values := make(map[string]any)

	for key, value := range options.strings() {
		if value != "" {
			values[key] = value
		}
	}

	if options.Env != nil {
		values[metaEnvKey] = maps.Clone(options.Env)
	}

	if options.ExtraPathDirs != nil {
		values[metaExtraPathDirsKey] = slices.Clone(options.ExtraPathDirs)
	}

	if options.ShellEnv != nil {
		values[metaShellEnvKey] = slices.Clone(options.ShellEnv)
	}

	if options.ContextWindow != 0 {
		values[metaContextWindowKey] = options.ContextWindow
	}

	return values
}

func (options NanocodexOptions) strings() map[string]string {
	values := make(map[string]string)
	for key, field := range stringOptionFields(&options) {
		values[key] = *field
	}

	return values
}

// stringOptionFields maps each string option's metadata key to its field.
func stringOptionFields(options *NanocodexOptions) map[string]*string {
	return map[string]*string{
		metaModelKey: &options.Model, metaBaseModelKey: &options.BaseModel, metaThinkingKey: &options.Thinking,
		metaAPIBaseURLKey: &options.APIBaseURL, metaWebsocketURLKey: &options.WebsocketURL, metaModelIDPrefixKey: &options.ModelIDPrefix,
		metaTransportKey: &options.Transport, metaAPIKeyEnvKey: &options.APIKeyEnv, metaAuthFileKey: &options.AuthFile,
	}
}

func (options NanocodexOptions) initialize(sessionID, nativeID string) nanocodex.Initialize {
	return nanocodex.Initialize{
		Model: options.Model, BaseModel: options.BaseModel, ContextWindow: options.ContextWindow, Thinking: options.Thinking,
		APIBaseURL: options.APIBaseURL, WebsocketURL: options.WebsocketURL, ModelIDPrefix: options.ModelIDPrefix, Transport: options.Transport,
		APIKeyEnv: options.APIKeyEnv, AuthFile: options.AuthFile, SessionID: sessionID, ResumeSessionID: nativeID,
		ShellEnv: slices.Clone(options.ShellEnv),
	}
}

// ValidateNanocodexSessionMeta runs the owned-namespace parsing of a session
// lifecycle request's _meta without an Agent and returns the same refusal.
func ValidateNanocodexSessionMeta(meta map[string]any) error {
	if _, err := parseSessionMeta(meta); err != nil {
		return err
	}

	return nil
}

// sessionMeta is what one session lifecycle request's _meta.nanocodex carried.
type sessionMeta struct {
	options   NanocodexOptions
	rawEvents bool
	// present records which option fields the request named, so a load or
	// resume inherits the stored value only for fields it left out, and a
	// native configuration refusal names only a field the request supplied.
	present map[string]bool
}

// parseSessionMeta validates the owned _meta.nanocodex namespace of one
// session lifecycle request. Unknown own-namespace keys fail closed; foreign
// namespaces are ignored; the lifecycle literal is refused by name.
func parseSessionMeta(meta map[string]any) (sessionMeta, *acp.RequestError) {
	if refusal := lifecycle.RejectKey(meta); refusal != nil {
		return sessionMeta{}, wire.ParamRefusal(refusal)
	}

	raw, exists := meta[vendor]
	if !exists {
		return sessionMeta{}, nil
	}

	vendorMeta, ok := raw.(map[string]any)
	if !ok {
		return sessionMeta{}, wire.Unsupported("_meta." + vendor)
	}

	parsed := sessionMeta{}

	for key := range vendorMeta {
		switch key {
		case metaOptionsKey, metaRawEventKey:
		default:
			return sessionMeta{}, wire.Unsupported("_meta." + vendor + "." + key)
		}
	}

	if rawEvent, ok := vendorMeta[metaRawEventKey]; ok {
		values, ok := rawEvent.(map[string]any)
		if !ok {
			return sessionMeta{}, wire.Unsupported("_meta." + vendor + "." + metaRawEventKey)
		}

		for key, item := range values {
			enabled, ok := item.(bool)
			if key != metaEnabledKey || !ok {
				return sessionMeta{}, wire.Unsupported("_meta." + vendor + "." + metaRawEventKey + "." + key)
			}

			parsed.rawEvents = enabled
		}
	}

	rawOptions, hasOptions := vendorMeta[metaOptionsKey]
	if !hasOptions {
		return parsed, nil
	}

	values, isObject := rawOptions.(map[string]any)
	if !isObject {
		return sessionMeta{}, wire.Unsupported(wire.MetaOptionPath(vendor, ""))
	}

	options, err := parseNanocodexOptions(values)
	if err != nil {
		return sessionMeta{}, err
	}

	parsed.options = options
	parsed.present = make(map[string]bool, len(values))

	for key := range values {
		parsed.present[key] = true
	}

	return parsed, nil
}

func parseNanocodexOptions(values map[string]any) (NanocodexOptions, *acp.RequestError) {
	options := NanocodexOptions{}

	for key, item := range values {
		path := wire.MetaOptionPath(vendor, key)

		switch key {
		case metaEnvKey:
			env, err := wire.StringMapOption(item, path)
			if err != nil {
				return NanocodexOptions{}, err
			}

			options.Env = env
		case metaExtraPathDirsKey:
			dirs, err := wire.StringSliceOption(item, path)
			if err != nil {
				return NanocodexOptions{}, err
			}

			options.ExtraPathDirs = dirs
		case metaShellEnvKey:
			names, err := wire.StringSliceOption(item, path)
			if err != nil {
				return NanocodexOptions{}, err
			}

			options.ShellEnv = names
		case metaContextWindowKey:
			tokens, ok := positiveInteger(item)
			if !ok {
				return NanocodexOptions{}, wire.Unsupported(path)
			}

			options.ContextWindow = tokens
		default:
			field, known := stringOptionFields(&options)[key]
			text, ok := item.(string)

			if !known || !ok || text == "" {
				return NanocodexOptions{}, wire.Unsupported(path)
			}

			*field = text
		}
	}

	return options, validateNanocodexOptions(options)
}

func validateNanocodexOptions(options NanocodexOptions) *acp.RequestError {
	if err := wire.ValidateSessionEnvironment(options.Env, options.ExtraPathDirs, wire.MetaOptionPath(vendor, "")); err != nil {
		return err
	}

	for name, value := range options.strings() {
		if strings.ContainsRune(value, 0) {
			return wire.Unsupported(wire.MetaOptionPath(vendor, name))
		}
	}

	if options.Model != "" && !validModel(options.Model) {
		return wire.Unsupported(wire.MetaOptionPath(vendor, metaModelKey))
	}

	if options.BaseModel != "" && !validModel(options.BaseModel) {
		return wire.Unsupported(wire.MetaOptionPath(vendor, metaBaseModelKey))
	}

	if options.ContextWindow < 0 {
		return wire.Unsupported(wire.MetaOptionPath(vendor, metaContextWindowKey))
	}

	if options.Transport != "" && options.Transport != "https" && options.Transport != "websocket" {
		return wire.Unsupported(wire.MetaOptionPath(vendor, metaTransportKey))
	}

	for name, value := range map[string]string{metaAPIBaseURLKey: options.APIBaseURL, metaWebsocketURLKey: options.WebsocketURL} {
		if value == "" {
			continue
		}

		parsed, err := url.Parse(value)
		if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
			return wire.Unsupported(wire.MetaOptionPath(vendor, name))
		}

		valid := parsed.Scheme == "http" || parsed.Scheme == "https"
		if name == metaWebsocketURLKey {
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

	if options.APIKeyEnv != "" && strings.ContainsAny(options.APIKeyEnv, "=\x00") {
		return wire.Unsupported(wire.MetaOptionPath(vendor, metaAPIKeyEnvKey))
	}

	if index := slices.IndexFunc(options.ShellEnv, func(name string) bool { return !validEnvName(name) }); index >= 0 {
		return wire.Unsupported(fmt.Sprintf("%s[%d]", wire.MetaOptionPath(vendor, metaShellEnvKey), index))
	}

	return nil
}

// validEnvName accepts a portable environment variable name: an ASCII letter
// or underscore followed by ASCII letters, digits, or underscores.
func validEnvName(name string) bool {
	for i, r := range name {
		letter := r == '_' || (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z')
		if !letter && (i == 0 || r < '0' || r > '9') {
			return false
		}
	}

	return name != ""
}

// forModelChange drops the settings that describe the previously selected model.
func (options NanocodexOptions) forModelChange() NanocodexOptions {
	options.BaseModel = ""
	options.ContextWindow = 0
	options.Thinking = ""

	return options
}

// positiveInteger accepts a whole JSON number, or the Go integer that
// in-process metadata carries, when it is positive.
func positiveInteger(value any) (int64, bool) {
	switch number := value.(type) {
	case int64:
		return number, number > 0
	case int:
		return int64(number), number > 0
	case float64:
		if number > 0 && number < math.MaxInt64 && number == math.Trunc(number) {
			return int64(number), true
		}
	}

	return 0, false
}

// pinRoute refuses moving a started session between a gateway route (a custom
// endpoint) and the native route. Native routes cannot read gateway summaries,
// and gateways cannot create native compaction items.
func pinRoute(stored, requested NanocodexOptions) error {
	gateway := func(o NanocodexOptions) bool { return o.APIBaseURL != "" || o.WebsocketURL != "" }
	if gateway(stored) == gateway(requested) {
		return nil
	}

	if (stored.APIBaseURL == "") != (requested.APIBaseURL == "") {
		return wire.Unsupported(wire.MetaOptionPath(vendor, metaAPIBaseURLKey))
	}

	return wire.Unsupported(wire.MetaOptionPath(vendor, metaWebsocketURLKey))
}

// inheritCarrier applies requested options over the stored record's. The base
// model, context window, and thinking level describe the stored model, so a
// model change drops them unless the request supplies them again.
func inheritCarrier(meta sessionMeta, record sessionRecord) NanocodexOptions {
	stored := record.Options
	if meta.present[metaModelKey] && meta.options.Model != stored.Model {
		stored = stored.forModelChange()
	}

	base := stored.values()
	maps.Copy(base, meta.options.values())

	if meta.present[metaEnvKey] {
		base[metaEnvKey] = maps.Clone(meta.options.Env)
	}

	if meta.present[metaExtraPathDirsKey] {
		base[metaExtraPathDirsKey] = slices.Clone(meta.options.ExtraPathDirs)
	}

	if meta.present[metaShellEnvKey] {
		base[metaShellEnvKey] = slices.Clone(meta.options.ShellEnv)
	}

	options, _ := parseNanocodexOptions(base)

	return options
}
