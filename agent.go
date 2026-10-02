package nanocodexacp

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"strings"
	"sync"

	"github.com/coder/acp-go-sdk"

	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/image"
	"github.com/savid/acp-go-core/lifecycle"
	"github.com/savid/acp-go-core/observer"
	"github.com/savid/acp-go-core/process"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-nanocodex/internal/nanocodex"
)

const (
	// RawEventMethod delivers opted-in native events during live turns.
	RawEventMethod = "_nanocodex/rawEvent"
	// SessionStoreFormat identifies native rollout rows and their configuration.
	SessionStoreFormat = "nanocodex-rollout-jsonl-v1"
	vendor             = "nanocodex"
)

//go:embed native/src/*.rs native/Cargo.toml native/Cargo.lock native/rust-toolchain.toml native/build.rs internal/nanocodex/protocol.go internal/nanocodex/client.go
var nativeSources embed.FS

var nativeHelperFingerprint = helperFingerprint()

// HelperRelease returns the helper release and build-input fingerprint that
// initialization requires from the helper executable.
func HelperRelease() (version, fingerprint string) {
	return nanocodex.HelperVersion, nativeHelperFingerprint
}

func helperFingerprint() string {
	hash := sha256.New()

	err := fs.WalkDir(nativeSources, ".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if entry.IsDir() {
			return nil
		}

		data, err := nativeSources.ReadFile(path)
		if err != nil {
			return err
		}

		_, _ = hash.Write([]byte(path))
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write(data)
		_, _ = hash.Write([]byte{0})

		return nil
	})
	if err != nil {
		panic(err)
	}

	return hex.EncodeToString(hash.Sum(nil))
}

type client interface {
	SessionUpdate(context.Context, acp.SessionNotification) error
	NotifyExtension(context.Context, string, any) error
}

type establishment struct {
	cancel context.CancelFunc
}

// Agent exposes the Nanocodex agent library through its native Rust helper.
type Agent struct {
	options    Options
	log        *slog.Logger
	observe    *observer.Observer
	optionErr  *acp.RequestError
	processEnv []string
	store      acpcore.SessionStore
	mu         sync.Mutex
	conn       client
	transport  *wire.Transport
	closed     bool
	closeDone  chan struct{}
	closeErr   error
	starts     map[*establishment]struct{}
	startWG    sync.WaitGroup
	starting   int
	lifecycle  lifecycle.Negotiated
	restores   wire.SessionRequests
	sessions   map[acp.SessionId]*session
	deleted    map[acp.SessionId]bool
	ephemeral  map[acp.SessionId]bool
}

var (
	_ acp.Agent                  = (*Agent)(nil)
	_ acp.AgentLoader            = (*Agent)(nil)
	_ acp.ExtensionMethodHandler = (*Agent)(nil)
)

// NewAgent creates an adapter. Invalid options are reported before native launch.
func NewAgent(opts ...Option) *Agent {
	options := applyOptions(opts)

	logger := options.Logger
	if logger == nil {
		logger = slog.Default()
	}

	store := options.SessionStore
	if store == nil {
		store = acpcore.NewInMemorySessionStore()
	}

	a := &Agent{options: options, log: logger, processEnv: os.Environ(), store: store,
		starts: make(map[*establishment]struct{}), closeDone: make(chan struct{}),
		sessions: make(map[acp.SessionId]*session), deleted: make(map[acp.SessionId]bool), ephemeral: make(map[acp.SessionId]bool),
		observe: observer.New(observer.Config{Vendor: vendor, NativeClient: "nanocodex", Version: options.AgentVersion, TracerProvider: options.TracerProvider, MeterProvider: options.MeterProvider, Propagator: options.TextMapPropagator})}
	a.optionErr = a.validateOptions()

	return a
}

func (a *Agent) validateOptions() *acp.RequestError {
	checks := []struct {
		field string
		err   error
	}{
		{"home", process.ValidateOptionalAbsolutePath(a.options.Home)},
		{"scratchDir", process.ValidateOptionalAbsolutePath(a.options.ScratchDir)},
		{"inputHandoffRoot", image.ValidateHandoffRoot(a.options.InputHandoffRoot)},
		{"env", process.ValidateNames(a.options.Env)},
		{"imageLimits", a.options.ImageLimits.core().Validate()},
		{"concurrencyLimits", wire.ValidateConcurrencyLimits(a.options.ConcurrencyLimits.MaxActiveSessions, a.options.ConcurrencyLimits.MaxConcurrentClientCalls)},
	}
	for _, check := range checks {
		if check.err != nil {
			return wire.InvalidOptions(vendor, check.field)
		}
	}

	if a.options.DefaultModel != "" && !validModel(a.options.DefaultModel) {
		return wire.InvalidOptions(vendor, "defaultModel")
	}

	seen := make(map[string]bool)
	for _, id := range a.options.ConfiguredModels {
		if !validModel(id) || seen[id] {
			return wire.InvalidOptions(vendor, "configuredModels")
		}

		seen[id] = true
	}

	return nil
}

func validModel(id string) bool {
	return id != "" && strings.TrimSpace(id) == id && !strings.ContainsAny(id, "\x00\r\n\t ")
}

// Serve runs ACP over the supplied streams and joins native cleanup on exit.
func Serve(ctx context.Context, input io.Reader, output io.Writer, opts ...Option) (returnErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}

	a := NewAgent(opts...)
	defer func() {
		if err := a.Close(); err != nil {
			returnErr = err
		}
	}()

	t := wire.NewTransport(input, output)
	defer t.Close()

	conn := acp.NewAgentSideConnection(a, t.Writer(), t.Reader())
	conn.SetLogger(a.log)
	a.attach(conn, t)
	t.Start()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-conn.Done():
		return nil
	}
}

func (a *Agent) attach(conn client, t *wire.Transport) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.conn = conn
	a.transport = t
}
func (a *Agent) connection() client {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.conn
}
func (a *Agent) transportRef() *wire.Transport {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.transport
}
func (a *Agent) lifecycleNegotiated() lifecycle.Negotiated {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.lifecycle
}
func (a *Agent) ensureOpen() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return wire.AgentClosed()
	}

	return nil
}

// Close stops accepting work, closes every session, and commits owed state.
func (a *Agent) Close() error {
	a.mu.Lock()
	if a.closed {
		done := a.closeDone
		a.mu.Unlock()
		<-done

		return a.closeErr
	}

	a.closed = true

	cancels := make([]context.CancelFunc, 0, len(a.starts))
	for start := range a.starts {
		cancels = append(cancels, start.cancel)
	}
	a.mu.Unlock()

	for _, cancel := range cancels {
		cancel()
	}

	a.startWG.Wait()

	a.mu.Lock()

	sessions := make([]*session, 0, len(a.sessions))
	for _, s := range a.sessions {
		sessions = append(sessions, s)
	}
	a.mu.Unlock()

	var errs []error
	for _, s := range sessions {
		errs = append(errs, s.close())
		a.detach(s)
	}

	a.mu.Lock()
	a.closeErr = errors.Join(errs...)
	close(a.closeDone)
	a.mu.Unlock()

	return a.closeErr
}

func (a *Agent) beginEstablishment(ctx context.Context) (context.Context, func(), error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return nil, nil, wire.AgentClosed()
	}

	ctx, cancel := context.WithCancel(ctx)
	start := &establishment{cancel: cancel}
	a.starts[start] = struct{}{}
	a.startWG.Add(1)

	return ctx, func() {
		cancel()
		a.mu.Lock()
		delete(a.starts, start)
		a.mu.Unlock()
		a.startWG.Done()
	}, nil
}

// Initialize advertises the implemented ACP surfaces and prompt-contained lifecycle.
func (a *Agent) Initialize(ctx context.Context, p acp.InitializeRequest) (resp acp.InitializeResponse, err error) {
	_, finish := a.observe.StartACP(ctx, p.Meta, acp.AgentMethodInitialize)
	defer func() { finish(err) }()

	if err := a.ensureOpen(); err != nil {
		return resp, err
	}

	if a.optionErr != nil {
		return resp, a.optionErr
	}

	meta := p.Meta
	if t := a.transportRef(); t != nil {
		meta = lifecycle.RetainRequestMetadata(meta, t.TakeRaw(acp.AgentMethodInitialize))
	}

	present, refusal := lifecycle.DecodeOffer(meta)
	if refusal != nil {
		return resp, wire.ParamRefusal(refusal)
	}

	var negotiated lifecycle.Negotiated
	if present {
		negotiated = lifecycle.Answer(lifecycle.Negotiated{UpdatesOutsidePrompt: false, ActivityKinds: []lifecycle.ActivityKind{}})
	}

	a.mu.Lock()
	a.lifecycle = negotiated
	a.mu.Unlock()

	capMeta := map[string]any{
		vendor: map[string]any{
			"rawEvent":     map[string]any{"method": RawEventMethod, "enabledBy": "_meta.nanocodex.rawEvent.enabled", "maxBytes": wire.RawEventMaxBytes, "defaultEnabled": false},
			"sessionStore": map[string]any{"format": SessionStoreFormat, "key": []string{"sessionId", "subpath"}},
		},
		wire.MediaEnvelopeKey: image.MediaEnvelope(a.options.ImageLimits.core(), image.Envelope{DocumentFormats: []string{}}),
	}
	if a.options.InputHandoffRoot != "" {
		capMeta[wire.HandoffKey] = image.HandoffAdvertisement()
	}

	if negotiated.Present() {
		resp.Meta = map[string]any{wire.LifecycleKey: negotiated.Advertisement()}
	}

	title := a.options.AgentTitle
	encoding := wire.SelectPositionEncoding(p.ClientCapabilities.PositionEncodings)
	resp.ProtocolVersion = acp.ProtocolVersionNumber
	resp.AgentInfo = &acp.Implementation{Name: a.options.AgentName, Title: &title, Version: a.options.AgentVersion}
	resp.AuthMethods = []acp.AuthMethod{}
	resp.AgentCapabilities = acp.AgentCapabilities{Meta: capMeta, LoadSession: true, PositionEncoding: &encoding,
		PromptCapabilities:  acp.PromptCapabilities{EmbeddedContext: true, Image: true},
		SessionCapabilities: acp.SessionCapabilities{Close: &acp.SessionCloseCapabilities{}, Delete: &acp.SessionDeleteCapabilities{}, List: &acp.SessionListCapabilities{}, Resume: &acp.SessionResumeCapabilities{}},
	}

	return resp, nil
}

func rejectLifecycle(meta map[string]any) error {
	if refusal := lifecycle.RejectKey(meta); refusal != nil {
		return wire.ParamRefusal(refusal)
	}

	return nil
}

// Authenticate leaves native credential management outside ACP.
func (a *Agent) Authenticate(_ context.Context, p acp.AuthenticateRequest) (acp.AuthenticateResponse, error) {
	if err := a.ensureOpen(); err != nil {
		return acp.AuthenticateResponse{}, err
	}

	if err := rejectLifecycle(p.Meta); err != nil {
		return acp.AuthenticateResponse{}, err
	}

	return acp.AuthenticateResponse{}, acp.NewInvalidParams(map[string]any{"methodId": p.MethodId})
}

// Logout is not supported by this adapter.
func (a *Agent) Logout(_ context.Context, p acp.LogoutRequest) (acp.LogoutResponse, error) {
	if err := a.ensureOpen(); err != nil {
		return acp.LogoutResponse{}, err
	}

	if err := rejectLifecycle(p.Meta); err != nil {
		return acp.LogoutResponse{}, err
	}

	return acp.LogoutResponse{}, acp.NewMethodNotFound(acp.AgentMethodLogout)
}

// SetSessionMode is required by the SDK but no session modes are advertised.
func (a *Agent) SetSessionMode(_ context.Context, p acp.SetSessionModeRequest) (acp.SetSessionModeResponse, error) {
	if err := wire.CheckSessionID(p.SessionId); err != nil {
		return acp.SetSessionModeResponse{}, err
	}

	if err := a.ensureOpen(); err != nil {
		return acp.SetSessionModeResponse{}, err
	}

	if err := rejectLifecycle(p.Meta); err != nil {
		return acp.SetSessionModeResponse{}, err
	}

	return acp.SetSessionModeResponse{}, acp.NewMethodNotFound(acp.AgentMethodSessionSetMode)
}

// HandleExtensionMethod refuses inbound extension methods.
func (a *Agent) HandleExtensionMethod(_ context.Context, method string, _ json.RawMessage) (any, error) {
	if err := a.ensureOpen(); err != nil {
		return nil, err
	}

	return nil, acp.NewMethodNotFound(method)
}
