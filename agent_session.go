package nanocodexacp

import (
	"context"
	"crypto/rand"
	"errors"
	"path/filepath"
	"sync"
	"time"

	"github.com/coder/acp-go-sdk"
	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/lifecycle"
	"github.com/savid/acp-go-core/wire"
)

func (a *Agent) validateStart(cwd string, dirs []string, mcp []acp.McpServer, meta map[string]any) (sessionMeta, error) {
	if err := a.ensureOpen(); err != nil {
		return sessionMeta{}, err
	}

	if a.optionErr != nil {
		return sessionMeta{}, a.optionErr
	}

	if !filepath.IsAbs(cwd) {
		return sessionMeta{}, wire.Unsupported("cwd")
	}

	if len(dirs) > 0 {
		return sessionMeta{}, wire.Unsupported("additionalDirectories")
	}

	if len(mcp) > 0 {
		return sessionMeta{}, wire.Unsupported("mcpServers")
	}

	parsed, err := parseSessionMeta(meta)
	if err != nil {
		return sessionMeta{}, err
	}

	return parsed, nil
}

func (a *Agent) reserveSlot() (func(), error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return nil, wire.AgentClosed()
	}

	if len(a.sessions)+a.starting >= a.options.ConcurrencyLimits.MaxActiveSessions {
		return nil, wire.Backpressure("active_sessions")
	}

	a.starting++

	return sync.OnceFunc(func() { a.mu.Lock(); a.starting--; a.mu.Unlock() }), nil
}

func (a *Agent) newSession(id acp.SessionId, cwd string, meta sessionMeta, ephemeral bool) (*session, error) {
	options := meta.options.clone()
	if options.Model == "" {
		options.Model = a.options.DefaultModel
	}

	home, err := a.nativeHome(options, cwd)
	if err != nil {
		return nil, wire.InternalFailure(vendor, "native_start")
	}

	return &session{agent: a, id: id, cwd: cwd, home: home, options: options, ephemeral: ephemeral, gate: make(chan struct{}, 1), closed: make(chan struct{}), raw: wire.NewRawEvents(vendor, string(id), "native", meta.rawEvents), updatedAt: time.Now().UnixMilli()}, nil
}

func (a *Agent) install(s *session) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return wire.AgentClosed()
	}

	if a.deleted[s.id] {
		return wire.UnknownSession()
	}

	a.sessions[s.id] = s
	if s.ephemeral {
		a.ephemeral[s.id] = true
	}

	a.observe.AddActiveSession(context.Background(), 1)

	return nil
}

func (a *Agent) detach(s *session) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.sessions[s.id] == s {
		delete(a.sessions, s.id)
		a.observe.AddActiveSession(context.Background(), -1)
	}
}

// NewSession creates native state and publishes its first durable generation.
func (a *Agent) NewSession(ctx context.Context, p acp.NewSessionRequest) (resp acp.NewSessionResponse, err error) {
	ctx, finish := a.observe.StartACP(ctx, p.Meta, acp.AgentMethodSessionNew)
	defer func() { finish(err) }()

	meta, err := a.validateStart(p.Cwd, p.AdditionalDirectories, p.McpServers, p.Meta)
	if err != nil {
		return resp, err
	}

	ctx, finishStart, err := a.beginEstablishment(ctx)
	if err != nil {
		return resp, err
	}
	defer finishStart()

	release, err := a.reserveSlot()
	if err != nil {
		return resp, err
	}
	defer release()

	common, refusal := wire.DecodeSessionMeta(p.Meta)
	if refusal != nil {
		return resp, refusal
	}

	s, err := a.newSession(acp.SessionId(rand.Text()), p.Cwd, meta, common.Ephemeral)
	if err != nil {
		return resp, err
	}

	if nativeErr := s.observeNative(ctx); nativeErr != nil {
		return resp, nativeLaunchError(nativeErr, meta.present)
	}

	if err = s.commitMirror(ctx); err != nil {
		return resp, wire.InternalFailure(vendor, "")
	}

	response := acp.NewSessionResponse{SessionId: s.id, Meta: wire.NativeSessionMeta(vendor, s.state.NativeSessionID), ConfigOptions: s.configOptions()}
	if err := a.install(s); err != nil {
		return resp, err
	}

	release()

	return response, nil
}

func (a *Agent) lookup(id acp.SessionId) (*session, error) {
	if err := wire.CheckSessionID(id); err != nil {
		return nil, err
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return nil, wire.AgentClosed()
	}

	s := a.sessions[id]
	if s == nil || a.deleted[id] {
		return nil, wire.UnknownSession()
	}

	return s, nil
}

// CloseSession releases the addressed session while preserving its native state.
func (a *Agent) CloseSession(ctx context.Context, p acp.CloseSessionRequest) (acp.CloseSessionResponse, error) {
	if err := wire.CheckSessionID(p.SessionId); err != nil {
		return acp.CloseSessionResponse{}, err
	}

	if _, err := parseSessionMeta(p.Meta); err != nil {
		return acp.CloseSessionResponse{}, err
	}

	s, err := a.lookup(p.SessionId)
	if err != nil {
		return acp.CloseSessionResponse{}, err
	}

	err = s.close()
	a.detach(s)

	return acp.CloseSessionResponse{}, err
}

// Cancel silently ignores unknown sessions and cancels only the addressed turn.
//
//nolint:nilerr // Session cancel is wire-silent for invalid or unknown sessions.
func (a *Agent) Cancel(_ context.Context, p acp.CancelNotification) error {
	if wire.CheckSessionID(p.SessionId) != nil {
		return nil
	}

	if lifecycle.RejectKey(p.Meta) != nil {
		return nil
	}

	s, err := a.lookup(p.SessionId)
	if err != nil {
		return nil
	}

	s.cancel()

	return nil
}

// UnstableDeleteSession tombstones store state before retiring a loaded session.
func (a *Agent) UnstableDeleteSession(ctx context.Context, p acp.UnstableDeleteSessionRequest) (acp.UnstableDeleteSessionResponse, error) {
	if err := wire.CheckSessionID(p.SessionId); err != nil {
		return acp.UnstableDeleteSessionResponse{}, err
	}

	if err := a.ensureOpen(); err != nil {
		return acp.UnstableDeleteSessionResponse{}, err
	}

	if _, err := parseSessionMeta(p.Meta); err != nil {
		return acp.UnstableDeleteSessionResponse{}, err
	}

	a.mu.Lock()
	ephemeral := a.ephemeral[p.SessionId]
	a.mu.Unlock()

	if !ephemeral {
		storeCtx, cancel := context.WithTimeout(ctx, acpcore.SessionStoreTimeout)
		err := a.store.Delete(storeCtx, acpcore.SessionKey{SessionID: string(p.SessionId)})

		cancel()

		if err != nil {
			return acp.UnstableDeleteSessionResponse{}, wire.InternalFailure(vendor, "")
		}
	}

	a.mu.Lock()
	a.deleted[p.SessionId] = true
	s := a.sessions[p.SessionId]
	a.mu.Unlock()

	if s != nil {
		err := s.close()
		a.detach(s)

		if err != nil {
			return acp.UnstableDeleteSessionResponse{}, err
		}
	}

	return acp.UnstableDeleteSessionResponse{}, nil
}

// ListSessions lists only live generations from the authoritative session store.
func (a *Agent) ListSessions(ctx context.Context, p acp.ListSessionsRequest) (acp.ListSessionsResponse, error) {
	if err := a.ensureOpen(); err != nil {
		return acp.ListSessionsResponse{}, err
	}

	if _, err := parseSessionMeta(p.Meta); err != nil {
		return acp.ListSessionsResponse{}, err
	}

	storeCtx, cancel := context.WithTimeout(ctx, acpcore.SessionStoreTimeout)
	summaries, err := a.store.ListSessions(storeCtx)

	cancel()

	if err != nil {
		return acp.ListSessionsResponse{}, wire.InternalFailure(vendor, "")
	}

	list := make([]acp.SessionInfo, 0, len(summaries))
	for _, summary := range summaries {
		stored, loadErr := a.loadStored(ctx, acp.SessionId(summary.SessionID))
		if loadErr != nil {
			if errors.Is(loadErr, errInvalidStoredSession) {
				continue
			}

			return acp.ListSessionsResponse{}, loadErr
		}

		if !stored.found {
			continue
		}

		r := stored.record
		if p.Cwd != nil && *p.Cwd != "" && *p.Cwd != r.Cwd {
			continue
		}

		a.mu.Lock()
		deleted := a.deleted[acp.SessionId(r.SessionID)]
		a.mu.Unlock()

		if deleted {
			continue
		}

		updated := time.UnixMilli(r.UpdatedAtUnixMilli).UTC().Format("2006-01-02T15:04:05.000Z")

		item := acp.SessionInfo{SessionId: acp.SessionId(r.SessionID), Cwd: r.Cwd, Meta: wire.NativeSessionMeta(vendor, r.NativeSessionID), UpdatedAt: &updated}
		if r.Title != "" {
			title := r.Title
			item.Title = &title
		}

		list = append(list, item)
	}

	page, cursor, err := wire.PaginateSessions(list, p.Cursor)

	return acp.ListSessionsResponse{Sessions: page, NextCursor: cursor}, err
}
