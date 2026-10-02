package nanocodexacp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/coder/acp-go-sdk"
	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/process"
	"github.com/savid/acp-go-core/sessionlog"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-nanocodex/internal/nanocodex"
)

type sessionRecord struct {
	SessionID          string           `json:"sessionId"`
	NativeSessionID    string           `json:"nativeSessionId"`
	Cwd                string           `json:"cwd"`
	RolloutRelative    string           `json:"rolloutRelative"`
	Options            NanocodexOptions `json:"options"`
	Title              string           `json:"title,omitempty"`
	Started            bool             `json:"started"`
	UpdatedAtUnixMilli int64            `json:"updatedAtUnixMilli"`
}

type storedSession struct {
	rows   [][]byte
	record sessionRecord
	found  bool
}

func (s *session) commitMirror(ctx context.Context) (err error) {
	s.mu.Lock()
	state := s.state
	home := s.home
	prior := s.rows
	binding := s.binding
	options := s.options.clone()
	title := s.title
	rt := s.rt
	s.mu.Unlock()

	if binding != nil {
		prior = nil

		defer func() {
			if err != nil {
				s.rollbackBinding()
			}
		}()
	}

	if state.NativeSessionID == "" {
		return nil
	}

	limit := state.CommittedBytes
	if rt == nil {
		limit = -1
	} else {
		select {
		case <-rt.proc.Done():
			limit = -1
		default:
		}
	}

	if limit < 0 && (rt == nil || len(rt.locks) == 0 || rt.locksReleased.Load()) {
		lock, lockErr := process.LockFile(filepath.Join(home, "nanocodex", "acp-locks", state.NativeSessionID+".lock"))
		if lockErr != nil {
			return lockErr
		}
		defer lock.Close()
	}

	rows, err := nanocodex.ReadRows(home, state.RolloutPath, limit)
	if err != nil {
		return err
	}

	if validateErr := nanocodex.ValidateRows(rows, state.NativeSessionID); validateErr != nil {
		return validateErr
	}

	if len(prior) > 0 {
		if _, _, reconcileErr := sessionlog.Reconcile(rows, prior); reconcileErr != nil {
			return reconcileErr
		}

		if len(rows) < len(prior) {
			return errors.New("native rollout shrank")
		}
	}

	rel, err := nanocodex.RolloutRelative(home, state.RolloutPath)
	if err != nil {
		return err
	}

	updated := time.Now().UnixMilli()
	started := !nanocodex.EmptyRows(rows)

	record := sessionRecord{SessionID: string(s.id), NativeSessionID: state.NativeSessionID, Cwd: s.cwd, RolloutRelative: rel, Options: options, Title: title, Started: started, UpdatedAtUnixMilli: updated}
	if !s.ephemeral {
		storeCtx, finish := s.agent.observe.StartSessionStore(ctx, "replace")
		err = sessionlog.Commit(storeCtx, s.agent.store, string(s.id), rows, record)
		finish(err)

		if err != nil {
			return err
		}
	}

	s.mu.Lock()
	s.rows = rows
	s.binding = nil
	s.started = started
	s.updatedAt = updated
	s.mu.Unlock()

	return nil
}

var errInvalidStoredSession = errors.New("invalid stored session")

type storeLoadError struct{ error }

type storeLoadBoundary struct{ acpcore.SessionStore }

func (s storeLoadBoundary) Load(ctx context.Context, id string) (map[string][]acpcore.SessionStoreEntry, error) {
	generation, err := s.SessionStore.Load(ctx, id)
	if err != nil {
		return nil, &storeLoadError{err}
	}

	return generation, nil
}

func (a *Agent) loadStored(ctx context.Context, id acp.SessionId) (storedSession, error) {
	if err := wire.CheckSessionID(id); err != nil {
		return storedSession{}, errInvalidStoredSession
	}

	var record sessionRecord

	rows, found, err := sessionlog.Load(ctx, storeLoadBoundary{a.store}, string(id), &record)
	if err != nil {
		if _, failed := errors.AsType[*storeLoadError](err); failed {
			return storedSession{}, wire.InternalFailure(vendor, "")
		}

		return storedSession{}, errInvalidStoredSession
	}

	if !found {
		return storedSession{}, nil
	}

	if record.SessionID != string(id) || !nanocodex.ValidSessionID(record.NativeSessionID) || !filepath.IsAbs(record.Cwd) || record.UpdatedAtUnixMilli <= 0 || !filepath.IsLocal(record.RolloutRelative) {
		return storedSession{}, errInvalidStoredSession
	}

	if err := validateNativeOptions(record.Options); err != nil {
		return storedSession{}, errInvalidStoredSession
	}

	if err := nanocodex.ValidateRows(rows, record.NativeSessionID); err != nil {
		return storedSession{}, errInvalidStoredSession
	}

	if !record.Started && !nanocodex.EmptyRows(rows) {
		return storedSession{}, errInvalidStoredSession
	}

	return storedSession{rows: rows, record: record, found: true}, nil
}

func (s *session) hydrate(stored storedSession) error {
	if !nanocodex.ValidSessionID(stored.record.NativeSessionID) {
		return wire.RestoreFailed(vendor)
	}

	path := filepath.Join(s.home, stored.record.RolloutRelative)
	if _, err := nanocodex.RolloutRelative(s.home, path); err != nil {
		return wire.RestoreFailed(vendor)
	}

	native, err := nanocodex.ReadRows(s.home, path, -1)
	if err != nil {
		return wire.RestoreFailed(vendor)
	}

	rows, nativeWins, err := sessionlog.Reconcile(native, stored.rows)
	if err != nil {
		return wire.RestoreFailed(vendor)
	}

	if err := nanocodex.ValidateRows(rows, stored.record.NativeSessionID); err != nil {
		return wire.RestoreFailed(vendor)
	}

	if nativeWins {
		info, statErr := os.Stat(path)
		if statErr != nil {
			return wire.RestoreFailed(vendor)
		}

		var completeBytes int64
		for _, row := range rows {
			completeBytes += int64(len(row) + 1)
		}

		nativeWins = info.Size() == completeBytes
	}

	if !nativeWins {
		if err := nanocodex.WriteRows(s.home, path, rows); err != nil {
			return wire.RestoreFailed(vendor)
		}
	}

	s.mu.Lock()
	s.state.NativeSessionID = stored.record.NativeSessionID
	s.state.RolloutPath = path
	s.rows = rows
	s.started = !nanocodex.EmptyRows(rows)
	s.title = stored.record.Title
	s.updatedAt = stored.record.UpdatedAtUnixMilli
	s.mu.Unlock()

	return nil
}

// LoadSession restores native state and replays the conversation to the client.
func (a *Agent) LoadSession(ctx context.Context, p acp.LoadSessionRequest) (acp.LoadSessionResponse, error) {
	response, err := a.restore(ctx, p.SessionId, p.Cwd, p.AdditionalDirectories, p.McpServers, p.Meta, true)
	if err != nil {
		return acp.LoadSessionResponse{}, err
	}

	return *response, nil
}

// ResumeSession restores native state without replaying it to the client.
func (a *Agent) ResumeSession(ctx context.Context, p acp.ResumeSessionRequest) (acp.ResumeSessionResponse, error) {
	response, err := a.restore(ctx, p.SessionId, p.Cwd, p.AdditionalDirectories, p.McpServers, p.Meta, false)
	if err != nil {
		return acp.ResumeSessionResponse{}, err
	}

	return acp.ResumeSessionResponse{Meta: response.Meta, ConfigOptions: response.ConfigOptions}, nil
}

func (a *Agent) restore(ctx context.Context, id acp.SessionId, cwd string, dirs []string, mcp []acp.McpServer, meta map[string]any, replay bool) (*acp.LoadSessionResponse, error) {
	if err := wire.CheckSessionID(id); err != nil {
		return nil, err
	}

	if err := wire.RefuseSessionMeta(meta); err != nil {
		return nil, err
	}

	parsed, err := a.validateStart(cwd, dirs, mcp, meta)
	if err != nil {
		return nil, err
	}

	ctx, finishStart, err := a.beginEstablishment(ctx)
	if err != nil {
		return nil, err
	}
	defer finishStart()

	release, err := a.restores.Acquire(id)
	if err != nil {
		return nil, err
	}
	defer release()

	a.mu.Lock()
	active := a.sessions[id]
	deleted := a.deleted[id]
	a.mu.Unlock()

	if deleted {
		return nil, wire.UnknownSession()
	}

	if active != nil {
		gate, gateErr := wire.AcquireSessionGate(active.gate, "session_restore")
		if gateErr != nil {
			return nil, gateErr
		}

		if openErr := active.requireOpen(); openErr != nil {
			gate()

			return nil, openErr
		}

		active.mu.Lock()
		active.closing = true
		active.mu.Unlock()
		gate()

		if err = active.close(); err != nil {
			a.detach(active)

			return nil, err
		}

		a.detach(active)
	}

	stored, err := a.loadStored(ctx, id)
	if err != nil {
		if errors.Is(err, errInvalidStoredSession) {
			return nil, wire.RestoreFailed(vendor)
		}

		return nil, err
	}

	if !stored.found {
		return nil, wire.UnknownSession()
	}

	parsed.options = parsed.inherit(stored.record.Options)
	if stored.record.Started && parsed.options.Model != stored.record.Options.Model {
		return nil, wire.Unsupported(wire.MetaOptionPath(vendor, "model"))
	}

	slot, err := a.reserveSlot()
	if err != nil {
		return nil, err
	}
	defer slot()

	s, err := a.newSession(id, cwd, parsed, false)
	if err != nil {
		return nil, err
	}

	s.state = nanocodex.State{NativeSessionID: stored.record.NativeSessionID, RolloutPath: filepath.Join(s.home, stored.record.RolloutRelative)}
	s.rows = stored.rows
	s.started = stored.record.Started
	s.title = stored.record.Title
	s.updatedAt = stored.record.UpdatedAtUnixMilli

	if err = s.observeNative(ctx); err != nil {
		return nil, nativeLaunchError(err, parsed.present)
	}

	if err = s.commitMirror(ctx); err != nil {
		return nil, wire.InternalFailure(vendor, "")
	}

	response := &acp.LoadSessionResponse{Meta: wire.NativeSessionMeta(vendor, s.state.NativeSessionID), ConfigOptions: s.configOptions()}
	s.gate <- struct{}{}

	var replayErr error

	defer func() {
		<-s.gate

		if replayErr != nil {
			_ = s.close()
			a.detach(s)
		}
	}()

	if installErr := a.install(s); installErr != nil {
		return nil, installErr
	}

	slot()

	if replay {
		if replayErr = s.replay(ctx); replayErr != nil {
			s.mu.Lock()
			s.closing = true
			s.mu.Unlock()

			if requestErr, ok := errors.AsType[*acp.RequestError](replayErr); ok {
				return nil, requestErr
			}

			return nil, wire.InternalFailure(vendor, "")
		}
	}

	return response, nil
}
