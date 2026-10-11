package nanocodexacp

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"time"

	"github.com/coder/acp-go-sdk"
	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/observer"
	"github.com/savid/acp-go-core/process"
	"github.com/savid/acp-go-core/sessionlog"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-nanocodex/internal/nanocodex"
)

type sessionRecord struct {
	Checkpoint         json.RawMessage  `json:"checkpoint,omitempty"`
	SessionID          string           `json:"sessionId"`
	NativeSessionID    string           `json:"nativeSessionId"`
	Cwd                string           `json:"cwd"`
	RolloutRelative    string           `json:"rolloutRelative"`
	Options            NanocodexOptions `json:"options"`
	Title              string           `json:"title,omitempty"`
	Started            bool             `json:"started"`
	UpdatedAtUnixMilli int64            `json:"updatedAtUnixMilli"`
	CostUSD            *float64         `json:"costUsd,omitempty"`
}

type storedSession struct {
	rows   [][]byte
	record sessionRecord
	found  bool
}

// updatedAtLayout formats session update times on the wire.
const updatedAtLayout = "2006-01-02T15:04:05.000Z"

func formatUpdatedAt(at time.Time) string { return at.UTC().Format(updatedAtLayout) }

func updatedAtMilli(value string) int64 {
	at, err := time.Parse(updatedAtLayout, value)
	if err != nil {
		return time.Now().UnixMilli()
	}

	return at.UnixMilli()
}

// commitMirror publishes the native rows appended since the last commit with
// the current configuration. The mirror reads only the rows after its
// committed position; the record carries the update time of the session's
// last turn or configuration change, so a commit that adds nothing writes
// nothing.
func (s *session) commitMirror(ctx context.Context) error { return s.commit(ctx, false) }

// commit is commitMirror for a caller that may already hold the native
// session lock.
func (s *session) commit(ctx context.Context, sessionLocked bool) (err error) {
	s.mu.Lock()
	state := s.state
	home := s.home
	binding := s.binding
	rt := s.rt
	s.mu.Unlock()

	if binding != nil {
		s.mirror.Reset()

		defer func() {
			if err != nil {
				s.rollbackBinding()
			}
		}()
	}

	if state.NativeSessionID == "" || s.ephemeral {
		s.mu.Lock()
		s.binding = nil
		s.mu.Unlock()

		return nil
	}

	limit, unlock, err := lockRollout(home, state, rt, sessionLocked)
	if err != nil {
		return err
	}
	defer unlock()

	rel, err := nanocodex.RolloutRelative(home, state.RolloutPath)
	if err != nil {
		return err
	}

	source := func(from sessionlog.Cursor) (sessionlog.Tail, error) {
		tail, readErr := nanocodex.ReadRows(home, state.RolloutPath, from, limit)
		if readErr != nil {
			return sessionlog.Tail{}, readErr
		}

		if tail.Full {
			return tail, nanocodex.ValidateRows(tail.Rows, state.NativeSessionID)
		}

		return tail, nanocodex.ValidateTail(tail.Rows)
	}

	var (
		checkpoint json.RawMessage
		stamp      nanocodex.CheckpointStamp
	)

	// The checkpoint is read after the rows, so the record describes them. A
	// conversation is started once its rollout holds more than its header.
	record := func(tail sessionlog.Tail) (any, error) {
		s.mu.Lock()
		defer s.mu.Unlock()

		if tail.Full {
			s.started = s.started || len(tail.Rows) != 1
		} else {
			s.started = s.started || len(tail.Rows) > 0
		}

		data, current, changed, readErr := nanocodex.ReadCheckpoint(home, state.RolloutPath, s.checkpointStamp)
		checkpoint, stamp = s.checkpoint, current

		switch {
		case readErr != nil:
			s.agent.log.WarnContext(ctx, "native checkpoint unavailable; mirroring native history")

			checkpoint = nil
		case changed:
			checkpoint = data
		}

		var cost *float64
		if s.cost != nil {
			cost = new(*s.cost)
		}

		return sessionRecord{Checkpoint: checkpoint, SessionID: string(s.id), NativeSessionID: state.NativeSessionID, Cwd: s.cwd, RolloutRelative: rel, Options: s.options.clone(), Title: s.title, Started: s.started, UpdatedAtUnixMilli: updatedAtMilli(s.updatedAt), CostUSD: cost}, nil
	}

	storeCtx, finish := s.agent.observe.StartSessionStore(ctx, observer.SessionStoreCommit)
	err = s.mirror.Commit(storeCtx, source, record)
	finish(err)

	if err != nil {
		return err
	}

	s.mu.Lock()
	s.checkpoint = checkpoint
	s.checkpointStamp = stamp
	s.binding = nil
	s.mu.Unlock()

	return nil
}

// lockRollout returns the byte boundary a commit may read: the helper's
// committed bytes while it runs, else the whole rollout. A stopped rollout is
// read under the writer lock, and the native session lock unless the caller
// holds it, after its final record is settled.
func lockRollout(home string, state nanocodex.State, rt *runtime, sessionLocked bool) (int64, func(), error) {
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

	if limit >= 0 {
		return limit, func() {}, nil
	}

	var locks []*process.FileLock

	unlock := func() {
		for _, lock := range locks {
			_ = lock.Close()
		}
	}

	names := []string{state.NativeSessionID + ".writer.lock"}
	if !sessionLocked && (rt == nil || len(rt.locks) == 0 || rt.locksReleased.Load()) {
		names = []string{state.NativeSessionID + ".lock", state.NativeSessionID + ".writer.lock"}
	}

	for _, name := range names {
		lock, err := process.LockFile(filepath.Join(home, "nanocodex", "acp-locks", name))
		if err != nil {
			unlock()

			return 0, nil, err
		}

		locks = append(locks, lock)
	}

	if err := nanocodex.RepairTail(home, state.RolloutPath); err != nil {
		unlock()

		return 0, nil, err
	}

	return limit, unlock, nil
}

var errInvalidStoredSession = errors.New("invalid stored session")

// storeLoadError marks a failure of the store itself. sessionlog reports a
// failed store read and an undecodable generation alike; a store outage is an
// internal failure, never a refused restore or a session listing skips.
type storeLoadError struct{ error }

// storeLoadBoundary tags the store's own Load failures as storeLoadError.
type storeLoadBoundary struct{ acpcore.SessionStore }

func (s storeLoadBoundary) Load(ctx context.Context, id string) (map[string][]acpcore.SessionStoreEntry, error) {
	generation, err := s.SessionStore.Load(ctx, id)
	if err != nil {
		return nil, &storeLoadError{err}
	}

	return generation, nil
}

// storedFailure separates a store that failed from a generation that is not
// a valid session.
func storedFailure(err error) error {
	if _, failed := errors.AsType[*storeLoadError](err); failed {
		return wire.InternalFailure(vendor, "")
	}

	return errInvalidStoredSession
}

// loadRecord reads and checks only the configuration of a stored session.
func (a *Agent) loadRecord(ctx context.Context, id acp.SessionId) (sessionRecord, bool, error) {
	if err := wire.CheckSessionID(id); err != nil {
		return sessionRecord{}, false, errInvalidStoredSession
	}

	var record sessionRecord

	found, err := sessionlog.LoadRecord(ctx, storeLoadBoundary{a.store}, string(id), &record)
	if err != nil {
		return sessionRecord{}, false, storedFailure(err)
	}

	if !found {
		return sessionRecord{}, false, nil
	}

	return record, true, validRecord(record, id)
}

func validRecord(record sessionRecord, id acp.SessionId) error {
	if record.SessionID != string(id) || !nanocodex.ValidSessionID(record.NativeSessionID) || !filepath.IsAbs(record.Cwd) || record.UpdatedAtUnixMilli <= 0 || !filepath.IsLocal(record.RolloutRelative) {
		return errInvalidStoredSession
	}

	if cost := record.CostUSD; cost != nil && *cost < 0 {
		return errInvalidStoredSession
	}

	if err := validateNanocodexOptions(record.Options); err != nil {
		return errInvalidStoredSession
	}

	return nil
}

func (a *Agent) loadStored(ctx context.Context, id acp.SessionId) (storedSession, error) {
	if err := wire.CheckSessionID(id); err != nil {
		return storedSession{}, errInvalidStoredSession
	}

	var record sessionRecord

	rows, found, err := sessionlog.Load(ctx, storeLoadBoundary{a.store}, string(id), &record)
	if err != nil {
		return storedSession{}, storedFailure(err)
	}

	if !found {
		return storedSession{}, nil
	}

	if err := validRecord(record, id); err != nil {
		return storedSession{}, err
	}

	if err := nanocodex.ValidateRows(rows, record.NativeSessionID); err != nil {
		return storedSession{}, errInvalidStoredSession
	}

	if record.Started == (len(rows) == 1) {
		return storedSession{}, errInvalidStoredSession
	}

	return storedSession{rows: rows, record: record, found: true}, nil
}

// hydrate reconciles the native rollout with a stored generation: native rows
// that extend the stored ones win, and a missing or shorter rollout is
// rewritten from the store. The stored rows are released once reconciled.
func (s *session) hydrate(ctx context.Context, stored *storedSession) error {
	if !nanocodex.ValidSessionID(stored.record.NativeSessionID) {
		return wire.RestoreFailed(vendor)
	}

	path := filepath.Join(s.home, stored.record.RolloutRelative)
	if _, err := nanocodex.RolloutRelative(s.home, path); err != nil {
		return wire.RestoreFailed(vendor)
	}

	lock, err := process.LockFile(filepath.Join(s.home, "nanocodex", "acp-locks", stored.record.NativeSessionID+".writer.lock"))
	if err != nil {
		return wire.RestoreFailed(vendor)
	}
	defer lock.Close()

	if err = nanocodex.RepairTail(s.home, path); err != nil {
		return wire.RestoreFailed(vendor)
	}

	storedRows := len(stored.rows)

	rows, err := s.mirror.Hydrate(ctx, path, stored.rows, stored.record, func(rows [][]byte) error {
		return nanocodex.ValidateRows(rows, stored.record.NativeSessionID)
	})
	stored.rows = nil

	if err != nil {
		return wire.RestoreFailed(vendor)
	}

	// Native rows newer than the store keep the native checkpoint, which the
	// next commit reads; otherwise the stored checkpoint is restored.
	checkpoint := stored.record.Checkpoint
	if len(rows) > storedRows {
		checkpoint = nil
	} else if err := nanocodex.WriteCheckpoint(s.home, path, checkpoint); err != nil {
		s.agent.log.WarnContext(ctx, "native checkpoint unavailable; restoring native history")
	}

	s.mu.Lock()
	s.state.NativeSessionID = stored.record.NativeSessionID
	s.state.RolloutPath = path
	s.checkpoint = checkpoint
	s.checkpointStamp = nanocodex.CheckpointStamp{}
	s.started = len(rows) != 1
	s.title = stored.record.Title
	s.mu.Unlock()

	return nil
}

// ensureNative keeps the native rollout of a committed session before a
// helper launch, while the session still holds its committed configuration.
// Committing adopts native rows that extend the store; a rollout that was
// removed, truncated, or no longer agrees is hydrated again from the store.
func (s *session) ensureNative(ctx context.Context) error {
	s.mu.Lock()
	state := s.state
	skip := state.NativeSessionID == "" || s.ephemeral || s.pending != nil
	s.mu.Unlock()

	if skip {
		return nil
	}

	lock, err := process.LockFile(filepath.Join(s.home, "nanocodex", "acp-locks", state.NativeSessionID+".lock"))
	if err != nil {
		return wire.RestoreFailed(vendor)
	}
	defer lock.Close()

	if s.commit(ctx, true) == nil {
		return nil
	}

	stored, err := s.agent.loadStored(ctx, s.id)
	if err != nil || !stored.found || stored.record.NativeSessionID != state.NativeSessionID || filepath.Join(s.home, stored.record.RolloutRelative) != state.RolloutPath {
		return wire.RestoreFailed(vendor)
	}

	return s.hydrate(ctx, &stored)
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

	parsed.options = inheritCarrier(parsed, stored.record)
	if stored.record.Started && parsed.options.Model != stored.record.Options.Model {
		return nil, wire.Unsupported(wire.MetaOptionPath(vendor, metaModelKey))
	}

	if stored.record.Started {
		if routeErr := pinRoute(stored.record.Options, parsed.options); routeErr != nil {
			return nil, routeErr
		}
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
	s.pending = &storedSession{rows: stored.rows, record: stored.record, found: true}
	stored.rows = nil
	s.checkpoint = stored.record.Checkpoint
	s.started = stored.record.Started
	s.title = stored.record.Title
	s.updatedAt = formatUpdatedAt(time.UnixMilli(stored.record.UpdatedAtUnixMilli))
	s.cost = stored.record.CostUSD

	if err = s.observeNative(ctx); err != nil {
		return nil, nativeLaunchError(err, parsed.present)
	}

	if err = s.commitMirror(ctx); err != nil {
		return nil, wire.InternalFailure(vendor, "")
	}

	response := &acp.LoadSessionResponse{Meta: wire.NativeSessionMeta(vendor, s.state.NativeSessionID), ConfigOptions: s.configOptions()}
	releaseReplay := wire.HoldSessionGate(s.gate)

	var replayErr error

	defer func() {
		releaseReplay()

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
