package nanocodexacp

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/google/uuid"
	"github.com/savid/acp-go-core/lifecycle"
	"github.com/savid/acp-go-core/process"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-nanocodex/internal/nanocodex"
)

const nativeTimeout = 20 * time.Second

type runtime struct {
	proc          *process.Process
	client        *nanocodex.Client
	locks         []*process.FileLock
	locksReleased atomic.Bool
	cleanupErr    error
}

type sessionBinding struct {
	state   nanocodex.State
	options NanocodexOptions
	rows    [][]byte
	started bool
}

type session struct {
	agent     *Agent
	id        acp.SessionId
	cwd       string
	ephemeral bool
	gate      chan struct{}
	mu        sync.Mutex
	closing   bool
	closed    chan struct{}
	closeOnce sync.Once
	closeErr  error
	poisoned  bool
	options   NanocodexOptions
	state     nanocodex.State
	binding   *sessionBinding
	home      string
	title     string
	updatedAt int64
	started   bool
	rows      [][]byte
	rt        *runtime
	turn      *turn
	lc        lifecycle.Publisher
	raw       *wire.RawEvents
}

func (a *Agent) environment(options NanocodexOptions) process.Environment {
	owned := make(map[string]string)
	if a.options.Home != "" {
		owned["CODEX_HOME"] = a.options.Home
	}

	return process.Environment{Process: a.processEnv, Agent: a.options.Env, Session: options.Env, Owned: owned, ExtraPathDirs: options.ExtraPathDirs, InternalPrefix: "ACP_GO_NANOCODEX_INTERNAL_"}
}

func (a *Agent) nativeHome(options NanocodexOptions, cwd string) (string, error) {
	env, err := a.environment(options).Build()
	if err != nil {
		return "", err
	}

	home, _ := process.Lookup(env, "CODEX_HOME")
	if home == "" {
		base, _ := process.Lookup(env, "HOME")
		if base == "" {
			return "", errors.New("native home is unavailable")
		}

		home = filepath.Join(base, ".codex")
	}

	if !filepath.IsAbs(home) {
		home = filepath.Join(cwd, home)
	}

	if err := os.MkdirAll(home, 0o700); err != nil {
		return "", err
	}

	return filepath.EvalSymlinks(home)
}

func (s *session) launch(ctx context.Context) (*runtime, error) {
	id := s.state.NativeSessionID
	if id == "" {
		generated, generateErr := uuid.NewV7()
		if generateErr != nil {
			return nil, wire.InternalFailure(vendor, "native_start")
		}

		id = generated.String()
	}

	lock, err := process.LockFile(filepath.Join(s.home, "nanocodex", "acp-locks", id+".lock"))
	if err != nil {
		if s.state.NativeSessionID == "" {
			return nil, wire.InternalFailure(vendor, "native_start")
		}

		return nil, wire.RestoreFailed(vendor)
	}

	locks := []*process.FileLock{lock}

	transferred := false
	defer func() {
		if !transferred {
			for _, held := range locks {
				_ = held.Close()
			}
		}
	}()

	if len(s.rows) > 0 {
		rel, relErr := nanocodex.RolloutRelative(s.home, s.state.RolloutPath)
		if relErr != nil {
			return nil, wire.RestoreFailed(vendor)
		}

		if hydrateErr := s.hydrate(storedSession{rows: s.rows, record: sessionRecord{NativeSessionID: s.state.NativeSessionID, RolloutRelative: rel, Title: s.title, UpdatedAtUnixMilli: s.updatedAt}}); hydrateErr != nil {
			return nil, hydrateErr
		}
	}

	if nanocodex.EmptyRows(s.rows) {
		generated, generateErr := uuid.NewV7()
		if generateErr != nil {
			return nil, wire.InternalFailure(vendor, "native_start")
		}

		id = generated.String()

		lock, err = process.LockFile(filepath.Join(s.home, "nanocodex", "acp-locks", id+".lock"))
		if err != nil {
			return nil, wire.InternalFailure(vendor, "native_start")
		}

		locks = append(locks, lock)
	}

	base, err := s.agent.environment(NanocodexOptions{}).Base()
	if err != nil {
		return nil, wire.InternalFailure(vendor, "native_start")
	}

	executable, err := process.ResolveExecutable(cmp.Or(s.agent.options.ExecutablePath, "acp-go-nanocodex-native"), base)
	if err != nil {
		return nil, wire.InternalFailure(vendor, "native_start")
	}

	environment := s.agent.environment(s.options)
	environment.Owned["CODEX_HOME"] = s.home

	env, err := environment.Build()
	if err != nil {
		return nil, wire.InternalFailure(vendor, "native_start")
	}

	if err = process.WriteSeedFiles(s.home, s.agent.options.SeedFiles); err != nil {
		if refusal := wire.SeedFileRefusal(err); refusal != nil {
			return nil, refusal
		}

		return nil, wire.InternalFailure(vendor, "native_start")
	}

	proc, err := process.Start(ctx, process.Request{Executable: executable, Dir: s.cwd, Env: env})
	if err != nil {
		return nil, wire.InternalFailure(vendor, "native_start")
	}

	rt := &runtime{proc: proc, client: nanocodex.NewClient(proc.Stdin(), proc.Stdout()), locks: locks}
	transferred = true

	initCtx, cancel := context.WithTimeout(ctx, nativeTimeout)
	defer cancel()

	var state nanocodex.State
	if err = rt.client.Call(initCtx, "initialize", s.options.initialize(id, s.state.NativeSessionID), &state, nil); err != nil {
		_ = rt.stop()

		if nativeErr, ok := errors.AsType[*nanocodex.Error](err); ok {
			s.agent.log.ErrorContext(ctx, "native initialization refused", slog.String("code", nativeErr.Code), slog.String("field", nativeErr.Field), slog.String("reason", nativeErr.Message))
		} else {
			s.agent.log.DebugContext(ctx, "native initialization failed", slog.Any("error", err))
		}

		if nativeErr, ok := errors.AsType[*nanocodex.Error](err); ok {
			switch nativeErr.Code {
			case nanocodex.InvalidConfig:
				return nil, nativeErr
			case "restore_failed":
				return nil, wire.RestoreFailed(vendor)
			}
		}

		return nil, wire.InternalFailure(vendor, "native_start")
	}

	if state.NativeSessionID != id {
		_ = rt.stop()

		return nil, wire.InternalFailure(vendor, "native_state")
	}

	if err = s.applyState(state); err != nil {
		_ = rt.stop()

		return nil, err
	}

	s.mu.Lock()
	s.rt = rt
	s.mu.Unlock()

	return rt, nil
}

func (rt *runtime) stop() error {
	callCtx, callCancel := context.WithTimeout(context.Background(), nativeTimeout)
	callErr := rt.client.Call(callCtx, "shutdown", struct{}{}, nil, nil)

	callCancel()

	if errors.Is(callErr, nanocodex.ErrClosed) {
		select {
		case <-rt.proc.Done():
			result, waitErr := rt.proc.Wait(context.Background())
			if waitErr == nil && result.ExitCode == 0 && result.Signal == 0 {
				callErr = nil
			}
		default:
		}
	}

	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), nativeTimeout)
	shutdownErr := rt.proc.Shutdown(cleanupCtx, 2*time.Second)

	cleanupCancel()

	if shutdownErr != nil {
		killErr := rt.proc.Kill()
		waitCtx, waitCancel := context.WithTimeout(context.Background(), nativeTimeout)
		_, waitErr := rt.proc.Wait(waitCtx)

		waitCancel()

		shutdownErr = errors.Join(shutdownErr, killErr, waitErr)
	}

	closeErr := rt.proc.Close()
	<-rt.client.Done()

	var lockErr error
	for _, lock := range rt.locks {
		lockErr = errors.Join(lockErr, lock.Close())
	}

	rt.locksReleased.Store(true)
	rt.cleanupErr = errors.Join(shutdownErr, closeErr, lockErr)

	return errors.Join(callErr, rt.cleanupErr)
}

func (s *session) applyState(state nanocodex.State) error {
	if state.ProtocolVersion != 1 || state.HelperVersion != nanocodex.HelperVersion || state.HelperFingerprint != nativeHelperFingerprint || !nanocodex.ValidSessionID(state.NativeSessionID) || state.Model == "" || state.CommittedBytes < 0 || !filepath.IsAbs(state.RolloutPath) {
		return wire.InternalFailure(vendor, "native_state")
	}

	if state.ReplacedNativeSessionID != "" && state.ReplacedNativeSessionID == state.NativeSessionID {
		return wire.InternalFailure(vendor, "native_state")
	}

	if _, err := nanocodex.RolloutRelative(s.home, state.RolloutPath); err != nil {
		return wire.InternalFailure(vendor, "native_state")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.state.NativeSessionID != "" && s.state.NativeSessionID != state.NativeSessionID {
		if s.started || !nanocodex.EmptyRows(s.rows) || state.ReplacedNativeSessionID != s.state.NativeSessionID {
			s.poisoned = true

			return wire.SessionPoisoned(vendor, "native_session_identity_drift")
		}

		s.binding = &sessionBinding{state: s.state, options: s.options.clone(), rows: s.rows, started: s.started}
	}

	s.state = state
	s.options.Model = state.Model
	s.options.Thinking = state.Thinking

	return nil
}

func (s *session) observeNative(ctx context.Context) error {
	rt, err := s.launch(ctx)
	if err != nil {
		return err
	}

	err = rt.stop()

	s.mu.Lock()
	s.rt = nil
	s.mu.Unlock()

	if err != nil {
		s.rollbackBinding()

		return wire.InternalFailure(vendor, "native_start")
	}

	return nil
}

func (s *session) rollbackBinding() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if binding := s.binding; binding != nil {
		s.state = binding.state
		s.options = binding.options
		s.rows = binding.rows
		s.started = binding.started
		s.binding = nil
	}
}

func nativeLaunchError(err error, supplied map[string]bool) error {
	if requestErr, ok := errors.AsType[*acp.RequestError](err); ok {
		return requestErr
	}

	if nativeErr, ok := errors.AsType[*nanocodex.Error](err); ok && nativeErr.Code == nanocodex.InvalidConfig && supplied[nativeErr.Field] {
		return wire.Unsupported(wire.MetaOptionPath(vendor, nativeErr.Field))
	}

	return wire.InternalFailure(vendor, "native_start")
}

func (s *session) close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closing = true
		t := s.turn
		s.mu.Unlock()

		if t != nil {
			s.cancel()

			timer := time.NewTimer(nativeTimeout)
			select {
			case <-t.done:
			case <-timer.C:
				s.mu.Lock()
				rt := s.rt
				s.mu.Unlock()

				if rt != nil {
					select {
					case <-rt.proc.Done():
					default:
						s.abortTurn(t, rt)
					}
				}
			}

			timer.Stop()
			<-t.done

			if t.cleanupErr != nil {
				s.closeErr = wire.InternalFailure(vendor, "")
			}
		}
		// Establishing and config operations own the same foreground gate.
		s.gate <- struct{}{}
		defer func() { <-s.gate; close(s.closed) }()

		if err := s.commitMirror(context.Background()); err != nil {
			s.closeErr = wire.InternalFailure(vendor, "")
		}

		s.lc.Fence()
	})
	<-s.closed

	return s.closeErr
}

func (s *session) abortTurn(t *turn, rt *runtime) {
	t.abortOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), nativeTimeout)
		defer cancel()

		if err := rt.proc.Shutdown(ctx, 2*time.Second); err != nil {
			_ = rt.proc.Kill()
			waitCtx, waitCancel := context.WithTimeout(context.Background(), nativeTimeout)
			_, _ = rt.proc.Wait(waitCtx)

			waitCancel()
		}

		_ = rt.proc.Close()
	})
}

func (s *session) emit(ctx context.Context, update acp.SessionUpdate) error {
	if conn := s.agent.connection(); conn != nil {
		return conn.SessionUpdate(ctx, acp.SessionNotification{SessionId: s.id, Update: update})
	}

	return nil
}

func (s *session) emitLifecycle(ctx context.Context, envelope map[string]any) error {
	if conn := s.agent.connection(); conn != nil {
		return conn.SessionUpdate(ctx, wire.LifecycleCarrier(s.id, envelope))
	}

	return nil
}

func (s *session) requireOpen() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closing {
		return wire.UnknownSession()
	}

	if s.poisoned {
		return wire.SessionPoisoned(vendor, "native_session_identity_drift")
	}

	return nil
}

func (s *session) processFailure(rt *runtime, err error) error {
	if requestErr, ok := errors.AsType[*acp.RequestError](err); ok {
		return requestErr
	}

	if nativeErr, ok := errors.AsType[*nanocodex.Error](err); ok {
		// Provider rejections and failed provider connections are provider
		// failures; other native codes report invalid provider data or helper
		// failures.
		cause := wire.CauseTransport
		if nativeErr.Code == "native_error" || nativeErr.Code == "connection_error" {
			cause = wire.CauseProvider
		}

		return wire.TurnFailed(vendor, wire.TurnFailure{Cause: cause, Message: nativeErr.Message, StatusCode: nativeErr.StatusCode, ProviderCode: nativeErr.ProviderCode})
	}

	cause := wire.CauseTransport
	message := fmt.Sprintf("native helper protocol: %v", err)

	if errors.Is(err, nanocodex.ErrClosed) {
		cause = wire.CauseProcessExit
		result, waitErr := rt.proc.Wait(context.Background())

		message = fmt.Sprintf("native process exited (code %d, signal %d): %s", result.ExitCode, result.Signal, rt.proc.StderrTail())
		if waitErr != nil {
			message = fmt.Sprintf("native process wait failed: %v; %s", waitErr, rt.proc.StderrTail())
		}
	}

	return wire.TurnFailed(vendor, wire.TurnFailure{Cause: cause, Message: message})
}
