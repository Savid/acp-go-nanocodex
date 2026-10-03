package nanocodexacp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-core/image"
	"github.com/savid/acp-go-core/lifecycle"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-nanocodex/internal/nanocodex"
)

type turn struct {
	done       chan struct{}
	cancelled  atomic.Bool
	cancelOnce sync.Once
	abortOnce  sync.Once
	accepted   atomic.Bool
	submission lifecycle.Submission
	cycle      lifecycle.Cycle
	response   acp.PromptResponse
	err        error
	cleanupErr error
	texts      map[string]string
	hasText    bool
	nativeSeq  uint64
}

// Prompt retains foreground ownership through persistence and terminal publication.
func (a *Agent) Prompt(ctx context.Context, p acp.PromptRequest) (resp acp.PromptResponse, err error) {
	if refusal := wire.CheckSessionID(p.SessionId); refusal != nil {
		return resp, refusal
	}

	s, err := a.lookup(p.SessionId)
	if err != nil {
		return resp, err
	}

	if transport := a.transportRef(); transport != nil {
		p.Meta = lifecycle.RetainRequestMetadata(p.Meta, transport.TakeRawPrompt(p.SessionId, p.Meta))
	}

	submission, refusal := lifecycle.DecodePromptCorrelation(p.Meta, a.lifecycleNegotiated())
	if refusal != nil {
		return resp, wire.ParamRefusal(refusal)
	}

	release, err := wire.AcquireSessionGate(s.gate, "session_prompt")
	if err != nil {
		return resp, err
	}

	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		release()

		return resp, wire.UnknownSession()
	}

	if s.poisoned {
		s.mu.Unlock()
		release()

		return resp, wire.SessionPoisoned(vendor, "native_session_identity_drift")
	}

	t := &turn{done: make(chan struct{}), submission: submission, texts: make(map[string]string)}
	s.turn = t
	s.mu.Unlock()

	go func() {
		defer close(t.done)
		defer release()

		workCtx, finish := a.observe.StartPrompt(context.WithoutCancel(ctx), p.Meta, s.options.Model)

		content, validationErr := s.mapPrompt(workCtx, p.Prompt)
		switch {
		case t.cancelled.Load():
			t.response = wire.CancelledResponse(p)
		case validationErr != nil:
			t.err = validationErr
		default:
			t.response, t.err = s.runPrompt(workCtx, p, content, t)
		}

		finish(observerPromptResult(t.response, t.err, s.options.Model))
		s.mu.Lock()
		if s.turn == t {
			s.turn = nil
		}
		s.mu.Unlock()
	}()

	<-t.done

	return t.response, t.err
}

func (s *session) mapPrompt(ctx context.Context, blocks []acp.ContentBlock) ([]nanocodex.Content, error) {
	decoded, refusal, err := image.ValidatePrompt(ctx, blocks, image.Options{Limits: s.agent.options.ImageLimits.core(), HandoffRoot: s.agent.options.InputHandoffRoot, Blobs: func(string) image.BlobDisposition { return image.BlobRefuse }})
	if err != nil {
		return nil, err
	}

	if refusal != nil {
		return nil, refusal.InvalidParams()
	}

	media := make(map[int]image.Decoded, len(decoded))
	for _, item := range decoded {
		media[item.Block] = item
	}

	content := make([]nanocodex.Content, 0, len(blocks))
	for i, block := range blocks {
		if img, ok := media[i]; ok {
			if block.Image != nil && wire.AudienceIsUserOnly(block.Image.Annotations) {
				continue
			}

			if block.Resource != nil && wire.AudienceIsUserOnly(block.Resource.Annotations) {
				continue
			}

			content = append(content, nanocodex.Content{Type: "image", ImageURL: "data:" + img.MIME + ";base64," + base64.StdEncoding.EncodeToString(img.Data)})

			continue
		}

		text := ""

		switch {
		case block.Text != nil:
			if wire.AudienceIsUserOnly(block.Text.Annotations) {
				continue
			}

			text = block.Text.Text
		case block.ResourceLink != nil:
			if wire.AudienceIsUserOnly(block.ResourceLink.Annotations) {
				continue
			}

			text = block.ResourceLink.Uri
		case block.Resource != nil && block.Resource.Resource.TextResourceContents != nil:
			if wire.AudienceIsUserOnly(block.Resource.Annotations) {
				continue
			}

			resource := block.Resource.Resource.TextResourceContents
			text = wire.ContextResourceText(resource.Uri, resource.Text)
		default:
			return nil, wire.Unsupported("prompt")
		}

		if text != "" {
			content = append(content, nanocodex.Content{Type: "text", Text: text})
		}
	}

	if len(content) == 0 {
		return nil, wire.Unsupported("prompt")
	}

	encoded, err := json.Marshal(map[string]any{"content": content})
	if err != nil || len(encoded) > nanocodex.MaxPromptBytes {
		return nil, acp.NewInvalidParams("prompt content exceeds 12 MiB")
	}

	return content, nil
}

func (s *session) runPrompt(ctx context.Context, p acp.PromptRequest, content []nanocodex.Content, t *turn) (resp acp.PromptResponse, err error) {
	rt, err := s.launch(ctx)
	if err != nil {
		return resp, nativeLaunchError(err, nil)
	}
	defer func() { s.mu.Lock(); s.rt = nil; s.mu.Unlock(); s.lc.Fence() }()
	// A pre-turn binding replacement must be durable before native admission.
	if err = s.commitMirror(ctx); err != nil {
		_ = rt.stop()
		t.cleanupErr = rt.cleanupErr

		return resp, wire.TurnFailed(vendor, wire.TurnFailure{Cause: wire.CauseTransport, Message: "session mirror commit failed"})
	}

	if t.cancelled.Load() {
		_ = rt.stop()
		t.cleanupErr = rt.cleanupErr

		return wire.CancelledResponse(p), nil
	}

	var (
		result       nanocodex.Result
		invariantErr error
	)

	err = rt.client.Call(ctx, "prompt", map[string]any{"content": content}, &result, func(frame nanocodex.Frame) error { return s.promptEvent(ctx, t, frame) })
	if err == nil {
		invariantErr = s.applyPromptResult(t, result)
		err = invariantErr
	}

	if err != nil {
		// Native failures flush their committed prefix; query it while idle.
		stateCtx, cancel := context.WithTimeout(context.Background(), nativeTimeout)

		var state nanocodex.State
		if stateErr := rt.client.Call(stateCtx, "state", struct{}{}, &state, nil); stateErr == nil {
			if applyErr := s.applyState(state); applyErr != nil {
				invariantErr = applyErr
				err = applyErr
			}
		}

		cancel()
	}

	if err == nil && !t.cancelled.Load() && !t.hasText && result.FinalMessage != "" {
		err = s.emit(ctx, acp.UpdateAgentMessageText(result.FinalMessage))
		t.hasText = true
	}

	stopErr := rt.stop()

	t.cleanupErr = rt.cleanupErr

	if err == nil {
		err = stopErr
	}

	if t.accepted.Load() {
		s.mu.Lock()

		s.started = true
		if s.title == "" {
			s.title = wire.PromptTitle(p.Prompt)
		}
		s.mu.Unlock()
	}

	commitErr := s.commitMirror(ctx)

	resp = acp.PromptResponse{StopReason: acp.StopReasonEndTurn, UserMessageId: p.MessageId, Meta: wire.NativeSessionMeta(vendor, s.state.NativeSessionID)}
	outcome := lifecycle.OutcomeSuccess

	switch {
	case invariantErr != nil:
		outcome = lifecycle.OutcomeFailed
		err = invariantErr
	case t.cancelled.Load() || result.StopReason == "cancelled":
		resp.StopReason = acp.StopReasonCancelled
		outcome = lifecycle.OutcomeCancelled
		err = nil
	case err != nil:
		outcome = lifecycle.OutcomeFailed
		err = s.processFailure(rt, err)
	case result.StopReason != "end_turn":
		outcome = lifecycle.OutcomeFailed
		err = wire.TurnFailed(vendor, wire.TurnFailure{Cause: wire.CauseTransport, Message: "unrecognized native stop reason"})
	}

	if commitErr != nil {
		if err == nil {
			err = wire.TurnFailed(vendor, wire.TurnFailure{Cause: wire.CauseTransport, Message: "session mirror commit failed"})
		}

		return acp.PromptResponse{}, err
	}

	if result.Usage != nil {
		resp.Usage = promptUsage(*result.Usage)
	}

	if t.accepted.Load() {
		if idleErr := s.lc.Idle(ctx, t.cycle, string(resp.StopReason), outcome); idleErr != nil {
			return acp.PromptResponse{}, wire.InternalFailure(vendor, "lifecycle")
		}
	}

	if err != nil {
		return acp.PromptResponse{}, err
	}

	return resp, nil
}

func (s *session) applyPromptResult(t *turn, result nanocodex.Result) error {
	if !t.accepted.Load() {
		return wire.TurnFailed(vendor, wire.TurnFailure{Cause: wire.CauseTransport, Message: "native prompt completed without acceptance"})
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if result.NativeSessionID != s.state.NativeSessionID || result.RolloutPath != s.state.RolloutPath {
		s.poisoned = true

		return wire.SessionPoisoned(vendor, "native_session_identity_drift")
	}

	s.state.CommittedBytes = result.CommittedBytes

	return nil
}

func (s *session) promptEvent(ctx context.Context, t *turn, frame nanocodex.Frame) error {
	if frame.Event == "accepted" {
		if !t.accepted.CompareAndSwap(false, true) {
			return errors.New("duplicate native acceptance")
		}

		if err := s.lc.Open(ctx, lifecycle.NewIncarnation(string(s.id)), s.agent.lifecycleNegotiated(), s.emitLifecycle); err != nil {
			return err
		}

		if err := s.lc.Accept(ctx, &t.cycle, t.submission); err != nil {
			return err
		}

		if t.cancelled.Load() {
			s.sendCancel(t)
		}

		return nil
	}

	if !t.accepted.Load() {
		return errors.New("native content preceded acceptance")
	}

	if t.cancelled.Load() {
		return nil
	}

	if frame.Event == "native" {
		var event nanocodex.Event
		if err := json.Unmarshal(frame.Data, &event); err != nil {
			return err
		}

		if event.Seq <= t.nativeSeq {
			return errors.New("native event sequence regressed")
		}

		t.nativeSeq = event.Seq

		if conn := s.agent.connection(); conn != nil {
			var raw map[string]any
			if err := json.Unmarshal(frame.Data, &raw); err != nil {
				return err
			}

			omitRawImages(raw)

			if err := s.raw.Emit(ctx, func(ctx context.Context, method string, params map[string]any) error {
				return conn.NotifyExtension(ctx, method, params)
			}, raw); err != nil {
				s.agent.observe.RecordRawMessageEmitFailure(ctx, err)
			}
		}

		return s.nativeUpdate(ctx, t, event)
	}

	return s.textUpdate(ctx, t, frame.Event, frame.Data)
}

func (s *session) cancel() {
	s.mu.Lock()
	t := s.turn
	s.mu.Unlock()

	if t == nil {
		return
	}

	t.cancelled.Store(true)
	s.sendCancel(t)
}

func (s *session) sendCancel(t *turn) {
	if !t.accepted.Load() {
		return
	}

	s.mu.Lock()
	rt := s.rt
	s.mu.Unlock()

	if rt == nil {
		return
	}

	t.cancelOnce.Do(func() {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), nativeTimeout)
			defer cancel()

			if err := rt.client.Call(ctx, "cancel", struct{}{}, nil, nil); err != nil {
				select {
				case <-t.done:
				case <-rt.client.Done():
				default:
					s.abortTurn(t, rt)
				}
			}
		}()
	})
}
