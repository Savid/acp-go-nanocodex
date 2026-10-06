package nanocodexacp

import (
	"context"
	"errors"
	"slices"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-nanocodex/internal/nanocodex"
)

func (s *session) configOptions() []acp.SessionConfigOption {
	s.mu.Lock()
	defer s.mu.Unlock()

	rows := make([]wire.ModelRow, 0, len(s.state.Models))
	thinking := make(acp.SessionConfigSelectOptionsUngrouped, 0)

	for _, model := range s.state.Models {
		metadata := make(map[string]any)
		if model.ContextWindow > 0 {
			metadata["contextWindow"] = model.ContextWindow
		}

		if len(model.Thinking) > 0 {
			metadata["supportedEffortLevels"] = slices.Clone(model.Thinking)
		}

		rows = append(rows, wire.ModelRow{ID: model.ID, Name: model.Name, Meta: metadata})
		if model.ID == s.options.Model {
			for _, effort := range model.Thinking {
				thinking = append(thinking, acp.SessionConfigSelectOption{Name: effort, Value: acp.SessionConfigValueId(effort)})
			}
		}
	}

	configured := append(slices.Clone(s.agent.options.ConfiguredModels), s.agent.options.DefaultModel)
	models := wire.ModelSelectOptions(vendor, s.options.Model, rows, configured)

	options := []acp.SessionConfigOption{{Select: &acp.SessionConfigOptionSelect{Id: configModel, Name: "Model", Type: "select", Category: new(acp.SessionConfigOptionCategoryModel), CurrentValue: acp.SessionConfigValueId(s.options.Model), Options: acp.SessionConfigSelectOptions{Ungrouped: &models}}}}
	if len(thinking) > 0 {
		options = append(options, acp.SessionConfigOption{Select: &acp.SessionConfigOptionSelect{Id: configThinking, Name: "Thought Level", Type: "select", Category: new(acp.SessionConfigOptionCategoryThoughtLevel), CurrentValue: acp.SessionConfigValueId(s.options.Thinking), Options: acp.SessionConfigSelectOptions{Ungrouped: &thinking}}})
	}

	return options
}

// SetSessionConfigOption validates native selectors and commits their configuration.
func (a *Agent) SetSessionConfigOption(ctx context.Context, params acp.SetSessionConfigOptionRequest) (acp.SetSessionConfigOptionResponse, error) {
	if params.ValueId == nil {
		if params.Boolean != nil {
			if err := wire.CheckSessionID(params.Boolean.SessionId); err != nil {
				return acp.SetSessionConfigOptionResponse{}, err
			}
		}

		return acp.SetSessionConfigOptionResponse{}, wire.Unsupported("type")
	}

	p := params.ValueId
	if err := wire.CheckSessionID(p.SessionId); err != nil {
		return acp.SetSessionConfigOptionResponse{}, err
	}

	if _, err := parseSessionMeta(p.Meta); err != nil {
		return acp.SetSessionConfigOptionResponse{}, err
	}

	s, err := a.lookup(p.SessionId)
	if err != nil {
		return acp.SetSessionConfigOptionResponse{}, err
	}

	release, err := wire.AcquireSessionGate(s.gate, "session_prompt")
	if err != nil {
		return acp.SetSessionConfigOptionResponse{}, err
	}
	defer release()

	if openErr := s.requireOpen(); openErr != nil {
		return acp.SetSessionConfigOptionResponse{}, openErr
	}

	s.mu.Lock()
	previousOptions := s.options.clone()
	previousState := s.state
	previousRows := s.rows
	candidate := s.options.clone()
	value := string(p.Value)

	switch p.ConfigId {
	case configModel:
		if !validModel(value) || (s.started && value != s.options.Model) {
			s.mu.Unlock()

			return acp.SetSessionConfigOptionResponse{}, wire.Unsupported("value")
		}

		if value != candidate.Model {
			candidate = candidate.forModelChange()
		}

		candidate.Model = value
	case configThinking:
		if value == "" {
			s.mu.Unlock()

			return acp.SetSessionConfigOptionResponse{}, wire.Unsupported("value")
		}

		candidate.Thinking = value
	default:
		s.mu.Unlock()

		return acp.SetSessionConfigOptionResponse{}, wire.Unsupported("configId")
	}

	s.options = candidate
	s.mu.Unlock()

	rollback := func() {
		s.mu.Lock()
		s.options = previousOptions
		s.state = previousState
		s.rows = previousRows
		s.binding = nil
		s.mu.Unlock()
	}

	if nativeErr := s.observeNative(ctx); nativeErr != nil {
		rollback()

		if refusal, ok := errors.AsType[*nanocodex.Error](nativeErr); ok && refusal.Code == nanocodex.InvalidConfig {
			if (p.ConfigId == configModel && refusal.Field == string(configModel)) || (p.ConfigId == configThinking && refusal.Field == metaThinking) {
				return acp.SetSessionConfigOptionResponse{}, wire.Unsupported("value")
			}
		}

		return acp.SetSessionConfigOptionResponse{}, nativeLaunchError(nativeErr, nil)
	}

	if commitErr := s.commitMirror(ctx); commitErr != nil {
		rollback()

		return acp.SetSessionConfigOptionResponse{}, wire.InternalFailure(vendor, "")
	}

	options := s.configOptions()
	_ = s.emit(ctx, acp.SessionUpdate{ConfigOptionUpdate: &acp.SessionConfigOptionUpdate{ConfigOptions: options}})

	return acp.SetSessionConfigOptionResponse{ConfigOptions: options}, nil
}
