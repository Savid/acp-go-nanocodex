package nanocodexacp

import (
	"bufio"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-core/image"
	"github.com/savid/acp-go-core/lifecycle"
	"github.com/savid/acp-go-core/wire"
	"github.com/stretchr/testify/require"
)

func TestConformanceInitializeAndConstructionRefusals(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		field  string
		option Option
	}{
		{"home", WithHome("relative")},
		{"inputHandoffRoot", WithInputHandoffRoot("relative")},
		{"configuredModels", WithConfiguredModels([]string{""})},
		{"configuredModels", WithConfiguredModels([]string{" space "})},
		{"configuredModels", WithConfiguredModels([]string{"duplicate", "duplicate"})},
		{"env", WithEnv(map[string]string{"BAD=NAME": "value"})},
	} {
		a := NewAgent(WithExecutablePath("/fixture/missing"), test.option)
		want := wire.InvalidOptions(vendor, test.field)
		_, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
		require.Equal(t, want, err)
		_, err = a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
		require.Equal(t, want, err)
		_, err = a.LoadSession(t.Context(), wire.LoadSessionRequest("unloaded", t.TempDir()))
		require.Equal(t, want, err)
		_, err = a.ResumeSession(t.Context(), acp.ResumeSessionRequest{SessionId: "unloaded", Cwd: t.TempDir()})
		require.Equal(t, want, err)
		require.NoError(t, a.Close())
	}
	for _, handoff := range []bool{false, true} {
		options := []Option{WithImageLimits(ImageLimits{})}
		if handoff {
			options = append(options, WithInputHandoffRoot(t.TempDir()))
		}
		a := NewAgent(options...)
		response, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
		require.NoError(t, err)
		require.Empty(t, response.AuthMethods)
		capabilities := response.AgentCapabilities
		require.True(t, capabilities.PromptCapabilities.Image)
		require.Equal(t, image.MediaEnvelope(ImageLimits{}.core(), image.Envelope{DocumentFormats: []string{}}), capabilities.Meta[wire.MediaEnvelopeKey])
		_, advertised := capabilities.Meta[wire.HandoffKey]
		require.Equal(t, handoff, advertised)
		raw, err := json.Marshal(capabilities)
		require.NoError(t, err)
		var shape map[string]any
		require.NoError(t, json.Unmarshal(raw, &shape))
		require.Empty(t, shape["mcpCapabilities"])
		for _, key := range []string{"nes", "providers", "modes"} {
			require.NotContains(t, shape, key)
		}
		sessionCapabilities, ok := shape["sessionCapabilities"].(map[string]any)
		require.True(t, ok)
		require.Equal(t, map[string]any{"close": map[string]any{}, "delete": map[string]any{}, "list": map[string]any{}, "resume": map[string]any{}}, sessionCapabilities)
		require.NoError(t, a.Close())
	}
}

func TestConformanceNativeProcessExitHasStableCause(t *testing.T) {
	t.Parallel()
	a, _, _, workspace := fixtureAgent(t)
	created := fixtureSession(t, a, workspace)
	for range 3 {
		response, err := a.Prompt(t.Context(), wire.TextPromptRequest(created.SessionId, "exit"))
		require.Empty(t, response.StopReason)
		requestErr, ok := errors.AsType[*acp.RequestError](err)
		require.True(t, ok)
		data, ok := requestErr.Data.(map[string]any)
		require.True(t, ok)
		require.Equal(t, "nanocodex_turn_failed", data["error"])
		require.Equal(t, "process_exit", data["cause"])
		require.Contains(t, data["message"], "code 37")
		require.Contains(t, data["message"], "fixture native crash")
	}
}

func TestConformanceImageHandoffAndEmbeddedGates(t *testing.T) {
	t.Parallel()
	const png = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aD1sAAAAASUVORK5CYII="
	bytes, err := base64.StdEncoding.DecodeString(png)
	require.NoError(t, err)
	root := t.TempDir()
	path := filepath.Join(root, "image.png")
	require.NoError(t, os.WriteFile(path, bytes, 0o600))
	a, _, _, workspace := fixtureAgent(t, WithInputHandoffRoot(root), WithImageLimits(ImageLimits{MaxInputBytesPerImage: int64(len(bytes)), MaxInputBytesPerPrompt: int64(len(bytes))}))
	created := fixtureSession(t, a, workspace)
	s, err := a.lookup(created.SessionId)
	require.NoError(t, err)
	embedded := acp.ImageBlock(png, "image/png")
	embedded.Image.Uri = new("file:///does/not/exist")
	mapped, err := s.mapPrompt(t.Context(), []acp.ContentBlock{embedded})
	require.NoError(t, err, "embedded data wins over URI")
	handoff := acp.ImageBlock("", "image/png")
	handoff.Image.Uri = new((&url.URL{Scheme: "file", Path: path}).String())
	handoff.Image.Meta = map[string]any{wire.HandoffKey: map[string]any{"version": 1, "digest": fmt.Sprintf("%x", sha256.Sum256(bytes)), "sizeBytes": len(bytes)}}
	fromFile, err := s.mapPrompt(t.Context(), []acp.ContentBlock{handoff})
	require.NoError(t, err)
	require.Equal(t, mapped, fromFile)
	raw, err := json.Marshal(fromFile)
	require.NoError(t, err)
	require.NotContains(t, string(raw), path)
	for _, blocks := range [][]acp.ContentBlock{
		{acp.ImageBlock("!!!", "image/png")},
		{acp.ImageBlock(png, "image/jpeg")},
		{acp.ImageBlock(png, "image/svg+xml")},
		{handoff, embedded},
	} {
		_, err = a.Prompt(t.Context(), wire.PromptRequest(created.SessionId, blocks...))
		require.Error(t, err)
		s.mu.Lock()
		started := s.started
		s.mu.Unlock()
		require.False(t, started, "rejected images must not reach a native turn")
	}
	badHandoff := handoff
	badImage := *handoff.Image
	badHandoff.Image = &badImage
	badImage.Meta = map[string]any{wire.HandoffKey: map[string]any{"version": 1, "digest": fmt.Sprintf("%x", sha256.Sum256(bytes)), "sizeBytes": len(bytes) - 1}}
	_, err = s.mapPrompt(t.Context(), []acp.ContentBlock{badHandoff})
	require.Contains(t, err.Error(), image.ErrorDigestMismatch)
	outside := filepath.Join(t.TempDir(), "outside.png")
	require.NoError(t, os.WriteFile(outside, bytes, 0o600))
	link := filepath.Join(root, "link.png")
	require.NoError(t, os.Symlink(outside, link))
	for _, path := range []string{link, root} {
		badImage.Uri = new((&url.URL{Scheme: "file", Path: path}).String())
		badImage.Meta = handoff.Image.Meta
		_, err = s.mapPrompt(t.Context(), []acp.ContentBlock{badHandoff})
		require.Error(t, err)
	}
}

func TestConformanceLifecycleAttributionAcrossPromptProcesses(t *testing.T) {
	t.Parallel()
	a, client, _, workspace := fixtureAgent(t)
	initialized, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber, Meta: map[string]any{lifecycle.MetaKey: map[string]any{"version": 1}}})
	require.NoError(t, err)
	negotiated := lifecycle.Negotiated{Version: 1, UpdatesOutsidePrompt: false, ActivityKinds: []lifecycle.ActivityKind{}}
	require.Equal(t, negotiated.Advertisement(), initialized.Meta[lifecycle.MetaKey])
	session := fixtureSession(t, a, workspace)
	for _, id := range []string{"first", "second"} {
		request := wire.PromptRequest(session.SessionId, acp.TextBlock(id))
		request.Meta = map[string]any{lifecycle.MetaKey: map[string]any{"version": 1, "submission": map[string]any{"submissionId": id, "clientNonce": "nonce-" + id}}}
		response, promptErr := a.Prompt(t.Context(), request)
		require.NoError(t, promptErr)
		require.Equal(t, acp.StopReasonEndTurn, response.StopReason)
	}
	_, frames := client.snapshot()
	require.NoError(t, lifecycle.CheckAttribution(negotiated, frames))
	var accepted, completed int
	streams := make(map[string]bool)
	for _, frame := range frames {
		delivery, decodeErr := lifecycle.DecodeSessionUpdate(frame, negotiated)
		if errors.Is(decodeErr, lifecycle.ErrNoEnvelope) {
			continue
		}
		require.NoError(t, decodeErr)
		streams[delivery.StreamID] = true
		if delivery.Event.PromptAccepted != nil {
			accepted++
		}
		if state := delivery.Event.State; state != nil && state.State == lifecycle.ForegroundIdle {
			completed++
			require.Equal(t, lifecycle.OutcomeSuccess, state.Outcome)
		}
	}
	require.Len(t, streams, 2)
	require.Equal(t, 2, accepted)
	require.Equal(t, 2, completed)
	require.Equal(t, "Hello world.Second answer.Hello world.Second answer.", client.text())
	_, err = a.Prompt(t.Context(), wire.PromptRequest(session.SessionId, acp.TextBlock("missing correlation")))
	require.Equal(t, wire.Missing(`_meta["`+lifecycle.MetaKey+`"]`), err)
}

func TestConformanceMetadataRefusedBeforeNativeLaunch(t *testing.T) {
	t.Parallel()
	a, _, _, workspace := fixtureAgent(t, WithExecutablePath("/fixture/missing-native"))
	tests := []struct {
		name    string
		request acp.NewSessionRequest
		field   string
	}{
		{"relative cwd", acp.NewSessionRequest{Cwd: "relative"}, "cwd"},
		{"additional directories", acp.NewSessionRequest{Cwd: workspace, AdditionalDirectories: []string{workspace}}, "additionalDirectories"},
		{"unknown native option", acp.NewSessionRequest{Cwd: workspace, Meta: map[string]any{"nanocodex": map[string]any{"options": map[string]any{"teleport": true}}}}, "_meta.nanocodex.options.teleport"},
		{"credential URL", acp.NewSessionRequest{Cwd: workspace, Meta: NewNanocodexOptions(WithNanocodexAPIBaseURL("https://user:secret@example.invalid/v1")).Meta()}, "_meta.nanocodex.options.apiBaseUrl"},
		{"bad environment", acp.NewSessionRequest{Cwd: workspace, Meta: NewNanocodexOptions(WithNanocodexEnv(map[string]string{"BAD=KEY": "x"})).Meta()}, "_meta.nanocodex.options.env.BAD=KEY"},
		{"bad ephemeral", acp.NewSessionRequest{Cwd: workspace, Meta: map[string]any{wire.SessionMetaKey: map[string]any{"ephemeral": "true"}}}, "_meta." + wire.SessionMetaKey + ".ephemeral"},
		{"lifecycle on new", acp.NewSessionRequest{Cwd: workspace, Meta: map[string]any{lifecycle.MetaKey: map[string]any{"version": 1}}}, `_meta["` + lifecycle.MetaKey + `"]`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := a.NewSession(t.Context(), test.request)
			require.Equal(t, wire.Unsupported(test.field), err)
		})
	}
}

func TestConformanceImageInputGatesAndOrderedNativeDelivery(t *testing.T) {
	t.Parallel()
	a, _, home, workspace := fixtureAgent(t, WithImageLimits(ImageLimits{MaxInputBytesPerImage: 128, MaxInputBytesPerPrompt: 128}))
	session := fixtureSession(t, a, workspace)
	for _, block := range []acp.ContentBlock{acp.ImageBlock("!!!", "image/png"), acp.ImageBlock(base64.StdEncoding.EncodeToString(make([]byte, 129)), "image/png")} {
		_, err := a.Prompt(t.Context(), wire.PromptRequest(session.SessionId, block))
		require.Error(t, err)
	}
	png := "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aD1sAAAAASUVORK5CYII="
	response, err := a.Prompt(t.Context(), wire.PromptRequest(session.SessionId, acp.TextBlock("before"), acp.ImageBlock(png, "image/png"), acp.TextBlock("after")))
	require.NoError(t, err)
	raw, err := os.ReadFile(rolloutPath(home, nativeID(t, response.Meta)))
	require.NoError(t, err)
	require.Contains(t, string(raw), `"input":[{"type":"text","text":"before"},{"type":"image","image_url":"data:image/png;base64,`+png+`"},{"type":"text","text":"after"}]`)
}

func TestConformanceEnvironmentCaptureScopeAndPathPrecedence(t *testing.T) {
	t.Setenv("NANOCODEX_TEST_INHERITED", "before-construction")
	t.Setenv("NANOCODEX_TEST_SCOPE", "process")
	t.Setenv("ACP_GO_NANOCODEX_INTERNAL_SECRET", "must-not-propagate")
	executable, err := os.Executable()
	require.NoError(t, err)
	baseDir, sessionDir, extraDir := t.TempDir(), t.TempDir(), t.TempDir()
	name := "fixture-native"
	require.NoError(t, os.Symlink(executable, filepath.Join(baseDir, name)))
	require.NoError(t, os.WriteFile(filepath.Join(sessionDir, name), []byte("#!/bin/sh\nexit 91\n"), 0o700))
	capture := filepath.Join(t.TempDir(), "capture.jsonl")
	env := map[string]string{fakeHelperEnv: "1", "GORACE": "atexit_sleep_ms=0", "PATH": baseDir, "NANOCODEX_TEST_SCOPE": "agent", "NANOCODEX_TEST_CAPTURE": capture}
	a, _, home, workspace := fixtureAgent(t, WithExecutablePath(name), WithEnv(env))
	t.Setenv("NANOCODEX_TEST_INHERITED", "after-construction")
	first := NewNanocodexOptions(WithNanocodexEnv(map[string]string{"NANOCODEX_TEST_SCOPE": "session", "PATH": sessionDir, "CODEX_HOME": t.TempDir()}), WithNanocodexExtraPathDirs(extraDir), WithNanocodexShellEnv("EXAMPLE_API_TOKEN"))
	fixtureSession(t, a, workspace, WithSessionNanocodexOptions(first))
	fixtureSession(t, a, workspace)
	file, err := os.Open(capture)
	require.NoError(t, err)
	defer file.Close()
	scanner := bufio.NewScanner(file)
	var captured []map[string]any
	for scanner.Scan() {
		var row map[string]any
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &row))
		captured = append(captured, row)
	}
	require.NoError(t, scanner.Err())
	require.Len(t, captured, 2)
	require.Equal(t, "session", captured[0]["scope"])
	require.Equal(t, "agent", captured[1]["scope"])
	require.Equal(t, extraDir+string(os.PathListSeparator)+sessionDir, captured[0]["path"])
	require.Equal(t, baseDir, captured[1]["path"])
	withShellEnv, ok := captured[0]["initialize"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, []any{"EXAMPLE_API_TOKEN"}, withShellEnv["shellEnv"])
	require.NotContains(t, captured[1]["initialize"], "shellEnv")
	expectedHome, err := filepath.EvalSymlinks(home)
	require.NoError(t, err)
	for _, row := range captured {
		require.Equal(t, "before-construction", row["inherited"])
		require.Equal(t, expectedHome, row["home"])
		require.Empty(t, row["internal"])
	}
}
