package nanocodex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type nativeRequest struct {
	ID     uint64          `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

func TestClientClosedReadPipeReportsStreamLoss(t *testing.T) {
	t.Parallel()
	inputReader, inputWriter := io.Pipe()
	outputReader, outputWriter, err := os.Pipe()
	require.NoError(t, err)
	client := NewClient(inputWriter, outputReader)
	t.Cleanup(func() {
		_ = inputReader.Close()
		_ = inputWriter.Close()
		_ = outputReader.Close()
		_ = outputWriter.Close()
		awaitClientValue(t, client.Done())
	})
	finished := make(chan error, 1)
	go func() { finished <- client.Call(t.Context(), "prompt", struct{}{}, nil, nil) }()
	readNativeRequest(t, json.NewDecoder(inputReader))
	require.NoError(t, outputReader.Close())
	require.ErrorIs(t, awaitClientValue(t, finished), ErrClosed)
}

type clientPipes struct {
	client   *Client
	requests *json.Decoder
	output   *io.PipeWriter
	stop     func()
}

type observedWriter struct {
	io.WriteCloser
	entered chan struct{}
	exited  chan struct{}
}

func (w observedWriter) Write(frame []byte) (int, error) {
	close(w.entered)
	defer close(w.exited)

	return w.WriteCloser.Write(frame)
}

func newClientPipes(t *testing.T) clientPipes {
	t.Helper()

	inputReader, inputWriter := io.Pipe()
	outputReader, outputWriter := io.Pipe()
	client := NewClient(inputWriter, outputReader)
	stop := func() {
		_ = inputReader.Close()
		_ = inputWriter.Close()
		_ = outputReader.Close()
		_ = outputWriter.Close()
	}
	t.Cleanup(func() {
		stop()
		awaitClientValue(t, client.Done())
	})

	return clientPipes{client: client, requests: json.NewDecoder(inputReader), output: outputWriter, stop: stop}
}

func awaitClientValue[T any](t *testing.T, values <-chan T) T {
	t.Helper()

	select {
	case value := <-values:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for helper protocol operation")

		var zero T

		return zero
	}
}

func readNativeRequest(t *testing.T, decoder *json.Decoder) nativeRequest {
	t.Helper()

	type decoded struct {
		request nativeRequest
		err     error
	}
	result := make(chan decoded, 1)
	go func() {
		var request nativeRequest
		err := decoder.Decode(&request)
		result <- decoded{request: request, err: err}
	}()
	value := awaitClientValue(t, result)
	require.NoError(t, value.err)
	require.NotZero(t, value.request.ID)

	return value.request
}

func writeNativeFrame(t *testing.T, output io.Writer, frame string) {
	t.Helper()

	result := make(chan error, 1)
	go func() {
		_, err := io.WriteString(output, frame+"\n")
		result <- err
	}()
	require.NoError(t, awaitClientValue(t, result))
}

func TestClientCorrelatesConcurrentCalls(t *testing.T) {
	t.Parallel()

	pipes := newClientPipes(t)
	type outcome struct {
		value string
		err   error
	}
	promptResult := make(chan outcome, 1)
	stateResult := make(chan outcome, 1)
	events := make(chan string, 3)
	call := func(method string, result chan<- outcome) {
		var value string
		err := pipes.client.Call(t.Context(), method, map[string]string{"method": method}, &value, func(frame Frame) error {
			events <- frame.Event

			return nil
		})
		result <- outcome{value: value, err: err}
	}
	go call("prompt", promptResult)
	go call("state", stateResult)
	requests := make(map[string]nativeRequest, 2)
	for range 2 {
		request := readNativeRequest(t, pipes.requests)
		requests[request.Method] = request
	}
	require.Len(t, requests, 2)
	require.NotEqual(t, requests["prompt"].ID, requests["state"].ID)
	writeNativeFrame(t, pipes.output, fmt.Sprintf(`{"id":%d,"result":"state-reply"}`, requests["state"].ID))
	state := awaitClientValue(t, stateResult)
	require.NoError(t, state.err)
	require.Equal(t, "state-reply", state.value)
	for _, name := range []string{"accepted", "native", "native"} {
		writeNativeFrame(t, pipes.output, fmt.Sprintf(`{"event":%q,"requestId":%d,"data":{}}`, name, requests["prompt"].ID))
	}
	writeNativeFrame(t, pipes.output, fmt.Sprintf(`{"id":%d,"result":"prompt-reply"}`, requests["prompt"].ID))
	prompt := awaitClientValue(t, promptResult)
	require.NoError(t, prompt.err)
	require.Equal(t, "prompt-reply", prompt.value)
	require.Equal(t, "accepted", awaitClientValue(t, events))
	require.Equal(t, "native", awaitClientValue(t, events))
	require.Equal(t, "native", awaitClientValue(t, events))
}

func TestClientDrainsEventsAndTerminalBeforeEOF(t *testing.T) {
	t.Parallel()

	pipes := newClientPipes(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	result := make(chan error, 1)
	var received []string
	var reply string
	go func() {
		result <- pipes.client.Call(t.Context(), "prompt", struct{}{}, &reply, func(frame Frame) error {
			received = append(received, frame.Event)
			if frame.Event == "accepted" {
				close(entered)
				select {
				case <-release:
				case <-t.Context().Done():
					return t.Context().Err()
				}
			}

			return nil
		})
	}()
	request := readNativeRequest(t, pipes.requests)
	writeNativeFrame(t, pipes.output, fmt.Sprintf(`{"event":"accepted","requestId":%d,"data":{}}`, request.ID))
	awaitClientValue(t, entered)
	writeNativeFrame(t, pipes.output, fmt.Sprintf(`{"event":"native","requestId":%d,"data":{}}`, request.ID))
	writeNativeFrame(t, pipes.output, fmt.Sprintf(`{"id":%d,"result":"complete"}`, request.ID))
	require.NoError(t, pipes.output.Close())
	awaitClientValue(t, pipes.client.Done())
	close(release)
	require.NoError(t, awaitClientValue(t, result))
	require.Equal(t, []string{"accepted", "native"}, received)
	require.Equal(t, "complete", reply)
}

func TestClientRejectsMalformedFrames(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name  string
		frame string
		want  string
	}{
		{name: "invalid JSON", frame: `{`, want: "invalid helper frame"},
		{name: "missing ID", frame: `{"result":{}}`, want: "invalid helper envelope"},
		{name: "missing disposition", frame: `{"id":1}`, want: "invalid helper envelope"},
		{name: "ambiguous disposition", frame: `{"id":1,"result":{},"error":{"code":"busy","message":"busy"}}`, want: "invalid helper envelope"},
		{name: "event carrying reply", frame: `{"event":"native","requestId":1,"id":1,"result":{},"data":{}}`, want: "invalid helper envelope"},
		{name: "event without data", frame: `{"event":"native","requestId":1}`, want: "invalid helper envelope"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			pipes := newClientPipes(t)
			result := make(chan error, 1)
			go func() {
				result <- pipes.client.Call(t.Context(), "state", struct{}{}, nil, nil)
			}()
			readNativeRequest(t, pipes.requests)
			writeNativeFrame(t, pipes.output, test.frame)
			require.ErrorContains(t, awaitClientValue(t, result), test.want)
			awaitClientValue(t, pipes.client.Done())
			require.ErrorContains(t, pipes.client.Call(t.Context(), "state", struct{}{}, nil, nil), test.want)
		})
	}
}

func TestClientBlockedWriteCancelsAndEOFReleasesWriter(t *testing.T) {
	t.Parallel()

	inputReader, inputWriter := io.Pipe()
	outputReader, outputWriter := io.Pipe()
	writer := observedWriter{WriteCloser: inputWriter, entered: make(chan struct{}), exited: make(chan struct{})}
	client := NewClient(writer, outputReader)
	t.Cleanup(func() {
		_ = inputReader.Close()
		_ = inputWriter.Close()
		_ = outputReader.Close()
		_ = outputWriter.Close()
		awaitClientValue(t, client.Done())
	})
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	first := make(chan error, 1)
	go func() {
		first <- client.Call(ctx, "prompt", struct{}{}, nil, nil)
	}()
	awaitClientValue(t, writer.entered)
	cancel()
	require.ErrorIs(t, awaitClientValue(t, first), context.Canceled)
	second := make(chan error, 1)
	go func() {
		second <- client.Call(t.Context(), "state", struct{}{}, nil, nil)
	}()
	require.NoError(t, outputWriter.Close())
	require.ErrorIs(t, awaitClientValue(t, second), ErrClosed)
	awaitClientValue(t, writer.exited)
	awaitClientValue(t, client.Done())
}

func TestClientCancellationDetachesLateReplies(t *testing.T) {
	t.Parallel()

	pipes := newClientPipes(t)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	result := make(chan error, 1)
	events := make(chan Frame, 1)
	go func() {
		result <- pipes.client.Call(ctx, "prompt", struct{}{}, nil, func(frame Frame) error {
			events <- frame

			return nil
		})
	}()
	request := readNativeRequest(t, pipes.requests)
	cancel()
	require.ErrorIs(t, awaitClientValue(t, result), context.Canceled)
	for range 20 {
		writeNativeFrame(t, pipes.output, fmt.Sprintf(`{"event":"native","requestId":%d,"data":{}}`, request.ID))
	}
	writeNativeFrame(t, pipes.output, fmt.Sprintf(`{"id":%d,"result":{}}`, request.ID))
	writeNativeFrame(t, pipes.output, `{"id":99999,"result":{}}`)
	go func() {
		result <- pipes.client.Call(t.Context(), "state", struct{}{}, nil, nil)
	}()
	next := readNativeRequest(t, pipes.requests)
	require.Greater(t, next.ID, request.ID)
	writeNativeFrame(t, pipes.output, fmt.Sprintf(`{"id":%d,"result":{}}`, next.ID))
	require.NoError(t, awaitClientValue(t, result))
	select {
	case <-events:
		t.Fatal("cancelled caller received a late event")
	default:
	}
}

func TestClientRejectsOversizedFrames(t *testing.T) {
	t.Run("outgoing", func(t *testing.T) {
		pipes := newClientPipes(t)
		err := pipes.client.Call(t.Context(), "prompt", strings.Repeat("x", MaxFrameBytes), nil, nil)
		require.ErrorContains(t, err, "request exceeds frame limit")
	})

	t.Run("incoming", func(t *testing.T) {
		pipes := newClientPipes(t)
		result := make(chan error, 1)
		go func() {
			result <- pipes.client.Call(t.Context(), "state", struct{}{}, nil, nil)
		}()
		request := readNativeRequest(t, pipes.requests)
		written := make(chan error, 1)
		go func() {
			_, err := io.Copy(pipes.output, io.MultiReader(
				strings.NewReader(fmt.Sprintf(`{"id":%d,"result":"`, request.ID)),
				strings.NewReader(strings.Repeat("x", MaxFrameBytes)),
				strings.NewReader("\"}\n"),
			))
			written <- err
		}()
		require.ErrorContains(t, awaitClientValue(t, result), "token too long")
		awaitClientValue(t, pipes.client.Done())
		pipes.stop()
		err := awaitClientValue(t, written)
		require.True(t, err == nil || errors.Is(err, io.ErrClosedPipe), "unexpected writer error: %v", err)
	})
}
