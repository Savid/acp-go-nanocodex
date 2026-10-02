// Package nanocodex implements the private Rust helper protocol.
package nanocodex

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
)

// MaxFrameBytes bounds a single helper protocol frame, excluding its newline.
const MaxFrameBytes = 32 << 20

// InvalidConfig identifies a refused initialization option.
const InvalidConfig = "invalid_config"

// ErrClosed reports loss of the helper stream.
var ErrClosed = errors.New("nanocodex helper stream closed")

// Error is a fixed helper error classification.
type Error struct {
	Code         string `json:"code"`
	Message      string `json:"message"`
	Field        string `json:"field,omitempty"`
	StatusCode   int    `json:"statusCode,omitempty"`
	ProviderCode string `json:"providerCode,omitempty"`
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

// Frame is one response or event. RequestID attributes an event to its prompt.
type Frame struct {
	ID        uint64          `json:"id,omitempty"`
	Result    json.RawMessage `json:"result,omitempty"`
	Error     *Error          `json:"error,omitempty"`
	Event     string          `json:"event,omitempty"`
	RequestID uint64          `json:"requestId,omitempty"`
	Data      json.RawMessage `json:"data,omitempty"`
}

type pending struct {
	frames chan Frame
	done   chan struct{}
}

type request struct {
	frame   []byte
	pending *pending
}

// Client correlates replies and streams each prompt's events in wire order.
type Client struct {
	input   io.WriteCloser
	output  io.ReadCloser
	writes  chan request
	mu      sync.Mutex
	pending map[uint64]*pending
	failure error
	next    atomic.Uint64
	done    chan struct{}
}

// NewClient starts the helper's dedicated reader and writer. A failed stream
// closes both pipes so blocked I/O cannot retain the process or its callers.
func NewClient(input io.WriteCloser, output io.ReadCloser) *Client {
	c := &Client{
		input: input, output: output, writes: make(chan request),
		pending: make(map[uint64]*pending), done: make(chan struct{}),
	}
	go c.write()
	go c.read()

	return c
}

// Done closes when the helper protocol stream fails or ends.
func (c *Client) Done() <-chan struct{} { return c.done }

// Call delivers events synchronously before decoding the terminal reply.
// Cancellation detaches this caller; cancel is a separate native operation.
func (c *Client) Call(ctx context.Context, method string, params any, result any, event func(Frame) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	id := c.next.Add(1)

	frame, err := json.Marshal(struct {
		ID     uint64 `json:"id"`
		Method string `json:"method"`
		Params any    `json:"params"`
	}{id, method, params})
	if err != nil {
		return fmt.Errorf("encode helper request: %w", err)
	}

	if len(frame) > MaxFrameBytes {
		return errors.New("helper request exceeds frame limit")
	}

	p := &pending{frames: make(chan Frame, 16), done: make(chan struct{})}

	c.mu.Lock()

	if c.failure != nil {
		err = c.failure
		c.mu.Unlock()

		return err
	}

	c.pending[id] = p
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.pending, id)
		close(p.done)
		c.mu.Unlock()
	}()

	select {
	case c.writes <- request{frame: append(frame, '\n'), pending: p}:
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return c.streamFailure()
	}

	for {
		item, readErr := c.nextFrame(ctx, p)
		if readErr != nil {
			return readErr
		}

		if item.Event != "" {
			if event != nil {
				if err := event(item); err != nil {
					return err
				}
			}

			continue
		}

		if item.Error != nil {
			return item.Error
		}

		if result != nil {
			if err := json.Unmarshal(item.Result, result); err != nil {
				return fmt.Errorf("decode helper result: %w", err)
			}
		}

		return nil
	}
}

func (c *Client) nextFrame(ctx context.Context, p *pending) (Frame, error) {
	select {
	case frame := <-p.frames:
		return frame, nil
	case <-ctx.Done():
		return Frame{}, ctx.Err()
	case <-c.done:
		// EOF may follow an already queued terminal reply and its events.
		select {
		case frame := <-p.frames:
			return frame, nil
		default:
			return Frame{}, c.streamFailure()
		}
	}
}

func (c *Client) streamFailure() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.failure
}

func (c *Client) fail(err error) {
	if errors.Is(err, os.ErrClosed) || errors.Is(err, io.ErrClosedPipe) || errors.Is(err, syscall.EPIPE) {
		err = ErrClosed
	}

	c.mu.Lock()

	if c.failure != nil {
		c.mu.Unlock()

		return
	}

	c.failure = err
	close(c.done)
	c.mu.Unlock()

	_ = c.input.Close()
	_ = c.output.Close()
}

func (c *Client) write() {
	for {
		select {
		case <-c.done:
			return
		case item := <-c.writes:
			select {
			case <-item.pending.done:
				continue
			case <-c.done:
				return
			default:
			}

			n, err := c.input.Write(item.frame)
			if err == nil && n != len(item.frame) {
				err = io.ErrShortWrite
			}

			if err != nil {
				c.fail(fmt.Errorf("write helper request: %w", err))

				return
			}
		}
	}
}

func (c *Client) read() {
	scanner := bufio.NewScanner(c.output)
	scanner.Buffer(make([]byte, 64<<10), MaxFrameBytes+1)

	for scanner.Scan() {
		if len(scanner.Bytes()) > MaxFrameBytes {
			c.fail(errors.New("helper response exceeds frame limit"))

			return
		}

		var frame Frame
		if err := json.Unmarshal(scanner.Bytes(), &frame); err != nil {
			c.fail(fmt.Errorf("invalid helper frame: %w", err))

			return
		}

		id, err := frame.requestID()
		if err != nil {
			c.fail(err)

			return
		}

		c.mu.Lock()
		p := c.pending[id]
		c.mu.Unlock()

		if p != nil {
			select {
			case p.frames <- frame:
			case <-p.done:
			case <-c.done:
				return
			}
		}
	}

	if err := scanner.Err(); err != nil {
		c.fail(fmt.Errorf("read helper stream: %w", err))

		return
	}

	c.fail(ErrClosed)
}

func (f Frame) requestID() (uint64, error) {
	if f.Event != "" {
		if f.RequestID != 0 && f.ID == 0 && f.Error == nil && len(f.Result) == 0 && len(f.Data) > 0 {
			return f.RequestID, nil
		}
	} else if f.ID != 0 && f.RequestID == 0 && len(f.Data) == 0 && (f.Error == nil) != (len(f.Result) == 0) {
		return f.ID, nil
	}

	return 0, errors.New("invalid helper envelope")
}
