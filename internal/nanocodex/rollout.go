package nanocodex

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/google/uuid"
	"github.com/savid/acp-go-core/sessionlog"
	"github.com/savid/acp-go-core/wire"
)

// Row is the native rollout envelope; Payload remains native JSON.
type Row struct {
	Timestamp string          `json:"timestamp"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
}

// RolloutRelative confines a native path to the session tree in its home.
func RolloutRelative(home, path string) (string, error) {
	rel, err := filepath.Rel(home, path)
	if err != nil || !filepath.IsLocal(rel) || !strings.HasPrefix(rel, "sessions"+string(filepath.Separator)) || filepath.Ext(rel) != ".jsonl" {
		return "", errors.New("invalid native rollout path")
	}

	return rel, nil
}

// openRollout opens the rollout beneath home without following a symlink out
// of it. A missing home or rollout fails with os.ErrNotExist.
func openRollout(home, path string, flag int) (*os.File, error) {
	rel, err := RolloutRelative(home, path)
	if err != nil {
		return nil, err
	}

	root, err := os.OpenRoot(home)
	if err != nil {
		return nil, err
	}

	defer root.Close()

	// O_NONBLOCK keeps a FIFO planted at the rollout path from blocking the open.
	file, err := root.OpenFile(rel, flag|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		_ = file.Close()

		return nil, err
	}

	if !info.Mode().IsRegular() {
		_ = file.Close()

		return nil, errors.New("native rollout must be a regular file")
	}

	return file, nil
}

// ReadRows returns the complete records after from, preserving their bytes. A
// non-negative limit is the helper's committed byte boundary: reading stops
// there and it must end a record. A negative limit reads every complete
// record. The tail is Full when it holds the whole history, as it does for the
// zero Cursor or a rollout that no longer extends from. A missing rollout
// reads as an empty history.
func ReadRows(home, path string, from sessionlog.Cursor, limit int64) (sessionlog.Tail, error) {
	file, err := openRollout(home, path, os.O_RDONLY)
	if errors.Is(err, os.ErrNotExist) {
		if limit > 0 {
			return sessionlog.Tail{}, errors.New("native rollout is shorter than commit watermark")
		}

		return sessionlog.Tail{Full: true}, nil
	}

	if err != nil {
		return sessionlog.Tail{}, err
	}

	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return sessionlog.Tail{}, err
	}

	if limit > info.Size() {
		return sessionlog.Tail{}, errors.New("native rollout is shorter than commit watermark")
	}

	tail, err := sessionlog.ReadJSONLFile(file, from, limit)
	if err != nil {
		return sessionlog.Tail{}, err
	}

	if limit >= 0 && tail.Next.Offset() != limit {
		return sessionlog.Tail{}, errors.New("incomplete native rollout record")
	}

	return tail, nil
}

// EachRow streams the non-empty records in the first limit bytes of the
// rollout to fn, one at a time.
func EachRow(home, path string, limit int64, fn func(row []byte) error) error {
	file, err := openRollout(home, path, os.O_RDONLY)
	if err != nil {
		return err
	}

	defer file.Close()

	// Compacted rows carry whole histories, so records have no length bound.
	reader := wire.NewLineReader(io.LimitReader(file, limit), math.MaxInt)

	for {
		row, readErr := reader.Next()
		if errors.Is(readErr, io.EOF) {
			return nil
		}

		if readErr != nil {
			return readErr
		}

		if row = bytes.TrimSpace(row); len(row) == 0 {
			continue
		}

		if err := fn(row); err != nil {
			return err
		}
	}
}

// RepairTail settles an unterminated final record of a stopped rollout while
// the caller holds its writer lock: a complete JSON record receives its
// newline and an invalid fragment is removed. It creates the rollout's
// directory beneath home so a later publish cannot leave it.
func RepairTail(home, path string) error {
	rel, err := RolloutRelative(home, path)
	if err != nil {
		return err
	}

	if mkdirErr := os.MkdirAll(home, 0o700); mkdirErr != nil {
		return mkdirErr
	}

	root, err := os.OpenRoot(home)
	if err != nil {
		return err
	}

	mkdirErr := root.MkdirAll(filepath.Dir(rel), 0o700)
	_ = root.Close()

	if mkdirErr != nil {
		return mkdirErr
	}

	file, err := openRollout(home, path, os.O_RDWR)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}

	if err != nil {
		return err
	}

	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return err
	}

	fragment, err := unterminatedTail(file, info.Size())
	if err != nil || len(fragment) == 0 {
		return err
	}

	if trimmed := bytes.TrimSpace(fragment); len(trimmed) > 0 && trimmed[0] == '{' && json.Valid(trimmed) {
		_, err = file.WriteAt([]byte{'\n'}, info.Size())
	} else {
		err = file.Truncate(info.Size() - int64(len(fragment)))
	}

	if err != nil {
		return err
	}

	return file.Sync()
}

// unterminatedTail returns the bytes after the last newline of a file of
// size bytes. A fragment longer than a protocol frame is refused.
func unterminatedTail(file *os.File, size int64) ([]byte, error) {
	const chunk = 64 << 10

	var fragment []byte

	for end := size; end > 0; {
		start := max(end-chunk, 0)
		block := make([]byte, end-start)

		if _, err := file.ReadAt(block, start); err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}

		if index := bytes.LastIndexByte(block, '\n'); index >= 0 {
			return append(block[index+1:], fragment...), nil
		}

		fragment = append(block, fragment...)
		if len(fragment) > MaxFrameBytes {
			return nil, errors.New("native rollout row exceeds limit")
		}

		end = start
	}

	return fragment, nil
}

// ValidSessionID requires the canonical UUID spelling used by native rollouts.
func ValidSessionID(value string) bool {
	id, err := uuid.Parse(value)

	return err == nil && id.String() == value
}

// ValidateRows checks a complete history: its header binds id and every row
// has the native row shape.
func ValidateRows(rows [][]byte, id string) error {
	if !ValidSessionID(id) {
		return errors.New("invalid native session identity")
	}

	if len(rows) == 0 {
		return errors.New("native rollout requires session metadata")
	}

	var header struct {
		Type    string `json:"type"`
		Payload struct {
			ID string `json:"id"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(rows[0], &header); err != nil {
		return err
	}

	if header.Type != "session_meta" || header.Payload.ID != id {
		return errors.New("native rollout identity mismatch")
	}

	return ValidateTail(rows)
}

// ValidateTail checks that every row has a timestamp, a type, and a payload.
func ValidateTail(rows [][]byte) error {
	for index, raw := range rows {
		var row struct {
			Timestamp string  `json:"timestamp"`
			Type      string  `json:"type"`
			Payload   present `json:"payload"`
		}
		if err := json.Unmarshal(raw, &row); err != nil {
			return err
		}

		if row.Timestamp == "" || row.Type == "" || !row.Payload {
			return fmt.Errorf("invalid native row %d", index)
		}
	}

	return nil
}

// present records that a member exists without copying its value.
type present bool

func (p *present) UnmarshalJSON(data []byte) error {
	*p = len(data) > 0

	return nil
}
