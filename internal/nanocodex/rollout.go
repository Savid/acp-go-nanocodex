package nanocodex

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/google/uuid"
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

// ReadRows reads only complete committed records, preserving their bytes.
// A negative limit reads all complete records for native reconciliation.
func ReadRows(home, path string, limit int64) ([][]byte, error) {
	rel, err := RolloutRelative(home, path)
	if err != nil {
		return nil, err
	}

	root, err := os.OpenRoot(home)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	defer root.Close()

	file, err := root.OpenFile(rel, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}

	if !info.Mode().IsRegular() {
		return nil, errors.New("native rollout must be a regular file")
	}

	var reader io.Reader = file

	if limit >= 0 {
		if info.Size() < limit {
			return nil, errors.New("native rollout is shorter than commit watermark")
		}

		reader = io.LimitReader(file, limit)
	}

	buffered := bufio.NewReader(reader)

	var rows [][]byte

	for {
		line, readErr := buffered.ReadBytes('\n')
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return nil, readErr
		}

		if len(line) == 0 {
			break
		}

		complete := line[len(line)-1] == '\n'
		if complete {
			line = line[:len(line)-1]
		}

		valid := len(line) > 0 && json.Valid(line)

		if !complete {
			if limit >= 0 {
				return nil, errors.New("incomplete native rollout record")
			}

			if !valid {
				if len(line) > MaxFrameBytes {
					return nil, errors.New("native rollout row exceeds limit")
				}

				break
			}
		}

		if !valid {
			return nil, errors.New("malformed native rollout record")
		}

		rows = append(rows, line)

		if readErr != nil {
			break
		}
	}

	return rows, nil
}

// ValidSessionID requires the canonical UUID spelling used by native rollouts.
func ValidSessionID(value string) bool {
	id, err := uuid.Parse(value)

	return err == nil && id.String() == value
}

// ValidateRows checks the native header binding and structural row shapes.
func ValidateRows(rows [][]byte, id string) error {
	if !ValidSessionID(id) {
		return errors.New("invalid native session identity")
	}

	if len(rows) == 0 {
		return errors.New("native rollout requires session metadata")
	}

	for index, raw := range rows {
		var row Row
		if err := json.Unmarshal(raw, &row); err != nil {
			return err
		}

		if row.Timestamp == "" || row.Type == "" || len(row.Payload) == 0 || !json.Valid(row.Payload) {
			return fmt.Errorf("invalid native row %d", index)
		}

		if index == 0 {
			var header struct {
				ID string `json:"id"`
			}
			if err := json.Unmarshal(row.Payload, &header); err != nil {
				return err
			}

			if row.Type != "session_meta" || header.ID != id {
				return errors.New("native rollout identity mismatch")
			}
		}
	}

	return nil
}

// EmptyRows recognizes only an unstarted native session header.
func EmptyRows(rows [][]byte) bool {
	if len(rows) != 1 {
		return false
	}

	var row Row

	return json.Unmarshal(rows[0], &row) == nil && row.Type == "session_meta"
}

// WriteRows atomically hydrates one native rollout beneath an opened home.
func WriteRows(home, path string, rows [][]byte) error {
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
	defer root.Close()

	if mkdirErr := root.MkdirAll(filepath.Dir(rel), 0o700); mkdirErr != nil {
		return mkdirErr
	}
	// The caller holds the native conversation lock while publishing.
	tmp := rel + ".acp-" + rand.Text() + ".tmp"

	file, err := root.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = root.Remove(tmp) }()

	for _, row := range rows {
		if bytes.ContainsRune(row, '\n') || !json.Valid(row) {
			_ = file.Close()

			return errors.New("invalid native row")
		}

		if _, err = file.Write(append(bytes.Clone(row), '\n')); err != nil {
			_ = file.Close()

			return err
		}
	}

	if err = file.Sync(); err != nil {
		_ = file.Close()

		return err
	}

	if err := file.Close(); err != nil {
		return err
	}

	return root.Rename(tmp, rel)
}
