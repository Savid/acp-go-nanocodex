package nanocodex

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"os"
	"syscall"
)

const maxCheckpointBytes = 1 << 20

// ReadCheckpoint reads optional metadata separately from authoritative native rows.
func ReadCheckpoint(home, path string) (json.RawMessage, error) {
	rel, err := RolloutRelative(home, path)
	if err != nil {
		return nil, err
	}

	root, err := os.OpenRoot(home)
	if err != nil {
		return nil, err
	}
	defer root.Close()

	file, err := root.OpenFile(rel+".acp-checkpoint.json", os.O_RDONLY|syscall.O_NONBLOCK, 0)
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

	if !info.Mode().IsRegular() || info.Size() > maxCheckpointBytes {
		return nil, errors.New("invalid native checkpoint")
	}

	data, err := io.ReadAll(io.LimitReader(file, maxCheckpointBytes+1))
	if err != nil {
		return nil, err
	}

	if len(data) > maxCheckpointBytes || !json.Valid(data) {
		return nil, errors.New("invalid native checkpoint")
	}

	return data, nil
}

// WriteCheckpoint atomically hydrates optional metadata while the caller holds the session lock.
func WriteCheckpoint(home, path string, data json.RawMessage) error {
	rel, err := RolloutRelative(home, path)
	if err != nil {
		return err
	}

	root, err := os.OpenRoot(home)
	if err != nil {
		return err
	}
	defer root.Close()

	rel += ".acp-checkpoint.json"
	if len(data) == 0 {
		err = root.Remove(rel)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}

		return err
	}

	if len(data) > maxCheckpointBytes || !json.Valid(data) {
		return errors.New("invalid native checkpoint")
	}

	stage := rel + "." + rand.Text() + ".tmp"

	file, err := root.OpenFile(stage, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = root.Remove(stage) }()
	defer file.Close()

	if _, err := file.Write(data); err != nil {
		return err
	}

	if err := file.Sync(); err != nil {
		return err
	}

	if err := file.Close(); err != nil {
		return err
	}

	return root.Rename(stage, rel)
}
