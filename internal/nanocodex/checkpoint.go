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

// CheckpointStamp identifies one version of a checkpoint file: its identity,
// size, and modification time. The zero stamp matches no file.
type CheckpointStamp struct {
	device, inode uint64
	size          int64
	modified      int64
}

// ReadCheckpoint reads optional metadata separately from authoritative native
// rows. When the file still carries stamp it is not read again and changed is
// false; a missing file reads as no checkpoint.
func ReadCheckpoint(home, path string, stamp CheckpointStamp) (data json.RawMessage, current CheckpointStamp, changed bool, err error) {
	rel, err := RolloutRelative(home, path)
	if err != nil {
		return nil, CheckpointStamp{}, true, err
	}

	root, err := os.OpenRoot(home)
	if err != nil {
		return nil, CheckpointStamp{}, true, err
	}
	defer root.Close()

	file, err := root.OpenFile(rel+".acp-checkpoint.json", os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if errors.Is(err, os.ErrNotExist) {
		return nil, CheckpointStamp{}, stamp != CheckpointStamp{}, nil
	}

	if err != nil {
		return nil, CheckpointStamp{}, true, err
	}

	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, CheckpointStamp{}, true, err
	}

	if !info.Mode().IsRegular() || info.Size() > maxCheckpointBytes {
		return nil, CheckpointStamp{}, true, errors.New("invalid native checkpoint")
	}

	current = CheckpointStamp{size: info.Size(), modified: info.ModTime().UnixNano()}
	if system, ok := info.Sys().(*syscall.Stat_t); ok {
		current.device, current.inode = statDevice(system), system.Ino
	}

	if current == stamp {
		return nil, current, false, nil
	}

	data, err = io.ReadAll(io.LimitReader(file, maxCheckpointBytes+1))
	if err != nil {
		return nil, CheckpointStamp{}, true, err
	}

	if len(data) > maxCheckpointBytes || !json.Valid(data) {
		return nil, CheckpointStamp{}, true, errors.New("invalid native checkpoint")
	}

	return data, current, true, nil
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
