package nanocodex

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

const rolloutSessionID = "019edc1a-c8ec-7fbc-8f19-53fb457aabb0"

func rolloutFixture(t *testing.T) (string, string) {
	t.Helper()

	home := t.TempDir()
	path := filepath.Join(home, "sessions", "2026", "rollout-"+rolloutSessionID+".jsonl")
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))

	return home, path
}

func rolloutHeader(id string) []byte {
	return fmt.Appendf(nil, `{"timestamp":"2026-10-02T00:00:00Z", "type":"session_meta", "payload":{"id":%q}}`, id)
}

func TestReadRowsPreservesCommittedBytes(t *testing.T) {
	t.Parallel()

	home, path := rolloutFixture(t)
	header := append(rolloutHeader(rolloutSessionID), '\r')
	committed := append(bytes.Clone(header), '\n')
	contents := append(bytes.Clone(committed), []byte(`{"unfinished":`)...)
	require.NoError(t, os.WriteFile(path, contents, 0o600))
	rows, err := ReadRows(home, path, int64(len(committed)))
	require.NoError(t, err)
	require.Equal(t, [][]byte{header}, rows)
	rows, err = ReadRows(home, path, -1)
	require.NoError(t, err)
	require.Equal(t, [][]byte{header}, rows)
	_, err = ReadRows(home, path, int64(len(header)))
	require.ErrorContains(t, err, "incomplete native rollout record")
	_, err = ReadRows(home, path, int64(len(contents)+1))
	require.ErrorContains(t, err, "shorter than commit watermark")
}

func TestReadRowsPreservesCompleteFinalRecordWithoutNewline(t *testing.T) {
	t.Parallel()

	home, path := rolloutFixture(t)
	header := rolloutHeader(rolloutSessionID)
	last := []byte(`{"timestamp":"2026-10-02T00:01:00Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"retained"}]}}`)
	contents := append(append(bytes.Clone(header), '\n'), last...)
	require.NoError(t, os.WriteFile(path, contents, 0o600))
	rows, err := ReadRows(home, path, -1)
	require.NoError(t, err)
	require.Equal(t, [][]byte{header, last}, rows)
	_, err = ReadRows(home, path, int64(len(contents)))
	require.ErrorContains(t, err, "incomplete native rollout record")
}

func TestReadRowsRefusesEscapesAndNonRegularFiles(t *testing.T) {
	t.Parallel()

	t.Run("outside path", func(t *testing.T) {
		t.Parallel()

		home, _ := rolloutFixture(t)
		_, err := ReadRows(home, filepath.Join(home, "..", "outside.jsonl"), -1)
		require.ErrorContains(t, err, "invalid native rollout path")
	})
	t.Run("outside symlink", func(t *testing.T) {
		t.Parallel()

		home, path := rolloutFixture(t)
		outside := filepath.Join(t.TempDir(), "outside.jsonl")
		require.NoError(t, os.WriteFile(outside, []byte("{}\n"), 0o600))
		require.NoError(t, os.Symlink(outside, path))
		_, err := ReadRows(home, path, -1)
		require.Error(t, err)
	})
	t.Run("directory", func(t *testing.T) {
		t.Parallel()

		home, path := rolloutFixture(t)
		require.NoError(t, os.Mkdir(path, 0o700))
		_, err := ReadRows(home, path, -1)
		require.ErrorContains(t, err, "must be a regular file")
	})
	t.Run("FIFO", func(t *testing.T) {
		t.Parallel()

		home, path := rolloutFixture(t)
		require.NoError(t, syscall.Mkfifo(path, 0o600))
		t.Cleanup(func() {
			file, err := os.OpenFile(path, os.O_RDWR|syscall.O_NONBLOCK, 0)
			if err == nil {
				_ = file.Close()
			}
		})
		result := make(chan error, 1)
		go func() {
			_, err := ReadRows(home, path, -1)
			result <- err
		}()
		require.ErrorContains(t, awaitClientValue(t, result), "must be a regular file")
	})
}

func TestReadRowsBoundsUnterminatedRecord(t *testing.T) {
	t.Parallel()

	home, path := rolloutFixture(t)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	require.NoError(t, file.Truncate(MaxFrameBytes+1))
	require.NoError(t, file.Close())
	_, err = ReadRows(home, path, -1)
	require.ErrorContains(t, err, "row exceeds limit")
}

func TestValidateRowsRequiresCanonicalIdentity(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		id    string
		valid bool
	}{
		{id: rolloutSessionID, valid: true},
		{id: strings.ToUpper(rolloutSessionID)},
		{id: strings.ReplaceAll(rolloutSessionID, "-", "")},
		{id: "urn:uuid:" + rolloutSessionID},
		{id: "../outside"},
		{id: ""},
	} {
		t.Run(test.id, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, test.valid, ValidSessionID(test.id))
			err := ValidateRows([][]byte{rolloutHeader(test.id)}, test.id)
			if test.valid {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "invalid native session identity")
			}
		})
	}
	require.ErrorContains(t, ValidateRows([][]byte{rolloutHeader("019edc1a-c8ec-7fbc-8f19-53fb457aabb1")}, rolloutSessionID), "identity mismatch")
}

func TestWriteRowsPublishesAtomicallyAndCleansOwnStaging(t *testing.T) {
	t.Parallel()

	home, path := rolloutFixture(t)
	original := append(rolloutHeader(rolloutSessionID), '\n')
	require.NoError(t, os.WriteFile(path, original, 0o600))
	unrelated := path + ".unrelated.tmp"
	require.NoError(t, os.WriteFile(unrelated, []byte("preserved"), 0o600))
	rows := [][]byte{rolloutHeader(rolloutSessionID), []byte(`{"type":"event_msg","timestamp":"2026-10-02T00:01:00Z","payload":{}}`)}
	err := WriteRows(home, path, [][]byte{rows[0], rows[1], []byte(`{`)})
	require.ErrorContains(t, err, "invalid native row")
	actual, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, original, actual)
	require.NoError(t, WriteRows(home, path, rows))
	actual, err = os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, append(bytes.Join(rows, []byte{'\n'}), '\n'), actual)
	entries, err := os.ReadDir(filepath.Dir(path))
	require.NoError(t, err)
	require.Len(t, entries, 2)
	actual, err = os.ReadFile(unrelated)
	require.NoError(t, err)
	require.Equal(t, "preserved", string(actual))
}
