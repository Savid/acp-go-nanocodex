package nanocodex

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/savid/acp-go-core/sessionlog"
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

func TestReadRowsStopsAtTheCommittedBoundary(t *testing.T) {
	t.Parallel()

	home, path := rolloutFixture(t)
	header := rolloutHeader(rolloutSessionID)
	committed := append(append(bytes.Clone(header), '\r'), '\n')
	contents := append(bytes.Clone(committed), []byte(`{"unfinished":`)...)
	require.NoError(t, os.WriteFile(path, contents, 0o600))
	tail, err := ReadRows(home, path, sessionlog.Cursor{}, int64(len(committed)))
	require.NoError(t, err)
	require.Equal(t, [][]byte{header}, tail.Rows)
	require.True(t, tail.Full)
	tail, err = ReadRows(home, path, sessionlog.Cursor{}, -1)
	require.NoError(t, err)
	require.Equal(t, [][]byte{header}, tail.Rows)
	_, err = ReadRows(home, path, sessionlog.Cursor{}, int64(len(header)))
	require.ErrorContains(t, err, "incomplete native rollout record")
	_, err = ReadRows(home, path, sessionlog.Cursor{}, int64(len(contents)+1))
	require.ErrorContains(t, err, "shorter than commit watermark")
}

func TestReadRowsContinuesFromItsCursor(t *testing.T) {
	t.Parallel()

	home, path := rolloutFixture(t)
	header := rolloutHeader(rolloutSessionID)
	row := []byte(`{"timestamp":"2026-10-02T00:01:00Z","type":"event_msg","payload":{}}`)
	require.NoError(t, os.WriteFile(path, append(bytes.Clone(header), '\n'), 0o600))
	first, err := ReadRows(home, path, sessionlog.Cursor{}, -1)
	require.NoError(t, err)
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = file.Write(append(bytes.Clone(row), '\n'))
	require.NoError(t, err)
	require.NoError(t, file.Close())
	next, err := ReadRows(home, path, first.Next, -1)
	require.NoError(t, err)
	require.False(t, next.Full)
	require.Equal(t, [][]byte{row}, next.Rows)
	require.NoError(t, os.WriteFile(path, append(bytes.Clone(row), '\n'), 0o600))
	rewritten, err := ReadRows(home, path, next.Next, -1)
	require.NoError(t, err)
	require.True(t, rewritten.Full)
	require.Equal(t, [][]byte{row}, rewritten.Rows)
}

func TestRepairTailSettlesTheFinalRecord(t *testing.T) {
	t.Parallel()

	header := append(rolloutHeader(rolloutSessionID), '\n')
	last := []byte(`{"timestamp":"2026-10-02T00:01:00Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"retained"}]}}`)
	for _, test := range []struct {
		name     string
		contents []byte
		want     []byte
		torn     sessionlog.TornTail
	}{
		{name: "complete record", contents: append(bytes.Clone(header), last...), want: append(append(bytes.Clone(header), last...), '\n'), torn: sessionlog.TornTail{Terminated: true}},
		{name: "invalid fragment", contents: append(bytes.Clone(header), `{"unfinished":`...), want: header, torn: sessionlog.TornTail{Cut: int64(len(`{"unfinished":`))}},
		{name: "terminated", contents: header, want: header},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			home, path := rolloutFixture(t)
			require.NoError(t, os.WriteFile(path, test.contents, 0o600))
			torn, err := RepairTail(home, path)
			require.NoError(t, err)
			require.Equal(t, test.torn, torn)
			actual, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, test.want, actual)
		})
	}
}

func TestRepairTailCreatesTheRolloutDirectoryInsideHome(t *testing.T) {
	t.Parallel()

	home := t.TempDir()
	path := filepath.Join(home, "sessions", "2026", "rollout.jsonl")
	_, err := RepairTail(home, path)
	require.NoError(t, err)
	info, err := os.Stat(filepath.Dir(path))
	require.NoError(t, err)
	require.True(t, info.IsDir())
	outside := t.TempDir()
	require.NoError(t, os.Symlink(outside, filepath.Join(home, "sessions", "escape")))
	_, err = RepairTail(home, filepath.Join(home, "sessions", "escape", "nested", "rollout.jsonl"))
	require.Error(t, err)
	_, err = os.Stat(filepath.Join(outside, "nested"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestRepairTailRefusesARolloutThatIsNotARegularFile(t *testing.T) {
	t.Parallel()

	home, path := rolloutFixture(t)
	outside := filepath.Join(t.TempDir(), "outside.jsonl")
	torn := []byte(`{"torn":`)
	require.NoError(t, os.WriteFile(outside, torn, 0o600))
	require.NoError(t, os.Symlink(outside, path))
	_, err := RepairTail(home, path)
	require.ErrorContains(t, err, "must be a regular file")
	contents, err := os.ReadFile(outside)
	require.NoError(t, err)
	require.Equal(t, torn, contents)
}

func TestEachRowStreamsCompleteRecords(t *testing.T) {
	t.Parallel()

	home, path := rolloutFixture(t)
	header := rolloutHeader(rolloutSessionID)
	committed := append(bytes.Clone(header), "\n\n"...)
	require.NoError(t, os.WriteFile(path, append(bytes.Clone(committed), `{"torn":`...), 0o600))

	var rows [][]byte
	require.NoError(t, EachRow(home, path, func(row []byte) error {
		rows = append(rows, row)

		return nil
	}))
	require.Equal(t, [][]byte{header}, rows)
	require.Error(t, EachRow(home, filepath.Join(filepath.Dir(path), "missing.jsonl"), func([]byte) error { return nil }))
}

func TestReadRowsRefusesEscapesAndNonRegularFiles(t *testing.T) {
	t.Parallel()

	t.Run("outside path", func(t *testing.T) {
		t.Parallel()

		home, _ := rolloutFixture(t)
		_, err := ReadRows(home, filepath.Join(home, "..", "outside.jsonl"), sessionlog.Cursor{}, -1)
		require.ErrorContains(t, err, "invalid native rollout path")
	})
	t.Run("outside symlink", func(t *testing.T) {
		t.Parallel()

		home, path := rolloutFixture(t)
		outside := filepath.Join(t.TempDir(), "outside.jsonl")
		require.NoError(t, os.WriteFile(outside, []byte("{}\n"), 0o600))
		require.NoError(t, os.Symlink(outside, path))
		_, err := ReadRows(home, path, sessionlog.Cursor{}, -1)
		require.Error(t, err)
	})
	t.Run("directory", func(t *testing.T) {
		t.Parallel()

		home, path := rolloutFixture(t)
		require.NoError(t, os.Mkdir(path, 0o700))
		_, err := ReadRows(home, path, sessionlog.Cursor{}, -1)
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
			_, err := ReadRows(home, path, sessionlog.Cursor{}, -1)
			result <- err
		}()
		require.ErrorContains(t, awaitClientValue(t, result), "must be a regular file")
	})
}

func TestRepairTailSettlesRecordsBeyondTheFrameBound(t *testing.T) {
	t.Parallel()

	header := append(rolloutHeader(rolloutSessionID), '\n')
	large := []byte(`{"type":"compacted","payload":{"replacement_history":[{"text":"` + strings.Repeat("x", MaxFrameBytes) + `"}]}}`)
	for _, test := range []struct {
		name     string
		fragment []byte
		want     []byte
	}{
		{name: "complete", fragment: large, want: append(append(bytes.Clone(header), large...), '\n')},
		{name: "torn", fragment: large[:len(large)-2], want: header},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			home, path := rolloutFixture(t)
			require.NoError(t, os.WriteFile(path, append(bytes.Clone(header), test.fragment...), 0o600))
			_, err := RepairTail(home, path)
			require.NoError(t, err)
			actual, err := os.ReadFile(path)
			require.NoError(t, err)
			require.True(t, bytes.Equal(test.want, actual))
		})
	}
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

func TestCompactedHistoryCanExceedProtocolFrameLimit(t *testing.T) {
	t.Parallel()
	home, path := rolloutFixture(t)
	row := []byte(`{"type":"compacted","payload":{"replacement_history":[{"type":"message","role":"user","content":[{"type":"input_text","text":"` + strings.Repeat("x", MaxFrameBytes) + `"}]}]}}`)
	require.NoError(t, os.WriteFile(path, append(bytes.Clone(row), '\n'), 0o600))
	tail, err := ReadRows(home, path, sessionlog.Cursor{}, -1)
	require.NoError(t, err)
	require.Equal(t, [][]byte{row}, tail.Rows)

	var streamed [][]byte
	require.NoError(t, EachRow(home, path, func(row []byte) error {
		streamed = append(streamed, row)

		return nil
	}))
	require.Equal(t, [][]byte{row}, streamed)
}
