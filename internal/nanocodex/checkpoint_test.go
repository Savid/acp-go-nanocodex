package nanocodex

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadCheckpointSkipsAnUnchangedFile(t *testing.T) {
	t.Parallel()

	home, path := rolloutFixture(t)
	data, stamp, changed, err := ReadCheckpoint(home, path, CheckpointStamp{})
	require.NoError(t, err)
	require.Nil(t, data)
	require.False(t, changed)

	first := json.RawMessage(`{"history_items":1}`)
	require.NoError(t, WriteCheckpoint(home, path, first))
	data, stamp, changed, err = ReadCheckpoint(home, path, stamp)
	require.NoError(t, err)
	require.True(t, changed)
	require.JSONEq(t, string(first), string(data))

	data, same, changed, err := ReadCheckpoint(home, path, stamp)
	require.NoError(t, err)
	require.False(t, changed)
	require.Nil(t, data)
	require.Equal(t, stamp, same)

	second := json.RawMessage(`{"history_items":2}`)
	require.NoError(t, WriteCheckpoint(home, path, second))
	data, _, changed, err = ReadCheckpoint(home, path, stamp)
	require.NoError(t, err)
	require.True(t, changed)
	require.JSONEq(t, string(second), string(data))

	require.NoError(t, WriteCheckpoint(home, path, nil))
	data, _, changed, err = ReadCheckpoint(home, path, stamp)
	require.NoError(t, err)
	require.True(t, changed)
	require.Nil(t, data)
}
