package main

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunVersionFlag(t *testing.T) {
	t.Parallel()

	var stdout, stderr bytes.Buffer

	code := run(context.Background(), []string{"-version"}, strings.NewReader(""), &stdout, &stderr)
	require.Equal(t, 0, code)
	require.Equal(t, "dev\n", stdout.String())
}

func TestRunBadFlag(t *testing.T) {
	t.Parallel()

	var stdout, stderr bytes.Buffer

	code := run(context.Background(), []string{"-bogus"}, strings.NewReader(""), &stdout, &stderr)
	require.Equal(t, 2, code)
	require.Contains(t, stderr.String(), "flag provided but not defined")
}

func TestRunServesUntilPeerCloses(t *testing.T) {
	t.Parallel()

	stdinReader, stdinWriter := io.Pipe()
	stdoutReader, stdoutWriter := io.Pipe()

	var stderr bytes.Buffer

	codes := make(chan int, 1)

	go func() {
		codes <- run(context.Background(), []string{"-path", "/nonexistent/acp-go-nanocodex-native", "-scratch-dir", t.TempDir()}, stdinReader, stdoutWriter, &stderr)
		_ = stdoutWriter.Close()
	}()

	_, err := io.WriteString(stdinWriter, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}`+"\n")
	require.NoError(t, err)

	line, err := bufio.NewReader(stdoutReader).ReadString('\n')
	require.NoError(t, err)
	require.Contains(t, line, `"protocolVersion"`)

	require.NoError(t, stdinWriter.Close())
	require.Equal(t, 0, <-codes)
}
