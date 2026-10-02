package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"

	"github.com/savid/acp-go-core/process"
	nanocodexacp "github.com/savid/acp-go-nanocodex"
)

func main() {
	if code := run(context.Background(), os.Args[1:], os.Stdin, os.Stdout, os.Stderr); code != 0 {
		os.Exit(code)
	}
}

func run(ctx context.Context, args []string, stdin io.Reader, stdout io.Writer, stderr io.Writer) int {
	flags := flag.NewFlagSet("acp-go-nanocodex", flag.ContinueOnError)
	flags.SetOutput(stderr)

	executablePath := flags.String("path", "", "Rust helper executable; a bare name is searched on PATH")
	home := flags.String("home", "", "Nanocodex config root passed as CODEX_HOME; empty inherits native resolution")
	scratchDir := flags.String("scratch-dir", "", "absolute scratch parent; this adapter allocates no scratch state")
	model := flags.String("model", "", "default model for new sessions")
	seedFiles := &process.SeedFileFlag{}
	flags.Var(seedFiles, "seed-file", "file seeded into the Nanocodex config root as <relpath>=<hostpath>; repeatable")
	debug := flags.Bool("debug", false, "write debug logs to stderr")
	printVersion := flags.Bool("version", false, "print adapter version and exit")
	printHelperRelease := flags.Bool("nanocodex-helper-release", false, "print the required helper release and fingerprint as JSON and exit")

	if err := flags.Parse(args); err != nil {
		return 2
	}

	if *printVersion {
		_, _ = fmt.Fprintln(stdout, version())

		return 0
	}

	if *printHelperRelease {
		return writeHelperRelease(stdout, stderr)
	}

	level := slog.LevelWarn
	if *debug {
		level = slog.LevelDebug
	}

	logger := slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: level}))

	telemetry, telemetryOptions, err := configureTelemetry(ctx, logger, version())
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "acp-go-nanocodex: configure OpenTelemetry: %v\n", err)

		return 1
	}

	logger = telemetry.Logger

	ctx, stop := signal.NotifyContext(ctx, forwardedSignals()...)
	defer stop()

	options := []nanocodexacp.Option{
		nanocodexacp.WithAgentVersion(version()),
		nanocodexacp.WithExecutablePath(*executablePath),
		nanocodexacp.WithHome(*home),
		nanocodexacp.WithScratchDir(*scratchDir),
		nanocodexacp.WithDefaultModel(*model),
		nanocodexacp.WithLogger(logger),
	}
	if len(seedFiles.Files) > 0 {
		options = append(options, nanocodexacp.WithSeedFiles(seedFiles.Files))
	}

	options = append(options, telemetryOptions...)

	serveErr := nanocodexacp.Serve(ctx, stdin, stdout, options...)
	shutdownErr := telemetry.Shutdown(context.Background())

	if serveErr != nil && ctx.Err() == nil {
		_, _ = fmt.Fprintf(stderr, "acp-go-nanocodex: %v\n", serveErr)

		return 1
	}

	if shutdownErr != nil {
		_, _ = fmt.Fprintf(stderr, "acp-go-nanocodex: shutdown OpenTelemetry: %v\n", shutdownErr)

		return 1
	}

	return 0
}

// writeHelperRelease prints the helper identity that initialization requires.
func writeHelperRelease(stdout, stderr io.Writer) int {
	helperVersion, helperFingerprint := nanocodexacp.HelperRelease()

	err := json.NewEncoder(stdout).Encode(struct {
		HelperVersion     string `json:"helperVersion"`
		HelperFingerprint string `json:"helperFingerprint"`
	}{helperVersion, helperFingerprint})
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "acp-go-nanocodex: write helper release: %v\n", err)

		return 1
	}

	return 0
}
