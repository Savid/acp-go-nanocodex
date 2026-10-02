package main

import (
	"context"
	"log/slog"

	"github.com/savid/acp-go-core/observer/exporters"
	nanocodexacp "github.com/savid/acp-go-nanocodex"
)

// configureTelemetry builds the exporters the OTEL_* environment enables and
// maps the configured providers onto the agent's options.
func configureTelemetry(ctx context.Context, baseLogger *slog.Logger, version string) (exporters.Bundle, []nanocodexacp.Option, error) {
	bundle, err := exporters.Configure(ctx, exporters.Config{Vendor: "nanocodex", Version: version, Logger: baseLogger})
	if err != nil {
		return exporters.Bundle{}, nil, err
	}

	options := []nanocodexacp.Option{nanocodexacp.WithTextMapPropagator(bundle.Propagator)}
	if bundle.TracerProvider != nil {
		options = append(options, nanocodexacp.WithTracerProvider(bundle.TracerProvider))
	}

	if bundle.MeterProvider != nil {
		options = append(options, nanocodexacp.WithMeterProvider(bundle.MeterProvider))
	}

	return bundle, options, nil
}
