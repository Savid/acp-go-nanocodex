package nanocodexacp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOptionsOwnCapturedMapsAndSlices(t *testing.T) {
	t.Parallel()

	env := map[string]string{"PROVIDER_ROUTE": "original"}
	files := map[string]string{"config.toml": "original"}
	models := []string{"original"}
	opts := []Option{WithEnv(env), WithSeedFiles(files), WithConfiguredModels(models)}
	env["PROVIDER_ROUTE"] = "changed"
	files["config.toml"] = "changed"
	models[0] = "changed"
	first := applyOptions(opts)
	require.Equal(t, "original", first.Env["PROVIDER_ROUTE"])
	require.Equal(t, "original", first.SeedFiles["config.toml"])
	require.Equal(t, []string{"original"}, first.ConfiguredModels)
	first.Env["PROVIDER_ROUTE"] = "reused"
	first.SeedFiles["config.toml"] = "reused"
	first.ConfiguredModels[0] = "reused"
	second := applyOptions(opts)
	require.Equal(t, "original", second.Env["PROVIDER_ROUTE"])
	require.Equal(t, "original", second.SeedFiles["config.toml"])
	require.Equal(t, []string{"original"}, second.ConfiguredModels)
}
