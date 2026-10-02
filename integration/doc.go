// Package integration holds the tests that run against the Nanocodex helper.
//
// The tests are behind the integration build tag and
// ACP_GO_NANOCODEX_RUN_INTEGRATION=1. The smoke tier spends no model tokens;
// ACP_GO_NANOCODEX_RUN_LIVE_TOKENS=1 additionally enables a live sentinel prompt.
// Live requires an explicit API key or ACP_GO_NANOCODEX_HOME, whose auth.json
// is copied to a temporary native home. ACP_GO_NANOCODEX_MODEL overrides its model.
//
// ACP_GO_NANOCODEX_HARNESS_PATH selects the helper binary, which otherwise
// defaults to bin/acp-go-nanocodex-native in the checkout.
// ACP_GO_NANOCODEX_AGENT_BINARY selects a prebuilt adapter and otherwise
// defaults to bin/acp-go-nanocodex in the checkout. Missing binaries skip smoke
// and fail explicitly enabled live tests.
package integration
