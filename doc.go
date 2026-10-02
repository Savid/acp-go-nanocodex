// Package nanocodexacp exposes the Nanocodex Rust agent library through ACP.
//
// Serve runs the protocol over caller-supplied streams. The adapter launches
// acp-go-nanocodex-native with the session workspace and environment for each
// prompt. WithExecutablePath selects the helper; WithHome selects its native
// configuration root. Build and install the Rust helper separately when using
// go get or go install; make build stages both executables under bin/.
//
// WithSessionStore selects the authoritative store for native rollout rows
// and session configuration. Its default is a fresh in-memory store. Native
// files remain available after closing the adapter.
//
// Authentication belongs to the native runtime. Configure OPENAI_API_KEY or
// native auth.json credentials through the inherited environment and home.
package nanocodexacp
