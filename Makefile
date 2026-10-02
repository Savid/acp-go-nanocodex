.DEFAULT_GOAL := help

CARGO_TARGET_DIR := $(CURDIR)/.tmp/cargo
CARGO_AUDIT_VERSION := 0.22.2
CARGO_AUDIT_ROOT := $(CURDIR)/.tmp/cargo-audit/$(CARGO_AUDIT_VERSION)
CARGO_AUDIT := $(CARGO_AUDIT_ROOT)/bin/cargo-audit

GOLANGCI_LINT_VERSION ?= v2.14.0
GOLANGCI_LINT := go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

.PHONY: audit build clean coverage-check fmt fmt-check help lint modernize-check native-build native-fmt native-fmt-check native-lint native-test native-vuln test test-integration-live test-integration-smoke tidy vuln

## build: build all packages and the command binary
build: native-build
	go build ./...
	go build -o bin/acp-go-nanocodex ./cmd/acp-go-nanocodex

GO_TEST_TIMEOUT ?= 40m

## test: run unit tests with race detector and shuffled order
test: native-test
	go test -race -shuffle=on -timeout=$(GO_TEST_TIMEOUT) ./...

## coverage-check: run shuffled race tests and report statement coverage
coverage-check: native-test
	go test -race -shuffle=on -coverprofile=coverage.out -covermode=atomic -timeout=$(GO_TEST_TIMEOUT) ./...
	@awk 'NR > 1 && $$(NF - 1) > 0 { found = 1 } END { if (!found) { print "coverage profile has no statement blocks"; exit 1 } }' coverage.out
	@report=$$(go tool cover -func=coverage.out) || exit $$?; printf '%s\n' "$$report" | awk '/^total:/ { found = 1; if ($$3 !~ /^[0-9]+([.][0-9]+)?%$$/) { print "invalid total coverage line"; exit 1 } printf "total coverage %s\n", $$3 } END { if (!found) { print "missing total coverage line"; exit 1 } }'

## test-integration-smoke: run integration tests against the installed Nanocodex helper without spending tokens
test-integration-smoke: build
	ACP_GO_NANOCODEX_RUN_LIVE_TOKENS=0 ACP_GO_NANOCODEX_RUN_INTEGRATION=1 go test -race -count=1 -tags=integration -timeout=300s -v ./integration/...

## test-integration-live: run integration tests that spend model tokens
test-integration-live: build
	ACP_GO_NANOCODEX_RUN_LIVE_TOKENS=1 ACP_GO_NANOCODEX_RUN_INTEGRATION=1 go test -race -count=1 -tags=integration -timeout=900s -v ./integration/...

## lint: run pinned golangci-lint
lint: native-lint
	$(GOLANGCI_LINT) run --timeout=10m --allow-parallel-runners ./...

## fmt: format code with golangci-lint
fmt: native-fmt
	gofmt -w $$(find . -name '*.go' -not -path './.git/*')
	$(GOLANGCI_LINT) fmt ./...

## fmt-check: require gofmt-clean Go files
fmt-check: native-fmt-check
	@test -z "$$(gofmt -l .)"

## tidy: verify module files are tidy
tidy:
	go mod tidy -diff

## vuln: scan Rust and Go dependencies for known vulnerabilities
vuln: native-vuln
	go tool govulncheck ./...

## modernize-check: check Go modernizations without changing files
modernize-check:
	go fix -diff ./...

## audit: run local checks
audit:
	$(MAKE) fmt-check
	$(MAKE) lint
	$(MAKE) build
	$(MAKE) coverage-check
	$(MAKE) tidy
	$(MAKE) vuln
	$(MAKE) modernize-check
	go mod verify

## clean: remove build artifacts
clean:
	rm -rf .tmp bin coverage.out

## help: show this help
help:
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' | sed -e 's/^/ /'

## native-build: build and stage the Rust helper
native-build:
	cd native && CARGO_TARGET_DIR="$(CARGO_TARGET_DIR)" cargo build --locked --release
	mkdir -p bin
	cp "$(CARGO_TARGET_DIR)/release/acp-go-nanocodex-native" bin/acp-go-nanocodex-native

## native-test: run deterministic Rust tests
native-test:
	cd native && CARGO_TARGET_DIR="$(CARGO_TARGET_DIR)" cargo test --locked

## native-lint: check the Rust helper with clippy
native-lint:
	cd native && CARGO_TARGET_DIR="$(CARGO_TARGET_DIR)" cargo clippy --locked --all-targets -- -D warnings

$(CARGO_AUDIT):
	cd native && cargo install --locked --version "$(CARGO_AUDIT_VERSION)" --root "$(CARGO_AUDIT_ROOT)" --target-dir "$(CARGO_AUDIT_ROOT)/target" cargo-audit

## native-vuln: scan Cargo.lock with pinned cargo-audit
native-vuln: $(CARGO_AUDIT)
	cd native && "$(CARGO_AUDIT)" audit --file Cargo.lock

## native-fmt: format Rust sources
native-fmt:
	cd native && cargo fmt --all

## native-fmt-check: require formatted Rust sources
native-fmt-check:
	cd native && cargo fmt --all -- --check
