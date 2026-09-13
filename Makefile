SHELL := /usr/bin/env bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

PYTHONPATH := python/cerbero-tooling/src
export PYTHONPATH

.PHONY: help doctor verify baseline-check format lint build test rust-check go-check python-check security-check contracts contracts-generate contracts-generated-check integration e2e ci dev-init dev-up dev-bootstrap dev-health dev-ingest dev-raw-preserver dev-down dev-reset remote-readiness

help: ## Show available targets.
	@awk 'BEGIN {FS = ":.*## "; printf "CERBERO bootstrap targets:\n"} /^[a-zA-Z0-9_.-]+:.*## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

doctor: ## Report local toolchain availability and versions.
	@./scripts/dev/doctor.sh

verify: ## Verify repository invariants and baseline source policy.
	@python3 scripts/ci/verify_repository.py
	@./scripts/docs/verify-baseline.sh
	@python3 scripts/ci/check_text_files.py

baseline-check: ## Verify the external architectural baseline source policy.
	@./scripts/docs/verify-baseline.sh

format: ## Check formatting for all implemented languages.
	@cargo fmt --all -- --check
	@./scripts/ci/gofmt-check.sh
	@python3 scripts/ci/check_python.py --format-only

lint: ## Run static checks for all implemented languages.
	@cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
	@./scripts/ci/go-vet.sh
	@python3 scripts/ci/check_python.py --lint-only

build: ## Build all implemented language components.
	@cargo build --workspace --locked
	@./scripts/ci/go-build.sh
	@python3 -m compileall -q python/cerbero-tooling/src python/cerbero-tooling/tests scripts

test: ## Run unit tests for all implemented language components.
	@cargo test --workspace --locked
	@./scripts/ci/go-test.sh
	@python3 -m unittest discover -s python/cerbero-tooling/tests -v

rust-check: ## Format, lint, build, and test the Rust workspace.
	@cargo fmt --all -- --check
	@cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
	@cargo build --workspace --locked
	@cargo test --workspace --locked

go-check: ## Format, vet, build, and test all Go modules.
	@./scripts/ci/gofmt-check.sh
	@./scripts/ci/go-vet.sh
	@./scripts/ci/go-build.sh
	@./scripts/ci/go-test.sh

python-check: ## Validate lock metadata, syntax, style, and unit tests.
	@uv lock --project python/cerbero-tooling --check
	@python3 scripts/ci/check_python.py
	@python3 -m unittest discover -s python/cerbero-tooling/tests -v

security-check: ## Run repository-local security checks.
	@./scripts/security/check-no-secrets.sh

contracts: ## Validate canonical Protobuf source and generated binding drift.
	@python3 scripts/ci/verify_repository.py --contracts-only
	@./scripts/contracts/check.sh
	@./scripts/contracts/check-generated.sh

contracts-generate: ## Generate pinned Rust and Go bindings from canonical Protobuf source.
	@./scripts/contracts/generate.sh

contracts-generated-check: ## Regenerate bindings and fail if tracked output drifts.
	@./scripts/contracts/check-generated.sh

integration: ## Run infrastructure, ingest, and raw-preservation integration tests (requires Docker Compose).
	@./scripts/tests/milestone0-integration.sh

e2e: ## Report the explicit staged E2E boundary after durable Raw Preservation.
	@./scripts/tests/milestone0-e2e-gate.sh

ci: verify format lint build test security-check contracts ## Local equivalent of the required CI gate.
	@echo "CERBERO local CI gate: PASS"

dev-init: ## Create local development environment file and runtime directories.
	@./scripts/dev/init.sh

dev-up: ## Start PostgreSQL, ClickHouse, and NATS JetStream.
	@./scripts/dev/up.sh

dev-bootstrap: ## Create JetStream topology and least-privilege development service identities.
	@./scripts/dev/bootstrap-nats.sh
	@./scripts/dev/bootstrap-postgres.sh

dev-health: ## Check development infrastructure health.
	@./scripts/dev/health.sh

dev-ingest: ## Run the DEVELOPMENT-ONLY JSON/HTTP ingest service from .env.
	@set -a; source .env; set +a; cd services/cerbero-ingest; exec go run .

dev-raw-preserver: ## Run the DEVELOPMENT-ONLY durable raw-preserver service from .env.
	@set -a; source .env; set +a; export CERBERO_NATS_URL="nats://127.0.0.1:$${NATS_PORT}"; cd services/cerbero-raw-preserver; exec go run .

dev-down: ## Stop development infrastructure without deleting data volumes.
	@./scripts/dev/down.sh

dev-reset: ## Stop development infrastructure and delete development volumes.
	@./scripts/dev/reset.sh

remote-readiness: ## Verify repository state before configuring/pushing a remote.
	@./scripts/git/remote-readiness.sh
