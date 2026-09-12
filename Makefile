SHELL := /usr/bin/env bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

PYTHONPATH := python/cerbero-tooling/src
export PYTHONPATH

.PHONY: help doctor verify baseline-check format lint build test rust-check go-check python-check security-check contracts integration e2e ci dev-init dev-up dev-bootstrap dev-health dev-down dev-reset remote-readiness

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

contracts: ## Validate the governed contract roots for the current milestone.
	@python3 scripts/ci/verify_repository.py --contracts-only

integration: ## Run Milestone 0 infrastructure integration tests (requires Docker Compose).
	@./scripts/tests/milestone0-integration.sh

e2e: ## Milestone 0 has no analytical E2E pipeline yet; verify the explicit gate.
	@./scripts/tests/milestone0-e2e-gate.sh

ci: verify format lint build test security-check contracts ## Local equivalent of the required bootstrap CI gate.
	@echo "CERBERO Milestone 0 local CI gate: PASS"

dev-init: ## Create local development environment file and runtime directories.
	@./scripts/dev/init.sh

dev-up: ## Start PostgreSQL, ClickHouse, and NATS JetStream.
	@./scripts/dev/up.sh

dev-bootstrap: ## Create the locked JetStream stream topology.
	@./scripts/dev/bootstrap-nats.sh

dev-health: ## Check development infrastructure health.
	@./scripts/dev/health.sh

dev-down: ## Stop development infrastructure without deleting data volumes.
	@./scripts/dev/down.sh

dev-reset: ## Stop development infrastructure and delete development volumes.
	@./scripts/dev/reset.sh

remote-readiness: ## Verify repository state before configuring/pushing a remote.
	@./scripts/git/remote-readiness.sh
