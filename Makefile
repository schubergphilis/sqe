# SQE top-level Makefile.
#
# Convenience wrappers around cargo, mdbook, and the ebook build pipeline.
# The actual build logic lives in:
#   - cargo (Cargo.toml): Rust binaries `sqe-cli` and `sqe-server`
#   - mdbook (docs/site/book/book.toml): the rust book
#   - pandoc (docs/site/ebook/Makefile): the PDF / EPUB ebook
#
# This Makefile orchestrates them so a contributor can run `make rustbook`
# without remembering the mdbook invocation. `make all` is the full gate:
# supply-chain audit, unit tests, deployable image, benchmark sweep (add
# `make all_trino` for the SQE-vs-Trino comparison). The old docs+image
# meaning of `all` is now `make docs-and-image`.

# ── Configuration ─────────────────────────────────────────────────────────
CARGO        ?= cargo
MDBOOK       ?= mdbook
BOOK_DIR     := docs/site/book
EBOOK_DIR    := docs/site/ebook
BOOK_OUT     := target/book
RELEASE_BIN  := target/release
DEBUG_BIN    := target/debug

# Which crate / binary names cargo knows about.
BIN_CLI      := sqe-cli
BIN_SERVER   := sqe-server

# ── Container image (adapted from data-platform/Makefile) ──────────────────
# Two build paths:
#   * `build` (and `sbom`) produce a LOCAL single-arch image via plain
#     `docker build`, loaded into the local image store for `docker run`/scan.
#   * `push` produces a multi-arch image (amd64+arm64) via `docker buildx` and
#     pushes it straight to the registry (manifest lists can't be `--load`ed).
#
# The image is built from Dockerfile.full by default (all catalog backends
# compiled in: Polaris + Nessie + Glue + HMS + Unity). Override for the slim
# image: `make SQE_DOCKERFILE=Dockerfile build`.
DOCKER         ?= docker
SQE_DOCKERFILE ?= Dockerfile.full

# Optional registry/namespace prefix for the LOCAL `build`. Empty = bare name.
# A trailing slash is added automatically when set.
REGISTRY     ?=
IMAGE_PREFIX := $(if $(REGISTRY),$(REGISTRY)/,)
SQE_IMAGE    := $(IMAGE_PREFIX)sqe

# Registry namespace `make push` tags into. `make login` (or `docker login`)
# must run first; credentials come from ./.env or REGISTRY_USER /
# REGISTRY_PASSWORD in the environment and are never written to this file.
PUSH_REGISTRY ?= repo.sovereign-data.org/chameleon
REGISTRY_HOST := $(firstword $(subst /, ,$(PUSH_REGISTRY)))
PUSH_SQE      := $(PUSH_REGISTRY)/sqe

# Multi-arch push platforms, the mutable tag published alongside the SHA, and
# the dedicated buildx builder (the default `docker` driver can't emit manifest
# lists, so `push` bootstraps this builder on demand).
PLATFORMS      ?= linux/amd64,linux/arm64
LATEST_TAG     ?= latest
BUILDX_BUILDER ?= sqe-multiarch

# Image tag = this repo's git short SHA: immutable, pins the exact commit, and
# matches CI's CI_COMMIT_SHORT_SHA. Override with `make IMAGE_TAG=<custom>`.
VCS_REF      := $(shell git rev-parse --short HEAD 2>/dev/null)
GIT_REVISION := $(shell git rev-parse HEAD 2>/dev/null)
IMAGE_TAG    ?= $(VCS_REF)
BUILD_DATE   := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
DATE         := $(shell date +%Y-%m-%d)
SBOM_DIR     := sbom

# `make sqe-config` stages every quickstart/*/sqe.toml into dist/sqe-config/
# for deployment (the runtime image is config-less: configs are mounted, see
# the data-platform quickstart compose). Override the source/output dirs below.
CONFIG_SRC_DIR ?= quickstart
CONFIG_OUT_DIR ?= dist/sqe-config

# Build args declared by the SQE Dockerfile(s) and stamped into OCI labels.
SQE_BUILD_ARGS := \
	--build-arg BUILD_DATE=$(BUILD_DATE) \
	--build-arg GIT_REVISION=$(GIT_REVISION) \
	--build-arg VERSION=$(IMAGE_TAG)

# ── Benchmarks ─────────────────────────────────────────────────────────────
# `make benchmark_sf1` etc. wrap scripts/benchmark-make-run.sh, which wraps
# scripts/benchmark-test.sh (brings up its own Polaris + RustFS test stack).
# Committed baseline numbers must come from PROFILE=release.
BENCH_PROFILE  ?= release
BENCH_SUITES   ?=
BENCH_LOG_DIR  ?= /tmp/sqe-bench-logs
TRINO_MEMORY   ?= 8g
BENCH_RUN      := scripts/benchmark-make-run.sh
BENCH_ENV       = PROFILE=$(BENCH_PROFILE) BENCH_LOG_DIR=$(BENCH_LOG_DIR)

.PHONY: help all all_trino docs-and-image check dev release rustbook ebook \
        ebook-pdf ebook-epub ebook-html \
        benchmark-charts test test-access-control test-access-control-spark \
        audit audit-advisories \
        audit-deny audit-licenses \
        benchmark_sf0.1 benchmark_sf1 benchmark_sf10 benchmark_all \
        benchmark_sf0.1_trino benchmark_sf1_trino benchmark_sf10_trino \
        benchmark_all_trino \
        clippy fmt fmt-check clean clean-rust clean-rustbook \
        clean-ebook clean-benchmark-charts clean-images check-tools maintain \
        build build-sqe sbom sbom-sqe sqe-config images \
        login buildx-builder push push-sqe leak-scan

# ── Default target ────────────────────────────────────────────────────────
help:
	@echo "SQE build targets:"
	@echo ""
	@echo "  Code:"
	@echo "    make check        Type-check all targets without linking (fastest feedback)"
	@echo "    make dev          Debug build of sqe-cli + sqe-server (fast compile)"
	@echo "    make release      Release build of sqe-cli + sqe-server (LTO, optimised)"
	@echo "    make test         cargo test --workspace"
	@echo "    make test-access-control  Ranger/Polaris access-control e2e (brings up the Ranger stack)"
	@echo "    make test-access-control-spark  the same gates asserted through Spark (adds a JVM per query)"
	@echo "    make clippy       cargo clippy --all-targets -- -D warnings"
	@echo "    make fmt          cargo fmt --all"
	@echo "    make fmt-check    cargo fmt --all --check"
	@echo ""
	@echo "  Supply chain:"
	@echo "    make audit        cargo audit + cargo deny check (advisories, bans, sources)"
	@echo "    make audit-advisories  cargo audit only (RUSTSEC advisory scan)"
	@echo "    make audit-deny   cargo deny check only (advisories, bans, sources)"
	@echo "    make audit-licenses    cargo deny check licenses (needs a [licenses] allow list)"
	@echo ""
	@echo "  Benchmarks (bring up their own Polaris + RustFS stack):"
	@echo "    make benchmark_sf0.1   All suites at SF0.1 (fast smoke)"
	@echo "    make benchmark_sf1     All suites at SF1"
	@echo "    make benchmark_sf10    All suites at SF10 (heavy)"
	@echo "    make benchmark_all     SF0.1 + SF1 + SF10, in that order"
	@echo "    Append _trino to any of them to also compare results against Trino,"
	@echo "    e.g. make benchmark_sf1_trino"
	@echo "    Knobs: BENCH_SUITES=\"tpch ssb\" (default: all suites)"
	@echo "           BENCH_PROFILE=dev-release (default release; baselines need release)"
	@echo "           TRINO_MEMORY=$(TRINO_MEMORY) (comparison mode only)"
	@echo "           BENCH_LOG_DIR=$(BENCH_LOG_DIR)"
	@echo ""
	@echo "  Documentation:"
	@echo "    make rustbook         Build the mdbook (HTML) at $(BOOK_OUT)"
	@echo "    make ebook            Build the ebook (PDF + EPUB) under $(EBOOK_DIR)/build"
	@echo "    make ebook-pdf        Build only the PDF"
	@echo "    make ebook-epub       Build only the EPUB"
	@echo "    make ebook-html       Build a self-contained HTML version"
	@echo "    make benchmark-charts Re-render docs/evidence/benchmark/charts/ from benchmarks/results/*.json"
	@echo ""
	@echo "  Container image:"
	@echo "    make build        Local single-arch image ($(SQE_IMAGE):$(IMAGE_TAG)) from $(SQE_DOCKERFILE)"
	@echo "    make sbom         CycloneDX SBOM of the built image -> $(SBOM_DIR)/sqe-$(DATE).json"
	@echo "    make sqe-config   Stage $(CONFIG_SRC_DIR)/*/sqe.toml -> $(CONFIG_OUT_DIR)/"
	@echo "    make images       build + sbom + sqe-config"
	@echo "    make login        docker login to $(REGISTRY_HOST) (creds from ./.env or env)"
	@echo "    make push         Multi-arch buildx build + push ($(PUSH_SQE):$(IMAGE_TAG) and :$(LATEST_TAG))"
	@echo ""
	@echo "  Combined:"
	@echo "    make all          audit + test + image build + benchmark_all (hours, not minutes:"
	@echo "                      full Dockerfile.full compile then SF0.1+SF1+SF10 over all suites)"
	@echo "    make all_trino    Same, with the SQE-vs-Trino comparison on"
	@echo "    make docs-and-image  dev build + rustbook + ebook + image build + sbom"
	@echo ""
	@echo "  Cleanup:"
	@echo "    make clean        Remove all build artefacts (cargo + book + ebook)"
	@echo "    make clean-rust   cargo clean"
	@echo "    make clean-rustbook  Remove $(BOOK_OUT)"
	@echo "    make clean-ebook  Remove $(EBOOK_DIR)/build"
	@echo "    make clean-images Remove $(CONFIG_OUT_DIR) staged configs"
	@echo "    make maintain     Incremental cache trim: cargo-sweep stale target/"
	@echo "                      artifacts, prune docker build cache, sweep /tmp logs"
	@echo ""
	@echo "  Diagnostics:"
	@echo "    make check-tools  Verify cargo / mdbook / pandoc / d2 / mmdc are present"
	@echo "    make leak-scan    Scan docs/site for secrets/PII before publishing"

# `all` is the full gate: supply chain, unit tests, the deployable image, then
# the benchmark sweep. `all_trino` is the same with SQE-vs-Trino comparison on.
# The previous meaning of `all` (docs + image) lives on as `docs-and-image`.
# Steps are recipe lines, not prerequisites, so they stay ordered under `-j`:
# the benchmark stack owns fixed ports and must not overlap with anything.
# This is a long gate, not a quick check: `build` compiles Dockerfile.full and
# the benchmark step cargo-builds sqe-bench/sqe-coordinator separately, then
# sweeps three scale factors. For a fast pre-push loop use `make audit test`.
all:
	$(MAKE) audit
	$(MAKE) test
	$(MAKE) build
	$(MAKE) benchmark_all

all_trino:
	$(MAKE) audit
	$(MAKE) test
	$(MAKE) build
	$(MAKE) benchmark_all_trino

docs-and-image: dev rustbook ebook build sbom

# ── Code: cargo builds ────────────────────────────────────────────────────
check:
	$(CARGO) check --workspace --all-targets --exclude sqe-cli
	$(CARGO) check --package sqe-cli --all-targets --no-default-features

dev:
	@echo "==> Building debug binaries ($(BIN_CLI), $(BIN_SERVER))"
	$(CARGO) build --no-default-features --bin $(BIN_CLI) --bin $(BIN_SERVER)
	@echo ""
	@echo "Binaries:"
	@ls -lh $(DEBUG_BIN)/$(BIN_CLI) $(DEBUG_BIN)/$(BIN_SERVER)

release:
	@echo "==> Building release binaries ($(BIN_CLI), $(BIN_SERVER))"
	$(CARGO) build --release --no-default-features --bin $(BIN_CLI) --bin $(BIN_SERVER)
	@echo ""
	@echo "Binaries:"
	@ls -lh $(RELEASE_BIN)/$(BIN_CLI) $(RELEASE_BIN)/$(BIN_SERVER)

test:
	@echo "==> Running unit tests"
	$(CARGO) test --workspace --exclude sqe-cli
	$(CARGO) test --package sqe-cli --no-default-features

# ── Access-control e2e (Polaris + Ranger + Keycloak) ──────────────────────
# Brings up a subset of quickstart/polaris-ranger-keycloak and runs the Rust
# access-control suite against it. Ranger Admin's first boot takes 2-4 minutes.
test-access-control:
	@scripts/access-control-test.sh

# ── Spark access-control parity (Polaris + Ranger + Keycloak + Spark) ─────
# Separate from test-access-control on purpose: that target deliberately excludes
# `spark` and `data-seed` from its dependency chain, which is what keeps it fast.
# Every assertion here starts a JVM.
test-access-control-spark:
	@scripts/spark-access-control-test.sh
# ── Supply chain: advisories, bans, sources ───────────────────────────────
# No extra flags: the ignore lists live in .cargo/audit.toml and deny.toml and
# deliberately differ (see the note at the top of deny.toml).
audit: audit-advisories audit-deny

audit-advisories:
	@command -v cargo-audit >/dev/null 2>&1 || \
		{ echo "cargo-audit not found. Install with: cargo install cargo-audit"; exit 1; }
	@echo "==> cargo audit (RUSTSEC advisories)"
	$(CARGO) audit

# License checking is deliberately NOT part of `audit`: deny.toml has no
# [licenses] section, and cargo-deny >= 0.16 rejects every license that is not
# explicitly allowed -- so `cargo deny check licenses` fails on MIT and
# Apache-2.0 today. `make audit-licenses` runs it for whoever adds that policy.
audit-deny:
	@command -v cargo-deny >/dev/null 2>&1 || \
		{ echo "cargo-deny not found. Install with: cargo install cargo-deny"; exit 1; }
	@echo "==> cargo deny check (advisories, bans, sources)"
	$(CARGO) deny check advisories bans sources

audit-licenses:
	@command -v cargo-deny >/dev/null 2>&1 || \
		{ echo "cargo-deny not found. Install with: cargo install cargo-deny"; exit 1; }
	@echo "==> cargo deny check licenses (needs a [licenses] allow list in deny.toml)"
	$(CARGO) deny check licenses

# ── Benchmarks ────────────────────────────────────────────────────────────
# One suite sweep per scale factor; the _trino variants add --compare-trino
# and fail the run if a Trino outage silently skipped any comparison.
benchmark_sf0.1:
	@$(BENCH_ENV) $(BENCH_RUN) 0.1 $(BENCH_SUITES)

benchmark_sf1:
	@$(BENCH_ENV) $(BENCH_RUN) 1 $(BENCH_SUITES)

benchmark_sf10:
	@$(BENCH_ENV) $(BENCH_RUN) 10 $(BENCH_SUITES)

benchmark_all:
	@$(BENCH_ENV) $(BENCH_RUN) 0.1 $(BENCH_SUITES)
	@$(BENCH_ENV) $(BENCH_RUN) 1 $(BENCH_SUITES)
	@$(BENCH_ENV) $(BENCH_RUN) 10 $(BENCH_SUITES)

benchmark_sf0.1_trino:
	@$(BENCH_ENV) TRINO_MEMORY=$(TRINO_MEMORY) $(BENCH_RUN) 0.1 --compare-trino $(BENCH_SUITES)

benchmark_sf1_trino:
	@$(BENCH_ENV) TRINO_MEMORY=$(TRINO_MEMORY) $(BENCH_RUN) 1 --compare-trino $(BENCH_SUITES)

benchmark_sf10_trino:
	@$(BENCH_ENV) TRINO_MEMORY=$(TRINO_MEMORY) $(BENCH_RUN) 10 --compare-trino $(BENCH_SUITES)

benchmark_all_trino:
	@$(BENCH_ENV) TRINO_MEMORY=$(TRINO_MEMORY) $(BENCH_RUN) 0.1 --compare-trino $(BENCH_SUITES)
	@$(BENCH_ENV) TRINO_MEMORY=$(TRINO_MEMORY) $(BENCH_RUN) 1 --compare-trino $(BENCH_SUITES)
	@$(BENCH_ENV) TRINO_MEMORY=$(TRINO_MEMORY) $(BENCH_RUN) 10 --compare-trino $(BENCH_SUITES)

clippy:
	@echo "==> Running clippy"
	$(CARGO) clippy --all-targets --all-features -- -D warnings

fmt:
	@echo "==> Formatting code"
	$(CARGO) fmt --all

fmt-check:
	@echo "==> Checking formatting"
	$(CARGO) fmt --all --check

# ── Container image: build / SBOM / config / push ─────────────────────────
# Build context is the repo root (the Dockerfile COPYs Cargo.toml, crates/,
# vendor/, xtask/ relative to it). `build` loads a single-arch image into the
# local docker store so you can `docker run` / scan it immediately.
build: build-sqe sqe-config  ## Local image build + staged configs

build-sqe:
	@echo "==> Building $(SQE_IMAGE):$(IMAGE_TAG) from $(SQE_DOCKERFILE)"
	$(DOCKER) build $(SQE_BUILD_ARGS) -t $(SQE_IMAGE):$(IMAGE_TAG) -f $(SQE_DOCKERFILE) .

# SBOM scans the freshly built local image (hence the build dependency) so the
# component list always matches what was just produced. Needs `syft` on PATH.
sbom: sbom-sqe

sbom-sqe: build-sqe
	@mkdir -p $(SBOM_DIR)
	@echo "==> SBOM -> $(SBOM_DIR)/sqe-$(DATE).json"
	syft docker:$(SQE_IMAGE):$(IMAGE_TAG) -o cyclonedx-json=$(SBOM_DIR)/sqe-$(DATE).json

# Stage deployable configs: copy every quickstart/*/sqe.toml into
# $(CONFIG_OUT_DIR)/<scenario>.toml and record the image ref they pair with.
sqe-config:
	@mkdir -p $(CONFIG_OUT_DIR)
	@for cfg in $(CONFIG_SRC_DIR)/*/sqe.toml; do \
		[ -f "$$cfg" ] || continue; \
		scenario=$$(basename $$(dirname $$cfg)); \
		cp "$$cfg" "$(CONFIG_OUT_DIR)/$$scenario.toml"; \
		echo "  staged $$scenario.toml"; \
	done
	@echo "$(PUSH_SQE):$(IMAGE_TAG)" > $(CONFIG_OUT_DIR)/IMAGE
	@echo "==> configs -> $(CONFIG_OUT_DIR)/ (pairs with $(PUSH_SQE):$(IMAGE_TAG))"

images: build-sqe sbom-sqe sqe-config  ## Local image + SBOM + staged configs

# ── Container image: push (multi-arch, production) ─────────────────────────
# `make login` once per session, then `make push`. Credentials are read from
# ./.env (gitignored) if present, else from REGISTRY_USER / REGISTRY_PASSWORD
# in the environment. They are never stored in this file.
login:
	@set -a; [ -f .env ] && . ./.env || true; set +a; \
		test -n "$$REGISTRY_USER" -a -n "$$REGISTRY_PASSWORD" || \
			{ echo "set REGISTRY_USER and REGISTRY_PASSWORD (in ./.env or the environment)"; exit 1; }; \
		printf '%s' "$$REGISTRY_PASSWORD" | $(DOCKER) login $(REGISTRY_HOST) -u "$$REGISTRY_USER" --password-stdin

# Ensure a docker-container-driver buildx builder exists (the default `docker`
# driver can't emit multi-platform manifest lists). Idempotent.
buildx-builder:
	@$(DOCKER) buildx inspect $(BUILDX_BUILDER) >/dev/null 2>&1 || \
		$(DOCKER) buildx create --name $(BUILDX_BUILDER) --driver docker-container --bootstrap >/dev/null

# buildx builds-and-pushes the multi-arch manifest list in one step (it does
# NOT depend on the local build-sqe; depending on it would compile twice).
push: push-sqe  ## Multi-arch build + push (run `make login` first)

push-sqe: buildx-builder
	@echo "==> buildx push $(PUSH_SQE):$(IMAGE_TAG) + :$(LATEST_TAG) [$(PLATFORMS)]"
	$(DOCKER) buildx build $(SQE_BUILD_ARGS) --builder $(BUILDX_BUILDER) --platform $(PLATFORMS) \
		-t $(PUSH_SQE):$(IMAGE_TAG) -t $(PUSH_SQE):$(LATEST_TAG) \
		-f $(SQE_DOCKERFILE) --push .

# ── Docs: rust book (mdbook) ──────────────────────────────────────────────
rustbook:
	@echo "==> Building rust book (mdbook) -> $(BOOK_OUT)"
	cd $(BOOK_DIR) && $(MDBOOK) build
	@echo ""
	@echo "Open: $(BOOK_OUT)/index.html"

# ── Docs: ebook (pandoc) ──────────────────────────────────────────────────
# Delegate to docs/site/ebook/Makefile; it owns the PDF / EPUB / HTML pipeline.
ebook:
	@echo "==> Building ebook (PDF + EPUB)"
	$(MAKE) -C $(EBOOK_DIR) all

ebook-pdf:
	@echo "==> Building ebook PDF"
	$(MAKE) -C $(EBOOK_DIR) pdf

ebook-epub:
	@echo "==> Building ebook EPUB"
	$(MAKE) -C $(EBOOK_DIR) epub

ebook-html:
	@echo "==> Building ebook HTML"
	$(MAKE) -C $(EBOOK_DIR) html

# ── Docs: benchmark history charts ────────────────────────────────────────
# Walks benchmarks/results/*.json and re-renders docs/evidence/benchmark/charts/.
# Needs matplotlib in a Python venv. The script self-tests for matplotlib
# and prints how to set it up if missing.
BENCH_PY ?= /tmp/sqe-bench-env/bin/python3

benchmark-charts:
	@if [ ! -x "$(BENCH_PY)" ]; then \
		echo "Python venv with matplotlib not found at $(BENCH_PY)."; \
		echo "Set it up once with:"; \
		echo "  uv venv /tmp/sqe-bench-env && uv pip install --python $(BENCH_PY) matplotlib"; \
		echo "Then re-run \`make benchmark-charts\`."; \
		exit 1; \
	fi
	@echo "==> Rendering benchmark charts -> docs/evidence/benchmark/charts/"
	$(BENCH_PY) scripts/render-benchmark-charts.py

# ── Cleanup ───────────────────────────────────────────────────────────────
clean: clean-rust clean-rustbook clean-ebook clean-benchmark-charts clean-images

maintain:
	@./scripts/dev-maintenance.sh

clean-rust:
	@echo "==> cargo clean"
	$(CARGO) clean

clean-rustbook:
	@echo "==> Removing $(BOOK_OUT)"
	rm -rf $(BOOK_OUT)

clean-ebook:
	@echo "==> Cleaning ebook build artefacts"
	$(MAKE) -C $(EBOOK_DIR) clean

clean-benchmark-charts:
	@echo "==> Removing docs/evidence/benchmark/charts/"
	rm -rf docs/evidence/benchmark/charts

clean-images:
	@echo "==> Removing $(CONFIG_OUT_DIR)"
	rm -rf $(CONFIG_OUT_DIR)

# ── Diagnostics ───────────────────────────────────────────────────────────
check-tools:
	@echo "==> Checking required tools"
	@for tool in cargo rustc cargo-audit cargo-deny mdbook mdbook-mermaid pandoc d2 mmdc; do \
		if command -v $$tool >/dev/null 2>&1; then \
			printf "  [ok]  %-18s %s\n" "$$tool" "$$(command -v $$tool)"; \
		else \
			printf "  [MISSING] %-15s install before running the relevant target\n" "$$tool"; \
		fi; \
	done
	@echo ""
	@echo "  audit needs:     cargo-audit, cargo-deny"
	@echo "  rustbook needs:  mdbook, mdbook-mermaid"
	@echo "  ebook needs:     pandoc, pandoc-crossref, d2, mmdc, xelatex (or weasyprint)"
	@echo "  ebook PDF needs: rsvg-convert (librsvg) or cairosvg for SVG -> PDF"

# ── Publish guard: secrets / PII scan ─────────────────────────────────────
leak-scan:
	@echo "==> Scanning docs/site for leaks"
	@bash scripts/leak-scan-site.sh docs/site
