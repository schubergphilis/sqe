# SQE top-level Makefile.
#
# Convenience wrappers around cargo, mdbook, and the ebook build pipeline.
# The actual build logic lives in:
#   - cargo (Cargo.toml): Rust binaries `sqe-cli` and `sqe-server`
#   - mdbook (docs/book/book.toml): the rust book
#   - pandoc (docs/ebook/Makefile): the PDF / EPUB ebook
#
# This Makefile orchestrates them so a contributor can run `make rustbook`
# without remembering the mdbook invocation, and `make all` to build
# everything in one shot.

# ── Configuration ─────────────────────────────────────────────────────────
CARGO        ?= cargo
MDBOOK       ?= mdbook
BOOK_DIR     := docs/book
EBOOK_DIR    := docs/ebook
BOOK_OUT     := target/book
RELEASE_BIN  := target/release
DEBUG_BIN    := target/debug

# Which crate / binary names cargo knows about.
BIN_CLI      := sqe-cli
BIN_SERVER   := sqe-server

.PHONY: help all dev release rustbook ebook ebook-pdf ebook-epub ebook-html \
        test clippy fmt fmt-check clean clean-rust clean-rustbook clean-ebook \
        check-tools

# ── Default target ────────────────────────────────────────────────────────
help:
	@echo "SQE build targets:"
	@echo ""
	@echo "  Code:"
	@echo "    make dev          Debug build of sqe-cli + sqe-server (fast compile)"
	@echo "    make release      Release build of sqe-cli + sqe-server (LTO, optimised)"
	@echo "    make test         cargo test --workspace"
	@echo "    make clippy       cargo clippy --all-targets -- -D warnings"
	@echo "    make fmt          cargo fmt --all"
	@echo "    make fmt-check    cargo fmt --all --check"
	@echo ""
	@echo "  Documentation:"
	@echo "    make rustbook     Build the mdbook (HTML) at $(BOOK_OUT)"
	@echo "    make ebook        Build the ebook (PDF + EPUB) under $(EBOOK_DIR)/build"
	@echo "    make ebook-pdf    Build only the PDF"
	@echo "    make ebook-epub   Build only the EPUB"
	@echo "    make ebook-html   Build a self-contained HTML version"
	@echo ""
	@echo "  Combined:"
	@echo "    make all          dev build + rustbook + ebook"
	@echo ""
	@echo "  Cleanup:"
	@echo "    make clean        Remove all build artefacts (cargo + book + ebook)"
	@echo "    make clean-rust   cargo clean"
	@echo "    make clean-rustbook  Remove $(BOOK_OUT)"
	@echo "    make clean-ebook  Remove $(EBOOK_DIR)/build"
	@echo ""
	@echo "  Diagnostics:"
	@echo "    make check-tools  Verify cargo / mdbook / pandoc / d2 / mmdc are present"

all: dev rustbook ebook

# ── Code: cargo builds ────────────────────────────────────────────────────
dev:
	@echo "==> Building debug binaries ($(BIN_CLI), $(BIN_SERVER))"
	$(CARGO) build --bin $(BIN_CLI) --bin $(BIN_SERVER)
	@echo ""
	@echo "Binaries:"
	@ls -lh $(DEBUG_BIN)/$(BIN_CLI) $(DEBUG_BIN)/$(BIN_SERVER)

release:
	@echo "==> Building release binaries ($(BIN_CLI), $(BIN_SERVER))"
	$(CARGO) build --release --bin $(BIN_CLI) --bin $(BIN_SERVER)
	@echo ""
	@echo "Binaries:"
	@ls -lh $(RELEASE_BIN)/$(BIN_CLI) $(RELEASE_BIN)/$(BIN_SERVER)

test:
	@echo "==> Running unit tests"
	$(CARGO) test --workspace

clippy:
	@echo "==> Running clippy"
	$(CARGO) clippy --all-targets --all-features -- -D warnings

fmt:
	@echo "==> Formatting code"
	$(CARGO) fmt --all

fmt-check:
	@echo "==> Checking formatting"
	$(CARGO) fmt --all --check

# ── Docs: rust book (mdbook) ──────────────────────────────────────────────
rustbook:
	@echo "==> Building rust book (mdbook) -> $(BOOK_OUT)"
	cd $(BOOK_DIR) && $(MDBOOK) build
	@echo ""
	@echo "Open: $(BOOK_OUT)/index.html"

# ── Docs: ebook (pandoc) ──────────────────────────────────────────────────
# Delegate to docs/ebook/Makefile; it owns the PDF / EPUB / HTML pipeline.
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

# ── Cleanup ───────────────────────────────────────────────────────────────
clean: clean-rust clean-rustbook clean-ebook

clean-rust:
	@echo "==> cargo clean"
	$(CARGO) clean

clean-rustbook:
	@echo "==> Removing $(BOOK_OUT)"
	rm -rf $(BOOK_OUT)

clean-ebook:
	@echo "==> Cleaning ebook build artefacts"
	$(MAKE) -C $(EBOOK_DIR) clean

# ── Diagnostics ───────────────────────────────────────────────────────────
check-tools:
	@echo "==> Checking required tools"
	@for tool in cargo rustc mdbook mdbook-mermaid pandoc d2 mmdc; do \
		if command -v $$tool >/dev/null 2>&1; then \
			printf "  [ok]  %-18s %s\n" "$$tool" "$$(command -v $$tool)"; \
		else \
			printf "  [MISSING] %-15s install before running the relevant target\n" "$$tool"; \
		fi; \
	done
	@echo ""
	@echo "  rustbook needs:  mdbook, mdbook-mermaid"
	@echo "  ebook needs:     pandoc, pandoc-crossref, d2, mmdc, xelatex (or weasyprint)"
	@echo "  ebook PDF needs: rsvg-convert (librsvg) or cairosvg for SVG -> PDF"
