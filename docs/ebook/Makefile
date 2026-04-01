CHAPTERS := $(sort $(wildcard chapters/*.md))
META     := metadata.yaml
OUT      := build
DIAGRAMS := $(wildcard diagrams/*.d2) $(wildcard diagrams/*.mmd)

.PHONY: all pdf epub html diagrams validate clean

all: diagrams pdf epub

# --- Diagrams ---
diagrams:
	@echo "==> Rendering diagrams"
	@mkdir -p diagrams/rendered
	@for f in diagrams/*.d2; do \
		[ -f "$$f" ] && d2 --theme 200 "$$f" "diagrams/rendered/$$(basename $$f .d2).svg" || true; \
	done
	@for f in diagrams/*.mmd; do \
		[ -f "$$f" ] && mmdc -i "$$f" -o "diagrams/rendered/$$(basename $$f .mmd).svg" || true; \
	done

# --- PDF (print-quality) ---
# Uses xelatex if available, falls back to weasyprint
PDF_ENGINE := $(shell command -v xelatex >/dev/null 2>&1 && echo xelatex || echo weasyprint)

pdf: $(OUT)/sovereign-by-design.pdf

$(OUT)/sovereign-by-design.pdf: $(META) $(CHAPTERS) | $(OUT)
	@echo "==> Building PDF (engine: $(PDF_ENGINE))"
	pandoc $(META) $(CHAPTERS) \
		--pdf-engine=$(PDF_ENGINE) \
		--filter pandoc-crossref \
		--top-level-division=chapter \
		--resource-path=.:diagrams/rendered \
		-o $@

# --- EPUB ---
epub: $(OUT)/sovereign-by-design.epub

$(OUT)/sovereign-by-design.epub: $(META) $(CHAPTERS) | $(OUT)
	@echo "==> Building EPUB"
	pandoc $(META) $(CHAPTERS) \
		--filter pandoc-crossref \
		--top-level-division=chapter \
		--resource-path=.:diagrams/rendered \
		--epub-chapter-level=1 \
		-o $@

# --- HTML (single page) ---
html: $(OUT)/sovereign-by-design.html

$(OUT)/sovereign-by-design.html: $(META) $(CHAPTERS) | $(OUT)
	@echo "==> Building HTML"
	pandoc $(META) $(CHAPTERS) \
		--filter pandoc-crossref \
		--top-level-division=chapter \
		--resource-path=.:diagrams/rendered \
		--standalone --self-contained \
		--template=default \
		-o $@

# --- Validation ---
validate: epub
	@echo "==> Validating EPUB"
	epubcheck $(OUT)/sovereign-by-design.epub

# --- Utilities ---
$(OUT):
	mkdir -p $(OUT)

clean:
	rm -rf $(OUT) diagrams/rendered

# --- Word count ---
wc:
	@echo "==> Word counts per chapter:"
	@for f in $(CHAPTERS); do \
		printf "  %6d  %s\n" $$(wc -w < "$$f") "$$f"; \
	done
	@echo "  ------"
	@printf "  %6d  total\n" $$(cat $(CHAPTERS) | wc -w)
