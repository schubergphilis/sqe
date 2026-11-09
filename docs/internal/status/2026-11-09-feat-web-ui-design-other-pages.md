**STATUS 2026-11-09: playground and OAuth2 pages on the web UI design (`feat/web-ui-design-other-pages`).**

Follow-up to !1693 (coordinator dashboard refresh). The other two HTML surfaces
now use the same tokens:

- `playground/www/index.html`: stylesheet and brand block only; class names and
  JS behaviour unchanged. The old `--amber/--ink/--dim/--faint` tokens are
  renamed to the dashboard names (`--accent/--text/--text-2/--text-3`), syntax
  colours moved to `--syn-*`, dark set via `prefers-color-scheme`. Primary
  button uses a fixed `#1A6FD6` fill so white text clears 4.5:1 in both themes.
- `crates/sqe-trino-compat/src/oauth2.rs`: the success and failure callback
  pages are built by a static `callback_page!` macro (no interpolation, no
  external requests); new unit test `callback_pages_are_self_contained_html`.

Checked in Playwright against the local wasm build and the rendered callback
pages, light and dark; `cargo test -p sqe-trino-compat --lib oauth2` 10 passed,
clippy clean. The playground still has a fixed 288 px rail and does not fit a
phone-width screen (unchanged from before).

**NEXT:** make the playground rail collapse below ~700 px wide.
