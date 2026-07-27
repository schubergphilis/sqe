# jiter (SQE vendor patch)

Copy of crates.io `jiter` **0.15.0** used by `datafusion-functions-json` 0.54.x.

## Why vendored

Upstream `jiter` 0.15 declares optional `pyo3 = "0.28.2"` (for the unused
`python` feature). Cargo still locks that optional package; Syft/grype then
flag **GHSA-36hh-v3qg-5jq4** / RUSTSEC-2026-0176 (OOB read in PyList/PyTuple
`nth` / `nth_back`, fixed in pyo3 **>= 0.29.0**).

`jiter` 0.16 already uses pyo3 0.29 but is not allowed by
`datafusion-functions-json`'s `jiter = "0.15.0"` requirement.

## SQE change

- Optional / dev / build `pyo3` and `pyo3-build-config` pins: `0.28.2` → **`0.29.0`**
- Applied via root `Cargo.toml` `[patch.crates-io] jiter = { path = "vendor/jiter" }`

Drop this vendor when `datafusion-functions-json` moves to jiter >= 0.16.
