# Unified Benchmark Harness (Read-Suite Path) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `sqe-bench run` verb + config-profile system + `scripts/benchmark.sh` infra shim so `sqe-bench run tpch ssb tpcds clickbench --profile local` reproduces today's full-attach read-suite results with **zero data load** and identical output artifacts.

**Architecture:** Two layers. `scripts/benchmark.sh` (bash) owns infrastructure: docker stack, bootstrap, coordinator spawn/health-wait/teardown. `sqe-bench run` (Rust, connect-only) owns benchmark logic: ATTACH the golden catalog over Flight, run each suite's queries with `--catalog golden`, emit `BENCH_SUMMARY` + JSON, optional Trino compare. The `run` verb reuses the existing `test`/`compare` code paths and adds a profile-driven ATTACH step. This plan is scoped to the **read path only**; write-suite `reset`/`copy` is a follow-up plan.

**Tech Stack:** Rust (clap derive, `serde`/`toml`, `arrow-flight`, `iceberg-catalog-rest`), bash. Depends on the attach-golden foundation (MR !654: `crates/sqe-catalog/src/mount.rs` S3 ATTACH options, `tests/benchmark-attach/coordinator-attach.toml`).

## Global Constraints

- **Depends on MR !654 being merged to main** (attach-golden read path). Rebase this branch onto main once !654 lands.
- **No committed secrets.** `benchmarks/profiles/*.toml` files that ship in the repo MUST NOT contain S3 secret keys or OAuth client secrets. Credentials resolve at runtime from env vars or a named AWS credentials profile. A secret-bearing profile variant (`*.local.toml`) is gitignored.
- **No secret logging.** The harness never prints S3 secret keys, OAuth client secrets, or bearer tokens. When echoing the ATTACH statement for debugging, secret-valued options are redacted to `***`.
- **Prefer the secret store over inline-SQL secrets.** Where the engine supports `CREATE SECRET` + `SECRET <name>`, use it; inline `S3_SECRET_KEY`/`TOKEN` in ATTACH SQL is a documented dev/bench fallback only.
- **Reuse, do not fork.** `run` calls the existing `test::run_benchmark_test`, `report::print_summary`, `report::write_json_report`, `comparison::run_comparison`. Do not duplicate query-loading or reporting logic.
- **Output artifacts unchanged:** per-query lines, `BENCH_SUMMARY:<suite>:pass:fail:diff:skip:error:total:ms`, `benchmarks/results/<suite>-sf<scale>-<protocol>-<ts>.json`, `benchmarks/results/compare-<suite>-sf<scale>-<ts>.json`.
- **Clippy strict:** `cargo clippy --all-targets --all-features -- -D warnings` must pass.
- **Bench e2e is stack-gated.** Full attach runs are not locally reproducible in unit tests. Unit-test the pure logic (profile parse, ATTACH-SQL build + redaction, CLI parse); gate the e2e integration test behind a running stack env var, mirroring the existing direct-sink gated test.

---

## File Structure

- Create `crates/sqe-bench/src/profile.rs` — profile schema (`serde`), TOML loader, runtime credential resolution, ATTACH-SQL builder with redaction. One responsibility: turn a profile name + env into a validated, secret-resolved config and the ATTACH statement.
- Create `benchmarks/profiles/local.toml` — the `local` preset (rustfs, manage_stack=true), no secrets (localhost quickstart defaults are non-secret, but creds still resolve via env with these as documented defaults).
- Modify `crates/sqe-bench/src/cli.rs` — add the `Run` subcommand variant.
- Modify `crates/sqe-bench/src/main.rs` — add the `Command::Run` dispatch arm.
- Create `crates/sqe-bench/src/run.rs` — the `run` orchestration: connect, attach, per-suite test, emit, optional compare.
- Modify `crates/sqe-bench/src/main.rs` — declare `mod profile;` and `mod run;`. **Important:** the `sqe-bench` binary has its own module tree — `main.rs` declares `mod cli; mod client; mod test; mod report; mod comparison;` directly; `lib.rs` only exports `generate`/`sink`. New modules must be `mod`-declared in `main.rs` (not `lib.rs`) to be visible to the dispatch arm, and their unit tests run under the **bin** target (`cargo test -p sqe-bench --bin sqe-bench <filter>`), not `--lib`.
- Create `scripts/benchmark.sh` — infra shim.
- Modify `.gitignore` — ignore `benchmarks/profiles/*.local.toml`.
- Create `crates/sqe-bench/tests/run_attach_gated.rs` — stack-gated e2e integration test.

---

## Task 1: Profile schema + TOML loader

**Files:**
- Create: `crates/sqe-bench/src/profile.rs`
- Modify: `crates/sqe-bench/src/main.rs` (add `mod profile;`)
- Test: inline `#[cfg(test)]` in `profile.rs`

**Interfaces:**
- Produces:
  - `pub struct Profile { pub name: String, pub manage_stack: bool, pub s3: S3Profile, pub polaris: PolarisProfile, pub concurrency: ConcurrencyProfile }`
  - `pub struct S3Profile { pub endpoint: String, pub region: String, pub path_style: bool, pub aws_profile: Option<String>, pub warehouse_bucket: String }`
  - `pub struct PolarisProfile { pub url: String, pub warehouse: String }`
  - `pub struct ConcurrencyProfile { pub manifest: u32, pub direct_read: u32, pub write_streams: u32 }`
  - `pub fn load_profile(name_or_path: &str) -> anyhow::Result<Profile>`

- [ ] **Step 1: Add module declaration**

In `crates/sqe-bench/src/main.rs`, add alongside the existing `mod cli; mod client; ...` lines:

```rust
mod profile;
```

- [ ] **Step 2: Write the failing test**

Create `crates/sqe-bench/src/profile.rs` with only the test module first:

```rust
//! Benchmark environment profiles: one TOML file per environment
//! (`local`, `storagegrid`, `r2`, `aws`, or a custom path) describing the
//! S3 store, the Polaris catalog, and the concurrency knobs. Secrets are
//! never stored here; they resolve at runtime from env vars or a named AWS
//! credentials profile (see `resolve_s3_credentials`).

#[cfg(test)]
mod tests {
    use super::*;

    const LOCAL_TOML: &str = r#"
name = "local"
manage_stack = true

[s3]
endpoint = "http://localhost:19000"
region = "us-east-1"
path_style = true
warehouse_bucket = "test-warehouse"

[polaris]
url = "http://localhost:18181/api/catalog"
warehouse = "test_warehouse"

[concurrency]
manifest = 64
direct_read = 8
write_streams = 8
"#;

    #[test]
    fn parses_a_full_profile() {
        let p = parse_profile_str(LOCAL_TOML).expect("parse");
        assert_eq!(p.name, "local");
        assert!(p.manage_stack);
        assert_eq!(p.s3.endpoint, "http://localhost:19000");
        assert!(p.s3.path_style);
        assert_eq!(p.polaris.warehouse, "test_warehouse");
        assert_eq!(p.concurrency.manifest, 64);
    }

    #[test]
    fn concurrency_defaults_apply_when_section_absent() {
        let toml = r#"
name = "min"
manage_stack = false
[s3]
endpoint = "http://x"
region = "us-east-1"
path_style = true
warehouse_bucket = "b"
[polaris]
url = "http://p"
warehouse = "w"
"#;
        let p = parse_profile_str(toml).expect("parse");
        assert_eq!(p.concurrency.manifest, 64);
        assert_eq!(p.concurrency.direct_read, 8);
    }

    #[test]
    fn rejects_inline_secret_keys() {
        let toml = r#"
name = "bad"
manage_stack = false
[s3]
endpoint = "http://x"
region = "us-east-1"
path_style = true
warehouse_bucket = "b"
secret_key = "AKIAsupersecret"
[polaris]
url = "http://p"
warehouse = "w"
"#;
        let err = parse_profile_str(toml).unwrap_err().to_string();
        assert!(err.contains("secret"), "got: {err}");
    }
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cargo test -p sqe-bench --bin sqe-bench profile::tests`
Expected: FAIL to compile ("cannot find function `parse_profile_str`").

- [ ] **Step 4: Write the implementation**

Prepend to `crates/sqe-bench/src/profile.rs` (above the test module):

```rust
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Profile {
    pub name: String,
    #[serde(default)]
    pub manage_stack: bool,
    pub s3: S3Profile,
    pub polaris: PolarisProfile,
    #[serde(default)]
    pub concurrency: ConcurrencyProfile,
}

#[derive(Debug, Clone, Deserialize)]
pub struct S3Profile {
    pub endpoint: String,
    #[serde(default = "default_region")]
    pub region: String,
    #[serde(default)]
    pub path_style: bool,
    /// Named AWS credentials profile to resolve access/secret keys from at
    /// runtime. `None` => resolve from AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY.
    #[serde(default)]
    pub aws_profile: Option<String>,
    pub warehouse_bucket: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PolarisProfile {
    pub url: String,
    pub warehouse: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConcurrencyProfile {
    #[serde(default = "default_manifest")]
    pub manifest: u32,
    #[serde(default = "default_direct_read")]
    pub direct_read: u32,
    #[serde(default = "default_write_streams")]
    pub write_streams: u32,
}

impl Default for ConcurrencyProfile {
    fn default() -> Self {
        Self {
            manifest: default_manifest(),
            direct_read: default_direct_read(),
            write_streams: default_write_streams(),
        }
    }
}

fn default_region() -> String {
    "us-east-1".to_string()
}
fn default_manifest() -> u32 {
    64
}
fn default_direct_read() -> u32 {
    8
}
fn default_write_streams() -> u32 {
    8
}

/// Parse a profile from a TOML string. Rejects any inline S3/OAuth secret so
/// a committed profile can never carry a credential (constraint: no committed
/// secrets). Callers use `load_profile` for the name/path resolution wrapper.
pub fn parse_profile_str(s: &str) -> anyhow::Result<Profile> {
    // Guard: forbid secret-bearing keys anywhere in the document. These belong
    // in env vars or a named AWS profile, never in a repo-tracked file.
    for forbidden in ["secret_key", "secret-key", "client_secret", "client-secret", "token ="] {
        if s.contains(forbidden) {
            anyhow::bail!(
                "profile contains a forbidden inline secret key ('{forbidden}'); \
                 resolve credentials from env or an AWS profile instead"
            );
        }
    }
    let profile: Profile = toml::from_str(s)?;
    Ok(profile)
}

/// Load a profile by preset name (`local`, `storagegrid`, `r2`, `aws`) from
/// `benchmarks/profiles/<name>.toml`, or from an explicit file path if the
/// argument contains a path separator or ends in `.toml`.
pub fn load_profile(name_or_path: &str) -> anyhow::Result<Profile> {
    let path = if name_or_path.contains('/') || name_or_path.ends_with(".toml") {
        std::path::PathBuf::from(name_or_path)
    } else {
        std::path::PathBuf::from(format!("benchmarks/profiles/{name_or_path}.toml"))
    };
    let text = std::fs::read_to_string(&path)
        .map_err(|e| anyhow::anyhow!("reading profile {}: {e}", path.display()))?;
    parse_profile_str(&text)
}
```

Ensure `toml` and `serde` (with `derive`) are dependencies of `crates/sqe-bench/Cargo.toml`. If `toml` is absent, add `toml = "0.8"` to `[dependencies]`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p sqe-bench --bin sqe-bench profile::tests`
Expected: PASS (3 tests).

- [ ] **Step 6: Commit**

```bash
git add crates/sqe-bench/src/profile.rs crates/sqe-bench/src/main.rs crates/sqe-bench/Cargo.toml
git commit -m "feat(bench): profile schema + TOML loader (no-committed-secrets guard)"
```

---

## Task 2: Runtime credential resolution + ATTACH-SQL builder with redaction

**Files:**
- Modify: `crates/sqe-bench/src/profile.rs`
- Test: inline `#[cfg(test)]` in `profile.rs`

**Interfaces:**
- Consumes: `Profile`, `S3Profile` (Task 1).
- Produces:
  - `pub struct S3Credentials { pub access_key: String, pub secret_key: String }`
  - `pub fn resolve_s3_credentials(s3: &S3Profile) -> anyhow::Result<S3Credentials>`
  - `pub fn build_attach_sql(catalog: &str, profile: &Profile, creds: &S3Credentials, token: &str) -> String`
  - `pub fn redact_attach_sql(sql: &str) -> String`

- [ ] **Step 1: Write the failing test**

Add to the `#[cfg(test)] mod tests` block in `profile.rs`:

```rust
    fn sample_profile() -> Profile {
        parse_profile_str(LOCAL_TOML).unwrap()
    }

    #[test]
    fn build_attach_sql_includes_s3_options() {
        let p = sample_profile();
        let creds = S3Credentials {
            access_key: "ak".to_string(),
            secret_key: "sk".to_string(),
        };
        let sql = build_attach_sql("golden", &p, &creds, "tok");
        assert!(sql.starts_with("ATTACH 'http://localhost:18181/api/catalog' AS golden"));
        assert!(sql.contains("TYPE iceberg_rest"));
        assert!(sql.contains("WAREHOUSE 'test_warehouse'"));
        assert!(sql.contains("S3_ENDPOINT 'http://localhost:19000'"));
        assert!(sql.contains("S3_ACCESS_KEY 'ak'"));
        assert!(sql.contains("S3_SECRET_KEY 'sk'"));
        assert!(sql.contains("S3_PATH_STYLE 'true'"));
        assert!(sql.contains("TOKEN 'tok'"));
    }

    #[test]
    fn redact_hides_secret_and_token_values() {
        let p = sample_profile();
        let creds = S3Credentials {
            access_key: "ak".to_string(),
            secret_key: "supersecret".to_string(),
        };
        let sql = build_attach_sql("golden", &p, &creds, "bearer-xyz");
        let red = redact_attach_sql(&sql);
        assert!(!red.contains("supersecret"), "secret leaked: {red}");
        assert!(!red.contains("bearer-xyz"), "token leaked: {red}");
        assert!(red.contains("S3_SECRET_KEY '***'"));
        assert!(red.contains("TOKEN '***'"));
        // Non-secret options remain visible for debugging.
        assert!(red.contains("S3_ENDPOINT 'http://localhost:19000'"));
    }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p sqe-bench --bin sqe-bench profile::tests::build_attach_sql_includes_s3_options`
Expected: FAIL to compile (`build_attach_sql`, `S3Credentials` not found).

- [ ] **Step 3: Write the implementation**

Add to `profile.rs` (above the test module):

```rust
/// Resolved S3 credentials. Held only in memory; never serialized or logged.
pub struct S3Credentials {
    pub access_key: String,
    pub secret_key: String,
}

/// Resolve S3 credentials at runtime. If the profile names an AWS credentials
/// profile, read it via `aws configure get` semantics through env; otherwise
/// fall back to the standard AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY vars.
/// Never reads secrets from the profile TOML (Task 1 forbids them).
pub fn resolve_s3_credentials(s3: &S3Profile) -> anyhow::Result<S3Credentials> {
    if let Some(prof) = &s3.aws_profile {
        let access_key = aws_config_get(prof, "aws_access_key_id")?;
        let secret_key = aws_config_get(prof, "aws_secret_access_key")?;
        return Ok(S3Credentials { access_key, secret_key });
    }
    let access_key = std::env::var("AWS_ACCESS_KEY_ID")
        .map_err(|_| anyhow::anyhow!("AWS_ACCESS_KEY_ID not set (and no aws_profile in profile)"))?;
    let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY")
        .map_err(|_| anyhow::anyhow!("AWS_SECRET_ACCESS_KEY not set (and no aws_profile in profile)"))?;
    Ok(S3Credentials { access_key, secret_key })
}

fn aws_config_get(profile: &str, key: &str) -> anyhow::Result<String> {
    let out = std::process::Command::new("aws")
        .args(["configure", "get", key, "--profile", profile])
        .output()
        .map_err(|e| anyhow::anyhow!("running `aws configure get {key}`: {e}"))?;
    if !out.status.success() {
        anyhow::bail!("aws profile '{profile}' has no {key}");
    }
    Ok(String::from_utf8(out.stdout)?.trim().to_string())
}

/// Build the coordinator-wide `ATTACH` statement for the golden catalog.
/// Secrets travel inline (dev/bench posture — the coordinator config is
/// localhost). Callers MUST log only `redact_attach_sql(&sql)`, never `sql`.
pub fn build_attach_sql(
    catalog: &str,
    profile: &Profile,
    creds: &S3Credentials,
    token: &str,
) -> String {
    format!(
        "ATTACH '{url}' AS {catalog} (\
         TYPE iceberg_rest, \
         WAREHOUSE '{warehouse}', \
         TOKEN '{token}', \
         S3_ENDPOINT '{endpoint}', \
         S3_REGION '{region}', \
         S3_ACCESS_KEY '{ak}', \
         S3_SECRET_KEY '{sk}', \
         S3_PATH_STYLE '{path_style}')",
        url = profile.polaris.url,
        warehouse = profile.polaris.warehouse,
        token = token,
        endpoint = profile.s3.endpoint,
        region = profile.s3.region,
        ak = creds.access_key,
        sk = creds.secret_key,
        path_style = profile.s3.path_style,
    )
}

/// Redact secret-valued options in an ATTACH statement for safe logging.
pub fn redact_attach_sql(sql: &str) -> String {
    let mut out = sql.to_string();
    for opt in ["S3_SECRET_KEY", "S3_ACCESS_KEY", "TOKEN"] {
        out = redact_option(&out, opt);
    }
    out
}

fn redact_option(sql: &str, opt: &str) -> String {
    // Replace `<OPT> '<value>'` with `<OPT> '***'`.
    let needle = format!("{opt} '");
    let Some(start) = sql.find(&needle) else {
        return sql.to_string();
    };
    let value_start = start + needle.len();
    let Some(rel_end) = sql[value_start..].find('\'') else {
        return sql.to_string();
    };
    let value_end = value_start + rel_end;
    format!("{}***{}", &sql[..value_start], &sql[value_end..])
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p sqe-bench --bin sqe-bench profile::tests`
Expected: PASS (all profile tests).

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-bench/src/profile.rs
git commit -m "feat(bench): runtime credential resolution + ATTACH-SQL builder with redaction"
```

---

## Task 3: `local.toml` preset + gitignore secret variants

**Files:**
- Create: `benchmarks/profiles/local.toml`
- Modify: `.gitignore`

**Interfaces:** none (data + config).

- [ ] **Step 1: Create the local preset**

Create `benchmarks/profiles/local.toml`:

```toml
# Local benchmark profile: docker test stack (Polaris + RustFS) on localhost.
# manage_stack=true => scripts/benchmark.sh brings the stack up and down.
#
# No secrets here. S3 credentials resolve at runtime from AWS_ACCESS_KEY_ID /
# AWS_SECRET_ACCESS_KEY (the local RustFS quickstart default is s3admin/s3admin;
# export them before running, or set aws_profile below to a named profile).
name = "local"
manage_stack = true

[s3]
endpoint = "http://localhost:19000"
region = "us-east-1"
path_style = true
warehouse_bucket = "test-warehouse"

[polaris]
url = "http://localhost:18181/api/catalog"
warehouse = "test_warehouse"

[concurrency]
manifest = 64
direct_read = 8
write_streams = 8
```

- [ ] **Step 2: Gitignore secret-bearing variants**

Append to `.gitignore`:

```
# Benchmark profiles carrying real credentials are never committed.
benchmarks/profiles/*.local.toml
benchmarks/profiles/*.secret.toml
```

- [ ] **Step 3: Verify the preset loads**

Run: `cargo test -p sqe-bench --bin sqe-bench profile::tests` (unchanged, still green) and manually:
Run: `cd $(git rev-parse --show-toplevel) && cargo run -p sqe-bench -- --help` to confirm the crate still builds.
Expected: build OK; profile file present.

- [ ] **Step 4: Commit**

```bash
git add benchmarks/profiles/local.toml .gitignore
git commit -m "feat(bench): local profile preset + gitignore secret profile variants"
```

---

## Task 4: `Run` CLI subcommand

**Files:**
- Modify: `crates/sqe-bench/src/cli.rs`
- Test: inline `#[cfg(test)]` in `cli.rs` (add if absent)

**Interfaces:**
- Produces: `Command::Run { suites: Vec<String>, profile: String, scale: f64, host: String, port: u16, compare_trino: bool, smoke: bool, query: Option<String> }`

- [ ] **Step 1: Write the failing test**

Add a test module at the bottom of `crates/sqe-bench/src/cli.rs`:

```rust
#[cfg(test)]
mod run_cli_tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn parses_run_with_multiple_suites() {
        let cli = Cli::parse_from([
            "sqe-bench", "run", "tpch", "ssb", "--profile", "local", "--scale", "10",
        ]);
        match cli.command {
            Command::Run { suites, profile, scale, compare_trino, .. } => {
                assert_eq!(suites, vec!["tpch", "ssb"]);
                assert_eq!(profile, "local");
                assert_eq!(scale, 10.0);
                assert!(!compare_trino);
            }
            _ => panic!("expected Run"),
        }
    }

    #[test]
    fn run_defaults_host_and_port() {
        let cli = Cli::parse_from(["sqe-bench", "run", "tpch", "--profile", "local"]);
        match cli.command {
            Command::Run { host, port, scale, .. } => {
                assert_eq!(host, "localhost");
                assert_eq!(port, 60051);
                assert_eq!(scale, 1.0);
            }
            _ => panic!("expected Run"),
        }
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p sqe-bench --bin sqe-bench run_cli_tests`
Expected: FAIL to compile (`Command::Run` variant not found).

- [ ] **Step 3: Add the `Run` variant**

In `crates/sqe-bench/src/cli.rs`, add to the `pub enum Command` (after the `Test` variant):

```rust
    /// Run one or more benchmark suites against a running coordinator using a
    /// config profile: ATTACH the golden catalog, run queries with zero load,
    /// emit BENCH_SUMMARY + JSON, and optionally compare against Trino.
    Run {
        /// Benchmark suites to run (tpch, ssb, tpcds, clickbench, ...).
        #[arg(value_name = "SUITE", required = true)]
        suites: Vec<String>,

        /// Config profile: `local` | `storagegrid` | `r2` | `aws` | a .toml path.
        #[arg(long, default_value = "local")]
        profile: String,

        /// Scale factor of the golden dataset (must match what was provisioned).
        #[arg(long, default_value_t = 1.0)]
        scale: f64,

        /// Coordinator host to connect to.
        #[arg(long, default_value = "localhost")]
        host: String,

        /// Coordinator Flight SQL port.
        #[arg(long, default_value_t = 60051)]
        port: u16,

        /// Also run the Trino comparison and emit compare-*.json.
        #[arg(long)]
        compare_trino: bool,

        /// Attach-vs-primary parity smoke instead of the full suite.
        #[arg(long)]
        smoke: bool,

        /// Run only a single query id (e.g. `q05`).
        #[arg(long)]
        query: Option<String>,
    },
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p sqe-bench --bin sqe-bench run_cli_tests`
Expected: PASS (2 tests). (`main.rs` will not yet compile because the `Command::Run` arm is missing — that is Task 5. If `cargo test` fails to build the binary, temporarily add `Command::Run { .. } => unimplemented!(),` to `main.rs`'s match, then remove it in Task 5. Prefer to do Task 5 immediately after.)

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-bench/src/cli.rs
git commit -m "feat(bench): add Run subcommand (multi-suite, profile-driven)"
```

---

## Task 5: `run` orchestration (attach + test + emit + compare)

**Files:**
- Create: `crates/sqe-bench/src/run.rs`
- Modify: `crates/sqe-bench/src/main.rs` (add `mod run;` + the `Command::Run` dispatch arm)
- Modify: `crates/sqe-bench/src/test.rs` (add `run_and_report` helper)

**Interfaces:**
- Consumes: `profile::{load_profile, resolve_s3_credentials, build_attach_sql, redact_attach_sql}` (Tasks 1-2); `client::create_client`, `BenchClient::execute_update` (`client/mod.rs:14-18`); `test::run_benchmark_test(client, benchmark, scale, query_filter, catalog, namespace_override)` (`test.rs:135`); `report::print_summary`, `report::write_json_report` (`report.rs:44,90`); `comparison::run_comparison(...)` (`comparison.rs:111`).
- Produces: `pub async fn run(args: RunArgs) -> anyhow::Result<()>` and `pub struct RunArgs { ... }`.

- [ ] **Step 1: Declare the module**

In `crates/sqe-bench/src/main.rs` add (alongside the other `mod` lines):

```rust
mod run;
```

- [ ] **Step 2: Write the orchestration**

Create `crates/sqe-bench/src/run.rs`:

```rust
//! The `run` verb: connect to a running coordinator, ATTACH the golden
//! catalog described by a profile, run each suite's queries with zero load,
//! emit the standard artifacts, and optionally compare against Trino.
//!
//! Read-path only. Every suite here is treated as a golden read suite. Write
//! suites (reset/copy) are a separate follow-up.

use crate::{client, comparison, profile, report};

/// Golden catalog alias attached coordinator-wide for the run.
const GOLDEN_CATALOG: &str = "golden";

pub struct RunArgs {
    pub suites: Vec<String>,
    pub profile: String,
    pub scale: f64,
    pub host: String,
    pub port: u16,
    pub compare_trino: bool,
    pub smoke: bool,
    pub query: Option<String>,
    /// Bearer token for the golden Polaris (the coordinator forwards it via
    /// bearer_passthrough). Sourced from env `BENCH_GOLDEN_TOKEN`.
    pub golden_token: String,
    /// Trino endpoint for `--compare-trino` (e.g. `localhost:18080`).
    pub trino_endpoint: Option<String>,
}

pub async fn run(args: RunArgs) -> anyhow::Result<()> {
    let profile = profile::load_profile(&args.profile)?;
    let creds = profile::resolve_s3_credentials(&profile.s3)?;

    let endpoint = format!("http://{}:{}", args.host, args.port);
    let bench_client =
        client::create_client("flight", &endpoint, None, None, None, None, None).await?;

    // ATTACH the golden catalog coordinator-wide, once, for every suite.
    let attach_sql =
        profile::build_attach_sql(GOLDEN_CATALOG, &profile, &creds, &args.golden_token);
    if std::env::var("BENCH_DEBUG").is_ok() {
        eprintln!(
            "[sqe-bench] attaching golden: {}",
            profile::redact_attach_sql(&attach_sql)
        );
    }
    // ATTACH is idempotent-ish: if already attached this run errors; treat an
    // "already attached" error as success so re-runs against a live coordinator
    // do not fail. Any other error is fatal (no silent fallback to load).
    if let Err(e) = bench_client.execute_update(&attach_sql).await {
        let msg = e.to_string();
        if !msg.contains("already") {
            return Err(anyhow::anyhow!(
                "ATTACH golden failed: {msg}. (statement: {})",
                profile::redact_attach_sql(&attach_sql)
            ));
        }
    }

    let mut any_failure = false;
    for suite in &args.suites {
        println!("\n=== {suite} (sf{}) via golden ===", args.scale);
        let results = test::run_and_report(
            bench_client.as_ref(),
            suite,
            args.scale,
            args.query.as_deref(),
        )
        .await?;
        if results.iter().any(|r| {
            matches!(
                r.status,
                crate::test::TestStatus::Fail(_) | crate::test::TestStatus::Error(_)
            )
        }) {
            any_failure = true;
        }

        if args.compare_trino {
            let trino_ep = args
                .trino_endpoint
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("--compare-trino needs BENCH_TRINO_ENDPOINT"))?;
            let trino_client =
                client::create_client("trino", trino_ep, None, None, None, None, None).await?;
            let report = comparison::run_comparison(
                suite,
                args.scale,
                bench_client.as_ref(),
                trino_client.as_ref(),
                &endpoint,
                trino_ep,
                args.query.as_deref(),
                "benchmarks/results",
            )
            .await?;
            println!(
                "compare {suite}: {} match, {} diff",
                report.summary.matched, report.summary.mismatched
            );
        }
    }

    if any_failure {
        anyhow::bail!("one or more suites had failing queries");
    }
    Ok(())
}

use crate::test;
```

Then add a small helper `run_and_report` to `crates/sqe-bench/src/test.rs` that wraps the existing runner + reporting so `run.rs` stays DRY. Add after `run_benchmark_test`:

```rust
/// Run a suite against the golden catalog and emit the standard artifacts
/// (per-query summary + JSON report). Returns the results for the caller's
/// pass/fail tally.
pub async fn run_and_report(
    client: &dyn BenchClient,
    benchmark: &str,
    scale: f64,
    query_filter: Option<&str>,
) -> anyhow::Result<Vec<QueryResult>> {
    let results =
        run_benchmark_test(client, benchmark, scale, query_filter, Some("golden"), None).await?;
    crate::report::print_summary(benchmark, scale, "flight", &results);
    let path = crate::report::write_json_report(benchmark, scale, "flight", &results)?;
    println!("Report written to: {path}");
    Ok(results)
}
```

Confirm `ComparisonSummary` field names: open `crates/sqe-bench/src/report.rs:233` and use the actual `matched`/`mismatched` field names (adjust the `println!` above if they differ, e.g. `total`/`diff`).

- [ ] **Step 3: Add the dispatch arm**

In `crates/sqe-bench/src/main.rs`, add before the closing `}` of the `match cli.command` (alongside the other arms):

```rust
        cli::Command::Run {
            suites,
            profile,
            scale,
            host,
            port,
            compare_trino,
            smoke,
            query,
        } => {
            let golden_token = std::env::var("BENCH_GOLDEN_TOKEN").map_err(|_| {
                anyhow::anyhow!("run requires BENCH_GOLDEN_TOKEN (bearer for the golden Polaris)")
            })?;
            let trino_endpoint = std::env::var("BENCH_TRINO_ENDPOINT").ok();
            run::run(run::RunArgs {
                suites,
                profile,
                scale,
                host,
                port,
                compare_trino,
                smoke,
                query,
                golden_token,
                trino_endpoint,
            })
            .await
        }
```

Because `main.rs` declares `mod run;` (Step 1), the arm calls `run::run(...)` directly — no `use` needed. `run.rs`'s `use crate::{client, comparison, profile, report, test}` resolves against `main.rs`'s module tree (the bin crate), where all those modules are declared.

- [ ] **Step 4: Build and run unit tests**

Run: `cargo build -p sqe-bench`
Expected: compiles clean.
Run: `cargo test -p sqe-bench --bin sqe-bench`
Expected: PASS (profile + cli tests).

- [ ] **Step 5: Clippy**

Run: `cargo clippy -p sqe-bench --all-targets -- -D warnings`
Expected: no warnings.

- [ ] **Step 6: Commit**

```bash
git add crates/sqe-bench/src/run.rs crates/sqe-bench/src/main.rs crates/sqe-bench/src/test.rs
git commit -m "feat(bench): run verb — attach golden + per-suite test + compare"
```

---

## Task 6: `scripts/benchmark.sh` infra shim

**Files:**
- Create: `scripts/benchmark.sh`

**Interfaces:** consumes `sqe-bench run`; reuses `tests/benchmark-attach/coordinator-attach.toml` (from !654) and the existing `scripts/bootstrap-test.sh` stack.

- [ ] **Step 1: Write the shim**

Create `scripts/benchmark.sh` (mode 0755):

```bash
#!/usr/bin/env bash
# Unified benchmark harness — infra layer. Owns the docker stack + coordinator
# lifecycle; delegates all benchmark logic to `sqe-bench run` (connect-only).
#
# Usage:
#   BENCH_PROFILE=local BENCH_SCALE=1 scripts/benchmark.sh tpch ssb tpcds clickbench
#
# Env:
#   BENCH_PROFILE   profile name/path (default: local)
#   BENCH_SCALE     scale factor (default: 1)
#   BENCH_COMPARE   set to 1 to add --compare-trino
#   BENCH_GOLDEN_TOKEN, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY  passed through
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PROFILE="${BENCH_PROFILE:-local}"
SCALE="${BENCH_SCALE:-1}"
SUITES=("$@")
[ ${#SUITES[@]} -gt 0 ] || { echo "usage: benchmark.sh <suite...>" >&2; exit 1; }

PROFILE_FILE="benchmarks/profiles/${PROFILE}.toml"
[ -f "$PROFILE_FILE" ] || { echo "ERROR: no profile $PROFILE_FILE" >&2; exit 1; }
COMPOSE_FILE="$ROOT_DIR/docker-compose.test.yml"

# manage_stack decides whether we own the docker lifecycle.
if grep -E '^manage_stack' "$PROFILE_FILE" | grep -q true; then
    MANAGE_STACK=1
else
    MANAGE_STACK=0
fi

COORD_PID=""
STACK_UP=""
cleanup() {
    [ -n "$COORD_PID" ] && kill "$COORD_PID" 2>/dev/null || true
    if [ "$MANAGE_STACK" = 1 ] && [ -n "$STACK_UP" ]; then
        docker compose -f "$COMPOSE_FILE" down 2>/dev/null || true
    fi
}
trap cleanup EXIT

echo "==> Building sqe-bench + sqe-coordinator"
cargo build --release -p sqe-bench -p sqe-coordinator

if [ "$MANAGE_STACK" = 1 ]; then
    echo "==> Bringing up test stack (Polaris + RustFS)"
    docker compose -f "$COMPOSE_FILE" up -d
    scripts/bootstrap-test.sh   # bucket + warehouse + grants (no args)
    STACK_UP=1
fi

echo "==> Starting coordinator (attach config)"
SQE_CONFIG="$ROOT_DIR/tests/benchmark-attach/coordinator-attach.toml" \
    target/release/sqe-coordinator &
COORD_PID=$!

# Health-wait on the Flight SQL port.
for _ in $(seq 1 60); do
    if nc -z localhost 60051 2>/dev/null; then break; fi
    sleep 1
done

COMPARE_FLAG=()
[ "${BENCH_COMPARE:-0}" = 1 ] && COMPARE_FLAG=(--compare-trino)

echo "==> Running suites: ${SUITES[*]}"
target/release/sqe-bench run "${SUITES[@]}" \
    --profile "$PROFILE" --scale "$SCALE" "${COMPARE_FLAG[@]}"
```

- [ ] **Step 2: Shellcheck**

Run: `shellcheck scripts/benchmark.sh`
Expected: no errors (warnings about `nc`/`seq` portability acceptable).

- [ ] **Step 3: Verify it fails fast without a stack**

Run: `BENCH_PROFILE=nonexistent scripts/benchmark.sh tpch`
Expected: `ERROR: no profile benchmarks/profiles/nonexistent.toml` and exit 1.

- [ ] **Step 4: Commit**

```bash
git add scripts/benchmark.sh
git commit -m "feat(bench): benchmark.sh infra shim (stack + coordinator lifecycle)"
```

---

## Task 7: Stack-gated e2e integration test

**Files:**
- Create: `crates/sqe-bench/tests/run_attach_gated.rs`

**Interfaces:** consumes the `sqe-bench` binary via `assert_cmd` or `std::process::Command`; gated behind `BENCH_STACK_UP=1` like the existing direct-sink gated test.

- [ ] **Step 1: Write the gated test**

Create `crates/sqe-bench/tests/run_attach_gated.rs`:

```rust
//! End-to-end: `sqe-bench run tpch --profile local` against a live stack with a
//! pre-provisioned golden catalog. Gated behind BENCH_STACK_UP=1 because it
//! needs Polaris + RustFS + a coordinator on localhost:60051 and a golden
//! `tpch_sf<scale>` namespace. Mirrors the direct-sink gated integration test.

use std::process::Command;

#[test]
fn run_tpch_via_golden_attach() {
    if std::env::var("BENCH_STACK_UP").as_deref() != Ok("1") {
        eprintln!("skipping: set BENCH_STACK_UP=1 with a live golden stack to run");
        return;
    }
    let token = std::env::var("BENCH_GOLDEN_TOKEN").expect("BENCH_GOLDEN_TOKEN");
    let bin = env!("CARGO_BIN_EXE_sqe-bench");
    let out = Command::new(bin)
        .args(["run", "tpch", "--profile", "local", "--scale", "0.01"])
        .env("BENCH_GOLDEN_TOKEN", token)
        .output()
        .expect("run sqe-bench");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "run failed: {stdout}\n{}", String::from_utf8_lossy(&out.stderr));
    assert!(stdout.contains("BENCH_SUMMARY:tpch:"), "no summary line: {stdout}");
}
```

- [ ] **Step 2: Verify it skips cleanly without a stack**

Run: `cargo test -p sqe-bench --test run_attach_gated`
Expected: PASS (prints "skipping" and returns; no stack required).

- [ ] **Step 3: Commit**

```bash
git add crates/sqe-bench/tests/run_attach_gated.rs
git commit -m "test(bench): stack-gated e2e for run verb via golden attach"
```

---

## Task 8: Docs + parity note

**Files:**
- Modify: `docs/site/book/src/features/benchmarks.md`
- Modify: `README.md`, `nextsteps.md`

- [ ] **Step 1: Document the run verb**

In `docs/site/book/src/features/benchmarks.md`, add a short "Unified harness (`benchmark.sh` + `sqe-bench run`)" section describing:
- `BENCH_PROFILE=local BENCH_SCALE=1 scripts/benchmark.sh tpch ssb tpcds clickbench`
- Profiles live in `benchmarks/profiles/`; credentials come from env/AWS profile, never the TOML.
- Read suites attach golden (zero load); write suites are the follow-up plan.

Follow the writing-style rules in `CLAUDE.md` (no emdash, no forbidden words). Do NOT delete the old `benchmark-*.sh` scripts yet — the migration/removal happens in a dedicated commit after `run` reaches parity (spec criterion 1 + criterion 6).

- [ ] **Step 2: Update roadmap/status**

Mark the read-path harness item in `README.md` roadmap and shift the `nextsteps.md` NEXT pointer to the write-suite reset plan.

- [ ] **Step 3: Verify no forbidden characters**

Run: `grep -n '—' docs/site/book/src/features/benchmarks.md`
Expected: zero hits in prose.

- [ ] **Step 4: Commit**

```bash
git add docs/site/book/src/features/benchmarks.md README.md nextsteps.md
git commit -m "docs(bench): document unified harness read-path run verb"
```

---

## Self-Review checklist (run after implementation)

1. **Spec coverage** (`2026-07-17-unified-benchmark-harness-design.md`):
   - Verbs `run` (read) — Tasks 4-5. `provision`/`reset` — follow-up plan (out of scope, stated).
   - Profiles `benchmarks/profiles/<name>.toml` — Tasks 1-3.
   - Multi-stream concurrency knobs surfaced in the profile schema — Task 1 (`ConcurrencyProfile`). Wiring them into the engine scan path is a follow-up; the schema carries them now.
   - Output artifacts unchanged — Task 5 reuses `report`/`comparison`.
   - Success criterion 1 (read suites reproduce today's results, zero load) — Tasks 5-7.
   - Success criterion 6 (script count drops) — deferred to the post-parity migration commit (Task 8 notes it, does not delete yet).
2. **Placeholder scan:** none — every code step has full code.
3. **Type consistency:** `run_and_report` (test.rs) called from `run.rs`; `Command::Run` fields match between cli.rs (Task 4) and the main.rs arm (Task 5); `RunArgs` fields match construction site. Verify `ComparisonSummary` field names against `report.rs:233` during Task 5 Step 2.

## Follow-up (separate plan, not this one)

- `provision` verb (build golden once, skip-if-exists, record write baselines).
- `reset` verb: Polaris REST `set-snapshot-ref` rollback guarded by `assert-ref-snapshot-id`; `copy` mode with `run_<id>` namespace isolation + validated drop; `--expire` behind an explicit flag + threshold warn.
- Wire `ConcurrencyProfile` into the engine scan/write concurrency knobs.
- Migration commit: fold + delete `benchmark-generate-all.sh`, `benchmark-publish-data.sh`, `benchmark-publish-iceberg.sh`, `benchmark-load.sh`, `benchmark-split.sh`, `ci/attach-parity-smoke.sh`; reduce `benchmark-test.sh` once `run` reaches parity.
