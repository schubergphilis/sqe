//! The `run` verb: connect to a running coordinator, ATTACH the golden
//! catalog described by a profile, run each suite's queries with zero load,
//! emit the standard artifacts, and optionally compare against Trino.
//!
//! Read-path only. Every suite here is treated as a golden read suite. Write
//! suites (reset/copy) are a separate follow-up.

use crate::{client, profile, report, status, suite};

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
    if args.smoke {
        anyhow::bail!(
            "--smoke parity mode is not yet implemented (tracked for a follow-up); omit --smoke to run the suites"
        );
    }

    let profile = profile::load_profile(&args.profile)?;
    let creds = profile::resolve_s3_credentials(&profile.s3)?;

    let endpoint = format!("http://{}:{}", args.host, args.port);
    // The golden bearer authenticates the Flight session (the coordinator's
    // bearer_passthrough provider maps it to an admin role for ATTACH) and is
    // forwarded to the golden Polaris/S3. Without it the session is anonymous
    // and ATTACH fails with "No authorization header".
    let bench_client: Box<dyn client::BenchClient> = Box::new(
        client::flight::FlightSqlBenchClient::with_token(&endpoint, &args.golden_token),
    );

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
        if !msg.contains("already attached") {
            return Err(anyhow::anyhow!(
                "ATTACH golden failed: {msg}. (statement: {})",
                profile::redact_attach_sql(&attach_sql)
            ));
        }
    }

    // Build the Trino client ONCE (compare mode) so each query's single SQE run
    // is paired with a single Trino run inside the one `run_suite` pass. The
    // `admin` user header is required (Trino rejects query submit with 401
    // otherwise) and `iceberg` matches the properties filename benchmark.sh
    // writes for the compare Trino (iceberg.properties -> catalog `iceberg`).
    // Mirrors the standalone `compare` verb defaults. Guard the missing endpoint
    // here so `--compare-trino` without BENCH_TRINO_ENDPOINT still fails fast.
    let trino_client: Option<Box<dyn client::BenchClient>> = if args.compare_trino {
        let trino_ep = args
            .trino_endpoint
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("--compare-trino needs BENCH_TRINO_ENDPOINT"))?;
        Some(Box::new(
            client::trino::TrinoBenchClient::new(trino_ep, Some("admin"), None)
                .with_catalog("iceberg"),
        ))
    } else {
        None
    };

    // Catalog resolution reproduces the two verified paths exactly. `run_suite`
    // applies the catalog prefix to the ONE namespace BOTH engines read:
    //   * SQE-only (no compare): `Some("golden")` -> SQE reads `golden.<ns>`
    //     via the attached golden catalog (the pre-refactor test path).
    //   * Compare: `None` -> both engines read bare `<ns>`; SQE resolves via its
    //     golden-default session, Trino via `.with_catalog("iceberg")`. Passing
    //     `Some("golden")` here would prefix Trino's SQL with `golden.`, which
    //     Trino parses as catalog=golden and fails to resolve.
    // The report JSON is identical either way -- the namespace is not serialized;
    // only rows/pass/diff are, and those do not change with the prefix.
    let catalog = if args.compare_trino {
        None
    } else {
        Some(GOLDEN_CATALOG)
    };

    let mut any_failure = false;
    for suite in &args.suites {
        println!("\n=== {suite} (sf{}) via golden ===", args.scale);

        // Single pass: SQE runs each query once (and Trino once, in compare
        // mode). The report results and the optional comparison both come from
        // this one sweep -- no separate compare re-execution of SQE.
        let out = suite::run_suite(
            bench_client.as_ref(),
            trino_client.as_deref(),
            suite,
            args.scale,
            args.query.as_deref(),
            catalog,
            None,
            &endpoint,
            args.trino_endpoint.as_deref(),
        )
        .await?;

        report::print_summary(suite, args.scale, "flight", &out.results);
        let path = report::write_json_report(suite, args.scale, "flight", &out.results)?;
        println!("Report written to: {path}");

        // Exit policy: fail the run only on genuine correctness/execution
        // failures (`Error | WrongRows`). Timeouts and vacuous results are
        // surfaced in the summary but do not fail the run (SP1 design).
        any_failure |= out
            .results
            .iter()
            .any(|r| status::is_real_failure(&r.outcome));

        if let Some(cmp) = out.comparison {
            let cpath =
                report::write_comparison_report(&cmp, suite, args.scale, "benchmarks/results")?;
            println!("Compare report written to: {cpath}");
            let summary = &cmp.summary;
            println!(
                "compare {suite}: {}/{} matched (row_diff {}, sqe_failed {}, trino_failed {}, both_failed {}, vacuous_bug {}); see compare JSON for full breakdown",
                summary.matched, summary.total,
                summary.row_diff, summary.sqe_failed, summary.trino_failed, summary.both_failed, summary.vacuous_bug,
            );
        }
    }

    if any_failure {
        anyhow::bail!("one or more suites had failing queries");
    }
    Ok(())
}
