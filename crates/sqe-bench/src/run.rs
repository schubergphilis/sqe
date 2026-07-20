//! The `run` verb: connect to a running coordinator, ATTACH the golden
//! catalog described by a profile, run each suite's queries with zero load,
//! emit the standard artifacts, and optionally compare against Trino.
//!
//! Read-path only. Every suite here is treated as a golden read suite. Write
//! suites (reset/copy) are a separate follow-up.

use crate::{client, comparison, profile, test};

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
    let bench_client: Box<dyn client::BenchClient> =
        Box::new(client::flight::FlightSqlBenchClient::with_token(
            &endpoint,
            &args.golden_token,
        ));

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
            // The comparison qualifies tables with bare 2-part `<ns>.<table>`
            // names, so the Trino session needs a default catalog. `iceberg`
            // matches the properties filename benchmark.sh writes for the
            // compare Trino (iceberg.properties -> catalog `iceberg`). Mirrors
            // the standalone `compare` verb; `create_client` leaves it unset.
            // `admin` user header is required (Trino rejects query submit with
            // 401 otherwise); matches the standalone `compare` verb default.
            let trino_client = client::trino::TrinoBenchClient::new(trino_ep, Some("admin"), None)
                .with_catalog("iceberg");
            let comparison_report = comparison::run_comparison(
                suite,
                args.scale,
                bench_client.as_ref(),
                &trino_client,
                &endpoint,
                trino_ep,
                args.query.as_deref(),
                "benchmarks/results",
            )
            .await?;
            let summary = &comparison_report.summary;
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
