use crate::client::BenchClient;
use crate::generate;
use crate::layout::table_layout;

pub struct S3Args {
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub endpoint: Option<String>,
    pub region: String,
}

pub struct LoadArgs<'a> {
    pub benchmark: &'a str,
    pub scale: f64,
    pub data_path: &'a str,
    pub s3_args: &'a S3Args,
    pub clean: bool,
    pub catalog: Option<&'a str>,
    pub namespace_override: Option<&'a str>,
    /// Emit Parquet bloom filters on join-key columns of the loaded tables
    /// (via the `write.parquet.bloom-filter-columns` table property on the
    /// CTAS). Off by default so baselines stay comparable; see
    /// [`bloom_columns`].
    pub bloom_filter: bool,
    /// Emit Puffin NDV sidecars after each load commit (via the
    /// `write.puffin.stats` table property on the CTAS). Off by default; the
    /// coordinator only consumes them when `query.puffin_statistics` is on.
    pub puffin_stats: bool,
    /// Load as Hive external tables under this prefix instead of Iceberg:
    /// `CREATE TABLE <root>.<ns>.<t> STORED AS PARQUET LOCATION
    /// '<prefix>/<ns>/<t>/' AS SELECT ...`, with `catalog` naming the Hive
    /// root. No partitioning and no Iceberg table properties (bloom / puffin
    /// are Iceberg writer levers); fact tables are sorted on their
    /// clustering key instead, so row-group pruning stays comparable.
    pub hive_location: Option<&'a str>,
}

/// True if `err` is a memory/resource-exhaustion failure (as opposed to a SQL
/// or transport error). Used to decide whether a failed sort-on-write CTAS
/// should fail over to an unsorted write.
fn is_resource_exhausted(err: &anyhow::Error) -> bool {
    let m = err.to_string().to_ascii_lowercase();
    m.contains("resources exhausted")
        || m.contains("failed to allocate")
        || m.contains("out of memory")
        || m.contains("memory limit")
        // The coordinator's admission gate rejects queries outright when the
        // memory pool is in the Red band (>95% utilized). A sort-on-write CTAS
        // whose non-spilling sort buffer fills the pool trips this at
        // submission, before adaptive sort-stripping runs. Treat the rejection
        // like execution-time exhaustion so the load fails over to an unsorted
        // write instead of aborting the whole benchmark load.
        || m.contains("server memory is")
        || m.contains("memory is red")
        || m.contains("95% utilized")
}

/// Strip the S3 credentials from an error before it is surfaced.
///
/// The load CTAS embeds the S3 access/secret keys inline in the SQL text
/// (`read_parquet` exposes no bind-parameter form), and the coordinator may
/// echo the failing statement back in its error message. A failed load must
/// not spill the credential into logs, so replace the known key values with
/// `***`. The original error is returned unchanged when it contains neither
/// value, preserving its source chain (e.g. for `is_resource_exhausted`).
fn redact_s3_secrets(err: anyhow::Error, s3: &S3Args) -> anyhow::Error {
    let original = err.to_string();
    let mut redacted = original.clone();
    for cred in [s3.secret_key.as_deref(), s3.access_key.as_deref()]
        .into_iter()
        .flatten()
        .filter(|c| !c.is_empty())
    {
        redacted = redacted.replace(cred, "***");
    }
    if redacted == original {
        err
    } else {
        anyhow::anyhow!("{redacted}")
    }
}

pub async fn load_benchmark(client: &dyn BenchClient, args: &LoadArgs<'_>) -> anyhow::Result<()> {
    let benchmark = args.benchmark;
    let scale = args.scale;
    let data_path = args.data_path;
    let s3_args = args.s3_args;
    let clean = args.clean;
    let catalog = args.catalog;
    let namespace_override = args.namespace_override;

    // Build the namespace: user override > auto-generated
    let ns_base = match namespace_override {
        Some(ns) => ns.to_string(),
        None if benchmark == "tpcbb" => crate::bench_namespace("tpcds", scale),
        None => crate::bench_namespace(benchmark, scale),
    };

    // Full qualified prefix: catalog.namespace or just namespace
    let qualified_ns = match catalog {
        Some(cat) => format!("{cat}.{ns_base}"),
        None => ns_base.clone(),
    };

    let gen = generate::get_generator(benchmark)?;

    println!("Loading {benchmark} SF{scale} into {qualified_ns}");

    // Create namespace (ignore error if exists)
    let _ = client
        .execute_update(&format!("CREATE SCHEMA IF NOT EXISTS {qualified_ns}"))
        .await;

    for table_def in gen.tables() {
        let table_path = format!("{data_path}/{benchmark}/sf{scale}/{}", table_def.name);

        if clean {
            let _ = client
                .execute_update(&format!(
                    "DROP TABLE IF EXISTS {qualified_ns}.{}",
                    table_def.name
                ))
                .await;
        }

        // Build the base CTAS: CREATE TABLE [TBLPROPERTIES (...)]
        // [PARTITIONED BY (...)] AS SELECT * FROM read_parquet(...).
        // Partitioning (coarse, manifest-level file pruning) is part of the
        // table definition; clustering (ORDER BY, the sort-on-write hint
        // below) is appended per the failover path.
        //
        // Clause order matters: the engine normalizes `PARTITIONED BY` to
        // sqlparser's `PARTITION BY`, and sqlparser only accepts table
        // options (TBLPROPERTIES / WITH) *before* PARTITION BY on a CTAS.
        // TBLPROPERTIES after PARTITIONED BY fails to parse (verified against
        // the engine parse path, not raw GenericDialect).
        let mut base_sql = format!("CREATE TABLE {qualified_ns}.{}", table_def.name);
        // Hive CTAS: LOCATION is required (SQE never picks a data path for
        // an external table), format defaults to Parquet but is stated.
        let hive_dir = args
            .hive_location
            .map(|loc| format!("{}/{ns_base}/{}", loc.trim_end_matches('/'), table_def.name));
        if let Some(dir) = &hive_dir {
            base_sql.push_str(&format!(" STORED AS PARQUET LOCATION '{dir}/'"));
        }
        // Bloom filters on join-key columns, when requested. The property
        // must be set at table-creation time -- it drives the CTAS data
        // write through SQE's Iceberg writer.
        // Both levers share one TBLPROPERTIES clause; sqlparser accepts a
        // single options list per CTAS.
        let layout = table_layout(benchmark, &table_def.name);
        let mut tbl_props: Vec<String> = Vec::new();
        if args.bloom_filter && hive_dir.is_none() {
            let cols = layout.bloom_columns;
            if !cols.is_empty() {
                tbl_props.push(format!(
                    "'write.parquet.bloom-filter-columns' = '{}'",
                    cols.join(",")
                ));
            }
        }
        if args.puffin_stats && hive_dir.is_none() {
            tbl_props.push("'write.puffin.stats' = 'true'".to_string());
        }
        if !tbl_props.is_empty() {
            base_sql.push_str(&format!(" TBLPROPERTIES ({})", tbl_props.join(", ")));
        }
        // Hive tables are never partitioned, so they take the sort instead.
        let partition = layout.partition.filter(|_| hive_dir.is_none());
        if let Some(spec) = partition {
            base_sql.push_str(&format!(" PARTITIONED BY ({spec})"));
        }
        // Object-store paths use the directory form: DataFusion's
        // `ListingTableUrl` only expands `*` globs on local filesystem
        // paths — on `s3://` the star is treated as a literal key and the
        // listing comes back empty. The directory form lists the prefix
        // and the reader's `.parquet` extension filter selects the files.
        if table_path.contains("://") {
            base_sql.push_str(&format!(" AS SELECT * FROM read_parquet('{}/'", table_path));
        } else {
            base_sql.push_str(&format!(
                " AS SELECT * FROM read_parquet('{}/*.parquet'",
                table_path
            ));
        }

        // Append S3 credentials if provided
        if let Some(ref key) = s3_args.access_key {
            base_sql.push_str(&format!(", access_key => '{key}'"));
        }
        if let Some(ref key) = s3_args.secret_key {
            base_sql.push_str(&format!(", secret_key => '{key}'"));
        }
        if let Some(ref ep) = s3_args.endpoint {
            base_sql.push_str(&format!(", endpoint => '{ep}'"));
        }
        base_sql.push_str(&format!(", region => '{}'", s3_args.region));
        base_sql.push(')');

        println!("  Loading {}.{}...", qualified_ns, table_def.name);

        // Sort-on-write: cluster fact tables by their date/range key so
        // row-group zone maps are tight and the reader can prune row groups.
        // The sort is an optimization, not a correctness requirement, so if it
        // exhausts memory (DataFusion's sort-merge cannot spill and OOMs rather
        // than degrading) we fail over to an unsorted write -- the data lands
        // correctly, just without clustering. A user's own CTAS ORDER BY is
        // never touched; this fallback applies only to the bench clustering hint.
        // A PARTITIONED write fans the sort-on-write ORDER BY into one
        // ExternalSorterMerge per output partition, and that merge phase cannot
        // spill. At SF10, ~84 monthly partitions x a large fact table exhaust
        // the pool (TPC-H orders at 15M rows survives; lineitem at 60M does
        // not -- three ExternalSorters held ~11 GB before the non-spillable
        // merge failed). When the table is already partitioned by its date key,
        // month-level partition pruning delivers the clustering benefit, so the
        // redundant, memory-unsafe sort is skipped. Unpartitioned fact tables
        // (SSB lineorder, TPC-DS *_sales) keep the single-stream sort, which
        // spills cleanly; if it still exhausts memory we fail over to unsorted.
        let partitioned = partition.is_some();
        match layout.clustering {
            Some(key) if !partitioned => {
                let sorted_sql = format!("{base_sql} ORDER BY {key}");
                if let Err(e) = client.execute_update(&sorted_sql).await {
                    let e = redact_s3_secrets(e, s3_args);
                    if is_resource_exhausted(&e) {
                        eprintln!(
                            "  ! sort-on-write OOM on {}.{} (key {key}); failing over to unsorted write",
                            qualified_ns, table_def.name
                        );
                        // Drop the partial table the failed sort may have left.
                        let _ = client
                            .execute_update(&format!(
                                "DROP TABLE IF EXISTS {qualified_ns}.{}",
                                table_def.name
                            ))
                            .await;
                        // A Hive DROP removes the manifest only; the failed
                        // sort's files stay under LOCATION and would be read
                        // back with the retry's. Retry into a fresh subdir.
                        let unsorted_sql = match &hive_dir {
                            Some(dir) => base_sql.replacen(
                                &format!("LOCATION '{dir}/'"),
                                &format!("LOCATION '{dir}/unsorted/'"),
                                1,
                            ),
                            None => base_sql.clone(),
                        };
                        client
                            .execute_update(&unsorted_sql)
                            .await
                            .map_err(|e| redact_s3_secrets(e, s3_args))?;
                    } else {
                        return Err(e);
                    }
                }
            }
            _ => {
                client
                    .execute_update(&base_sql)
                    .await
                    .map_err(|e| redact_s3_secrets(e, s3_args))?;
            }
        }
        println!("  Done: {}.{}", qualified_ns, table_def.name);
    }

    println!("Done. All tables loaded into {qualified_ns}.");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s3(access: Option<&str>, secret: Option<&str>) -> S3Args {
        S3Args {
            access_key: access.map(str::to_string),
            secret_key: secret.map(str::to_string),
            endpoint: None,
            region: "us-east-1".to_string(),
        }
    }

    #[test]
    fn redact_strips_both_credentials_from_error_text() {
        let s3 = s3(Some("AKIAEXAMPLE"), Some("super-secret-value"));
        let err = anyhow::anyhow!(
            "Query failed: syntax at secret_key => 'super-secret-value', access_key => 'AKIAEXAMPLE'"
        );
        let msg = redact_s3_secrets(err, &s3).to_string();
        assert!(!msg.contains("super-secret-value"), "secret leaked: {msg}");
        assert!(!msg.contains("AKIAEXAMPLE"), "access key leaked: {msg}");
        assert!(msg.contains("***"));
    }

    #[test]
    fn redact_preserves_resource_exhausted_detection() {
        // Redaction must not clobber the markers is_resource_exhausted keys on.
        let s3 = s3(None, Some("s3cr3t"));
        let err = anyhow::anyhow!("Query failed: Resources exhausted: failed to allocate");
        let redacted = redact_s3_secrets(err, &s3);
        assert!(is_resource_exhausted(&redacted));
    }

    #[test]
    fn redact_is_noop_when_no_credentials_present_in_message() {
        let s3 = s3(Some("AKIAEXAMPLE"), Some("super-secret-value"));
        let err = anyhow::anyhow!("connection refused");
        assert_eq!(
            redact_s3_secrets(err, &s3).to_string(),
            "connection refused"
        );
    }

    #[test]
    fn redact_ignores_empty_credentials() {
        // An empty secret string must not turn every char boundary into ***.
        let s3 = s3(Some(""), Some(""));
        let err = anyhow::anyhow!("plain error");
        assert_eq!(redact_s3_secrets(err, &s3).to_string(), "plain error");
    }
}
