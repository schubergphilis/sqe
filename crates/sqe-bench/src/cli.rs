use clap::{Parser, Subcommand, ValueEnum};

#[derive(Parser)]
#[command(
    name = "sqe-bench",
    version,
    about = "SQE benchmark data generator, loader, and query tester"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Generate Parquet data files for a benchmark suite
    Generate {
        /// Benchmark suite to generate (tpch, ssb)
        #[arg(value_name = "BENCHMARK")]
        benchmark: String,

        /// Scale factor (e.g. 1 = 1 GB for TPC-H, 10 = 10x)
        #[arg(long, default_value_t = 1.0)]
        scale: f64,

        /// Output directory for Parquet files
        #[arg(long, default_value = "data")]
        output: String,

        /// S3 endpoint URL (if writing directly to object storage)
        #[arg(long, env = "AWS_ENDPOINT_URL")]
        s3_endpoint: Option<String>,

        /// S3 access key ID
        #[arg(long, env = "AWS_ACCESS_KEY_ID")]
        s3_access_key: Option<String>,

        /// S3 secret access key
        #[arg(long, env = "AWS_SECRET_ACCESS_KEY")]
        s3_secret_key: Option<String>,

        /// S3 bucket name (required when writing to S3)
        #[arg(long, env = "BENCH_S3_BUCKET")]
        s3_bucket: Option<String>,

        /// S3 region
        #[arg(long, env = "AWS_DEFAULT_REGION", default_value = "us-east-1")]
        s3_region: String,
    },

    /// Load generated data into SQE via Iceberg REST catalog
    Load {
        /// Benchmark suite to load (tpch, ssb)
        #[arg(value_name = "BENCHMARK")]
        benchmark: String,

        /// Scale factor used when generating the data
        #[arg(long, default_value_t = 1.0)]
        scale: f64,

        /// Directory containing generated Parquet files
        #[arg(long, default_value = "data")]
        data: String,

        /// Wire protocol to use for loading
        #[arg(long, default_value = "flight")]
        protocol: Protocol,

        /// Coordinator host
        #[arg(long, default_value = "localhost")]
        host: String,

        /// Coordinator port
        #[arg(long, default_value_t = 50051u16)]
        port: u16,

        /// Catalog name (e.g. main_warehouse). If set, tables are created as
        /// <catalog>.<namespace>.<table> instead of <namespace>.<table>.
        #[arg(long, env = "SQE_CATALOG")]
        catalog: Option<String>,

        /// Override the auto-generated namespace (default: <benchmark>_sf<scale>)
        #[arg(long, env = "SQE_NAMESPACE")]
        namespace: Option<String>,

        /// Drop and recreate tables before loading
        #[arg(long, default_value_t = false)]
        clean: bool,

        /// S3 endpoint URL
        #[arg(long, env = "AWS_ENDPOINT_URL")]
        s3_endpoint: Option<String>,

        /// S3 access key ID
        #[arg(long, env = "AWS_ACCESS_KEY_ID")]
        s3_access_key: Option<String>,

        /// S3 secret access key
        #[arg(long, env = "AWS_SECRET_ACCESS_KEY")]
        s3_secret_key: Option<String>,

        /// S3 bucket name
        #[arg(long, env = "BENCH_S3_BUCKET")]
        s3_bucket: Option<String>,

        /// S3 region
        #[arg(long, env = "AWS_DEFAULT_REGION", default_value = "us-east-1")]
        s3_region: String,

        /// Username for authentication (OIDC password grant)
        #[arg(long, env = "SQE_USER")]
        username: Option<String>,

        /// Password for authentication (OIDC password grant)
        #[arg(long, env = "SQE_PASSWORD")]
        password: Option<String>,

        /// OAuth2 token endpoint for client_credentials auth
        #[arg(long, env = "SQE_TOKEN_ENDPOINT")]
        token_endpoint: Option<String>,

        /// OAuth2 client ID
        #[arg(long, env = "SQE_CLIENT_ID")]
        client_id: Option<String>,

        /// OAuth2 client secret
        #[arg(long, env = "SQE_CLIENT_SECRET")]
        client_secret: Option<String>,
    },

    /// Run benchmark queries and report timing
    Test {
        /// Benchmark suite to test (tpch, ssb)
        #[arg(value_name = "BENCHMARK")]
        benchmark: String,

        /// Scale factor (informational, used in result metadata)
        #[arg(long, default_value_t = 1.0)]
        scale: f64,

        /// Wire protocol to use for queries
        #[arg(long, default_value = "flight")]
        protocol: Protocol,

        /// Coordinator host
        #[arg(long, default_value = "localhost")]
        host: String,

        /// Coordinator port
        #[arg(long, default_value_t = 50051u16)]
        port: u16,

        /// Run only the specified query (e.g. "q1" or "1"); runs all if omitted
        #[arg(long)]
        query: Option<String>,

        /// Catalog name (must match the value used during load)
        #[arg(long, env = "SQE_CATALOG")]
        catalog: Option<String>,

        /// Override namespace (must match the value used during load)
        #[arg(long, env = "SQE_NAMESPACE")]
        namespace: Option<String>,

        /// Username for authentication (OIDC password grant)
        #[arg(long, env = "SQE_USER")]
        username: Option<String>,

        /// Password for authentication (OIDC password grant)
        #[arg(long, env = "SQE_PASSWORD")]
        password: Option<String>,

        /// OAuth2 token endpoint for client_credentials auth
        #[arg(long, env = "SQE_TOKEN_ENDPOINT")]
        token_endpoint: Option<String>,

        /// OAuth2 client ID
        #[arg(long, env = "SQE_CLIENT_ID")]
        client_id: Option<String>,

        /// OAuth2 client secret
        #[arg(long, env = "SQE_CLIENT_SECRET")]
        client_secret: Option<String>,
    },

    /// Compare SQE vs Trino: run identical benchmark queries against both and diff results
    Compare {
        /// Benchmark suite (tpch, tpcds, ssb)
        #[arg(value_name = "BENCHMARK")]
        benchmark: String,

        /// Scale factor
        #[arg(long, default_value_t = 1.0)]
        scale: f64,

        /// SQE Flight SQL host
        #[arg(long, default_value = "localhost")]
        sqe_host: String,

        /// SQE Flight SQL port
        #[arg(long, default_value_t = 50051u16)]
        sqe_port: u16,

        /// SQE auth username
        #[arg(long, env = "SQE_USER")]
        sqe_username: Option<String>,

        /// SQE auth password
        #[arg(long, env = "SQE_PASSWORD")]
        sqe_password: Option<String>,

        /// Trino HTTP URL (e.g., http://localhost:8080)
        #[arg(long)]
        trino_url: String,

        /// Trino user
        #[arg(long, default_value = "admin")]
        trino_user: String,

        /// Trino catalog (default: same as benchmark namespace)
        #[arg(long)]
        trino_catalog: Option<String>,

        /// Trino schema (default: same as benchmark namespace)
        #[arg(long)]
        trino_schema: Option<String>,

        /// Single query to compare (e.g., "q1" or "1")
        #[arg(long)]
        query: Option<String>,

        /// Output directory for comparison report
        #[arg(long, default_value = "benchmarks/results")]
        output: String,
    },
}

#[derive(Clone, ValueEnum)]
pub enum Protocol {
    /// Arrow Flight SQL (gRPC/HTTP2)
    Flight,
    /// Trino-compat HTTP REST
    Http,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compare_subcommand_parses() {
        let args = Cli::parse_from([
            "sqe-bench", "compare", "tpch",
            "--scale", "1",
            "--sqe-host", "localhost",
            "--sqe-port", "50051",
            "--trino-url", "http://localhost:8080",
        ]);
        match args.command {
            Command::Compare {
                benchmark,
                scale,
                sqe_host,
                sqe_port,
                trino_url,
                ..
            } => {
                assert_eq!(benchmark, "tpch");
                assert_eq!(scale, 1.0);
                assert_eq!(sqe_host, "localhost");
                assert_eq!(sqe_port, 50051);
                assert_eq!(trino_url, "http://localhost:8080");
            }
            _ => panic!("expected Compare command"),
        }
    }
}
