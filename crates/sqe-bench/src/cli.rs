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

        /// Username for authentication
        #[arg(long, env = "SQE_USER")]
        username: Option<String>,

        /// Password for authentication
        #[arg(long, env = "SQE_PASSWORD")]
        password: Option<String>,
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

        /// Username for authentication
        #[arg(long, env = "SQE_USER")]
        username: Option<String>,

        /// Password for authentication
        #[arg(long, env = "SQE_PASSWORD")]
        password: Option<String>,
    },
}

#[derive(Clone, ValueEnum)]
pub enum Protocol {
    /// Arrow Flight SQL (gRPC/HTTP2)
    Flight,
    /// Trino-compat HTTP REST
    Http,
}
