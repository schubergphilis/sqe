use sqe_core::SqeConfig;
use sqe_worker::flight_service::WorkerFlightService;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("sqe=info".parse()?))
        .json()
        .init();

    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "sqe.toml".to_string());
    let config = SqeConfig::load(&config_path)?;

    let port = config.worker.flight_port;
    let addr = format!("0.0.0.0:{port}").parse()?;

    tracing::info!("Starting SQE worker on port {port}");

    let flight_service = WorkerFlightService::new();

    tonic::transport::Server::builder()
        .add_service(flight_service.into_server())
        .serve(addr)
        .await?;

    Ok(())
}
