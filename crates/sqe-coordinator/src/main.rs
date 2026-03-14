use std::sync::Arc;

use sqe_core::SqeConfig;
use tracing_subscriber::EnvFilter;

use sqe_coordinator::flight_sql::SqeFlightSqlService;
use sqe_coordinator::QueryHandler;
use sqe_coordinator::SessionManager;

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

    tracing::info!(
        "Starting SQE coordinator on Flight SQL port {}",
        config.coordinator.flight_sql_port
    );

    // Initialize auth
    let authenticator = Arc::new(sqe_auth::Authenticator::new(&config.auth).await?);

    // Initialize session manager
    let session_manager = Arc::new(SessionManager::new(authenticator.clone()));

    // Initialize policy (passthrough)
    let policy_enforcer: Arc<dyn sqe_policy::PolicyEnforcer> =
        Arc::new(sqe_policy::PassthroughEnforcer);

    // Initialize query handler
    let query_handler = Arc::new(QueryHandler::new(policy_enforcer, config.clone()));

    // Start Flight SQL server
    let flight_service =
        SqeFlightSqlService::new(session_manager, query_handler, config.clone());
    let addr = format!("0.0.0.0:{}", config.coordinator.flight_sql_port).parse()?;

    tracing::info!("SQE coordinator listening on {}", addr);

    tonic::transport::Server::builder()
        .add_service(arrow_flight::flight_service_server::FlightServiceServer::new(
            flight_service,
        ))
        .serve(addr)
        .await?;

    Ok(())
}
