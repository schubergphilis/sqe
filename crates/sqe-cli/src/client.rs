/// Common result type returned by both Flight and HTTP clients.
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

/// Trait abstracting over Flight SQL and HTTP backends.
#[async_trait::async_trait]
pub trait SqlClient: Send {
    async fn execute(&mut self, sql: &str) -> Result<QueryResult, Box<dyn std::error::Error>>;
}
