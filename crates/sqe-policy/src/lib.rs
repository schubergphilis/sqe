use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use sqe_core::SessionUser;

#[async_trait]
pub trait PolicyEnforcer: Send + Sync {
    async fn evaluate(
        &self,
        user: &SessionUser,
        plan: LogicalPlan,
    ) -> sqe_core::Result<LogicalPlan>;
}

pub struct PassthroughEnforcer;

#[async_trait]
impl PolicyEnforcer for PassthroughEnforcer {
    async fn evaluate(
        &self,
        _user: &SessionUser,
        plan: LogicalPlan,
    ) -> sqe_core::Result<LogicalPlan> {
        Ok(plan)
    }
}
