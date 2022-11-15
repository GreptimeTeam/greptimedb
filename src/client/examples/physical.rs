use std::sync::Arc;

use client::{Client, Database};
use common_grpc::MockExecution;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use tracing::{event, Level};

fn main() {
    tracing::subscriber::set_global_default(tracing_subscriber::FmtSubscriber::builder().finish())
        .unwrap();

    run();
}

#[tokio::main]
async fn run() {
    let client = Client::with_urls(vec!["127.0.0.1:3001"]);
    let db = Database::new("greptime", client);

    let physical = mock_physical_plan();
    let result = db.physical_plan(physical, None).await;

    event!(Level::INFO, "result: {:#?}", result);
}

fn mock_physical_plan() -> Arc<dyn ExecutionPlan> {
    let id_expr = Arc::new(Column::new("id", 0)) as Arc<dyn PhysicalExpr>;
    let age_expr = Arc::new(Column::new("age", 2)) as Arc<dyn PhysicalExpr>;
    let expr = vec![(id_expr, "id".to_string()), (age_expr, "age".to_string())];

    let input =
        Arc::new(MockExecution::new("mock_input_exec".to_string())) as Arc<dyn ExecutionPlan>;
    let projection = ProjectionExec::try_new(expr, input).unwrap();
    Arc::new(projection)
}
