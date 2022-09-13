use std::sync::Arc;

use api::v1::{AdminExpr, AdminResult, ObjectExpr, ObjectResult};
use async_trait::async_trait;
use query::Output;

use crate::error::Result;

/// All query handler traits for various request protocols, like SQL or GRPC.
/// Instance that wishes to support certain request protocol, just implement the corresponding
/// trait, the Server will handle codec for you.
///
/// Note:
/// Query handlers are not confined to only handle read requests, they are expecting to handle
/// write requests too. So the "query" here not might seem ambiguity. However, "query" has been
/// used as some kind of "convention", it's the "Q" in "SQL". So we might better stick to the
/// word "query".

pub type SqlQueryHandlerRef = Arc<dyn SqlQueryHandler + Send + Sync>;
pub type GrpcQueryHandlerRef = Arc<dyn GrpcQueryHandler + Send + Sync>;
pub type GrpcAdminHandlerRef = Arc<dyn GrpcAdminHandler + Send + Sync>;

#[async_trait]
pub trait SqlQueryHandler {
    async fn do_query(&self, query: &str) -> Result<Output>;
    async fn insert_script(&self, name: &str, script: &str) -> Result<()>;
    async fn execute_script(&self, name: &str) -> Result<Output>;
}

#[async_trait]
pub trait GrpcQueryHandler {
    async fn do_query(&self, query: ObjectExpr) -> Result<ObjectResult>;
}

#[async_trait]
pub trait GrpcAdminHandler {
    async fn exec_admin_request(&self, expr: AdminExpr) -> Result<AdminResult>;
}
