use std::sync::Arc;

use api::prometheus::remote::{ReadRequest, WriteRequest};
use api::v1::{AdminExpr, AdminResult, ObjectExpr, ObjectResult};
use async_trait::async_trait;
use common_query::Output;

use crate::context::Context;
use crate::error::Result;
use crate::influxdb::InfluxdbRequest;
use crate::opentsdb::codec::DataPoint;
use crate::prometheus::Metrics;

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
pub type OpentsdbProtocolHandlerRef = Arc<dyn OpentsdbProtocolHandler + Send + Sync>;
pub type InfluxdbLineProtocolHandlerRef = Arc<dyn InfluxdbLineProtocolHandler + Send + Sync>;
pub type PrometheusProtocolHandlerRef = Arc<dyn PrometheusProtocolHandler + Send + Sync>;

#[async_trait]
pub trait SqlQueryHandler {
    async fn do_query(&self, query: &str, ctx: &Context) -> Result<Output>;
    async fn insert_script(&self, name: &str, script: &str, ctx: &Context) -> Result<()>;
    async fn execute_script(&self, name: &str, ctx: &Context) -> Result<Output>;
}

#[async_trait]
pub trait GrpcQueryHandler {
    async fn do_query(&self, query: ObjectExpr, ctx: &Context) -> Result<ObjectResult>;
}

#[async_trait]
pub trait GrpcAdminHandler {
    async fn exec_admin_request(&self, expr: AdminExpr, ctx: &Context) -> Result<AdminResult>;
}

#[async_trait]
pub trait InfluxdbLineProtocolHandler {
    /// A successful request will not return a response.
    /// Only on error will the socket return a line of data.
    async fn exec(&self, request: &InfluxdbRequest, ctx: &Context) -> Result<()>;
}

#[async_trait]
pub trait OpentsdbProtocolHandler {
    /// A successful request will not return a response.
    /// Only on error will the socket return a line of data.
    async fn exec(&self, data_point: &DataPoint, ctx: &Context) -> Result<()>;
}

pub struct PrometheusResponse {
    pub content_type: String,
    pub content_encoding: String,
    pub body: Vec<u8>,
}

#[async_trait]
pub trait PrometheusProtocolHandler {
    /// Handling prometheus remote write requests
    async fn write(&self, request: WriteRequest, ctx: &Context) -> Result<()>;
    /// Handling prometheus remote read requests
    async fn read(&self, request: ReadRequest, ctx: &Context) -> Result<PrometheusResponse>;
    /// Handling push gateway requests
    async fn ingest_metrics(&self, metrics: Metrics, ctx: &Context) -> Result<()>;
}
