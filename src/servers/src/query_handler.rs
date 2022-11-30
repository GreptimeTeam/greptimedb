// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use api::prometheus::remote::{ReadRequest, WriteRequest};
use api::v1::{AdminExpr, AdminResult, ObjectExpr, ObjectResult};
use async_trait::async_trait;
use common_query::Output;
use session::context::SessionContext;

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
pub type ScriptHandlerRef = Arc<dyn ScriptHandler + Send + Sync>;

#[async_trait]
pub trait SqlQueryHandler {
    async fn do_query(&self, query: &str, session_ctx: Arc<SessionContext>) -> Result<Output>;
}

#[async_trait]
pub trait ScriptHandler {
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

#[async_trait]
pub trait InfluxdbLineProtocolHandler {
    /// A successful request will not return a response.
    /// Only on error will the socket return a line of data.
    async fn exec(&self, request: &InfluxdbRequest) -> Result<()>;
}

#[async_trait]
pub trait OpentsdbProtocolHandler {
    /// A successful request will not return a response.
    /// Only on error will the socket return a line of data.
    async fn exec(&self, data_point: &DataPoint) -> Result<()>;
}

pub struct PrometheusResponse {
    pub content_type: String,
    pub content_encoding: String,
    pub body: Vec<u8>,
}

#[async_trait]
pub trait PrometheusProtocolHandler {
    /// Handling prometheus remote write requests
    async fn write(&self, database: &str, request: WriteRequest) -> Result<()>;
    /// Handling prometheus remote read requests
    async fn read(&self, database: &str, request: ReadRequest) -> Result<PrometheusResponse>;
    /// Handling push gateway requests
    async fn ingest_metrics(&self, metrics: Metrics) -> Result<()>;
}
