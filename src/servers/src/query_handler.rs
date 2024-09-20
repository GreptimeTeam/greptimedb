// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! All query handler traits for various request protocols, like SQL or GRPC.
//! Instance that wishes to support certain request protocol, just implement the corresponding
//! trait, the Server will handle codec for you.
//!
//! Note:
//! Query handlers are not confined to only handle read requests, they are expecting to handle
//! write requests too. So the "query" here not might seem ambiguity. However, "query" has been
//! used as some kind of "convention", it's the "Q" in "SQL". So we might better stick to the
//! word "query".

pub mod grpc;
pub mod sql;

use std::collections::HashMap;
use std::sync::Arc;

use api::prom_store::remote::ReadRequest;
use api::v1::RowInsertRequests;
use async_trait::async_trait;
use common_query::Output;
use headers::HeaderValue;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use pipeline::{GreptimeTransformer, Pipeline, PipelineInfo, PipelineVersion};
use serde_json::Value;
use session::context::QueryContextRef;

use crate::error::Result;
use crate::influxdb::InfluxdbRequest;
use crate::opentsdb::codec::DataPoint;
use crate::prom_store::Metrics;

pub type OpentsdbProtocolHandlerRef = Arc<dyn OpentsdbProtocolHandler + Send + Sync>;
pub type InfluxdbLineProtocolHandlerRef = Arc<dyn InfluxdbLineProtocolHandler + Send + Sync>;
pub type PromStoreProtocolHandlerRef = Arc<dyn PromStoreProtocolHandler + Send + Sync>;
pub type OpenTelemetryProtocolHandlerRef = Arc<dyn OpenTelemetryProtocolHandler + Send + Sync>;
pub type ScriptHandlerRef = Arc<dyn ScriptHandler + Send + Sync>;
pub type LogHandlerRef = Arc<dyn LogHandler + Send + Sync>;

#[async_trait]
pub trait ScriptHandler {
    async fn insert_script(
        &self,
        query_ctx: QueryContextRef,
        name: &str,
        script: &str,
    ) -> Result<()>;
    async fn execute_script(
        &self,
        query_ctx: QueryContextRef,
        name: &str,
        params: HashMap<String, String>,
    ) -> Result<Output>;
}

#[async_trait]
pub trait InfluxdbLineProtocolHandler {
    /// A successful request will not return a response.
    /// Only on error will the socket return a line of data.
    async fn exec(&self, request: InfluxdbRequest, ctx: QueryContextRef) -> Result<Output>;
}

#[async_trait]
pub trait OpentsdbProtocolHandler {
    /// A successful request will not return a response.
    /// Only on error will the socket return a line of data.
    async fn exec(&self, data_points: Vec<DataPoint>, ctx: QueryContextRef) -> Result<usize>;
}

pub struct PromStoreResponse {
    pub content_type: HeaderValue,
    pub content_encoding: HeaderValue,
    pub resp_metrics: HashMap<String, Value>,
    pub body: Vec<u8>,
}

#[async_trait]
pub trait PromStoreProtocolHandler {
    /// Handling prometheus remote write requests
    async fn write(
        &self,
        request: RowInsertRequests,
        ctx: QueryContextRef,
        with_metric_engine: bool,
    ) -> Result<Output>;

    /// Handling prometheus remote read requests
    async fn read(&self, request: ReadRequest, ctx: QueryContextRef) -> Result<PromStoreResponse>;
    /// Handling push gateway requests
    async fn ingest_metrics(&self, metrics: Metrics) -> Result<()>;
}

pub enum PipelineWay {
    Identity,
    Custom(Arc<Pipeline<GreptimeTransformer>>),
}
#[async_trait]
pub trait OpenTelemetryProtocolHandler: LogHandler {
    /// Handling opentelemetry metrics request
    async fn metrics(
        &self,
        request: ExportMetricsServiceRequest,
        ctx: QueryContextRef,
    ) -> Result<Output>;

    /// Handling opentelemetry traces request
    async fn traces(
        &self,
        request: ExportTraceServiceRequest,
        ctx: QueryContextRef,
    ) -> Result<Output>;

    async fn logs(
        &self,
        request: ExportLogsServiceRequest,
        pipeline: PipelineWay,
        table_name: String,
        ctx: QueryContextRef,
    ) -> Result<Output>;
}

/// LogHandler is responsible for handling log related requests.
/// It should be able to insert logs and manage pipelines.
/// The pipeline is a series of transformations that can be applied to logs.
/// The pipeline is stored in the database and can be retrieved by name.
#[async_trait]
pub trait LogHandler {
    async fn insert_logs(&self, log: RowInsertRequests, ctx: QueryContextRef) -> Result<Output>;

    async fn get_pipeline(
        &self,
        name: &str,
        version: PipelineVersion,
        query_ctx: QueryContextRef,
    ) -> Result<Arc<Pipeline<GreptimeTransformer>>>;

    async fn insert_pipeline(
        &self,
        name: &str,
        content_type: &str,
        pipeline: &str,
        query_ctx: QueryContextRef,
    ) -> Result<PipelineInfo>;

    async fn delete_pipeline(
        &self,
        name: &str,
        version: PipelineVersion,
        query_ctx: QueryContextRef,
    ) -> Result<Option<()>>;
}
