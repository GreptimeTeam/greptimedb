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
//!
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
use catalog::CatalogManager;
use common_query::Output;
use datatypes::timestamp::TimestampNanosecond;
use headers::HeaderValue;
use log_query::LogQuery;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use otel_arrow_rust::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;
use pipeline::{GreptimePipelineParams, Pipeline, PipelineInfo, PipelineVersion, PipelineWay};
use serde_json::Value;
use session::context::{QueryContext, QueryContextRef};

use crate::error::Result;
use crate::http::jaeger::QueryTraceParams;
use crate::influxdb::InfluxdbRequest;
use crate::opentsdb::codec::DataPoint;
use crate::prom_store::Metrics;
pub type OpentsdbProtocolHandlerRef = Arc<dyn OpentsdbProtocolHandler + Send + Sync>;
pub type InfluxdbLineProtocolHandlerRef = Arc<dyn InfluxdbLineProtocolHandler + Send + Sync>;
pub type PromStoreProtocolHandlerRef = Arc<dyn PromStoreProtocolHandler + Send + Sync>;
pub type OpenTelemetryProtocolHandlerRef = Arc<dyn OpenTelemetryProtocolHandler + Send + Sync>;
pub type PipelineHandlerRef = Arc<dyn PipelineHandler + Send + Sync>;
pub type LogQueryHandlerRef = Arc<dyn LogQueryHandler + Send + Sync>;
pub type JaegerQueryHandlerRef = Arc<dyn JaegerQueryHandler + Send + Sync>;

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

#[async_trait]
pub trait OpenTelemetryProtocolHandler: PipelineHandler {
    /// Handling opentelemetry metrics request
    async fn metrics(
        &self,
        request: ExportMetricsServiceRequest,
        ctx: QueryContextRef,
    ) -> Result<Output>;

    /// Handling opentelemetry traces request
    async fn traces(
        &self,
        pipeline_handler: PipelineHandlerRef,
        request: ExportTraceServiceRequest,
        pipeline: PipelineWay,
        pipeline_params: GreptimePipelineParams,
        table_name: String,
        ctx: QueryContextRef,
    ) -> Result<Output>;

    async fn logs(
        &self,
        pipeline_handler: PipelineHandlerRef,
        request: ExportLogsServiceRequest,
        pipeline: PipelineWay,
        pipeline_params: GreptimePipelineParams,
        table_name: String,
        ctx: QueryContextRef,
    ) -> Result<Vec<Output>>;
}

/// PipelineHandler is responsible for handling pipeline related requests.
///
/// The "Pipeline" is a series of transformations that can be applied to unstructured
/// data like logs. This handler is responsible to manage pipelines and accept data for
/// processing.
///
/// The pipeline is stored in the database and can be retrieved by its name.
#[async_trait]
pub trait PipelineHandler {
    async fn insert(&self, input: RowInsertRequests, ctx: QueryContextRef) -> Result<Output>;

    async fn get_pipeline(
        &self,
        name: &str,
        version: PipelineVersion,
        query_ctx: QueryContextRef,
    ) -> Result<Arc<Pipeline>>;

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

    async fn get_table(
        &self,
        table: &str,
        query_ctx: &QueryContext,
    ) -> std::result::Result<Option<Arc<table::Table>>, catalog::error::Error>;

    //// Build a pipeline from a string.
    fn build_pipeline(&self, pipeline: &str) -> Result<Pipeline>;

    /// Get a original pipeline by name.
    async fn get_pipeline_str(
        &self,
        name: &str,
        version: PipelineVersion,
        query_ctx: QueryContextRef,
    ) -> Result<(String, TimestampNanosecond)>;
}

/// Handle log query requests.
#[async_trait]
pub trait LogQueryHandler {
    /// Execute a log query.
    async fn query(&self, query: LogQuery, ctx: QueryContextRef) -> Result<Output>;

    /// Get catalog manager.
    fn catalog_manager(&self, ctx: &QueryContext) -> Result<&dyn CatalogManager>;
}

/// Handle Jaeger query requests.
#[async_trait]
pub trait JaegerQueryHandler {
    /// Get trace services. It's used for `/api/services` API.
    async fn get_services(&self, ctx: QueryContextRef) -> Result<Output>;

    /// Get Jaeger operations. It's used for `/api/operations` and `/api/services/{service_name}/operations` API.
    async fn get_operations(
        &self,
        ctx: QueryContextRef,
        service_name: &str,
        span_kind: Option<&str>,
        start_time: Option<i64>,
        end_time: Option<i64>,
    ) -> Result<Output>;

    /// Retrieves a trace by its unique identifier.
    ///
    /// This method is used to handle requests to the `/api/traces/{trace_id}` endpoint.
    /// It accepts optional `start_time` and `end_time` parameters in nanoseconds to filter the trace data within a specific time range.
    async fn get_trace(
        &self,
        ctx: QueryContextRef,
        trace_id: &str,
        start_time: Option<i64>,
        end_time: Option<i64>,
    ) -> Result<Output>;

    /// Find traces by query params. It's used for `/api/traces` API.
    async fn find_traces(
        &self,
        ctx: QueryContextRef,
        query_params: QueryTraceParams,
    ) -> Result<Output>;
}
