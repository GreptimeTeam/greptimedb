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

use std::any::Any;
use std::sync::Arc;
use std::time::Duration;

use api::region::RegionResponse;
use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_function::function::FunctionRef;
use common_function::scalars::aggregate::AggregateFunctionMetaRef;
use common_query::prelude::ScalarUdf;
use common_query::Output;
use common_runtime::Runtime;
use datafusion_expr::LogicalPlan;
use query::dataframe::DataFrame;
use query::planner::LogicalPlanner;
use query::query_engine::{DescribeResult, QueryEngineState};
use query::{QueryEngine, QueryEngineContext};
use session::context::QueryContextRef;
use store_api::metadata::RegionMetadataRef;
use store_api::region_engine::{RegionEngine, RegionRole, RegionScannerRef, SetReadonlyResponse};
use store_api::region_request::{AffectedRows, RegionRequest};
use store_api::storage::{RegionId, ScanRequest};
use table::TableRef;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::error::Error;
use crate::event_listener::NoopRegionServerEventListener;
use crate::region_server::RegionServer;

pub struct MockQueryEngine;

#[async_trait]
impl QueryEngine for MockQueryEngine {
    fn as_any(&self) -> &dyn Any {
        self as _
    }

    fn planner(&self) -> Arc<dyn LogicalPlanner> {
        unimplemented!()
    }

    fn name(&self) -> &str {
        "MockQueryEngine"
    }

    async fn describe(
        &self,
        _plan: LogicalPlan,
        _query_ctx: QueryContextRef,
    ) -> query::error::Result<DescribeResult> {
        unimplemented!()
    }

    async fn execute(
        &self,
        _plan: LogicalPlan,
        _query_ctx: QueryContextRef,
    ) -> query::error::Result<Output> {
        unimplemented!()
    }

    fn register_udf(&self, _udf: ScalarUdf) {}

    fn register_aggregate_function(&self, _func: AggregateFunctionMetaRef) {}

    fn register_function(&self, _func: FunctionRef) {}

    fn read_table(&self, _table: TableRef) -> query::error::Result<DataFrame> {
        unimplemented!()
    }

    fn engine_context(&self, _query_ctx: QueryContextRef) -> QueryEngineContext {
        unimplemented!()
    }
    fn engine_state(&self) -> &QueryEngineState {
        unimplemented!()
    }
}

/// Create a region server without any engine
pub fn mock_region_server() -> RegionServer {
    RegionServer::new(
        Arc::new(MockQueryEngine),
        Runtime::builder().build().unwrap(),
        Box::new(NoopRegionServerEventListener),
    )
}

pub type MockRequestHandler =
    Box<dyn Fn(RegionId, RegionRequest) -> Result<AffectedRows, Error> + Send + Sync>;

pub struct MockRegionEngine {
    sender: Sender<(RegionId, RegionRequest)>,
    pub(crate) handle_request_delay: Option<Duration>,
    pub(crate) handle_request_mock_fn: Option<MockRequestHandler>,
    pub(crate) mock_role: Option<Option<RegionRole>>,
    engine: String,
}

impl MockRegionEngine {
    pub fn new(engine: &str) -> (Arc<Self>, Receiver<(RegionId, RegionRequest)>) {
        let (tx, rx) = tokio::sync::mpsc::channel(8);

        (
            Arc::new(Self {
                handle_request_delay: None,
                sender: tx,
                handle_request_mock_fn: None,
                mock_role: None,
                engine: engine.to_string(),
            }),
            rx,
        )
    }

    pub fn with_mock_fn(
        engine: &str,
        mock_fn: MockRequestHandler,
    ) -> (Arc<Self>, Receiver<(RegionId, RegionRequest)>) {
        let (tx, rx) = tokio::sync::mpsc::channel(8);

        (
            Arc::new(Self {
                handle_request_delay: None,
                sender: tx,
                handle_request_mock_fn: Some(mock_fn),
                mock_role: None,
                engine: engine.to_string(),
            }),
            rx,
        )
    }

    pub fn with_custom_apply_fn<F>(
        engine: &str,
        apply: F,
    ) -> (Arc<Self>, Receiver<(RegionId, RegionRequest)>)
    where
        F: FnOnce(&mut MockRegionEngine),
    {
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        let mut region_engine = Self {
            handle_request_delay: None,
            sender: tx,
            handle_request_mock_fn: None,
            mock_role: None,
            engine: engine.to_string(),
        };

        apply(&mut region_engine);

        (Arc::new(region_engine), rx)
    }
}

#[async_trait::async_trait]
impl RegionEngine for MockRegionEngine {
    fn name(&self) -> &str {
        &self.engine
    }

    async fn handle_request(
        &self,
        region_id: RegionId,
        request: RegionRequest,
    ) -> Result<RegionResponse, BoxedError> {
        if let Some(delay) = self.handle_request_delay {
            tokio::time::sleep(delay).await;
        }
        if let Some(mock_fn) = &self.handle_request_mock_fn {
            return mock_fn(region_id, request)
                .map_err(BoxedError::new)
                .map(RegionResponse::new);
        };

        let _ = self.sender.send((region_id, request)).await;
        Ok(RegionResponse::new(0))
    }

    async fn handle_query(
        &self,
        _region_id: RegionId,
        _request: ScanRequest,
    ) -> Result<RegionScannerRef, BoxedError> {
        unimplemented!()
    }

    async fn get_metadata(&self, _region_id: RegionId) -> Result<RegionMetadataRef, BoxedError> {
        unimplemented!()
    }

    fn region_disk_usage(&self, _region_id: RegionId) -> Option<i64> {
        unimplemented!()
    }

    async fn stop(&self) -> Result<(), BoxedError> {
        Ok(())
    }

    fn set_writable(&self, _region_id: RegionId, _writable: bool) -> Result<(), BoxedError> {
        Ok(())
    }

    async fn set_readonly_gracefully(
        &self,
        _region_id: RegionId,
    ) -> Result<SetReadonlyResponse, BoxedError> {
        unimplemented!()
    }

    fn role(&self, _region_id: RegionId) -> Option<RegionRole> {
        if let Some(role) = self.mock_role {
            return role;
        }
        Some(RegionRole::Leader)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
