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
use common_function::function_factory::ScalarFunctionFactory;
use common_query::Output;
use common_runtime::Runtime;
use common_runtime::runtime::{BuilderBuild, RuntimeTrait};
use datafusion::catalog::TableFunction;
use datafusion::dataframe::DataFrame;
use datafusion_expr::{AggregateUDF, LogicalPlan};
use query::planner::LogicalPlanner;
use query::query_engine::{DescribeResult, QueryEngineState};
use query::{QueryEngine, QueryEngineContext};
use servers::grpc::FlightCompression;
use session::context::QueryContextRef;
use store_api::metadata::RegionMetadataRef;
use store_api::region_engine::{
    RegionEngine, RegionRole, RegionScannerRef, RegionStatistic, RemapManifestsRequest,
    RemapManifestsResponse, SetRegionRoleStateResponse, SettableRegionRoleState,
    SyncRegionFromRequest, SyncRegionFromResponse,
};
use store_api::region_request::{AffectedRows, RegionRequest};
use store_api::storage::{RegionId, ScanRequest, SequenceNumber};
use table::TableRef;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::error::{Error, NotYetImplementedSnafu};
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

    fn register_aggregate_function(&self, _func: AggregateUDF) {}

    fn register_scalar_function(&self, _func: ScalarFunctionFactory) {}

    fn register_table_function(&self, _func: Arc<TableFunction>) {}

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
        FlightCompression::default(),
    )
}

pub type MockRequestHandler =
    Box<dyn Fn(RegionId, RegionRequest) -> Result<AffectedRows, Error> + Send + Sync>;

pub type MockSetReadonlyGracefullyHandler =
    Box<dyn Fn(RegionId) -> Result<SetRegionRoleStateResponse, Error> + Send + Sync>;

pub type MockGetMetadataHandler =
    Box<dyn Fn(RegionId) -> Result<RegionMetadataRef, Error> + Send + Sync>;

pub struct MockRegionEngine {
    sender: Sender<(RegionId, RegionRequest)>,
    pub(crate) handle_request_delay: Option<Duration>,
    pub(crate) handle_request_mock_fn: Option<MockRequestHandler>,
    pub(crate) handle_set_readonly_gracefully_mock_fn: Option<MockSetReadonlyGracefullyHandler>,
    pub(crate) handle_get_metadata_mock_fn: Option<MockGetMetadataHandler>,
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
                handle_set_readonly_gracefully_mock_fn: None,
                handle_get_metadata_mock_fn: None,
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
                handle_set_readonly_gracefully_mock_fn: None,
                handle_get_metadata_mock_fn: None,
                mock_role: None,
                engine: engine.to_string(),
            }),
            rx,
        )
    }

    pub fn with_metadata_mock_fn(
        engine: &str,
        mock_fn: MockGetMetadataHandler,
    ) -> (Arc<Self>, Receiver<(RegionId, RegionRequest)>) {
        let (tx, rx) = tokio::sync::mpsc::channel(8);

        (
            Arc::new(Self {
                handle_request_delay: None,
                sender: tx,
                handle_request_mock_fn: None,
                handle_set_readonly_gracefully_mock_fn: None,
                handle_get_metadata_mock_fn: Some(mock_fn),
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
            handle_set_readonly_gracefully_mock_fn: None,
            handle_get_metadata_mock_fn: None,
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
        Err(BoxedError::new(
            NotYetImplementedSnafu { what: "blah" }.build(),
        ))
    }

    async fn get_metadata(&self, region_id: RegionId) -> Result<RegionMetadataRef, BoxedError> {
        if let Some(mock_fn) = &self.handle_get_metadata_mock_fn {
            return mock_fn(region_id).map_err(BoxedError::new);
        };

        unimplemented!()
    }

    fn region_statistic(&self, _region_id: RegionId) -> Option<RegionStatistic> {
        unimplemented!()
    }

    async fn get_committed_sequence(&self, _: RegionId) -> Result<SequenceNumber, BoxedError> {
        unimplemented!()
    }

    async fn stop(&self) -> Result<(), BoxedError> {
        Ok(())
    }

    fn set_region_role(&self, _region_id: RegionId, _role: RegionRole) -> Result<(), BoxedError> {
        Ok(())
    }

    async fn set_region_role_state_gracefully(
        &self,
        region_id: RegionId,
        _region_role_state: SettableRegionRoleState,
    ) -> Result<SetRegionRoleStateResponse, BoxedError> {
        if let Some(mock_fn) = &self.handle_set_readonly_gracefully_mock_fn {
            return mock_fn(region_id).map_err(BoxedError::new);
        };

        unreachable!()
    }

    fn role(&self, _region_id: RegionId) -> Option<RegionRole> {
        if let Some(role) = self.mock_role {
            return role;
        }
        Some(RegionRole::Leader)
    }

    async fn sync_region(
        &self,
        _region_id: RegionId,
        _request: SyncRegionFromRequest,
    ) -> Result<SyncRegionFromResponse, BoxedError> {
        unimplemented!()
    }

    async fn remap_manifests(
        &self,
        _request: RemapManifestsRequest,
    ) -> Result<RemapManifestsResponse, BoxedError> {
        unimplemented!()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
