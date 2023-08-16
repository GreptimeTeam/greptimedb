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
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use common_base::bytes::Bytes;
use common_query::{DfPhysicalPlan, Output};
use common_recordbatch::SendableRecordBatchStream;
use common_telemetry::info;
use dashmap::DashMap;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionState;
use datafusion_expr::{Expr, TableType};
use datatypes::arrow::datatypes::SchemaRef;
use snafu::{OptionExt, ResultExt};
use store_api::region_engine::RegionEngineRef;
use store_api::region_request::RegionRequest;
use store_api::storage::RegionId;

use crate::error::{
    HandleRegionRequestSnafu, RegionEngineNotFoundSnafu, RegionNotFoundSnafu, Result,
};

#[derive(Default)]
pub struct RegionServer {
    engines: HashMap<String, RegionEngineRef>,
    region_map: DashMap<RegionId, RegionEngineRef>,
}

impl RegionServer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_engine(&mut self, engine: RegionEngineRef) {
        let engine_name = engine.name();
        self.engines.insert(engine_name.to_string(), engine);
    }

    pub async fn handle_request(
        &self,
        region_id: RegionId,
        request: RegionRequest,
    ) -> Result<Output> {
        // TODO(ruihang): add some metrics

        let region_change = match &request {
            RegionRequest::Create(create) => RegionChange::Register(create.engine.clone()),
            RegionRequest::Open(open) => RegionChange::Register(open.engine.clone()),
            RegionRequest::Close(_) | RegionRequest::Drop(_) => RegionChange::Deregisters,
            RegionRequest::Write(_)
            | RegionRequest::Delete(_)
            | RegionRequest::Alter(_)
            | RegionRequest::Flush(_)
            | RegionRequest::Compact(_) => RegionChange::None,
        };

        let engine = match &region_change {
            RegionChange::Register(engine_type) => self
                .engines
                .get(engine_type)
                .with_context(|| RegionEngineNotFoundSnafu { name: engine_type })?
                .clone(),
            RegionChange::None | RegionChange::Deregisters => self
                .region_map
                .get(&region_id)
                .with_context(|| RegionNotFoundSnafu { region_id })?
                .clone(),
        };
        let engine_type = engine.name();

        let result = engine
            .handle_request(region_id, request)
            .await
            .with_context(|_| HandleRegionRequestSnafu { region_id })?;

        match region_change {
            RegionChange::None => {}
            RegionChange::Register(_) => {
                info!("Region {region_id} is registered to engine {engine_type}");
                self.region_map.insert(region_id, engine);
            }
            RegionChange::Deregisters => {
                info!("Region {region_id} is deregistered from engine {engine_type}");
                self.region_map.remove(&region_id);
            }
        }

        Ok(result)
    }

    #[allow(unused_variables)]
    pub fn handle_read(
        &self,
        region_id: RegionId,
        plan: Bytes,
    ) -> Result<SendableRecordBatchStream> {
        todo!()
    }
}

enum RegionChange {
    None,
    Register(String),
    Deregisters,
}

#[allow(dead_code)]
struct DummyCatalogList {}

#[allow(dead_code)]
#[allow(unused_variables)]
impl DummyCatalogList {
    pub fn new(region_id: RegionId) -> Self {
        todo!()
    }
}

/// For [TableProvider](datafusion::datasource::TableProvider)
struct DummyTableProvider {}

#[async_trait]
impl TableProvider for DummyTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        todo!()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn DfPhysicalPlan>> {
        todo!()
    }
}
