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

use std::collections::HashMap;

use common_query::Output;
use common_telemetry::info;
use dashmap::DashMap;
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
    region_map: DashMap<RegionId, String>,
}

impl RegionServer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_engine(&mut self, engine: RegionEngineRef) {
        let engine_name = engine.name();
        self.engines.insert(engine_name.clone(), engine);
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
            | RegionRequest::Read(_)
            | RegionRequest::Delete(_)
            | RegionRequest::Alter(_)
            | RegionRequest::Flush(_)
            | RegionRequest::Compact(_) => RegionChange::None,
        };

        let mut _dashmap_guard = None;
        let engine_type = match &region_change {
            RegionChange::Register(engine_type) => engine_type,
            RegionChange::None | RegionChange::Deregisters => {
                let guard = self
                    .region_map
                    .get(&region_id)
                    .with_context(|| RegionNotFoundSnafu { region_id })?;
                _dashmap_guard = Some(guard);
                _dashmap_guard.as_ref().unwrap()
            }
        };

        let engine = self
            .engines
            .get(engine_type)
            .with_context(|| RegionEngineNotFoundSnafu { name: engine_type })?;
        let result = engine
            .handle_request(region_id, request)
            .await
            .with_context(|_| HandleRegionRequestSnafu { region_id })?;

        match region_change {
            RegionChange::None => {}
            RegionChange::Register(_) => {
                info!("Region {region_id} is registered to engine {engine_type}");
                self.region_map.insert(region_id, engine_type.to_string());
            }
            RegionChange::Deregisters => {
                info!("Region {region_id} is deregistered from engine {engine_type}");
                self.region_map.remove(&region_id);
            }
        }

        Ok(result)
    }
}

enum RegionChange {
    None,
    Register(String),
    Deregisters,
}
