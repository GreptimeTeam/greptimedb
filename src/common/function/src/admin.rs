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

mod add_region_follower;
mod flush_compact_region;
mod flush_compact_table;
mod metadata_snaphost;
mod migrate_region;
mod remove_region_follower;

use std::sync::Arc;

use add_region_follower::AddRegionFollowerFunction;
use flush_compact_region::{CompactRegionFunction, FlushRegionFunction};
use flush_compact_table::{CompactTableFunction, FlushTableFunction};
use metadata_snaphost::{DumpMetadataFunction, RestoreMetadataFunction};
use migrate_region::MigrateRegionFunction;
use remove_region_follower::RemoveRegionFollowerFunction;

use crate::flush_flow::FlushFlowFunction;
use crate::function_registry::FunctionRegistry;

/// Table functions
pub(crate) struct AdminFunction;

impl AdminFunction {
    /// Register all table functions to [`FunctionRegistry`].
    pub fn register(registry: &FunctionRegistry) {
        registry.register_async(Arc::new(MigrateRegionFunction));
        registry.register_async(Arc::new(AddRegionFollowerFunction));
        registry.register_async(Arc::new(RemoveRegionFollowerFunction));
        registry.register_async(Arc::new(FlushRegionFunction));
        registry.register_async(Arc::new(CompactRegionFunction));
        registry.register_async(Arc::new(FlushTableFunction));
        registry.register_async(Arc::new(CompactTableFunction));
        registry.register_async(Arc::new(FlushFlowFunction));
        registry.register_async(Arc::new(DumpMetadataFunction));
        registry.register_async(Arc::new(RestoreMetadataFunction));
    }
}
