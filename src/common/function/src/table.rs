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

mod flush_compact_region;
mod flush_compact_table;
mod migrate_region;

use std::sync::Arc;

use flush_compact_region::{CompactRegionFunction, FlushRegionFunction};
use flush_compact_table::{CompactTableFunction, FlushTableFunction};
use migrate_region::MigrateRegionFunction;

use crate::flush_flow::FlushFlowFunction;
use crate::function_registry::FunctionRegistry;

/// Table functions
pub(crate) struct TableFunction;

impl TableFunction {
    /// Register all table functions to [`FunctionRegistry`].
    pub fn register(registry: &FunctionRegistry) {
        registry.register(Arc::new(MigrateRegionFunction));
        registry.register(Arc::new(FlushRegionFunction));
        registry.register(Arc::new(CompactRegionFunction));
        registry.register(Arc::new(FlushTableFunction));
        registry.register(Arc::new(CompactTableFunction));
        registry.register(Arc::new(FlushFlowFunction));
    }
}
