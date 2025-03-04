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

//! # Deprecate Warning:
//!
//! This module is deprecated and will be removed in the future.
//! All UDAF implementation here are not maintained and should
//! not be used before they are refactored into the `src/aggr`
//! version.

use std::sync::Arc;

use common_query::logical_plan::AggregateFunctionCreatorRef;

use crate::function_registry::FunctionRegistry;
use crate::scalars::vector::product::VectorProductCreator;
use crate::scalars::vector::sum::VectorSumCreator;

/// A function creates `AggregateFunctionCreator`.
/// "Aggregator" *is* AggregatorFunction. Since the later one is long, we named an short alias for it.
/// The two names might be used interchangeably.
type AggregatorCreatorFunction = Arc<dyn Fn() -> AggregateFunctionCreatorRef + Send + Sync>;

/// `AggregateFunctionMeta` dynamically creates AggregateFunctionCreator.
#[derive(Clone)]
pub struct AggregateFunctionMeta {
    name: String,
    args_count: u8,
    creator: AggregatorCreatorFunction,
}

pub type AggregateFunctionMetaRef = Arc<AggregateFunctionMeta>;

impl AggregateFunctionMeta {
    pub fn new(name: &str, args_count: u8, creator: AggregatorCreatorFunction) -> Self {
        Self {
            name: name.to_string(),
            args_count,
            creator,
        }
    }

    pub fn name(&self) -> String {
        self.name.to_string()
    }

    pub fn args_count(&self) -> u8 {
        self.args_count
    }

    pub fn create(&self) -> AggregateFunctionCreatorRef {
        (self.creator)()
    }
}

pub(crate) struct AggregateFunctions;

impl AggregateFunctions {
    pub fn register(registry: &FunctionRegistry) {
        registry.register_aggregate_function(Arc::new(AggregateFunctionMeta::new(
            "vec_sum",
            1,
            Arc::new(|| Arc::new(VectorSumCreator::default())),
        )));
        registry.register_aggregate_function(Arc::new(AggregateFunctionMeta::new(
            "vec_product",
            1,
            Arc::new(|| Arc::new(VectorProductCreator::default())),
        )));

        #[cfg(feature = "geo")]
        registry.register_aggregate_function(Arc::new(AggregateFunctionMeta::new(
            "json_encode_path",
            3,
            Arc::new(|| Arc::new(super::geo::encoding::JsonPathEncodeFunctionCreator::default())),
        )));
    }
}
