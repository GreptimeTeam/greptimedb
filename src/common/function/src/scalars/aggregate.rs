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

mod argmax;
mod argmin;
mod diff;
mod mean;
mod percentile;
mod polyval;
mod scipy_stats_norm_cdf;
mod scipy_stats_norm_pdf;

use std::sync::Arc;

pub use argmax::ArgmaxAccumulatorCreator;
pub use argmin::ArgminAccumulatorCreator;
use common_query::logical_plan::AggregateFunctionCreatorRef;
pub use diff::DiffAccumulatorCreator;
pub use mean::MeanAccumulatorCreator;
pub use percentile::PercentileAccumulatorCreator;
pub use polyval::PolyvalAccumulatorCreator;
pub use scipy_stats_norm_cdf::ScipyStatsNormCdfAccumulatorCreator;
pub use scipy_stats_norm_pdf::ScipyStatsNormPdfAccumulatorCreator;

use crate::function_registry::FunctionRegistry;

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
        macro_rules! register_aggr_func {
            ($name :expr, $arg_count :expr, $creator :ty) => {
                registry.register_aggregate_function(Arc::new(AggregateFunctionMeta::new(
                    $name,
                    $arg_count,
                    Arc::new(|| Arc::new(<$creator>::default())),
                )));
            };
        }

        register_aggr_func!("diff", 1, DiffAccumulatorCreator);
        register_aggr_func!("mean", 1, MeanAccumulatorCreator);
        register_aggr_func!("polyval", 2, PolyvalAccumulatorCreator);
        register_aggr_func!("argmax", 1, ArgmaxAccumulatorCreator);
        register_aggr_func!("argmin", 1, ArgminAccumulatorCreator);
        register_aggr_func!("percentile", 2, PercentileAccumulatorCreator);
        register_aggr_func!("scipystatsnormcdf", 2, ScipyStatsNormCdfAccumulatorCreator);
        register_aggr_func!("scipystatsnormpdf", 2, ScipyStatsNormPdfAccumulatorCreator);
    }
}
