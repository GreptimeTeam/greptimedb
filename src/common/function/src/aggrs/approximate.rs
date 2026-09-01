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

use datafusion_expr::AggregateUDF;
use datatypes::arrow::datatypes::DataType;

use crate::aggrs::aggr_wrapper::DeltaMergeWrapper;
use crate::function_registry::FunctionRegistry;

pub mod hll;
pub mod uddsketch;
pub mod welford;

pub(crate) struct ApproximateFunction;

impl ApproximateFunction {
    pub fn register(registry: &FunctionRegistry) {
        let uddsketch_state = uddsketch::UddSketchState::state_udf_impl();
        let uddsketch_merge = uddsketch::UddSketchState::merge_udf_impl();
        let uddsketch_delta = AggregateUDF::new_from_impl(DeltaMergeWrapper::new(
            uddsketch_merge.clone(),
            uddsketch::UDDSKETCH_STATE_NAME,
            vec![DataType::Int64, DataType::Float64, DataType::Binary],
            DataType::Binary,
        ));
        registry.register_aggr(uddsketch_state);
        registry.register_aggr(uddsketch_merge);
        registry.register_aggr(uddsketch_delta);

        // hll
        let hll_state = hll::HllState::state_udf_impl();
        let hll_merge = hll::HllState::merge_udf_impl();
        let hll_delta = AggregateUDF::new_from_impl(DeltaMergeWrapper::new(
            hll_merge.clone(),
            hll::HLL_NAME,
            vec![DataType::Binary],
            DataType::Binary,
        ));
        registry.register_aggr(hll_state);
        registry.register_aggr(hll_merge);
        registry.register_aggr(hll_delta);

        // welford
        let welford_state = welford::WelfordAccumulator::state_udf_impl();
        let welford_merge = welford::WelfordAccumulator::merge_udf_impl();
        let welford_delta = AggregateUDF::new_from_impl(DeltaMergeWrapper::new(
            welford_merge.clone(),
            welford::STDDEV_POP_STATE_NAME,
            vec![DataType::Binary],
            DataType::Binary,
        ));
        registry.register_aggr(welford_state);
        registry.register_aggr(welford_merge);
        registry.register_aggr(welford_delta);
    }
}
