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

//! PromQL `min` and `max` aggregation operators.
//!
//! PromQL ignores NaN samples unless every sample of a group is NaN. DataFusion's
//! `min`/`max` order NaN differently depending on the code path (its grouped
//! accumulator lets NaN replace the current value and be replaced by the next one),
//! so the result depends on the order of the input.

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Float64Type};
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, GroupsAccumulator, Signature, Volatility,
};
use datafusion_common::ScalarValue;
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::prim_op::PrimitiveGroupsAccumulator;

use crate::function_registry::FunctionRegistry;

/// `max` or `min` with PromQL NaN semantics.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Extremum {
    name: &'static str,
    is_max: bool,
    signature: Signature,
}

impl Extremum {
    pub const MAX_NAME: &'static str = "prom_max";
    pub const MIN_NAME: &'static str = "prom_min";

    /// Registers both functions; the registry also derives their state functions, which
    /// distributed plans need to split the aggregation between datanodes and the frontend.
    pub fn register(registry: &FunctionRegistry) {
        registry.register_aggr(Self::max_udaf());
        registry.register_aggr(Self::min_udaf());
    }

    pub fn max_udaf() -> AggregateUDF {
        AggregateUDF::from(Self::new(Self::MAX_NAME, true))
    }

    pub fn min_udaf() -> AggregateUDF {
        AggregateUDF::from(Self::new(Self::MIN_NAME, false))
    }

    fn new(name: &'static str, is_max: bool) -> Self {
        Self {
            name,
            is_max,
            signature: Signature::exact(vec![DataType::Float64], Volatility::Immutable),
        }
    }
}

/// Folds `value` into `current` like Prometheus' aggregation: a NaN is replaced by any
/// sample, and a NaN sample never replaces a number.
fn fold(is_max: bool, current: &mut f64, value: f64) {
    let better = if is_max {
        value > *current
    } else {
        value < *current
    };
    if current.is_nan() || better {
        *current = value;
    }
}

impl AggregateUDFImpl for Extremum {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Float64)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> DfResult<Box<dyn Accumulator>> {
        Ok(Box::new(ExtremumAccumulator {
            is_max: self.is_max,
            value: None,
        }))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> DfResult<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(
            format!("{}[{}]", args.name, self.name),
            DataType::Float64,
            true,
        ))])
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> DfResult<Box<dyn GroupsAccumulator>> {
        let is_max = self.is_max;
        Ok(Box::new(
            PrimitiveGroupsAccumulator::<Float64Type, _>::new(&DataType::Float64, move |cur, v| {
                fold(is_max, cur, v)
            })
            .with_starting_value(f64::NAN),
        ))
    }
}

#[derive(Debug)]
struct ExtremumAccumulator {
    is_max: bool,
    value: Option<f64>,
}

impl ExtremumAccumulator {
    fn update(&mut self, values: &ArrayRef) {
        for v in values.as_primitive::<Float64Type>().iter().flatten() {
            match &mut self.value {
                Some(current) => fold(self.is_max, current, v),
                None => self.value = Some(v),
            }
        }
    }
}

impl Accumulator for ExtremumAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        self.update(&values[0]);
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        self.update(&states[0]);
        Ok(())
    }

    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Float64(self.value)])
    }

    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        Ok(ScalarValue::Float64(self.value))
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::Float64Array;
    use datafusion::logical_expr::EmitTo;

    use super::*;

    fn groups_result(
        is_max: bool,
        values: Vec<Option<f64>>,
        groups: Vec<usize>,
    ) -> Vec<Option<f64>> {
        let num_groups = groups.iter().max().map_or(0, |g| g + 1);
        let mut acc =
            PrimitiveGroupsAccumulator::<Float64Type, _>::new(&DataType::Float64, move |cur, v| {
                fold(is_max, cur, v)
            })
            .with_starting_value(f64::NAN);
        let values: ArrayRef = Arc::new(Float64Array::from(values));
        acc.update_batch(&[values], &groups, None, num_groups)
            .unwrap();
        let out = acc.evaluate(EmitTo::All).unwrap();
        out.as_primitive::<Float64Type>().iter().collect()
    }

    fn is_nan(v: Option<f64>) -> bool {
        v.is_some_and(f64::is_nan)
    }

    #[test]
    fn nan_is_ignored_unless_the_group_is_all_nan() {
        // Group 0 has NaN on both sides of a number; group 1 is all NaN; group 2 has only NULL.
        let values = vec![
            Some(f64::NAN),
            Some(3.0),
            Some(f64::NAN),
            Some(f64::NAN),
            None,
        ];
        let groups = vec![0, 0, 0, 1, 2];
        for is_max in [true, false] {
            let out = groups_result(is_max, values.clone(), groups.clone());
            assert_eq!(out[0], Some(3.0));
            assert!(is_nan(out[1]));
            assert_eq!(out[2], None);
        }
    }

    #[test]
    fn result_does_not_depend_on_input_order() {
        let forward = vec![Some(1.0), Some(f64::NAN), Some(4.0), Some(2.0)];
        let mut backward = forward.clone();
        backward.reverse();
        assert_eq!(
            groups_result(true, forward.clone(), vec![0; 4]),
            vec![Some(4.0)]
        );
        assert_eq!(
            groups_result(true, backward.clone(), vec![0; 4]),
            vec![Some(4.0)]
        );
        assert_eq!(groups_result(false, forward, vec![0; 4]), vec![Some(1.0)]);
        assert_eq!(groups_result(false, backward, vec![0; 4]), vec![Some(1.0)]);
    }

    #[test]
    fn merged_partial_states_keep_nan_semantics() {
        let mut partial_nan = ExtremumAccumulator {
            is_max: true,
            value: None,
        };
        partial_nan.update(&(Arc::new(Float64Array::from(vec![f64::NAN])) as ArrayRef));
        let mut partial_num = ExtremumAccumulator {
            is_max: true,
            value: None,
        };
        partial_num.update(&(Arc::new(Float64Array::from(vec![2.0, 5.0])) as ArrayRef));

        let mut merged = ExtremumAccumulator {
            is_max: true,
            value: None,
        };
        for partial in [&mut partial_nan, &mut partial_num] {
            let state = partial.state().unwrap()[0].to_array().unwrap();
            merged.merge_batch(&[state]).unwrap();
        }
        assert_eq!(merged.evaluate().unwrap(), ScalarValue::Float64(Some(5.0)));
    }
}
