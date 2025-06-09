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

//! Implementation of the scalar function `uddsketch_calc`.

use std::fmt;
use std::fmt::Display;
use std::sync::Arc;

use common_query::error::{DowncastVectorSnafu, InvalidFuncArgsSnafu, Result};
use common_query::prelude::{Signature, Volatility};
use datatypes::data_type::ConcreteDataType;
use datatypes::prelude::Vector;
use datatypes::scalars::{ScalarVector, ScalarVectorBuilder};
use datatypes::vectors::{BinaryVector, Float64VectorBuilder, MutableVector, VectorRef};
use snafu::OptionExt;
use uddsketch::UDDSketch;

use crate::function::{Function, FunctionContext};
use crate::function_registry::FunctionRegistry;

const NAME: &str = "uddsketch_calc";

/// UddSketchCalcFunction implements the scalar function `uddsketch_calc`.
///
/// It accepts two arguments:
/// 1. A percentile (as f64) for which to compute the estimated quantile (e.g. 0.95 for p95).
/// 2. The serialized UDDSketch state, as produced by the aggregator (binary).
///
/// For each row, it deserializes the sketch and returns the computed quantile value.
#[derive(Debug, Default)]
pub struct UddSketchCalcFunction;

impl UddSketchCalcFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register(Arc::new(UddSketchCalcFunction));
    }
}

impl Display for UddSketchCalcFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", NAME.to_ascii_uppercase())
    }
}

impl Function for UddSketchCalcFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::float64_datatype())
    }

    fn signature(&self) -> Signature {
        // First argument: percentile (float64)
        // Second argument: UDDSketch state (binary)
        Signature::exact(
            vec![
                ConcreteDataType::float64_datatype(),
                ConcreteDataType::binary_datatype(),
            ],
            Volatility::Immutable,
        )
    }

    fn eval(&self, _func_ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        if columns.len() != 2 {
            return InvalidFuncArgsSnafu {
                err_msg: format!("uddsketch_calc expects 2 arguments, got {}", columns.len()),
            }
            .fail();
        }

        let perc_vec = &columns[0];
        let sketch_vec = columns[1]
            .as_any()
            .downcast_ref::<BinaryVector>()
            .with_context(|| DowncastVectorSnafu {
                err_msg: format!("expect BinaryVector, got {}", columns[1].vector_type_name()),
            })?;
        let len = sketch_vec.len();
        let mut builder = Float64VectorBuilder::with_capacity(len);

        for i in 0..len {
            let perc_opt = perc_vec.get(i).as_f64_lossy();
            let sketch_opt = sketch_vec.get_data(i);

            if sketch_opt.is_none() || perc_opt.is_none() {
                builder.push_null();
                continue;
            }

            let sketch_bytes = sketch_opt.unwrap();
            let perc = perc_opt.unwrap();

            // Deserialize the UDDSketch from its bincode representation
            let sketch: UDDSketch = match bincode::deserialize(sketch_bytes) {
                Ok(s) => s,
                Err(e) => {
                    common_telemetry::trace!("Failed to deserialize UDDSketch: {}", e);
                    builder.push_null();
                    continue;
                }
            };

            // Check if the sketch is empty, if so, return null
            // This is important to avoid panics when calling estimate_quantile on an empty sketch
            // In practice, this will happen if input is all null
            if sketch.bucket_iter().count() == 0 {
                builder.push_null();
                continue;
            }
            // Compute the estimated quantile from the sketch
            let result = sketch.estimate_quantile(perc);
            builder.push(Some(result));
        }

        Ok(builder.to_vector())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::vectors::{BinaryVector, Float64Vector};

    use super::*;

    #[test]
    fn test_uddsketch_calc_function() {
        let function = UddSketchCalcFunction;
        assert_eq!("uddsketch_calc", function.name());
        assert_eq!(
            ConcreteDataType::float64_datatype(),
            function
                .return_type(&[ConcreteDataType::float64_datatype()])
                .unwrap()
        );

        // Create a test sketch
        let mut sketch = UDDSketch::new(128, 0.01);
        sketch.add_value(10.0);
        sketch.add_value(20.0);
        sketch.add_value(30.0);
        sketch.add_value(40.0);
        sketch.add_value(50.0);
        sketch.add_value(60.0);
        sketch.add_value(70.0);
        sketch.add_value(80.0);
        sketch.add_value(90.0);
        sketch.add_value(100.0);

        // Get expected values directly from the sketch
        let expected_p50 = sketch.estimate_quantile(0.5);
        let expected_p90 = sketch.estimate_quantile(0.9);
        let expected_p95 = sketch.estimate_quantile(0.95);

        let serialized = bincode::serialize(&sketch).unwrap();
        let percentiles = vec![0.5, 0.9, 0.95];

        let args: Vec<VectorRef> = vec![
            Arc::new(Float64Vector::from_vec(percentiles.clone())),
            Arc::new(BinaryVector::from(vec![Some(serialized.clone()); 3])),
        ];

        let result = function.eval(&FunctionContext::default(), &args).unwrap();
        assert_eq!(result.len(), 3);

        // Test median (p50)
        assert!(
            matches!(result.get(0), datatypes::value::Value::Float64(v) if (v - expected_p50).abs() < 1e-10)
        );
        // Test p90
        assert!(
            matches!(result.get(1), datatypes::value::Value::Float64(v) if (v - expected_p90).abs() < 1e-10)
        );
        // Test p95
        assert!(
            matches!(result.get(2), datatypes::value::Value::Float64(v) if (v - expected_p95).abs() < 1e-10)
        );
    }

    #[test]
    fn test_uddsketch_calc_function_errors() {
        let function = UddSketchCalcFunction;

        // Test with invalid number of arguments
        let args: Vec<VectorRef> = vec![Arc::new(Float64Vector::from_vec(vec![0.95]))];
        let result = function.eval(&FunctionContext::default(), &args);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("uddsketch_calc expects 2 arguments"));

        // Test with invalid binary data
        let args: Vec<VectorRef> = vec![
            Arc::new(Float64Vector::from_vec(vec![0.95])),
            Arc::new(BinaryVector::from(vec![Some(vec![1, 2, 3])])), // Invalid binary data
        ];
        let result = function.eval(&FunctionContext::default(), &args).unwrap();
        assert_eq!(result.len(), 1);
        assert!(matches!(result.get(0), datatypes::value::Value::Null));
    }
}
