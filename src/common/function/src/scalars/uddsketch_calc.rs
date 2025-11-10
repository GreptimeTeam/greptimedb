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

use datafusion_common::DataFusionError;
use datafusion_common::arrow::array::{Array, AsArray, Float64Builder};
use datafusion_common::arrow::datatypes::{DataType, Float64Type};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, Volatility};
use uddsketch::UDDSketch;

use crate::function::{Function, extract_args};
use crate::function_registry::FunctionRegistry;

const NAME: &str = "uddsketch_calc";

/// UddSketchCalcFunction implements the scalar function `uddsketch_calc`.
///
/// It accepts two arguments:
/// 1. A percentile (as f64) for which to compute the estimated quantile (e.g. 0.95 for p95).
/// 2. The serialized UDDSketch state, as produced by the aggregator (binary).
///
/// For each row, it deserializes the sketch and returns the computed quantile value.
#[derive(Debug)]
pub(crate) struct UddSketchCalcFunction {
    signature: Signature,
}

impl UddSketchCalcFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register_scalar(UddSketchCalcFunction::default());
    }
}

impl Default for UddSketchCalcFunction {
    fn default() -> Self {
        Self {
            // First argument: percentile (float64)
            // Second argument: UDDSketch state (binary)
            signature: Signature::exact(
                vec![DataType::Float64, DataType::Binary],
                Volatility::Immutable,
            ),
        }
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

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Float64)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [arg0, arg1] = extract_args(self.name(), &args)?;

        let Some(percentages) = arg0.as_primitive_opt::<Float64Type>() else {
            return Err(DataFusionError::Execution(format!(
                "'{}' expects 1st argument to be Float64 datatype, got {}",
                self.name(),
                arg0.data_type()
            )));
        };
        let Some(sketch_vec) = arg1.as_binary_opt::<i32>() else {
            return Err(DataFusionError::Execution(format!(
                "'{}' expects 2nd argument to be Binary datatype, got {}",
                self.name(),
                arg1.data_type()
            )));
        };
        let len = sketch_vec.len();
        let mut builder = Float64Builder::with_capacity(len);

        for i in 0..len {
            let perc_opt = percentages.is_valid(i).then(|| percentages.value(i));
            let sketch_opt = sketch_vec.is_valid(i).then(|| sketch_vec.value(i));

            if sketch_opt.is_none() || perc_opt.is_none() {
                builder.append_null();
                continue;
            }

            let sketch_bytes = sketch_opt.unwrap();
            let perc = perc_opt.unwrap();

            // Deserialize the UDDSketch from its bincode representation
            let sketch: UDDSketch = match bincode::deserialize(sketch_bytes) {
                Ok(s) => s,
                Err(e) => {
                    common_telemetry::trace!("Failed to deserialize UDDSketch: {}", e);
                    builder.append_null();
                    continue;
                }
            };

            // Check if the sketch is empty, if so, return null
            // This is important to avoid panics when calling estimate_quantile on an empty sketch
            // In practice, this will happen if input is all null
            if sketch.bucket_iter().count() == 0 {
                builder.append_null();
                continue;
            }
            // Compute the estimated quantile from the sketch
            let result = sketch.estimate_quantile(perc);
            builder.append_value(result);
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::Field;
    use datafusion_common::arrow::array::{BinaryArray, Float64Array};

    use super::*;

    #[test]
    fn test_uddsketch_calc_function() {
        let function = UddSketchCalcFunction::default();
        assert_eq!("uddsketch_calc", function.name());
        assert_eq!(
            DataType::Float64,
            function.return_type(&[DataType::Float64]).unwrap()
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

        let args = vec![
            ColumnarValue::Array(Arc::new(Float64Array::from(percentiles.clone()))),
            ColumnarValue::Array(Arc::new(BinaryArray::from_iter_values(vec![serialized; 3]))),
        ];

        let result = function
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields: vec![],
                number_rows: 3,
                return_field: Arc::new(Field::new("x", DataType::Float64, false)),
                config_options: Arc::new(Default::default()),
            })
            .unwrap();
        let ColumnarValue::Array(result) = result else {
            unreachable!()
        };
        let result = result.as_primitive::<Float64Type>();
        assert_eq!(result.len(), 3);

        // Test median (p50)
        assert!((result.value(0) - expected_p50).abs() < 1e-10);
        // Test p90
        assert!((result.value(1) - expected_p90).abs() < 1e-10);
        // Test p95
        assert!((result.value(2) - expected_p95).abs() < 1e-10);
    }

    #[test]
    fn test_uddsketch_calc_function_errors() {
        let function = UddSketchCalcFunction::default();

        // Test with invalid number of arguments
        let result = function.invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(Float64Array::from(vec![
                0.95,
            ])))],
            arg_fields: vec![],
            number_rows: 0,
            return_field: Arc::new(Field::new("x", DataType::Float64, false)),
            config_options: Arc::new(Default::default()),
        });
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Execution error: uddsketch_calc function requires 2 arguments, got 1")
        );

        // Test with invalid binary data
        let args = vec![
            ColumnarValue::Array(Arc::new(Float64Array::from(vec![0.95]))),
            ColumnarValue::Array(Arc::new(BinaryArray::from_iter(vec![Some(vec![1, 2, 3])]))),
        ];
        let result = function
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields: vec![],
                number_rows: 0,
                return_field: Arc::new(Field::new("x", DataType::Float64, false)),
                config_options: Arc::new(Default::default()),
            })
            .unwrap();
        let ColumnarValue::Array(result) = result else {
            unreachable!()
        };
        let result = result.as_primitive::<Float64Type>();
        assert_eq!(result.len(), 1);
        assert!(result.is_null(0));
    }
}
