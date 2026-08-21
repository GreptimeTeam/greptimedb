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

//! Implementation of the scalar function `uddsketch_rank`.

use std::fmt;
use std::fmt::Display;
use std::sync::Arc;

use datafusion_common::DataFusionError;
use datafusion_common::arrow::array::{Array, AsArray, Float64Builder};
use datafusion_common::arrow::datatypes::{DataType, Float64Type};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, Volatility};
use uddsketch::UddSketchRef;

use crate::function::{Function, extract_args};
use crate::function_registry::FunctionRegistry;

const NAME: &str = "uddsketch_rank";

/// Implements the scalar function `uddsketch_rank`.
#[derive(Debug)]
pub(crate) struct UddSketchRankFunction {
    signature: Signature,
}

impl UddSketchRankFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register_scalar(UddSketchRankFunction::default());
    }
}

impl Default for UddSketchRankFunction {
    fn default() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Float64, DataType::Binary],
                Volatility::Immutable,
            ),
        }
    }
}

impl Display for UddSketchRankFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", NAME.to_ascii_uppercase())
    }
}

impl Function for UddSketchRankFunction {
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

        let Some(values) = arg0.as_primitive_opt::<Float64Type>() else {
            return Err(DataFusionError::Execution(format!(
                "'{}' expects 1st argument to be Float64 datatype, got {}",
                self.name(),
                arg0.data_type()
            )));
        };
        let Some(sketches) = arg1.as_binary_opt::<i32>() else {
            return Err(DataFusionError::Execution(format!(
                "'{}' expects 2nd argument to be Binary datatype, got {}",
                self.name(),
                arg1.data_type()
            )));
        };
        let mut builder = Float64Builder::with_capacity(sketches.len());

        for i in 0..sketches.len() {
            if values.is_null(i) || sketches.is_null(i) {
                builder.append_null();
                continue;
            }

            match UddSketchRef::parse(sketches.value(i))
                .and_then(|sketch| sketch.rank(values.value(i)))
            {
                Ok(Some(rank)) => builder.append_value(rank),
                Ok(None) => builder.append_null(),
                Err(error) => {
                    common_telemetry::trace!("Failed to calculate UDDSketch rank: {}", error);
                    builder.append_null();
                }
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::Field;
    use datafusion_common::arrow::array::{Array, AsArray, BinaryArray, Float64Array};
    use datafusion_common::arrow::datatypes::{DataType, Float64Type};
    use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
    use uddsketch::UddSketch;

    use super::UddSketchRankFunction;
    use crate::function::Function;
    use crate::uddsketch_compat;

    fn invoke(
        function: &UddSketchRankFunction,
        values: Float64Array,
        states: BinaryArray,
    ) -> Float64Array {
        let number_rows = values.len();
        let result = function
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(Arc::new(values)),
                    ColumnarValue::Array(Arc::new(states)),
                ],
                arg_fields: vec![],
                number_rows,
                return_field: Arc::new(Field::new("x", DataType::Float64, true)),
                config_options: Arc::new(Default::default()),
            })
            .unwrap();
        let ColumnarValue::Array(result) = result else {
            unreachable!()
        };
        result.as_primitive::<Float64Type>().clone()
    }

    #[test]
    fn test_uddsketch_rank_function_name_and_return_type() {
        let function = UddSketchRankFunction::default();

        assert_eq!("uddsketch_rank", function.name());
        assert_eq!(
            DataType::Float64,
            function
                .return_type(&[DataType::Float64, DataType::Binary])
                .unwrap()
        );
    }

    #[test]
    fn test_uddsketch_rank_function_canonical_state() {
        let function = UddSketchRankFunction::default();
        let mut sketch = UddSketch::new(128, 0.01).unwrap();
        sketch
            .add_batch(&[10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0])
            .unwrap();
        let values = [5.0, 10.0, 55.0, 100.0, 110.0];
        let expected = values.map(|value| sketch.rank(value).unwrap().unwrap());
        let encoded = sketch.encode().unwrap();
        let states = BinaryArray::from_iter_values((0..values.len()).map(|_| encoded.as_slice()));

        let result = invoke(&function, Float64Array::from(values.to_vec()), states);

        assert_eq!(result.values(), &expected);
    }

    #[test]
    fn test_uddsketch_rank_function_returns_null_for_invalid_rows() {
        let function = UddSketchRankFunction::default();
        let empty = UddSketch::new(128, 0.01).unwrap().encode().unwrap();
        let mut populated = UddSketch::new(128, 0.01).unwrap();
        populated.add(1.0).unwrap();
        let populated = populated.encode().unwrap();
        let malformed = [1, 2, 3];
        let states = BinaryArray::from_iter([
            Some(populated.as_slice()),
            None,
            Some(empty.as_slice()),
            Some(malformed.as_slice()),
            Some(uddsketch_compat::LEGACY_STATE),
            Some(populated.as_slice()),
        ]);
        let values = Float64Array::from(vec![
            None,
            Some(1.0),
            Some(1.0),
            Some(1.0),
            Some(1.0),
            Some(f64::NAN),
        ]);

        let result = invoke(&function, values, states);

        assert_eq!(result.null_count(), 6);
    }
}
