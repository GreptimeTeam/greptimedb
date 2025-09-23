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

//! Implementation of the scalar function `hll_count`.

use std::fmt;
use std::fmt::Display;
use std::sync::Arc;

use common_query::error::Result;
use datafusion_common::DataFusionError;
use datafusion_common::arrow::array::{Array, AsArray, UInt64Builder};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, Volatility};
use datatypes::arrow::datatypes::DataType;
use hyperloglogplus::HyperLogLog;

use crate::aggrs::approximate::hll::HllStateType;
use crate::function::{Function, extract_args};
use crate::function_registry::FunctionRegistry;

const NAME: &str = "hll_count";

/// HllCalcFunction implements the scalar function `hll_count`.
///
/// It accepts one argument:
/// 1. The serialized HyperLogLogPlus state, as produced by the aggregator (binary).
///
/// For each row, it deserializes the sketch and returns the estimated cardinality.
#[derive(Debug, Default)]
pub struct HllCalcFunction;

impl HllCalcFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register_scalar(HllCalcFunction);
    }
}

impl Display for HllCalcFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", NAME.to_ascii_uppercase())
    }
}

impl Function for HllCalcFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::UInt64)
    }

    fn signature(&self) -> Signature {
        // Only argument: HyperLogLogPlus state (binary)
        Signature::exact(vec![DataType::Binary], Volatility::Immutable)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [arg0] = extract_args(self.name(), &args)?;

        let Some(hll_vec) = arg0.as_binary_opt::<i32>() else {
            return Err(DataFusionError::Execution(format!(
                "'{}' expects argument to be Binary datatype, got {}",
                self.name(),
                arg0.data_type()
            )));
        };
        let len = hll_vec.len();
        let mut builder = UInt64Builder::with_capacity(len);

        for i in 0..len {
            let hll_opt = hll_vec.is_valid(i).then(|| hll_vec.value(i));

            if hll_opt.is_none() {
                builder.append_null();
                continue;
            }

            let hll_bytes = hll_opt.unwrap();

            // Deserialize the HyperLogLogPlus from its bincode representation
            let mut hll: HllStateType = match bincode::deserialize(hll_bytes) {
                Ok(h) => h,
                Err(e) => {
                    common_telemetry::trace!("Failed to deserialize HyperLogLogPlus: {}", e);
                    builder.append_null();
                    continue;
                }
            };

            builder.append_value(hll.count().round() as u64);
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::Field;
    use datafusion_common::arrow::array::BinaryArray;
    use datafusion_common::arrow::datatypes::UInt64Type;

    use super::*;
    use crate::utils::FixedRandomState;

    #[test]
    fn test_hll_count_function() {
        let function = HllCalcFunction;
        assert_eq!("hll_count", function.name());
        assert_eq!(
            DataType::UInt64,
            function.return_type(&[DataType::UInt64]).unwrap()
        );

        // Create a test HLL
        let mut hll = HllStateType::new(14, FixedRandomState::new()).unwrap();
        for i in 1..=10 {
            hll.insert(&i.to_string());
        }

        let serialized_bytes = bincode::serialize(&hll).unwrap();
        let args = vec![ColumnarValue::Array(Arc::new(BinaryArray::from_iter(
            vec![Some(serialized_bytes)],
        )))];

        let result = function
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields: vec![],
                number_rows: 1,
                return_field: Arc::new(Field::new("x", DataType::UInt64, false)),
                config_options: Arc::new(Default::default()),
            })
            .unwrap();
        let ColumnarValue::Array(result) = result else {
            unreachable!()
        };
        let result = result.as_primitive::<UInt64Type>();
        assert_eq!(result.len(), 1);

        // Test cardinality estimate
        assert_eq!(result.value(0), 10);
    }

    #[test]
    fn test_hll_count_function_errors() {
        let function = HllCalcFunction;

        // Test with invalid number of arguments
        let result = function.invoke_with_args(ScalarFunctionArgs {
            args: vec![],
            arg_fields: vec![],
            number_rows: 0,
            return_field: Arc::new(Field::new("x", DataType::UInt64, false)),
            config_options: Arc::new(Default::default()),
        });
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Execution error: hll_count function requires 1 argument, got 0")
        );

        // Test with invalid binary data
        let result = function
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(Arc::new(BinaryArray::from_iter(
                    vec![Some(vec![1, 2, 3])],
                )))],
                arg_fields: vec![],
                number_rows: 0,
                return_field: Arc::new(Field::new("x", DataType::UInt64, false)),
                config_options: Arc::new(Default::default()),
            })
            .unwrap();
        let ColumnarValue::Array(result) = result else {
            unreachable!()
        };
        let result = result.as_primitive::<UInt64Type>();
        assert_eq!(result.len(), 1);
        assert!(result.is_null(0));
    }
}
