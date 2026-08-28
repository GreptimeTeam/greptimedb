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

//! Implementation of the scalar function `stddev_pop_calc`.

use std::fmt;
use std::fmt::Display;
use std::sync::Arc;

use datafusion_common::DataFusionError;
use datafusion_common::arrow::array::{Array, AsArray, Float64Builder};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, Volatility};
use datatypes::arrow::datatypes::DataType;

use crate::aggrs::approximate::welford::WelfordState;
use crate::function::{Function, extract_args};
use crate::function_registry::FunctionRegistry;

const NAME: &str = "stddev_pop_calc";

/// Calculates population standard deviation from a serialized Welford state.
#[derive(Debug)]
pub(crate) struct WelfordStddevFunction {
    signature: Signature,
}

impl WelfordStddevFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register_scalar(Self::default());
    }
}

impl Default for WelfordStddevFunction {
    fn default() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Binary], Volatility::Immutable),
        }
    }
}

impl Display for WelfordStddevFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", NAME.to_ascii_uppercase())
    }
}

impl Function for WelfordStddevFunction {
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
        let [arg] = extract_args(self.name(), &args)?;
        let Some(states) = arg.as_binary_opt::<i32>() else {
            return Err(DataFusionError::Execution(format!(
                "'{}' expects argument to be Binary datatype, got {}",
                self.name(),
                arg.data_type()
            )));
        };
        let mut builder = Float64Builder::with_capacity(states.len());
        for state in states.iter() {
            match state.and_then(decode_population_stddev) {
                Some(stddev) => builder.append_value(stddev),
                None => builder.append_null(),
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

fn decode_population_stddev(encoded: &[u8]) -> Option<f64> {
    match WelfordState::decode(encoded) {
        Ok(state) => state.population_stddev(),
        Err(error) => {
            common_telemetry::trace!("Failed to decode Welford state: {}", error);
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::Field;
    use datafusion_common::arrow::array::{Array, AsArray, BinaryArray};
    use datafusion_common::arrow::datatypes::Float64Type;
    use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
    use datatypes::arrow::datatypes::DataType;

    use super::*;
    use crate::aggrs::approximate::welford::WelfordState;
    use crate::function::Function;

    fn invoke(states: BinaryArray) -> ColumnarValue {
        WelfordStddevFunction::default()
            .invoke_with_args(ScalarFunctionArgs {
                number_rows: states.len(),
                args: vec![ColumnarValue::Array(Arc::new(states))],
                arg_fields: vec![],
                return_field: Arc::new(Field::new("x", DataType::Float64, true)),
                config_options: Arc::new(Default::default()),
            })
            .unwrap()
    }

    #[test]
    fn test_populated_welford_state_returns_population_stddev() {
        let populated = WelfordState {
            count: 4,
            mean: 2.5,
            m2: 5.0,
        }
        .encode();

        let ColumnarValue::Array(output) =
            invoke(BinaryArray::from(vec![Some(populated.as_slice())]))
        else {
            panic!("Expected array result");
        };
        let output = output.as_primitive::<Float64Type>();
        assert!((output.value(0) - 1.25_f64.sqrt()).abs() < 1e-12);
    }

    #[test]
    fn test_empty_malformed_and_null_states_return_null() {
        let empty = WelfordState::default().encode();

        let ColumnarValue::Array(output) = invoke(BinaryArray::from(vec![
            Some(empty.as_slice()),
            Some(b"invalid".as_slice()),
            None,
        ])) else {
            panic!("Expected array result");
        };
        assert_eq!(output.null_count(), 3);
    }

    #[test]
    fn test_stddev_pop_calc_metadata() {
        let function = WelfordStddevFunction::default();

        assert_eq!(function.name(), "stddev_pop_calc");
        assert_eq!(
            function.return_type(&[DataType::Binary]).unwrap(),
            DataType::Float64
        );
    }

    #[test]
    fn test_stddev_pop_calc_rejects_wrong_argument_count() {
        let error = WelfordStddevFunction::default()
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![],
                arg_fields: vec![],
                number_rows: 0,
                return_field: Arc::new(Field::new("x", DataType::Float64, true)),
                config_options: Arc::new(Default::default()),
            })
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("stddev_pop_calc function requires 1 argument, got 0")
        );
    }
}
