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

//! Implementation of the scalar function `avg_calc`.

use std::fmt;
use std::fmt::Display;
use std::sync::Arc;

use datafusion_common::arrow::array::{Array, AsArray, Float64Builder};
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, Volatility};
use datatypes::arrow::datatypes::DataType;

use crate::aggrs::approximate::avg::AvgState;
use crate::function::Function;
use crate::function_registry::FunctionRegistry;

const NAME: &str = "avg_calc";

/// Calculates an average from a serialized AVG1 state.
#[derive(Debug)]
pub(crate) struct AvgCalcFunction {
    signature: Signature,
}

impl AvgCalcFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register_scalar(Self::default());
    }
}

impl Default for AvgCalcFunction {
    fn default() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Binary], Volatility::Immutable),
        }
    }
}

impl Display for AvgCalcFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", NAME.to_ascii_uppercase())
    }
}

impl Function for AvgCalcFunction {
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
        let [arg] = datafusion_common::utils::take_function_args(self.name(), &args.args)?;
        match arg {
            ColumnarValue::Scalar(ScalarValue::Binary(state)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(
                    state
                        .as_deref()
                        .map(AvgState::decode)
                        .transpose()?
                        .and_then(|state| state.average()),
                )))
            }
            ColumnarValue::Scalar(ScalarValue::Null) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(None)))
            }
            ColumnarValue::Array(states) => {
                let Some(states) = states.as_binary_opt::<i32>() else {
                    return Err(invalid_type(self.name(), states.data_type()));
                };
                let mut builder = Float64Builder::with_capacity(states.len());
                for state in states.iter() {
                    builder.append_option(match state {
                        Some(state) => AvgState::decode(state)?.average(),
                        None => None,
                    });
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
            _ => Err(invalid_type(self.name(), arg.data_type())),
        }
    }
}

fn invalid_type(name: &str, data_type: &DataType) -> DataFusionError {
    DataFusionError::Execution(format!(
        "'{name}' expects argument to be Binary datatype, got {data_type}"
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::Field;
    use datafusion::arrow::array::{Array, AsArray, BinaryArray, Float64Array};
    use datafusion::logical_expr::Accumulator;
    use datafusion::prelude::SessionContext;
    use datafusion_common::arrow::datatypes::Float64Type;
    use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};

    use super::*;
    use crate::aggrs::approximate::avg::AvgAccumulator;
    use crate::function::{Function, FunctionContext};
    use crate::function_registry::FUNCTION_REGISTRY;

    fn produce_state(values: Vec<Option<f64>>) -> Vec<u8> {
        let mut accumulator = AvgAccumulator::default();
        accumulator
            .update_batch(&[Arc::new(Float64Array::from(values))])
            .unwrap();
        let ScalarValue::Binary(Some(state)) = accumulator.evaluate().unwrap() else {
            panic!("AVG state must be binary");
        };
        state
    }

    fn invoke(arg: ColumnarValue, number_rows: usize) -> datafusion_common::Result<ColumnarValue> {
        AvgCalcFunction::default().invoke_with_args(ScalarFunctionArgs {
            args: vec![arg],
            arg_fields: vec![],
            number_rows,
            return_field: Arc::new(Field::new("x", DataType::Float64, true)),
            config_options: Arc::new(Default::default()),
        })
    }

    #[test]
    fn scalar_and_array_states_decode_to_averages() {
        let state = produce_state(vec![Some(1.0), Some(2.0), Some(6.0)]);
        let ColumnarValue::Scalar(ScalarValue::Float64(Some(value))) =
            invoke(ColumnarValue::Scalar(ScalarValue::Binary(Some(state))), 1).unwrap()
        else {
            panic!("Expected Float64 scalar");
        };
        assert_eq!(value, 3.0);

        let ColumnarValue::Scalar(ScalarValue::Float64(None)) =
            invoke(ColumnarValue::Scalar(ScalarValue::Binary(None)), 1).unwrap()
        else {
            panic!("Expected NULL Float64 scalar");
        };
        let empty = produce_state(vec![None]);
        let ColumnarValue::Scalar(ScalarValue::Float64(None)) = invoke(
            ColumnarValue::Scalar(ScalarValue::Binary(Some(empty.clone()))),
            1,
        )
        .unwrap() else {
            panic!("Expected NULL Float64 scalar");
        };

        let infinity = produce_state(vec![Some(f64::INFINITY)]);
        let nan = produce_state(vec![Some(f64::NAN)]);
        let ColumnarValue::Array(result) = invoke(
            ColumnarValue::Array(Arc::new(BinaryArray::from(vec![
                Some(empty.as_slice()),
                None,
                Some(infinity.as_slice()),
                Some(nan.as_slice()),
            ]))),
            4,
        )
        .unwrap() else {
            panic!("Expected Float64 array");
        };
        let result = result.as_primitive::<Float64Type>();
        assert!(result.is_null(0));
        assert!(result.is_null(1));
        assert_eq!(result.value(2), f64::INFINITY);
        assert!(result.value(3).is_nan());
    }

    #[test]
    fn malformed_and_unknown_version_states_fail_the_whole_batch() {
        let valid = produce_state(vec![Some(3.0)]);
        let mut unknown_version = valid.clone();
        unknown_version[..4].copy_from_slice(b"AVG2");

        assert!(
            invoke(
                ColumnarValue::Scalar(ScalarValue::Binary(Some(unknown_version.clone()))),
                1,
            )
            .is_err()
        );
        assert!(
            invoke(
                ColumnarValue::Array(Arc::new(BinaryArray::from(vec![
                    Some(valid.as_slice()),
                    Some(b"malformed".as_slice()),
                    Some(unknown_version.as_slice()),
                ]))),
                3,
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn registry_query_decodes_avg_state_and_weighted_avg_merge() {
        let ctx = SessionContext::new();
        let avg_calc = FUNCTION_REGISTRY
            .get_function(NAME)
            .expect("avg_calc must be registered")
            .provide(FunctionContext::default());
        ctx.register_udf(avg_calc);
        for name in ["avg_state", "avg_merge"] {
            ctx.register_udaf(
                FUNCTION_REGISTRY
                    .get_aggr_func(name)
                    .expect("AVG aggregate must be registered"),
            );
        }

        let batches = ctx
            .sql(
                "SELECT avg_calc(avg_state(CAST(value AS DOUBLE))) FROM \
                 (VALUES (1.0), (2.0), (6.0)) AS values_table(value)",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let result = batches[0].column(0).as_primitive::<Float64Type>();
        assert_eq!(result.value(0), 3.0);

        let batches = ctx
            .sql(
                "WITH states AS (\
                 SELECT avg_state(CAST(value AS DOUBLE)) AS state FROM (VALUES (1.0), (3.0)) AS left_values(value) \
                 UNION ALL \
                 SELECT avg_state(CAST(value AS DOUBLE)) AS state FROM (VALUES (6.0)) AS right_values(value)\
                 ) SELECT avg_calc(avg_merge(state)) FROM states",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let result = batches[0].column(0).as_primitive::<Float64Type>();
        assert_eq!(result.value(0), 10.0 / 3.0);
    }
}
