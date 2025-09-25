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

use std::fmt;

use common_query::error::{self, Result};
use datafusion::arrow::compute::kernels::numeric;
use datafusion_common::arrow::compute::kernels::cast;
use datafusion_common::arrow::datatypes::DataType;
use datafusion_expr::type_coercion::aggregates::NUMERICS;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, Volatility};
use snafu::ResultExt;

use crate::function::{Function, extract_args};

/// generates rates from a sequence of adjacent data points.
#[derive(Clone, Debug, Default)]
pub struct RateFunction;

impl fmt::Display for RateFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RATE")
    }
}

impl Function for RateFunction {
    fn name(&self) -> &str {
        "rate"
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn signature(&self) -> Signature {
        Signature::uniform(2, NUMERICS.to_vec(), Volatility::Immutable)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [val, ts] = extract_args(self.name(), &args)?;
        let val_0 = val.slice(0, val.len() - 1);
        let val_1 = val.slice(1, val.len() - 1);
        let dv = numeric::sub(&val_1, &val_0).context(error::ArrowComputeSnafu)?;
        let ts_0 = ts.slice(0, ts.len() - 1);
        let ts_1 = ts.slice(1, ts.len() - 1);
        let dt = numeric::sub(&ts_1, &ts_0).context(error::ArrowComputeSnafu)?;

        let dv = cast::cast(&dv, &DataType::Float64).context(error::TypeCastSnafu {
            typ: DataType::Float64,
        })?;
        let dt = cast::cast(&dt, &DataType::Float64).context(error::TypeCastSnafu {
            typ: DataType::Float64,
        })?;
        let rate = numeric::div(&dv, &dt).context(error::ArrowComputeSnafu)?;

        Ok(ColumnarValue::Array(rate))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::Field;
    use datafusion_common::arrow::array::{AsArray, Float32Array, Float64Array, Int64Array};
    use datafusion_common::arrow::datatypes::Float64Type;
    use datafusion_expr::TypeSignature;

    use super::*;
    #[test]
    fn test_rate_function() {
        let rate = RateFunction;
        assert_eq!("rate", rate.name());
        assert_eq!(DataType::Float64, rate.return_type(&[]).unwrap());
        assert!(matches!(rate.signature(),
                         Signature {
                             type_signature: TypeSignature::Uniform(2, valid_types),
                             volatility: Volatility::Immutable
                         } if  valid_types == NUMERICS
        ));
        let values = vec![1.0, 3.0, 6.0];
        let ts = vec![0, 1, 2];

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Float32Array::from(values))),
                ColumnarValue::Array(Arc::new(Int64Array::from(ts))),
            ],
            arg_fields: vec![],
            number_rows: 3,
            return_field: Arc::new(Field::new("x", DataType::Float64, false)),
            config_options: Arc::new(Default::default()),
        };
        let result = rate
            .invoke_with_args(args)
            .and_then(|x| x.to_array(2))
            .unwrap();
        let vector = result.as_primitive::<Float64Type>();
        let expect = &Float64Array::from(vec![2.0, 3.0]);
        assert_eq!(expect, vector);
    }
}
