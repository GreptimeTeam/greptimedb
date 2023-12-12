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
use common_query::prelude::{Signature, Volatility};
use datafusion::arrow::compute::kernels::numeric;
use datatypes::arrow::compute::kernels::cast;
use datatypes::arrow::datatypes::DataType;
use datatypes::prelude::*;
use datatypes::vectors::{Helper, VectorRef};
use snafu::ResultExt;

use crate::function::{Function, FunctionContext};

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
        "prom_rate"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::float64_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::uniform(2, ConcreteDataType::numerics(), Volatility::Immutable)
    }

    fn eval(&self, _func_ctx: FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        let val = &columns[0].to_arrow_array();
        let val_0 = val.slice(0, val.len() - 1);
        let val_1 = val.slice(1, val.len() - 1);
        let dv = numeric::sub(&val_1, &val_0).context(error::ArrowComputeSnafu)?;
        let ts = &columns[1].to_arrow_array();
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
        let v = Helper::try_into_vector(&rate).context(error::FromArrowArraySnafu)?;

        Ok(v)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_query::prelude::TypeSignature;
    use datatypes::vectors::{Float32Vector, Float64Vector, Int64Vector};

    use super::*;
    #[test]
    fn test_rate_function() {
        let rate = RateFunction;
        assert_eq!("prom_rate", rate.name());
        assert_eq!(
            ConcreteDataType::float64_datatype(),
            rate.return_type(&[]).unwrap()
        );
        assert!(matches!(rate.signature(),
                         Signature {
                             type_signature: TypeSignature::Uniform(2, valid_types),
                             volatility: Volatility::Immutable
                         } if  valid_types == ConcreteDataType::numerics()
        ));
        let values = vec![1.0, 3.0, 6.0];
        let ts = vec![0, 1, 2];

        let args: Vec<VectorRef> = vec![
            Arc::new(Float32Vector::from_vec(values)),
            Arc::new(Int64Vector::from_vec(ts)),
        ];
        let vector = rate.eval(FunctionContext::default(), &args).unwrap();
        let expect: VectorRef = Arc::new(Float64Vector::from_vec(vec![2.0, 3.0]));
        assert_eq!(expect, vector);
    }
}
