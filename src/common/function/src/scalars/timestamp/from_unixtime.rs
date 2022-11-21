// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! from_unixtime function.
/// TODO(dennis) It can be removed after we upgrade datafusion.
use std::fmt;
use std::sync::Arc;

use common_query::error::{IntoVectorSnafu, UnsupportedInputDataTypeSnafu};
use common_query::prelude::{Signature, Volatility};
use datatypes::arrow::compute::arithmetics;
use datatypes::arrow::datatypes::DataType as ArrowDatatype;
use datatypes::arrow::scalar::PrimitiveScalar;
use datatypes::prelude::ConcreteDataType;
use datatypes::vectors::{TimestampVector, VectorRef};
use snafu::ResultExt;

use crate::error::Result;
use crate::scalars::function::{Function, FunctionContext};

#[derive(Clone, Debug, Default)]
pub struct FromUnixtimeFunction;

const NAME: &str = "from_unixtime";

impl Function for FromUnixtimeFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::timestamp_millis_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::uniform(
            1,
            vec![ConcreteDataType::int64_datatype()],
            Volatility::Immutable,
        )
    }

    fn eval(&self, _func_ctx: FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        match columns[0].data_type() {
            ConcreteDataType::Int64(_) => {
                let array = columns[0].to_arrow_array();
                // Our timestamp vector's time unit is millisecond
                let array = arithmetics::mul_scalar(
                    &*array,
                    &PrimitiveScalar::new(ArrowDatatype::Int64, Some(1000i64)),
                );

                Ok(Arc::new(
                    TimestampVector::try_from_arrow_array(array).context(IntoVectorSnafu {
                        data_type: ArrowDatatype::Int64,
                    })?,
                ))
            }
            _ => UnsupportedInputDataTypeSnafu {
                function: NAME,
                datatypes: columns.iter().map(|c| c.data_type()).collect::<Vec<_>>(),
            }
            .fail()
            .map_err(|e| e.into()),
        }
    }
}

impl fmt::Display for FromUnixtimeFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FROM_UNIXTIME")
    }
}

#[cfg(test)]
mod tests {
    use common_query::prelude::TypeSignature;
    use datatypes::value::Value;
    use datatypes::vectors::Int64Vector;

    use super::*;

    #[test]
    fn test_from_unixtime() {
        let f = FromUnixtimeFunction::default();
        assert_eq!("from_unixtime", f.name());
        assert_eq!(
            ConcreteDataType::timestamp_millis_datatype(),
            f.return_type(&[]).unwrap()
        );

        assert!(matches!(f.signature(),
                         Signature {
                             type_signature: TypeSignature::Uniform(1, valid_types),
                             volatility: Volatility::Immutable
                         } if  valid_types == vec![ConcreteDataType::int64_datatype()]
        ));

        let times = vec![Some(1494410783), None, Some(1494410983)];
        let args: Vec<VectorRef> = vec![Arc::new(Int64Vector::from(times.clone()))];

        let vector = f.eval(FunctionContext::default(), &args).unwrap();
        assert_eq!(3, vector.len());
        for (i, t) in times.iter().enumerate() {
            let v = vector.get(i);
            if i == 1 {
                assert_eq!(Value::Null, v);
                continue;
            }
            match v {
                Value::Timestamp(ts) => {
                    assert_eq!(ts.value(), t.unwrap() * 1000);
                }
                _ => unreachable!(),
            }
        }
    }
}
