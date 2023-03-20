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
use std::str::FromStr;
use std::sync::Arc;

use common_query::error::{Result, UnsupportedInputDataTypeSnafu};
use common_query::prelude::{Signature, Volatility};
use common_time::timestamp::TimeUnit;
use common_time::Timestamp;
use datatypes::prelude::ConcreteDataType;
use datatypes::types::StringType;
use datatypes::vectors::{Int64Vector, StringVector, Vector, VectorRef};

use crate::scalars::function::{Function, FunctionContext};

#[derive(Clone, Debug, Default)]
pub struct ToUnixtimeFunction;

const NAME: &str = "to_unixtime";

fn convert_to_seconds(arg: &str) -> Option<i64> {
    match Timestamp::from_str(arg) {
        Ok(ts) => {
            let sec_mul = (TimeUnit::Second.factor() / ts.unit().factor()) as i64;
            Some(ts.value().div_euclid(sec_mul))
        }
        Err(_err) => {
            // error!("Failed to parse {} to Timestamp value", arg);
            None
        }
    }
}

impl Function for ToUnixtimeFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::int64_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::exact(
            vec![ConcreteDataType::String(StringType)],
            Volatility::Immutable,
        )
    }

    fn eval(&self, _func_ctx: FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        match columns[0].data_type() {
            ConcreteDataType::String(_) => {
                let array = columns[0].to_arrow_array();
                let vector = StringVector::try_from_arrow_array(&array).unwrap();
                Ok(Arc::new(Int64Vector::from(
                    (0..vector.len())
                        .map(|i| convert_to_seconds(&vector.get(i).to_string()))
                        .collect::<Vec<_>>(),
                )))
            }
            _ => UnsupportedInputDataTypeSnafu {
                function: NAME,
                datatypes: columns.iter().map(|c| c.data_type()).collect::<Vec<_>>(),
            }
            .fail(),
        }
    }
}

impl fmt::Display for ToUnixtimeFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TO_UNIXTIME")
    }
}

#[cfg(test)]
mod tests {
    use common_query::prelude::TypeSignature;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::types::StringType;
    use datatypes::value::Value;
    use datatypes::vectors::StringVector;

    use super::{ToUnixtimeFunction, *};
    use crate::scalars::Function;

    #[test]
    fn test_to_unixtime() {
        let f = ToUnixtimeFunction::default();
        assert_eq!("to_unixtime", f.name());
        assert_eq!(
            ConcreteDataType::int64_datatype(),
            f.return_type(&[]).unwrap()
        );

        assert!(matches!(f.signature(),
                         Signature {
                            type_signature: TypeSignature::Exact(valid_types),
                            volatility: Volatility::Immutable
                         } if  valid_types == vec![ConcreteDataType::String(StringType)]
        ));

        let times = vec![
            Some("2023-03-01T06:35:02Z"),
            None,
            Some("2022-06-30T23:59:60Z"),
            Some("invalid_time_stamp"),
        ];
        let results = vec![Some(1677652502), None, Some(1656633600), None];
        let args: Vec<VectorRef> = vec![Arc::new(StringVector::from(times.clone()))];
        let vector = f.eval(FunctionContext::default(), &args).unwrap();
        assert_eq!(4, vector.len());
        for (i, _t) in times.iter().enumerate() {
            let v = vector.get(i);
            if i == 1 || i == 3 {
                assert_eq!(Value::Null, v);
                continue;
            }
            match v {
                Value::Int64(ts) => {
                    assert_eq!(ts, (*results.get(i).unwrap()).unwrap());
                }
                _ => unreachable!(),
            }
        }
    }
}
