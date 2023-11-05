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

use common_query::error::{InvalidFuncArgsSnafu, Result, UnsupportedInputDataTypeSnafu};
use common_query::prelude::{Signature, Volatility};
use common_time::Timestamp;
use datatypes::prelude::{ConcreteDataType, ScalarVector};
use datatypes::vectors::{Int64Vector, StringVector, VectorRef};
use snafu::ensure;

use crate::scalars::function::{Function, FunctionContext};

#[derive(Clone, Debug, Default)]
pub struct ToUnixtimeFunction;

const NAME: &str = "to_unixtime";

fn convert_to_seconds(arg: &Option<&str>) -> Option<i64> {
    match arg {
        Some(arg) => match Timestamp::from_str(arg) {
            Ok(ts) => Some(ts.split().0),
            Err(_err) => None,
        },
        None => None,
    }
}

fn convert_timestamps_to_seconds(vector: &VectorRef) -> Vec<Option<i64>> {
    (0..vector.len())
        .map(|i| vector.get(i).as_timestamp().map(|ts| ts.split().0))
        .collect::<Vec<Option<i64>>>()
}

impl Function for ToUnixtimeFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::int64_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::uniform(
            1,
            vec![
                ConcreteDataType::string_datatype(),
                ConcreteDataType::int32_datatype(),
                ConcreteDataType::int64_datatype(),
                ConcreteDataType::timestamp_second_datatype(),
                ConcreteDataType::timestamp_millisecond_datatype(),
                ConcreteDataType::timestamp_microsecond_datatype(),
                ConcreteDataType::timestamp_nanosecond_datatype(),
            ],
            Volatility::Immutable,
        )
    }

    fn eval(&self, _func_ctx: FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure!(
            columns.len() == 1,
            InvalidFuncArgsSnafu {
                err_msg: format!(
                    "The length of the args is not correct, expect exactly one, have: {}",
                    columns.len()
                ),
            }
        );

        let vector = &columns[0];

        match columns[0].data_type() {
            ConcreteDataType::String(_) => {
                let vector = vector.as_any().downcast_ref::<StringVector>().unwrap();

                Ok(Arc::new(Int64Vector::from(
                    vector
                        .iter_data()
                        .map(|v| convert_to_seconds(&v))
                        .collect::<Vec<_>>(),
                )))
            }
            ConcreteDataType::Int64(_) | ConcreteDataType::Int32(_) => {
                // Safety: cast always successfully at here
                Ok(vector.cast(&ConcreteDataType::int64_datatype()).unwrap())
            }
            ConcreteDataType::Timestamp(_) => {
                let seconds = convert_timestamps_to_seconds(vector);
                Ok(Arc::new(Int64Vector::from(seconds)))
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
    use datatypes::value::Value;
    use datatypes::vectors::{StringVector, TimestampMillisecondVector, TimestampSecondVector};

    use super::{ToUnixtimeFunction, *};
    use crate::scalars::Function;

    #[test]
    fn test_string_to_unixtime() {
        let f = ToUnixtimeFunction;
        assert_eq!("to_unixtime", f.name());
        assert_eq!(
            ConcreteDataType::int64_datatype(),
            f.return_type(&[]).unwrap()
        );

        assert!(matches!(f.signature(),
                         Signature {
                             type_signature: TypeSignature::Uniform(1, valid_types),
                             volatility: Volatility::Immutable
                         } if  valid_types == vec![
                             ConcreteDataType::string_datatype(),
                             ConcreteDataType::int32_datatype(),
                             ConcreteDataType::int64_datatype(),
                             ConcreteDataType::timestamp_second_datatype(),
                             ConcreteDataType::timestamp_millisecond_datatype(),
                             ConcreteDataType::timestamp_microsecond_datatype(),
                             ConcreteDataType::timestamp_nanosecond_datatype(),
                         ]
        ));

        let times = vec![
            Some("2023-03-01T06:35:02Z"),
            None,
            Some("2022-06-30T23:59:60Z"),
            Some("invalid_time_stamp"),
        ];
        let results = [Some(1677652502), None, Some(1656633600), None];
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

    #[test]
    fn test_int_to_unixtime() {
        let f = ToUnixtimeFunction;
        assert_eq!("to_unixtime", f.name());
        assert_eq!(
            ConcreteDataType::int64_datatype(),
            f.return_type(&[]).unwrap()
        );

        assert!(matches!(f.signature(),
                         Signature {
                             type_signature: TypeSignature::Uniform(1, valid_types),
                             volatility: Volatility::Immutable
                         } if  valid_types == vec![
                             ConcreteDataType::string_datatype(),
                             ConcreteDataType::int32_datatype(),
                             ConcreteDataType::int64_datatype(),
                             ConcreteDataType::timestamp_second_datatype(),
                             ConcreteDataType::timestamp_millisecond_datatype(),
                             ConcreteDataType::timestamp_microsecond_datatype(),
                             ConcreteDataType::timestamp_nanosecond_datatype(),
                         ]
        ));

        let times = vec![Some(3_i64), None, Some(5_i64), None];
        let results = [Some(3), None, Some(5), None];
        let args: Vec<VectorRef> = vec![Arc::new(Int64Vector::from(times.clone()))];
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

    #[test]
    fn test_timestamp_to_unixtime() {
        let f = ToUnixtimeFunction;
        assert_eq!("to_unixtime", f.name());
        assert_eq!(
            ConcreteDataType::int64_datatype(),
            f.return_type(&[]).unwrap()
        );

        assert!(matches!(f.signature(),
                         Signature {
                             type_signature: TypeSignature::Uniform(1, valid_types),
                             volatility: Volatility::Immutable
                         } if  valid_types == vec![
                             ConcreteDataType::string_datatype(),
                             ConcreteDataType::int32_datatype(),
                             ConcreteDataType::int64_datatype(),
                             ConcreteDataType::timestamp_second_datatype(),
                             ConcreteDataType::timestamp_millisecond_datatype(),
                             ConcreteDataType::timestamp_microsecond_datatype(),
                             ConcreteDataType::timestamp_nanosecond_datatype(),
                         ]
        ));

        let times = vec![Some(123), None, Some(42), None];
        let results = [Some(123), None, Some(42), None];
        let ts_vector = TimestampSecondVector::from(times.clone());
        let args: Vec<VectorRef> = vec![Arc::new(ts_vector)];
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

        let times = vec![Some(123000), None, Some(42000), None];
        let results = [Some(123), None, Some(42), None];
        let ts_vector = TimestampMillisecondVector::from(times.clone());
        let args: Vec<VectorRef> = vec![Arc::new(ts_vector)];
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
