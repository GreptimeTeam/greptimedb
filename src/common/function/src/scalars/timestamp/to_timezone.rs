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
use common_time::timezone::TimeZone;
use common_time::Timestamp;
use datatypes::prelude::{ConcreteDataType, Vector};
use datatypes::types::TimestampType;
use datatypes::value::Value;
use datatypes::vectors::{
    Int64Vector, StringVector, TimestampMicrosecondVector, TimestampMillisecondVector,
    TimestampNanosecondVector, TimestampSecondVector, VectorRef,
};
use snafu::ensure;

use crate::scalars::function::{Function, FunctionContext};

#[derive(Clone, Debug, Default)]
pub struct ToTimeZoneFunction;

const NAME: &str = "to_timezone";

fn convert_to_timezone(arg: &str) -> Option<TimeZone> {
    match TimeZone::from_tz_string(arg) {
        Ok(ts) => ts,
        Err(_err) => None,
    }
}

fn convert_to_timestamp(arg: &Value) -> Option<Timestamp> {
    match arg {
        Value::Int64(ts) => Some(Timestamp::from(*ts)),
        Value::String(ts) => match Timestamp::from_str(ts.as_utf8()) {
            Ok(ts) => Some(ts),
            Err(_) => None,
        },
        Value::Timestamp(ts) => Some(*ts),
        _ => None,
    }
}

fn process_to_timezone(
    times: Vec<Option<Timestamp>>,
    tzs: Vec<Option<TimeZone>>,
) -> Vec<Option<String>> {
    times
        .iter()
        .zip(tzs.iter())
        .map(|(time, tz)| match (time, tz) {
            (Some(time), _) => Some(time.to_timezone_aware_string(tz.clone())),
            _ => None,
        })
        .collect::<Vec<Option<String>>>()
}

impl Function for ToTimeZoneFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::string_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::uniform(
            2,
            vec![
                ConcreteDataType::timestamp_second_datatype(),
                ConcreteDataType::timestamp_microsecond_datatype(),
                ConcreteDataType::timestamp_millisecond_datatype(),
                ConcreteDataType::timestamp_nanosecond_datatype(),
                ConcreteDataType::int64_datatype(),
                ConcreteDataType::string_datatype(),
            ],
            Volatility::Immutable,
        )
    }

    fn eval(&self, _func_ctx: FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure!(
            columns.len() == 2,
            InvalidFuncArgsSnafu {
                err_msg: format!(
                    "The length of the args is not correct, expect exactly 2, have: {}",
                    columns.len()
                ),
            }
        );

        let times = match columns[0].data_type() {
            ConcreteDataType::String(_) => {
                let array = columns[0].to_arrow_array();
                let vector = StringVector::try_from_arrow_array(&array).unwrap();
                Ok((0..vector.len())
                    .map(|i| convert_to_timestamp(&vector.get(i)))
                    .collect::<Vec<_>>())
            }
            ConcreteDataType::Int64(_) | ConcreteDataType::Int32(_) => {
                let array = columns[0].to_arrow_array();
                let vector = Int64Vector::try_from_arrow_array(&array).unwrap();
                Ok((0..vector.len())
                    .map(|i| convert_to_timestamp(&vector.get(i)))
                    .collect::<Vec<_>>())
            }
            ConcreteDataType::Timestamp(ts) => {
                let array = columns[0].to_arrow_array();
                match ts {
                    TimestampType::Second(_) => {
                        let vector = paste::expr!(TimestampSecondVector::try_from_arrow_array(
                            array
                        )
                        .unwrap());
                        Ok((0..vector.len())
                            .map(|i| convert_to_timestamp(&vector.get(i)))
                            .collect::<Vec<_>>())
                    }
                    TimestampType::Millisecond(_) => {
                        let vector = paste::expr!(
                            TimestampMillisecondVector::try_from_arrow_array(array).unwrap()
                        );
                        Ok((0..vector.len())
                            .map(|i| convert_to_timestamp(&vector.get(i)))
                            .collect::<Vec<_>>())
                    }
                    TimestampType::Microsecond(_) => {
                        let vector = paste::expr!(
                            TimestampMicrosecondVector::try_from_arrow_array(array).unwrap()
                        );
                        Ok((0..vector.len())
                            .map(|i| convert_to_timestamp(&vector.get(i)))
                            .collect::<Vec<_>>())
                    }
                    TimestampType::Nanosecond(_) => {
                        let vector = paste::expr!(TimestampNanosecondVector::try_from_arrow_array(
                            array
                        )
                        .unwrap());
                        Ok((0..vector.len())
                            .map(|i| convert_to_timestamp(&vector.get(i)))
                            .collect::<Vec<_>>())
                    }
                }
            }
            _ => UnsupportedInputDataTypeSnafu {
                function: NAME,
                datatypes: columns.iter().map(|c| c.data_type()).collect::<Vec<_>>(),
            }
            .fail(),
        };

        let tzs = match columns[1].data_type() {
            ConcreteDataType::String(_) => {
                let array = columns[1].to_arrow_array();
                let vector = StringVector::try_from_arrow_array(&array).unwrap();
                Ok((0..vector.len())
                    .map(|i| convert_to_timezone(&vector.get(i).to_string()))
                    .collect::<Vec<_>>())
            }
            _ => UnsupportedInputDataTypeSnafu {
                function: NAME,
                datatypes: columns.iter().map(|c| c.data_type()).collect::<Vec<_>>(),
            }
            .fail(),
        };

        match (times, tzs) {
            (Ok(times), Ok(tzs)) => Ok(Arc::new(StringVector::from(process_to_timezone(
                times, tzs,
            )))),
            _ => UnsupportedInputDataTypeSnafu {
                function: NAME,
                datatypes: columns.iter().map(|c| c.data_type()).collect::<Vec<_>>(),
            }
            .fail(),
        }
    }
}

impl fmt::Display for ToTimeZoneFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TO_TIMEZONE")
    }
}

#[cfg(test)]
mod tests {

    use common_query::prelude::TypeSignature;
    use datatypes::scalars::ScalarVector;
    use datatypes::timestamp::{
        TimestampMicrosecond, TimestampMillisecond, TimestampNanosecond, TimestampSecond,
    };
    use datatypes::vectors::{Int64Vector, StringVector};

    use super::{ToTimeZoneFunction, *};
    use crate::scalars::Function;

    #[test]
    fn test_string_to_timezone() {
        let f = ToTimeZoneFunction;
        assert_eq!("to_timezone", f.name());

        assert_eq!(
            ConcreteDataType::string_datatype(),
            f.return_type(&[]).unwrap()
        );

        assert!(matches!(f.signature(),
        Signature {
                type_signature: TypeSignature::Uniform(2, valid_types),
                volatility: Volatility::Immutable
            } if  valid_types == vec![
                ConcreteDataType::timestamp_second_datatype(),
                ConcreteDataType::timestamp_microsecond_datatype(),
                ConcreteDataType::timestamp_millisecond_datatype(),
                ConcreteDataType::timestamp_nanosecond_datatype(),
                ConcreteDataType::int64_datatype(),
                ConcreteDataType::string_datatype(),
            ]
        ));

        let times = vec![
            Some("2022-09-20T14:16:43.012345Z"),
            None,
            Some("2022-09-20T14:16:43.012345+08:00"),
            None,
        ];
        let tzs = vec![Some("America/New_York"), None, Some("Europe/Moscow"), None];
        let results = vec![
            Some("2022-09-20 10:16:43.012345"),
            None,
            Some("2022-09-20 09:16:43.012345"),
            None,
        ];
        let args: Vec<VectorRef> = vec![
            Arc::new(StringVector::from(times.clone())),
            Arc::new(StringVector::from(tzs.clone())),
        ];
        let vector = f.eval(FunctionContext::default(), &args).unwrap();
        let expect_times: VectorRef = Arc::new(StringVector::from(results));
        assert_eq!(expect_times, vector);
    }

    #[test]
    fn test_i64_to_timezone() {
        let f = ToTimeZoneFunction;
        assert_eq!("to_timezone", f.name());

        assert_eq!(
            ConcreteDataType::string_datatype(),
            f.return_type(&[]).unwrap()
        );

        assert!(matches!(f.signature(),
        Signature {
                type_signature: TypeSignature::Uniform(2, valid_types),
                volatility: Volatility::Immutable
            } if  valid_types == vec![
                ConcreteDataType::timestamp_second_datatype(),
                ConcreteDataType::timestamp_microsecond_datatype(),
                ConcreteDataType::timestamp_millisecond_datatype(),
                ConcreteDataType::timestamp_nanosecond_datatype(),
                ConcreteDataType::int64_datatype(),
                ConcreteDataType::string_datatype(),
            ]
        ));

        let times = vec![Some(0), None, Some(0), None];
        let tzs = vec![Some("America/New_York"), None, Some("Europe/Moscow"), None];
        let results = vec![
            Some("1969-12-31 19:00:00"),
            None,
            Some("1970-01-01 03:00:00"),
            None,
        ];
        let args: Vec<VectorRef> = vec![
            Arc::new(Int64Vector::from(times.clone())),
            Arc::new(StringVector::from(tzs.clone())),
        ];
        let vector = f.eval(FunctionContext::default(), &args).unwrap();
        assert_eq!(4, vector.len());
        let expect_times: VectorRef = Arc::new(StringVector::from(results));
        assert_eq!(expect_times, vector);
    }

    #[test]
    fn test_timestamp_to_timezone() {
        let f = ToTimeZoneFunction  ;
        assert_eq!("to_timezone", f.name());

        assert_eq!(
            ConcreteDataType::string_datatype(),
            f.return_type(&[]).unwrap()
        );

        assert!(matches!(f.signature(),
        Signature {
                type_signature: TypeSignature::Uniform(2, valid_types),
                volatility: Volatility::Immutable
            } if  valid_types == vec![
                ConcreteDataType::timestamp_second_datatype(),
                ConcreteDataType::timestamp_microsecond_datatype(),
                ConcreteDataType::timestamp_millisecond_datatype(),
                ConcreteDataType::timestamp_nanosecond_datatype(),
                ConcreteDataType::int64_datatype(),
                ConcreteDataType::string_datatype(),
            ]
        ));

        let results = vec![
            Some("1969-12-31 19:00:01"),
            None,
            Some("1970-01-01 03:00:01"),
            None,
        ];
        let times: Vec<Option<TimestampSecond>> = vec![
            Some(TimestampSecond::new(1)),
            None,
            Some(TimestampSecond::new(1)),
            None,
        ];
        let ts_vector: TimestampSecondVector =
            TimestampSecondVector::from_owned_iterator(times.into_iter());
        let tzs = vec![Some("America/New_York"), None, Some("Europe/Moscow"), None];
        let args: Vec<VectorRef> = vec![
            Arc::new(ts_vector),
            Arc::new(StringVector::from(tzs.clone())),
        ];
        let vector = f.eval(FunctionContext::default(), &args).unwrap();
        assert_eq!(4, vector.len());
        let expect_times: VectorRef = Arc::new(StringVector::from(results));
        assert_eq!(expect_times, vector);

        let results = vec![
            Some("1969-12-31 19:00:00.001"),
            None,
            Some("1970-01-01 03:00:00.001"),
            None,
        ];
        let times: Vec<Option<TimestampMillisecond>> = vec![
            Some(TimestampMillisecond::new(1)),
            None,
            Some(TimestampMillisecond::new(1)),
            None,
        ];
        let ts_vector: TimestampMillisecondVector =
            TimestampMillisecondVector::from_owned_iterator(times.into_iter());
        let args: Vec<VectorRef> = vec![
            Arc::new(ts_vector),
            Arc::new(StringVector::from(tzs.clone())),
        ];
        let vector = f.eval(FunctionContext::default(), &args).unwrap();
        assert_eq!(4, vector.len());
        let expect_times: VectorRef = Arc::new(StringVector::from(results));
        assert_eq!(expect_times, vector);

        let results = vec![
            Some("1969-12-31 19:00:00.000001"),
            None,
            Some("1970-01-01 03:00:00.000001"),
            None,
        ];
        let times: Vec<Option<TimestampMicrosecond>> = vec![
            Some(TimestampMicrosecond::new(1)),
            None,
            Some(TimestampMicrosecond::new(1)),
            None,
        ];
        let ts_vector: TimestampMicrosecondVector =
            TimestampMicrosecondVector::from_owned_iterator(times.into_iter());

        let args: Vec<VectorRef> = vec![
            Arc::new(ts_vector),
            Arc::new(StringVector::from(tzs.clone())),
        ];
        let vector = f.eval(FunctionContext::default(), &args).unwrap();
        assert_eq!(4, vector.len());
        let expect_times: VectorRef = Arc::new(StringVector::from(results));
        assert_eq!(expect_times, vector);

        let results = vec![
            Some("1969-12-31 19:00:00.000000001"),
            None,
            Some("1970-01-01 03:00:00.000000001"),
            None,
        ];
        let times: Vec<Option<TimestampNanosecond>> = vec![
            Some(TimestampNanosecond::new(1)),
            None,
            Some(TimestampNanosecond::new(1)),
            None,
        ];
        let ts_vector: TimestampNanosecondVector =
            TimestampNanosecondVector::from_owned_iterator(times.into_iter());

        let args: Vec<VectorRef> = vec![
            Arc::new(ts_vector),
            Arc::new(StringVector::from(tzs.clone())),
        ];
        let vector = f.eval(FunctionContext::default(), &args).unwrap();
        assert_eq!(4, vector.len());
        let expect_times: VectorRef = Arc::new(StringVector::from(results));
        assert_eq!(expect_times, vector);
    }
}
