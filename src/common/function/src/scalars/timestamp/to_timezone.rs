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
use std::sync::Arc;

use common_query::error::{InvalidFuncArgsSnafu, Result, UnsupportedInputDataTypeSnafu};
use common_query::prelude::Signature;
use common_time::{Timestamp, Timezone};
use datatypes::data_type::ConcreteDataType;
use datatypes::prelude::VectorRef;
use datatypes::types::TimestampType;
use datatypes::value::Value;
use datatypes::vectors::{
    StringVector, TimestampMicrosecondVector, TimestampMillisecondVector,
    TimestampNanosecondVector, TimestampSecondVector, Vector,
};
use snafu::{ensure, OptionExt};

use crate::function::{Function, FunctionContext};
use crate::helper;

#[derive(Clone, Debug, Default)]
pub struct ToTimezoneFunction;

const NAME: &str = "to_timezone";

fn convert_to_timezone(arg: &str) -> Option<Timezone> {
    Timezone::from_tz_string(arg).ok()
}

fn convert_to_timestamp(arg: &Value) -> Option<Timestamp> {
    match arg {
        Value::Timestamp(ts) => Some(*ts),
        _ => None,
    }
}

impl fmt::Display for ToTimezoneFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TO_TIMEZONE")
    }
}

impl Function for ToTimezoneFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        // type checked by signature - MUST BE timestamp
        Ok(input_types[0].clone())
    }

    fn signature(&self) -> Signature {
        helper::one_of_sigs2(
            vec![
                ConcreteDataType::timestamp_second_datatype(),
                ConcreteDataType::timestamp_millisecond_datatype(),
                ConcreteDataType::timestamp_microsecond_datatype(),
                ConcreteDataType::timestamp_nanosecond_datatype(),
            ],
            vec![ConcreteDataType::string_datatype()],
        )
    }

    fn eval(&self, _ctx: FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure!(
            columns.len() == 2,
            InvalidFuncArgsSnafu {
                err_msg: format!(
                    "The length of the args is not correct, expect exactly 2, have: {}",
                    columns.len()
                ),
            }
        );

        // TODO: maybe support epoch timestamp? https://github.com/GreptimeTeam/greptimedb/issues/3477
        let ts = columns[0].data_type().as_timestamp().with_context(|| {
            UnsupportedInputDataTypeSnafu {
                function: NAME,
                datatypes: columns.iter().map(|c| c.data_type()).collect::<Vec<_>>(),
            }
        })?;
        let array = columns[0].to_arrow_array();
        let times = match ts {
            TimestampType::Second(_) => {
                let vector = TimestampSecondVector::try_from_arrow_array(array).unwrap();
                (0..vector.len())
                    .map(|i| convert_to_timestamp(&vector.get(i)))
                    .collect::<Vec<_>>()
            }
            TimestampType::Millisecond(_) => {
                let vector = TimestampMillisecondVector::try_from_arrow_array(array).unwrap();
                (0..vector.len())
                    .map(|i| convert_to_timestamp(&vector.get(i)))
                    .collect::<Vec<_>>()
            }
            TimestampType::Microsecond(_) => {
                let vector = TimestampMicrosecondVector::try_from_arrow_array(array).unwrap();
                (0..vector.len())
                    .map(|i| convert_to_timestamp(&vector.get(i)))
                    .collect::<Vec<_>>()
            }
            TimestampType::Nanosecond(_) => {
                let vector = TimestampNanosecondVector::try_from_arrow_array(array).unwrap();
                (0..vector.len())
                    .map(|i| convert_to_timestamp(&vector.get(i)))
                    .collect::<Vec<_>>()
            }
        };

        let tzs = {
            let array = columns[1].to_arrow_array();
            let vector = StringVector::try_from_arrow_array(&array)
                .ok()
                .with_context(|| UnsupportedInputDataTypeSnafu {
                    function: NAME,
                    datatypes: columns.iter().map(|c| c.data_type()).collect::<Vec<_>>(),
                })?;
            (0..vector.len())
                .map(|i| convert_to_timezone(&vector.get(i).to_string()))
                .collect::<Vec<_>>()
        };

        let result = times
            .iter()
            .zip(tzs.iter())
            .map(|(time, tz)| match (time, tz) {
                (Some(time), _) => Some(time.to_timezone_aware_string(tz.as_ref())),
                _ => None,
            })
            .collect::<Vec<Option<String>>>();
        Ok(Arc::new(StringVector::from(result)))
    }
}

#[cfg(test)]
mod tests {

    use datatypes::scalars::ScalarVector;
    use datatypes::timestamp::{
        TimestampMicrosecond, TimestampMillisecond, TimestampNanosecond, TimestampSecond,
    };
    use datatypes::vectors::StringVector;

    use super::*;

    #[test]
    fn test_timestamp_to_timezone() {
        let f = ToTimezoneFunction;
        assert_eq!("to_timezone", f.name());

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
