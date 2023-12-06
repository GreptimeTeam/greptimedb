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

use common_query::error::{
    ExecuteSnafu, InvalidFuncArgsSnafu, Result, UnsupportedInputDataTypeSnafu,
};
use common_query::prelude::Signature;
use common_time::{Date, DateTime};
use datatypes::data_type::DataType;
use datatypes::prelude::ConcreteDataType;
use datatypes::value::ValueRef;
use datatypes::vectors::VectorRef;
use snafu::ensure;

use crate::helper;
use crate::scalars::function::{Function, FunctionContext};

/// A function adds an interval value to Timestamp, Date or DateTime, and return the result.
#[derive(Clone, Debug, Default)]
pub struct DateAddFunction;

const NAME: &str = "date_add";

impl Function for DateAddFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(input_types[0].clone())
    }

    fn signature(&self) -> Signature {
        helper::one_of_sigs2(
            vec![
                ConcreteDataType::timestamp_second_datatype(),
                ConcreteDataType::timestamp_millisecond_datatype(),
                ConcreteDataType::timestamp_microsecond_datatype(),
                ConcreteDataType::timestamp_nanosecond_datatype(),
                ConcreteDataType::date_datatype(),
                ConcreteDataType::datetime_datatype(),
            ],
            vec![
                ConcreteDataType::interval_month_day_nano_datatype(),
                ConcreteDataType::interval_year_month_datatype(),
                ConcreteDataType::interval_day_time_datatype(),
            ],
        )
    }

    fn eval(&self, _func_ctx: FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure!(
            columns.len() == 2,
            InvalidFuncArgsSnafu {
                err_msg: format!(
                    "The length of the args is not correct, expect 2, have: {}",
                    columns.len()
                ),
            }
        );

        let left = &columns[0];
        let right = &columns[1];

        let size = left.len();
        let left_datatype = columns[0].data_type();
        match left_datatype {
            ConcreteDataType::Timestamp(_) => {
                let mut result = left_datatype.create_mutable_vector(size);
                for i in 0..size {
                    let ts = left.get(i).as_timestamp();
                    let interval = right.get(i).as_interval();
                    let ts = ts
                        .map(|ts| {
                            if let Some(interval) = interval {
                                let duration = interval
                                    .to_duration()
                                    .map_err(|e| {
                                        ExecuteSnafu {
                                            msg: format!("{e}"),
                                        }
                                        .build()
                                    })?
                                    .into();
                                ts.add_duration(duration).map_err(|e| {
                                    ExecuteSnafu {
                                        msg: format!("{e}"),
                                    }
                                    .build()
                                })
                            } else {
                                Ok(ts)
                            }
                        })
                        .transpose()?;
                    result.push_value_ref(ValueRef::from(ts));
                }

                Ok(result.to_vector())
            }
            ConcreteDataType::Date(_) => {
                let mut result = left_datatype.create_mutable_vector(size);
                for i in 0..size {
                    let date = left.get(i).as_date();
                    let interval = right.get(i).as_interval();
                    let date = date
                        .map(|date| {
                            if let Some(interval) = interval {
                                let days = interval.to_nanosecond()
                                    / common_time::interval::NANOS_PER_DAY as i128;
                                let days: i32 = days.try_into().map_err(|e| {
                                    ExecuteSnafu {
                                        msg: format!("{e}"),
                                    }
                                    .build()
                                })?;

                                Ok(Date::new(date.val() + days))
                            } else {
                                Ok(date)
                            }
                        })
                        .transpose()?;
                    result.push_value_ref(ValueRef::from(date));
                }

                Ok(result.to_vector())
            }
            ConcreteDataType::DateTime(_) => {
                let mut result = left_datatype.create_mutable_vector(size);
                for i in 0..size {
                    let datetime = left.get(i).as_datetime();
                    let interval = right.get(i).as_interval();
                    let datetime = datetime.map(|datetime| {
                        if let Some(interval) = interval {
                            let millis = interval.to_nanosecond() as i64
                                / common_time::interval::NANOS_PER_MILLI;
                            DateTime::new(datetime.val() + millis)
                        } else {
                            datetime
                        }
                    });
                    result.push_value_ref(ValueRef::from(datetime));
                }

                Ok(result.to_vector())
            }
            _ => UnsupportedInputDataTypeSnafu {
                function: NAME,
                datatypes: columns.iter().map(|c| c.data_type()).collect::<Vec<_>>(),
            }
            .fail(),
        }
    }
}

impl fmt::Display for DateAddFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DATE_ADD")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_query::prelude::{TypeSignature, Volatility};
    use datatypes::prelude::ConcreteDataType;
    use datatypes::value::Value;
    use datatypes::vectors::{
        DateTimeVector, DateVector, IntervalDayTimeVector, IntervalYearMonthVector,
        TimestampSecondVector,
    };

    use super::{DateAddFunction, *};
    use crate::scalars::Function;

    #[test]
    fn test_date_add_misc() {
        let f = DateAddFunction;
        assert_eq!("date_add", f.name());
        assert_eq!(
            ConcreteDataType::timestamp_microsecond_datatype(),
            f.return_type(&[ConcreteDataType::timestamp_microsecond_datatype()])
                .unwrap()
        );
        assert_eq!(
            ConcreteDataType::timestamp_second_datatype(),
            f.return_type(&[ConcreteDataType::timestamp_second_datatype()])
                .unwrap()
        );
        assert_eq!(
            ConcreteDataType::date_datatype(),
            f.return_type(&[ConcreteDataType::date_datatype()]).unwrap()
        );
        assert_eq!(
            ConcreteDataType::datetime_datatype(),
            f.return_type(&[ConcreteDataType::datetime_datatype()])
                .unwrap()
        );
        assert!(matches!(f.signature(),
                         Signature {
                             type_signature: TypeSignature::OneOf(sigs),
                             volatility: Volatility::Immutable
                         } if  sigs.len() == 18));
    }

    #[test]
    fn test_timestamp_date_add() {
        let f = DateAddFunction;

        let times = vec![Some(123), None, Some(42), None];
        // Intervals in milliseconds
        let intervals = vec![1000, 2000, 3000, 1000];
        let results = [Some(124), None, Some(45), None];

        let time_vector = TimestampSecondVector::from(times.clone());
        let interval_vector = IntervalDayTimeVector::from_vec(intervals);
        let args: Vec<VectorRef> = vec![Arc::new(time_vector), Arc::new(interval_vector)];
        let vector = f.eval(FunctionContext::default(), &args).unwrap();

        assert_eq!(4, vector.len());
        for (i, _t) in times.iter().enumerate() {
            let v = vector.get(i);
            let result = results.get(i).unwrap();

            if result.is_none() {
                assert_eq!(Value::Null, v);
                continue;
            }
            match v {
                Value::Timestamp(ts) => {
                    assert_eq!(ts.value(), result.unwrap());
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn test_date_date_add() {
        let f = DateAddFunction;
        let days_per_month = 30;

        let dates = vec![Some(123), None, Some(42), None];
        // Intervals in months
        let intervals = vec![1, 2, 3, 1];
        let results = [
            Some(days_per_month + 123),
            None,
            Some(3 * days_per_month + 42),
            None,
        ];

        let date_vector = DateVector::from(dates.clone());
        let interval_vector = IntervalYearMonthVector::from_vec(intervals);
        let args: Vec<VectorRef> = vec![Arc::new(date_vector), Arc::new(interval_vector)];
        let vector = f.eval(FunctionContext::default(), &args).unwrap();

        assert_eq!(4, vector.len());
        for (i, _t) in dates.iter().enumerate() {
            let v = vector.get(i);
            let result = results.get(i).unwrap();

            if result.is_none() {
                assert_eq!(Value::Null, v);
                continue;
            }
            match v {
                Value::Date(date) => {
                    assert_eq!(date.val(), result.unwrap());
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn test_datetime_date_add() {
        let f = DateAddFunction;
        let millis_per_month = 3600 * 24 * 30 * 1000;

        let dates = vec![Some(123), None, Some(42), None];
        // Intervals in months
        let intervals = vec![1, 2, 3, 1];
        let results = [
            Some(millis_per_month + 123),
            None,
            Some(3 * millis_per_month + 42),
            None,
        ];

        let date_vector = DateTimeVector::from(dates.clone());
        let interval_vector = IntervalYearMonthVector::from_vec(intervals);
        let args: Vec<VectorRef> = vec![Arc::new(date_vector), Arc::new(interval_vector)];
        let vector = f.eval(FunctionContext::default(), &args).unwrap();

        assert_eq!(4, vector.len());
        for (i, _t) in dates.iter().enumerate() {
            let v = vector.get(i);
            let result = results.get(i).unwrap();

            if result.is_none() {
                assert_eq!(Value::Null, v);
                continue;
            }
            match v {
                Value::DateTime(date) => {
                    assert_eq!(date.val(), result.unwrap());
                }
                _ => unreachable!(),
            }
        }
    }
}
