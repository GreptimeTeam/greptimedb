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

use common_query::error::{ArrowComputeSnafu, IntoVectorSnafu, InvalidFuncArgsSnafu, Result};
use datafusion_expr::Signature;
use datatypes::arrow::compute::kernels::numeric;
use datatypes::arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use datatypes::vectors::{Helper, VectorRef};
use snafu::{ResultExt, ensure};

use crate::function::{Function, FunctionContext};
use crate::helper;

/// A function subtracts an interval value to Timestamp, Date, and return the result.
/// The implementation of datetime type is based on Date64 which is incorrect so this function
/// doesn't support the datetime type.
#[derive(Clone, Debug, Default)]
pub struct DateSubFunction;

const NAME: &str = "date_sub";

impl Function for DateSubFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, input_types: &[DataType]) -> Result<DataType> {
        Ok(input_types[0].clone())
    }

    fn signature(&self) -> Signature {
        helper::one_of_sigs2(
            vec![
                DataType::Date32,
                DataType::Timestamp(TimeUnit::Second, None),
                DataType::Timestamp(TimeUnit::Millisecond, None),
                DataType::Timestamp(TimeUnit::Microsecond, None),
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ],
            vec![
                DataType::Interval(IntervalUnit::MonthDayNano),
                DataType::Interval(IntervalUnit::YearMonth),
                DataType::Interval(IntervalUnit::DayTime),
            ],
        )
    }

    fn eval(&self, _func_ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure!(
            columns.len() == 2,
            InvalidFuncArgsSnafu {
                err_msg: format!(
                    "The length of the args is not correct, expect 2, have: {}",
                    columns.len()
                ),
            }
        );

        let left = columns[0].to_arrow_array();
        let right = columns[1].to_arrow_array();

        let result = numeric::sub(&left, &right).context(ArrowComputeSnafu)?;
        let arrow_type = result.data_type().clone();
        Helper::try_into_vector(result).context(IntoVectorSnafu {
            data_type: arrow_type,
        })
    }
}

impl fmt::Display for DateSubFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DATE_SUB")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion_expr::{TypeSignature, Volatility};
    use datatypes::arrow::datatypes::IntervalDayTime;
    use datatypes::value::Value;
    use datatypes::vectors::{
        DateVector, IntervalDayTimeVector, IntervalYearMonthVector, TimestampSecondVector,
    };

    use super::{DateSubFunction, *};

    #[test]
    fn test_date_sub_misc() {
        let f = DateSubFunction;
        assert_eq!("date_sub", f.name());
        assert_eq!(
            DataType::Timestamp(TimeUnit::Microsecond, None),
            f.return_type(&[DataType::Timestamp(TimeUnit::Microsecond, None)])
                .unwrap()
        );
        assert_eq!(
            DataType::Timestamp(TimeUnit::Second, None),
            f.return_type(&[DataType::Timestamp(TimeUnit::Second, None)])
                .unwrap()
        );
        assert_eq!(
            DataType::Date32,
            f.return_type(&[DataType::Date32]).unwrap()
        );
        assert!(
            matches!(f.signature(),
                         Signature {
                             type_signature: TypeSignature::OneOf(sigs),
                             volatility: Volatility::Immutable
                         } if  sigs.len() == 15),
            "{:?}",
            f.signature()
        );
    }

    #[test]
    fn test_timestamp_date_sub() {
        let f = DateSubFunction;

        let times = vec![Some(123), None, Some(42), None];
        // Intervals in milliseconds
        let intervals = vec![
            IntervalDayTime::new(0, 1000),
            IntervalDayTime::new(0, 2000),
            IntervalDayTime::new(0, 3000),
            IntervalDayTime::new(0, 1000),
        ];
        let results = [Some(122), None, Some(39), None];

        let time_vector = TimestampSecondVector::from(times.clone());
        let interval_vector = IntervalDayTimeVector::from_vec(intervals);
        let args: Vec<VectorRef> = vec![Arc::new(time_vector), Arc::new(interval_vector)];
        let vector = f.eval(&FunctionContext::default(), &args).unwrap();

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
    fn test_date_date_sub() {
        let f = DateSubFunction;
        let days_per_month = 30;

        let dates = vec![
            Some(123 * days_per_month),
            None,
            Some(42 * days_per_month),
            None,
        ];
        // Intervals in months
        let intervals = vec![1, 2, 3, 1];
        let results = [Some(3659), None, Some(1168), None];

        let date_vector = DateVector::from(dates.clone());
        let interval_vector = IntervalYearMonthVector::from_vec(intervals);
        let args: Vec<VectorRef> = vec![Arc::new(date_vector), Arc::new(interval_vector)];
        let vector = f.eval(&FunctionContext::default(), &args).unwrap();

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
}
