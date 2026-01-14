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

use common_query::error::ArrowComputeSnafu;
use datafusion::logical_expr::ColumnarValue;
use datafusion_expr::{ScalarFunctionArgs, Signature};
use datatypes::arrow::compute::kernels::numeric;
use datatypes::arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use snafu::ResultExt;

use crate::function::{Function, extract_args};
use crate::helper;

/// A function subtracts an interval value to Timestamp, Date, and return the result.
/// The implementation of datetime type is based on Date64 which is incorrect so this function
/// doesn't support the datetime type.
#[derive(Clone, Debug)]
pub(crate) struct DateSubFunction {
    signature: Signature,
}

impl Default for DateSubFunction {
    fn default() -> Self {
        Self {
            signature: helper::one_of_sigs2(
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
            ),
        }
    }
}

impl Function for DateSubFunction {
    fn name(&self) -> &str {
        "date_sub"
    }

    fn return_type(&self, input_types: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(input_types[0].clone())
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [left, right] = extract_args(self.name(), &args)?;

        let result = numeric::sub(&left, &right).context(ArrowComputeSnafu)?;
        Ok(ColumnarValue::Array(result))
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

    use arrow_schema::Field;
    use datafusion::arrow::array::{
        Array, AsArray, Date32Array, IntervalDayTimeArray, IntervalYearMonthArray,
        TimestampSecondArray,
    };
    use datafusion::arrow::datatypes::{Date32Type, IntervalDayTime, TimestampSecondType};
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::{TypeSignature, Volatility};

    use super::{DateSubFunction, *};

    #[test]
    fn test_date_sub_misc() {
        let f = DateSubFunction::default();
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
                             volatility: Volatility::Immutable,
                             ..
                         } if  sigs.len() == 15),
            "{:?}",
            f.signature()
        );
    }

    #[test]
    fn test_timestamp_date_sub() {
        let f = DateSubFunction::default();

        let times = vec![Some(123), None, Some(42), None];
        // Intervals in milliseconds
        let intervals = vec![
            IntervalDayTime::new(0, 1000),
            IntervalDayTime::new(0, 2000),
            IntervalDayTime::new(0, 3000),
            IntervalDayTime::new(0, 1000),
        ];
        let results = [Some(122), None, Some(39), None];

        let args = vec![
            ColumnarValue::Array(Arc::new(TimestampSecondArray::from(times.clone()))),
            ColumnarValue::Array(Arc::new(IntervalDayTimeArray::from(intervals))),
        ];

        let vector = f
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields: vec![],
                number_rows: 4,
                return_field: Arc::new(Field::new(
                    "x",
                    DataType::Timestamp(TimeUnit::Second, None),
                    true,
                )),
                config_options: Arc::new(ConfigOptions::new()),
            })
            .and_then(|v| ColumnarValue::values_to_arrays(&[v]))
            .map(|mut a| a.remove(0))
            .unwrap();
        let vector = vector.as_primitive::<TimestampSecondType>();

        assert_eq!(4, vector.len());
        for (i, _t) in times.iter().enumerate() {
            let result = results.get(i).unwrap();

            if let Some(x) = result {
                assert!(vector.is_valid(i));
                assert_eq!(vector.value(i), *x);
            } else {
                assert!(vector.is_null(i));
            }
        }
    }

    #[test]
    fn test_date_date_sub() {
        let f = DateSubFunction::default();
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

        let args = vec![
            ColumnarValue::Array(Arc::new(Date32Array::from(dates.clone()))),
            ColumnarValue::Array(Arc::new(IntervalYearMonthArray::from(intervals))),
        ];

        let vector = f
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields: vec![],
                number_rows: 4,
                return_field: Arc::new(Field::new(
                    "x",
                    DataType::Timestamp(TimeUnit::Second, None),
                    true,
                )),
                config_options: Arc::new(ConfigOptions::new()),
            })
            .and_then(|v| ColumnarValue::values_to_arrays(&[v]))
            .map(|mut a| a.remove(0))
            .unwrap();
        let vector = vector.as_primitive::<Date32Type>();

        assert_eq!(4, vector.len());
        for (i, _t) in dates.iter().enumerate() {
            let result = results.get(i).unwrap();

            if let Some(x) = result {
                assert!(vector.is_valid(i));
                assert_eq!(vector.value(i), *x);
            } else {
                assert!(vector.is_null(i));
            }
        }
    }
}
