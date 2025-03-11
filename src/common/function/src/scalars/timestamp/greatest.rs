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

use std::fmt::{self};

use common_query::error::{
    self, ArrowComputeSnafu, InvalidFuncArgsSnafu, Result, UnsupportedInputDataTypeSnafu,
};
use common_query::prelude::{Signature, Volatility};
use datafusion::arrow::compute::kernels::cmp::gt;
use datatypes::arrow::array::AsArray;
use datatypes::arrow::compute::cast;
use datatypes::arrow::compute::kernels::zip;
use datatypes::arrow::datatypes::{
    DataType as ArrowDataType, Date32Type, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
};
use datatypes::prelude::ConcreteDataType;
use datatypes::types::TimestampType;
use datatypes::vectors::{Helper, VectorRef};
use snafu::{ensure, ResultExt};

use crate::function::{Function, FunctionContext};

#[derive(Clone, Debug, Default)]
pub struct GreatestFunction;

const NAME: &str = "greatest";

macro_rules! gt_time_types {
    ($ty: ident, $columns:expr) => {{
        let column1 = $columns[0].to_arrow_array();
        let column2 = $columns[1].to_arrow_array();

        let column1 = column1.as_primitive::<$ty>();
        let column2 = column2.as_primitive::<$ty>();
        let boolean_array = gt(&column1, &column2).context(ArrowComputeSnafu)?;

        let result = zip::zip(&boolean_array, &column1, &column2).context(ArrowComputeSnafu)?;
        Helper::try_into_vector(&result).context(error::FromArrowArraySnafu)
    }};
}

impl Function for GreatestFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        ensure!(
            input_types.len() == 2,
            InvalidFuncArgsSnafu {
                err_msg: format!(
                    "The length of the args is not correct, expect exactly two, have: {}",
                    input_types.len()
                )
            }
        );

        match &input_types[0] {
            ConcreteDataType::String(_) => Ok(ConcreteDataType::timestamp_millisecond_datatype()),
            ConcreteDataType::Date(_) => Ok(ConcreteDataType::date_datatype()),
            ConcreteDataType::Timestamp(ts_type) => Ok(ConcreteDataType::Timestamp(*ts_type)),
            _ => UnsupportedInputDataTypeSnafu {
                function: NAME,
                datatypes: input_types,
            }
            .fail(),
        }
    }

    fn signature(&self) -> Signature {
        Signature::uniform(
            2,
            vec![
                ConcreteDataType::string_datatype(),
                ConcreteDataType::date_datatype(),
                ConcreteDataType::timestamp_nanosecond_datatype(),
                ConcreteDataType::timestamp_microsecond_datatype(),
                ConcreteDataType::timestamp_millisecond_datatype(),
                ConcreteDataType::timestamp_second_datatype(),
            ],
            Volatility::Immutable,
        )
    }

    fn eval(&self, _func_ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure!(
            columns.len() == 2,
            InvalidFuncArgsSnafu {
                err_msg: format!(
                    "The length of the args is not correct, expect exactly two, have: {}",
                    columns.len()
                ),
            }
        );
        match columns[0].data_type() {
            ConcreteDataType::String(_) => {
                let column1 = cast(
                    &columns[0].to_arrow_array(),
                    &ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
                )
                .context(ArrowComputeSnafu)?;
                let column1 = column1.as_primitive::<TimestampMillisecondType>();
                let column2 = cast(
                    &columns[1].to_arrow_array(),
                    &ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
                )
                .context(ArrowComputeSnafu)?;
                let column2 = column2.as_primitive::<TimestampMillisecondType>();
                let boolean_array = gt(&column1, &column2).context(ArrowComputeSnafu)?;
                let result =
                    zip::zip(&boolean_array, &column1, &column2).context(ArrowComputeSnafu)?;
                Ok(Helper::try_into_vector(&result).context(error::FromArrowArraySnafu)?)
            }
            ConcreteDataType::Date(_) => gt_time_types!(Date32Type, columns),
            ConcreteDataType::Timestamp(ts_type) => match ts_type {
                TimestampType::Second(_) => gt_time_types!(TimestampSecondType, columns),
                TimestampType::Millisecond(_) => {
                    gt_time_types!(TimestampMillisecondType, columns)
                }
                TimestampType::Microsecond(_) => {
                    gt_time_types!(TimestampMicrosecondType, columns)
                }
                TimestampType::Nanosecond(_) => {
                    gt_time_types!(TimestampNanosecondType, columns)
                }
            },
            _ => UnsupportedInputDataTypeSnafu {
                function: NAME,
                datatypes: columns.iter().map(|c| c.data_type()).collect::<Vec<_>>(),
            }
            .fail(),
        }
    }
}

impl fmt::Display for GreatestFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GREATEST")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_time::timestamp::TimeUnit;
    use common_time::{Date, Timestamp};
    use datatypes::types::{
        DateType, TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
        TimestampSecondType,
    };
    use datatypes::value::Value;
    use datatypes::vectors::{
        DateVector, StringVector, TimestampMicrosecondVector, TimestampMillisecondVector,
        TimestampNanosecondVector, TimestampSecondVector, Vector,
    };
    use paste::paste;

    use super::*;
    #[test]
    fn test_greatest_takes_string_vector() {
        let function = GreatestFunction;
        assert_eq!(
            function
                .return_type(&[
                    ConcreteDataType::string_datatype(),
                    ConcreteDataType::string_datatype()
                ])
                .unwrap(),
            ConcreteDataType::timestamp_millisecond_datatype()
        );
        let columns = vec![
            Arc::new(StringVector::from(vec![
                "1970-01-01".to_string(),
                "2012-12-23".to_string(),
            ])) as _,
            Arc::new(StringVector::from(vec![
                "2001-02-01".to_string(),
                "1999-01-01".to_string(),
            ])) as _,
        ];

        let result = function
            .eval(&FunctionContext::default(), &columns)
            .unwrap();
        let result = result
            .as_any()
            .downcast_ref::<TimestampMillisecondVector>()
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(
            result.get(0),
            Value::Timestamp(Timestamp::from_str("2001-02-01 00:00:00", None).unwrap())
        );
        assert_eq!(
            result.get(1),
            Value::Timestamp(Timestamp::from_str("2012-12-23 00:00:00", None).unwrap())
        );
    }

    #[test]
    fn test_greatest_takes_date_vector() {
        let function = GreatestFunction;
        assert_eq!(
            function
                .return_type(&[
                    ConcreteDataType::date_datatype(),
                    ConcreteDataType::date_datatype()
                ])
                .unwrap(),
            ConcreteDataType::Date(DateType)
        );

        let columns = vec![
            Arc::new(DateVector::from_slice(vec![-1, 2])) as _,
            Arc::new(DateVector::from_slice(vec![0, 1])) as _,
        ];

        let result = function
            .eval(&FunctionContext::default(), &columns)
            .unwrap();
        let result = result.as_any().downcast_ref::<DateVector>().unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(
            result.get(0),
            Value::Date(Date::from_str_utc("1970-01-01").unwrap())
        );
        assert_eq!(
            result.get(1),
            Value::Date(Date::from_str_utc("1970-01-03").unwrap())
        );
    }

    #[test]
    fn test_greatest_takes_datetime_vector() {
        let function = GreatestFunction;
        assert_eq!(
            function
                .return_type(&[
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    ConcreteDataType::timestamp_millisecond_datatype()
                ])
                .unwrap(),
            ConcreteDataType::timestamp_millisecond_datatype()
        );

        let columns = vec![
            Arc::new(TimestampMillisecondVector::from_slice(vec![-1, 2])) as _,
            Arc::new(TimestampMillisecondVector::from_slice(vec![0, 1])) as _,
        ];

        let result = function
            .eval(&FunctionContext::default(), &columns)
            .unwrap();
        let result = result
            .as_any()
            .downcast_ref::<TimestampMillisecondVector>()
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(
            result.get(0),
            Value::Timestamp(Timestamp::from_str("1970-01-01 00:00:00", None).unwrap())
        );
        assert_eq!(
            result.get(1),
            Value::Timestamp(Timestamp::from_str("1970-01-01 00:00:00.002", None).unwrap())
        );
    }

    macro_rules! test_timestamp {
        ($type: expr,$unit: ident) => {
            paste! {
                #[test]
                fn [<test_greatest_takes_ $unit:lower _vector>]() {
                    let function = GreatestFunction;
                    assert_eq!(
                        function.return_type(&[$type, $type]).unwrap(),
                        ConcreteDataType::Timestamp(TimestampType::$unit([<Timestamp $unit Type>]))
                    );

                    let columns = vec![
                        Arc::new([<Timestamp $unit Vector>]::from_slice(vec![-1, 2])) as _,
                        Arc::new([<Timestamp $unit Vector>]::from_slice(vec![0, 1])) as _,
                    ];

                    let result = function.eval(&FunctionContext::default(), &columns).unwrap();
                    let result = result.as_any().downcast_ref::<[<Timestamp $unit Vector>]>().unwrap();
                    assert_eq!(result.len(), 2);
                    assert_eq!(
                        result.get(0),
                        Value::Timestamp(Timestamp::new(0, TimeUnit::$unit))
                    );
                    assert_eq!(
                        result.get(1),
                        Value::Timestamp(Timestamp::new(2, TimeUnit::$unit))
                    );
                }
            }
        }
    }

    test_timestamp!(
        ConcreteDataType::timestamp_nanosecond_datatype(),
        Nanosecond
    );
    test_timestamp!(
        ConcreteDataType::timestamp_microsecond_datatype(),
        Microsecond
    );
    test_timestamp!(
        ConcreteDataType::timestamp_millisecond_datatype(),
        Millisecond
    );
    test_timestamp!(ConcreteDataType::timestamp_second_datatype(), Second);
}
