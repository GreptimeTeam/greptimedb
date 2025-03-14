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

use common_error::ext::BoxedError;
use common_query::error::{self, InvalidFuncArgsSnafu, Result, UnsupportedInputDataTypeSnafu};
use common_query::prelude::Signature;
use datatypes::prelude::{ConcreteDataType, MutableVector, ScalarVectorBuilder};
use datatypes::vectors::{StringVectorBuilder, VectorRef};
use snafu::{ensure, ResultExt};

use crate::function::{Function, FunctionContext};
use crate::helper;

/// A function that formats timestamp/date/datetime into string by the format
#[derive(Clone, Debug, Default)]
pub struct DateFormatFunction;

const NAME: &str = "date_format";

impl Function for DateFormatFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::string_datatype())
    }

    fn signature(&self) -> Signature {
        helper::one_of_sigs2(
            vec![
                ConcreteDataType::date_datatype(),
                ConcreteDataType::timestamp_second_datatype(),
                ConcreteDataType::timestamp_millisecond_datatype(),
                ConcreteDataType::timestamp_microsecond_datatype(),
                ConcreteDataType::timestamp_nanosecond_datatype(),
            ],
            vec![ConcreteDataType::string_datatype()],
        )
    }

    fn eval(&self, func_ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
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
        let formats = &columns[1];

        let size = left.len();
        let left_datatype = columns[0].data_type();
        let mut results = StringVectorBuilder::with_capacity(size);

        match left_datatype {
            ConcreteDataType::Timestamp(_) => {
                for i in 0..size {
                    let ts = left.get(i).as_timestamp();
                    let format = formats.get(i).as_string();

                    let result = match (ts, format) {
                        (Some(ts), Some(fmt)) => Some(
                            ts.as_formatted_string(&fmt, Some(&func_ctx.query_ctx.timezone()))
                                .map_err(BoxedError::new)
                                .context(error::ExecuteSnafu)?,
                        ),
                        _ => None,
                    };

                    results.push(result.as_deref());
                }
            }
            ConcreteDataType::Date(_) => {
                for i in 0..size {
                    let date = left.get(i).as_date();
                    let format = formats.get(i).as_string();

                    let result = match (date, format) {
                        (Some(date), Some(fmt)) => date
                            .as_formatted_string(&fmt, Some(&func_ctx.query_ctx.timezone()))
                            .map_err(BoxedError::new)
                            .context(error::ExecuteSnafu)?,
                        _ => None,
                    };

                    results.push(result.as_deref());
                }
            }
            _ => {
                return UnsupportedInputDataTypeSnafu {
                    function: NAME,
                    datatypes: columns.iter().map(|c| c.data_type()).collect::<Vec<_>>(),
                }
                .fail();
            }
        }

        Ok(results.to_vector())
    }
}

impl fmt::Display for DateFormatFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DATE_FORMAT")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_query::prelude::{TypeSignature, Volatility};
    use datatypes::prelude::{ConcreteDataType, ScalarVector};
    use datatypes::value::Value;
    use datatypes::vectors::{DateVector, StringVector, TimestampSecondVector};

    use super::{DateFormatFunction, *};

    #[test]
    fn test_date_format_misc() {
        let f = DateFormatFunction;
        assert_eq!("date_format", f.name());
        assert_eq!(
            ConcreteDataType::string_datatype(),
            f.return_type(&[ConcreteDataType::timestamp_microsecond_datatype()])
                .unwrap()
        );
        assert_eq!(
            ConcreteDataType::string_datatype(),
            f.return_type(&[ConcreteDataType::timestamp_second_datatype()])
                .unwrap()
        );
        assert_eq!(
            ConcreteDataType::string_datatype(),
            f.return_type(&[ConcreteDataType::date_datatype()]).unwrap()
        );
        assert!(matches!(f.signature(),
                         Signature {
                             type_signature: TypeSignature::OneOf(sigs),
                             volatility: Volatility::Immutable
                         } if  sigs.len() == 5));
    }

    #[test]
    fn test_timestamp_date_format() {
        let f = DateFormatFunction;

        let times = vec![Some(123), None, Some(42), None];
        let formats = vec![
            "%Y-%m-%d %T.%3f",
            "%Y-%m-%d %T.%3f",
            "%Y-%m-%d %T.%3f",
            "%Y-%m-%d %T.%3f",
        ];
        let results = [
            Some("1970-01-01 00:02:03.000"),
            None,
            Some("1970-01-01 00:00:42.000"),
            None,
        ];

        let time_vector = TimestampSecondVector::from(times.clone());
        let interval_vector = StringVector::from_vec(formats);
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
                Value::String(s) => {
                    assert_eq!(s.as_utf8(), result.unwrap());
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn test_date_date_format() {
        let f = DateFormatFunction;

        let dates = vec![Some(123), None, Some(42), None];
        let formats = vec![
            "%Y-%m-%d %T.%3f",
            "%Y-%m-%d %T.%3f",
            "%Y-%m-%d %T.%3f",
            "%Y-%m-%d %T.%3f",
        ];
        let results = [
            Some("1970-05-04 00:00:00.000"),
            None,
            Some("1970-02-12 00:00:00.000"),
            None,
        ];

        let date_vector = DateVector::from(dates.clone());
        let interval_vector = StringVector::from_vec(formats);
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
                Value::String(s) => {
                    assert_eq!(s.as_utf8(), result.unwrap());
                }
                _ => unreachable!(),
            }
        }
    }
}
