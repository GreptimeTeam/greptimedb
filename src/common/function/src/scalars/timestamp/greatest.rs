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
use std::str::FromStr;

use common_query::error::{
    self, ArrowComputeSnafu, FromArrowArraySnafu, InvalidFuncArgsSnafu, Result,
    UnsupportedInputDataTypeSnafu,
};
use common_query::prelude::{Signature, Volatility};
use common_time::Date;
use datatypes::arrow::array::Date32Array;
use datatypes::arrow::compute::kernels::comparison::gt_dyn;
use datatypes::arrow::compute::kernels::zip;
use datatypes::prelude::ConcreteDataType;
use datatypes::vectors::{DateVector, Helper, StringVector, Vector, VectorRef};
use snafu::{ensure, ResultExt};

use crate::scalars::function::{Function, FunctionContext};

#[derive(Clone, Debug, Default)]
pub struct GreatestFunction;

const NAME: &str = "greatest";

fn convert_to_date(arg: &str) -> Option<i32> {
    match Date::from_str(arg) {
        Ok(ts) => Some(ts.val()),
        Err(_err) => None,
    }
}

fn string_vector_to_date32_array(column: &VectorRef) -> Result<Date32Array> {
    let column =
        StringVector::try_from_arrow_array(column.to_arrow_array()).context(FromArrowArraySnafu)?;
    let column = (0..column.len())
        .map(|idx| convert_to_date(&column.get(idx).to_string()))
        .collect::<Vec<_>>();
    Ok(Date32Array::from_iter(column))
}

fn date_vector_to_date32_array(column: &VectorRef) -> Result<Date32Array> {
    let column =
        DateVector::try_from_arrow_array(column.to_arrow_array()).context(FromArrowArraySnafu)?;
    let column = (0..column.len())
        .map(|idx| {
            column
                .get(idx)
                .as_value_ref()
                .as_date()
                .unwrap()
                .map(|x| x.val())
        })
        .collect::<Vec<_>>();
    Ok(Date32Array::from_iter(column))
}

impl Function for GreatestFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::date_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::uniform(
            2,
            vec![
                ConcreteDataType::string_datatype(),
                ConcreteDataType::date_datatype(),
            ],
            Volatility::Immutable,
        )
    }

    fn eval(&self, _func_ctx: FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
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
                let column1 = string_vector_to_date32_array(&columns[0])?;
                let column2 = string_vector_to_date32_array(&columns[1])?;
                let boolean_array = gt_dyn(&column1, &column2).context(ArrowComputeSnafu)?;
                let result =
                    zip::zip(&boolean_array, &column1, &column2).context(ArrowComputeSnafu)?;
                Ok(Helper::try_into_vector(&result).context(error::FromArrowArraySnafu)?)
            }
            ConcreteDataType::Date(_) => {
                let column1 = date_vector_to_date32_array(&columns[0])?;
                let column2 = date_vector_to_date32_array(&columns[1])?;
                let boolean_array = gt_dyn(&column1, &column2).context(ArrowComputeSnafu)?;
                let result =
                    zip::zip(&boolean_array, &column1, &column2).context(ArrowComputeSnafu)?;
                Ok(Helper::try_into_vector(&result).context(error::FromArrowArraySnafu)?)
            }
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
    use std::str::FromStr;
    use std::sync::Arc;

    use common_time::Date;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::types::DateType;
    use datatypes::value::Value;
    use datatypes::vectors::{DateVector, StringVector, Vector};

    use super::GreatestFunction;
    use crate::scalars::function::FunctionContext;
    use crate::scalars::Function;

    #[test]
    fn test_greatest_takes_string_vector() {
        let function = GreatestFunction;
        assert_eq!(
            function.return_type(&[]).unwrap(),
            ConcreteDataType::Date(DateType)
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

        let result = function.eval(FunctionContext::default(), &columns).unwrap();
        let result = result.as_any().downcast_ref::<DateVector>().unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(
            result.get(0),
            Value::Date(Date::from_str("2001-02-01").unwrap())
        );
        assert_eq!(
            result.get(1),
            Value::Date(Date::from_str("2012-12-23").unwrap())
        );
    }

    #[test]
    fn test_greatest_takes_date_vector() {
        let function = GreatestFunction;
        assert_eq!(
            function.return_type(&[]).unwrap(),
            ConcreteDataType::Date(DateType)
        );
        let columns = vec![
            Arc::new(DateVector::from_slice(vec![-1, 2])) as _,
            Arc::new(DateVector::from_slice(vec![0, 1])) as _,
        ];

        let result = function.eval(FunctionContext::default(), &columns).unwrap();
        let result = result.as_any().downcast_ref::<DateVector>().unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(
            result.get(0),
            Value::Date(Date::from_str("1970-01-01").unwrap())
        );
        assert_eq!(
            result.get(1),
            Value::Date(Date::from_str("1970-01-03").unwrap())
        );
    }
}
