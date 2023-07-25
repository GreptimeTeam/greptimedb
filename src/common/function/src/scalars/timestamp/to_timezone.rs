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
use common_query::prelude::{Signature, Volatility};
use common_time::timezone;
use datatypes::prelude::ConcreteDataType;
use datatypes::vectors::{Int64Vector, StringVector, VectorRef};
use snafu::ensure;

use crate::scalars::function::{Function, FunctionContext};

#[derive(Clone, Debug, Default)]
pub struct ToTimeZoneFunction;

const NAME: &str = "to_timezone";

impl Function for ToTimeZoneFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::int64_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::uniform(
            2,
            vec![
                ConcreteDataType::string_datatype(),
                ConcreteDataType::int64_datatype(),
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

        todo!()
    }
}

impl fmt::Display for ToTimeZoneFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TO_TIMEZONE")
    }
}

#[cfg(test)]
mod tests {}
