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

use std::sync::Arc;
use std::{env, fmt};

use common_query::error::Result;
use common_query::prelude::{Signature, Volatility};
use datatypes::data_type::ConcreteDataType;
use datatypes::vectors::{StringVector, VectorRef};
use session::context::Channel;

use crate::function::{Function, FunctionContext};

#[derive(Clone, Debug, Default)]
pub(crate) struct VersionFunction;

impl fmt::Display for VersionFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "VERSION")
    }
}

impl Function for VersionFunction {
    fn name(&self) -> &str {
        "version"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::string_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::nullary(Volatility::Immutable)
    }

    fn eval(&self, func_ctx: &FunctionContext, _columns: &[VectorRef]) -> Result<VectorRef> {
        let version = match func_ctx.query_ctx.channel() {
            Channel::Mysql => {
                format!(
                    "{}-greptimedb-{}",
                    std::env::var("GREPTIMEDB_MYSQL_SERVER_VERSION")
                        .unwrap_or_else(|_| "8.4.2".to_string()),
                    env!("CARGO_PKG_VERSION")
                )
            }
            Channel::Postgres => {
                format!("16.3-greptimedb-{}", env!("CARGO_PKG_VERSION"))
            }
            _ => env!("CARGO_PKG_VERSION").to_string(),
        };
        let result = StringVector::from(vec![version]);
        Ok(Arc::new(result))
    }
}
