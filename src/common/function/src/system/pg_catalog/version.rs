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

use crate::function::{Function, FunctionContext};

#[derive(Clone, Debug, Default)]
pub(crate) struct PGVersionFunction;

impl fmt::Display for PGVersionFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, crate::pg_catalog_func_fullname!("VERSION"))
    }
}

impl Function for PGVersionFunction {
    fn name(&self) -> &str {
        crate::pg_catalog_func_fullname!("version")
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::string_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::exact(vec![], Volatility::Immutable)
    }

    fn eval(&self, _func_ctx: &FunctionContext, _columns: &[VectorRef]) -> Result<VectorRef> {
        let result = StringVector::from(vec![format!(
            "PostgreSQL 16.3 GreptimeDB {}",
            env!("CARGO_PKG_VERSION")
        )]);
        Ok(Arc::new(result))
    }
}
