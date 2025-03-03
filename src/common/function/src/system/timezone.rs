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
use std::sync::Arc;

use common_query::error::Result;
use common_query::prelude::{Signature, Volatility};
use datatypes::prelude::{ConcreteDataType, ScalarVector};
use datatypes::vectors::{StringVector, VectorRef};

use crate::function::{Function, FunctionContext};

/// A function to return current session timezone.
#[derive(Clone, Debug, Default)]
pub struct TimezoneFunction;

const NAME: &str = "timezone";

impl Function for TimezoneFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::string_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::nullary(Volatility::Immutable)
    }

    fn eval(&self, func_ctx: &FunctionContext, _columns: &[VectorRef]) -> Result<VectorRef> {
        let tz = func_ctx.query_ctx.timezone().to_string();

        Ok(Arc::new(StringVector::from_slice(&[&tz])) as _)
    }
}

impl fmt::Display for TimezoneFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TIMEZONE")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use session::context::QueryContextBuilder;

    use super::*;
    #[test]
    fn test_build_function() {
        let build = TimezoneFunction;
        assert_eq!("timezone", build.name());
        assert_eq!(
            ConcreteDataType::string_datatype(),
            build.return_type(&[]).unwrap()
        );
        assert_eq!(build.signature(), Signature::nullary(Volatility::Immutable));

        let query_ctx = QueryContextBuilder::default().build().into();

        let func_ctx = FunctionContext {
            query_ctx,
            ..Default::default()
        };
        let vector = build.eval(&func_ctx, &[]).unwrap();
        let expect: VectorRef = Arc::new(StringVector::from(vec!["UTC"]));
        assert_eq!(expect, vector);
    }
}
