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

/// A function to return current schema name.
#[derive(Clone, Debug, Default)]
pub struct DatabaseFunction;

#[derive(Clone, Debug, Default)]
pub struct CurrentSchemaFunction;
pub struct SessionUserFunction;

const DATABASE_FUNCTION_NAME: &str = "database";
const CURRENT_SCHEMA_FUNCTION_NAME: &str = "current_schema";
const SESSION_USER_FUNCTION_NAME: &str = "session_user";

impl Function for DatabaseFunction {
    fn name(&self) -> &str {
        DATABASE_FUNCTION_NAME
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::string_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::uniform(0, vec![], Volatility::Immutable)
    }

    fn eval(&self, func_ctx: FunctionContext, _columns: &[VectorRef]) -> Result<VectorRef> {
        let db = func_ctx.query_ctx.current_schema();

        Ok(Arc::new(StringVector::from_slice(&[&db])) as _)
    }
}

impl Function for CurrentSchemaFunction {
    fn name(&self) -> &str {
        CURRENT_SCHEMA_FUNCTION_NAME
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::string_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::uniform(0, vec![], Volatility::Immutable)
    }

    fn eval(&self, func_ctx: FunctionContext, _columns: &[VectorRef]) -> Result<VectorRef> {
        let db = func_ctx.query_ctx.current_schema();

        Ok(Arc::new(StringVector::from_slice(&[&db])) as _)
    }
}

impl Function for SessionUserFunction {
    fn name(&self) -> &str {
        SESSION_USER_FUNCTION_NAME
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::string_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::uniform(0, vec![], Volatility::Immutable)
    }

    fn eval(&self, func_ctx: FunctionContext, _columns: &[VectorRef]) -> Result<VectorRef> {
        let user = func_ctx.query_ctx.current_user();

        Ok(Arc::new(StringVector::from_slice(&[user.username()])) as _)
    }
    
}

impl fmt::Display for DatabaseFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DATABASE")
    }
}

impl fmt::Display for CurrentSchemaFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CURRENT_SCHEMA")
    }
}

impl fmt::Display for SessionUserFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SESSION_USER")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_query::prelude::TypeSignature;
    use session::context::QueryContextBuilder;

    use super::*;
    #[test]
    fn test_build_function() {
        let build = DatabaseFunction;
        assert_eq!("database", build.name());
        assert_eq!(
            ConcreteDataType::string_datatype(),
            build.return_type(&[]).unwrap()
        );
        assert!(matches!(build.signature(),
                         Signature {
                             type_signature: TypeSignature::Uniform(0, valid_types),
                             volatility: Volatility::Immutable
                         } if  valid_types == vec![]
        ));

        let query_ctx = QueryContextBuilder::default()
            .current_schema("test_db".to_string())
            .build()
            .into();

        let func_ctx = FunctionContext {
            query_ctx,
            ..Default::default()
        };
        let vector = build.eval(func_ctx, &[]).unwrap();
        let expect: VectorRef = Arc::new(StringVector::from(vec!["test_db"]));
        assert_eq!(expect, vector);
    }
}
