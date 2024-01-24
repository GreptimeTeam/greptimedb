use std::fmt::{self};
use std::sync::Arc;

use common_query::error::Result;
use common_query::prelude::{Signature, Volatility};
use datatypes::prelude::{ConcreteDataType, ScalarVector};
use datatypes::vectors::{StringVector, VectorRef};

use crate::function::{Function, FunctionContext};

#[derive(Clone, Debug, Default)]
pub struct DatabaseFunction;

const NAME: &str = "database";

impl Function for DatabaseFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::string_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::uniform(0, vec![], Volatility::Immutable)
    }

    fn eval(&self, func_ctx: FunctionContext, _columns: &[VectorRef]) -> Result<VectorRef> {
        let db = func_ctx.query_ctx.current_schema();

        Ok(Arc::new(StringVector::from_slice(&[db])) as _)
    }
}

impl fmt::Display for DatabaseFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DATABASE")
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
            .build();

        let func_ctx = FunctionContext { query_ctx };
        let vector = build.eval(func_ctx, &[]).unwrap();
        let expect: VectorRef = Arc::new(StringVector::from(vec!["test_db"]));
        assert_eq!(expect, vector);
    }
}
