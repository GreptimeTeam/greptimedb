use std::fmt;
use std::sync::Arc;

use common_query::prelude::{Signature, Volatility};
use datatypes::data_type::ConcreteDataType;
use datatypes::prelude::VectorRef;

use crate::error::Result;
use crate::scalars::expression::{scalar_binary_op, EvalContext};
use crate::scalars::function::{Function, FunctionContext};

#[derive(Clone, Default)]
pub(crate) struct TestAndFunction;

impl Function for TestAndFunction {
    fn name(&self) -> &str {
        "test_and"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::boolean_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::exact(
            vec![
                ConcreteDataType::boolean_datatype(),
                ConcreteDataType::boolean_datatype(),
            ],
            Volatility::Immutable,
        )
    }

    fn eval(&self, _func_ctx: FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        let col = scalar_binary_op::<bool, bool, bool, _>(
            &columns[0],
            &columns[1],
            scalar_and,
            &mut EvalContext::default(),
        )?;
        Ok(Arc::new(col))
    }
}

impl fmt::Display for TestAndFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TEST_AND")
    }
}

#[inline]
fn scalar_and(left: Option<bool>, right: Option<bool>, _ctx: &mut EvalContext) -> Option<bool> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left && right),
        _ => None,
    }
}
