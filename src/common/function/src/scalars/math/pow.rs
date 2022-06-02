use std::fmt;
use std::sync::Arc;

use common_query::prelude::{Signature, Volatility};
use datatypes::data_type::DataType;
use datatypes::prelude::ConcreteDataType;
use datatypes::type_id::LogicalTypeId;
use datatypes::vectors::VectorRef;
use datatypes::with_match_primitive_type_id;
use num::traits::Pow;
use num_traits::AsPrimitive;

use crate::error::Result;
use crate::scalars::expression::{scalar_binary_op, EvalContext};
use crate::scalars::function::{Function, FunctionContext};
use crate::scalars::numerics;

#[derive(Clone, Debug)]
pub struct PowFunction;

impl Function for PowFunction {
    fn name(&self) -> &str {
        "pow"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::float64_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::uniform(2, numerics(), Volatility::Immutable)
    }

    fn eval(&self, _func_ctx: FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        with_match_primitive_type_id!(columns[0].data_type().logical_type_id(), |$S| {
            with_match_primitive_type_id!(columns[1].data_type().logical_type_id(), |$T| {
                let col = scalar_binary_op::<$S, $T, f64, _>(&columns[0], &columns[1], scalar_pow, &mut EvalContext::default())?;
                Ok(Arc::new(col))
            },{
                unreachable!()
            })
        },{
            unreachable!()
        })
    }
}

#[inline]
fn scalar_pow<S, T>(value: Option<S>, base: Option<T>, _ctx: &mut EvalContext) -> Option<f64>
where
    S: AsPrimitive<f64>,
    T: AsPrimitive<f64>,
{
    match (value, base) {
        (Some(value), Some(base)) => Some(value.as_().pow(base.as_())),
        _ => None,
    }
}

impl fmt::Display for PowFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "POW")
    }
}
