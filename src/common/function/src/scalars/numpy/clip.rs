use std::fmt;
use crate::scalars::function::{FunctionContext, Function};
use crate::error::Result;
use common_query::prelude::{Volatility,Signature};
use datatypes::vectors::VectorRef;
use datatypes::data_type::ConcreteDataType;
use crate::scalars::numpy::NUMERICS;
use crate::scalars::scalar_binary_op;

#[derive(Clone, Debug)]
pub struct ClipFuncton;

impl Function for ClipFuncton {
    fn name(&self) -> &str {
        "clip"
    }

    fn return_type(&self, input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        let iter = input_types.iter();

        if iter.any(|ty| matches!(ty, ConcreteDataType::Float64(_))) {
            Ok(ConcreteDataType::float64_datatype())
        } else if iter.any(|ty| matches!(ty, ConcreteDataType::Float32(_))) {
            Ok(ConcreteDataType::float32_datatype())
        } else {
            Ok(ConcreteDataType::int64_datatype())
        }
    }

    fn signature(&self) -> Signature {
        Signature::uniform(3, NUMERICS.to_vec(), Volatility::Immutable)
    }


    fn eval(&self, func_ctx: FunctionContext,  columns: &[VectorRef]) -> Result<VectorRef> {

    }
}


impl fmt::Display for ClipFuncton {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CLIP")
    }
}
