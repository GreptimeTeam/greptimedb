use std::borrow::Cow;
use std::fmt::Display;

use common_query::error::InvalidFuncArgsSnafu;
use common_query::prelude::{Signature, TypeSignature, Volatility};
use datatypes::prelude::ConcreteDataType;
use datatypes::scalars::ScalarVectorBuilder;
use datatypes::vectors::{Float32VectorBuilder, MutableVector, VectorRef};
use nalgebra::DVectorView;
use snafu::ensure;

use crate::function::{Function, FunctionContext};
use crate::scalars::vector::impl_conv::{as_veclit, as_veclit_if_const};

const NAME: &str = "vec_elem_sum";

#[derive(Debug, Clone, Default)]
pub struct ElemSumFunction;

impl Function for ElemSumFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(
        &self,
        _input_types: &[ConcreteDataType],
    ) -> common_query::error::Result<ConcreteDataType> {
        Ok(ConcreteDataType::float32_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::one_of(
            vec![
                TypeSignature::Exact(vec![ConcreteDataType::string_datatype()]),
                TypeSignature::Exact(vec![ConcreteDataType::binary_datatype()]),
            ],
            Volatility::Immutable,
        )
    }

    fn eval(
        &self,
        _func_ctx: FunctionContext,
        columns: &[VectorRef],
    ) -> common_query::error::Result<VectorRef> {
        ensure!(
            columns.len() == 1,
            InvalidFuncArgsSnafu {
                err_msg: format!(
                    "The length of the args is not correct, expect exactly one, have: {}",
                    columns.len()
                )
            }
        );
        let arg0 = &columns[0];

        let len = arg0.len();
        let mut result = Float32VectorBuilder::with_capacity(len);
        if len == 0 {
            return Ok(result.to_vector());
        }

        let arg0_const = as_veclit_if_const(arg0)?;

        for i in 0..len {
            let arg0 = match arg0_const.as_ref() {
                Some(arg0) => Some(Cow::Borrowed(arg0.as_ref())),
                None => as_veclit(arg0.get_ref(i))?,
            };
            let Some(arg0) = arg0 else {
                result.push_null();
                continue;
            };
            result.push(Some(DVectorView::from_slice(&arg0, arg0.len()).sum()));
        }

        Ok(result.to_vector())
    }
}

impl Display for ElemSumFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", NAME.to_ascii_uppercase())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::vectors::StringVector;

    use super::*;
    use crate::function::FunctionContext;

    #[test]
    fn test_elem_sum() {
        let func = ElemSumFunction;

        let input0 = Arc::new(StringVector::from(vec![
            Some("[1.0,2.0,3.0]".to_string()),
            Some("[4.0,5.0,6.0]".to_string()),
            None,
        ]));

        let result = func.eval(FunctionContext::default(), &[input0]).unwrap();

        let result = result.as_ref();
        assert_eq!(result.len(), 3);
        assert_eq!(result.get_ref(0).as_f32().unwrap(), Some(6.0));
        assert_eq!(result.get_ref(1).as_f32().unwrap(), Some(15.0));
        assert_eq!(result.get_ref(2).as_f32().unwrap(), None);
    }
}
