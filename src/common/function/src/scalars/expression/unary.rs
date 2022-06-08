use datatypes::prelude::*;
use snafu::ResultExt;

use crate::error::{GetScalarVectorSnafu, Result};
use crate::scalars::expression::ctx::EvalContext;

/// TODO: remove the allow_unused when it's used.
#[allow(unused)]
pub fn scalar_unary_op<L: Scalar, O: Scalar, F>(
    l: &VectorRef,
    f: F,
    ctx: &mut EvalContext,
) -> Result<<O as Scalar>::VectorType>
where
    F: Fn(Option<L::RefType<'_>>, &mut EvalContext) -> Option<O>,
{
    let left = VectorHelper::check_get_scalar::<L>(l).context(GetScalarVectorSnafu)?;
    let it = left.iter_data().map(|a| f(a, ctx));
    let result = <O as Scalar>::VectorType::from_owned_iterator(it);

    if let Some(error) = ctx.error.take() {
        return Err(error);
    }

    Ok(result)
}
