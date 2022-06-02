use datatypes::prelude::*;
use datatypes::vectors::ConstantVector;

use super::ctx::EvalContext;
use crate::error::Result;

pub fn scalar_binary_op<L: Scalar, R: Scalar, O: Scalar, F>(
    l: &VectorRef,
    r: &VectorRef,
    f: F,
    ctx: &mut EvalContext,
) -> Result<<O as Scalar>::VectorType>
where
    F: Fn(Option<L::RefType<'_>>, Option<R::RefType<'_>>, &mut EvalContext) -> Option<O>,
{
    debug_assert!(
        l.len() == r.len(),
        "Size of vectors must match to apply binary expression"
    );

    let result = match (l.is_const(), r.is_const()) {
        (false, true) => {
            let left: &<L as Scalar>::VectorType = unsafe { VectorHelper::static_cast(l) };
            let rc: &ConstantVector = unsafe { VectorHelper::static_cast(r) };
            let right: &<R as Scalar>::VectorType =
                unsafe { VectorHelper::static_cast(rc.inner()) };
            let b = right.get_data(0);

            let it = left.iter_data().map(|a| f(a, b, ctx));
            <O as Scalar>::VectorType::from_owned_iterator(it)
        }

        (false, false) => {
            let left: &<L as Scalar>::VectorType = unsafe { VectorHelper::static_cast(l) };
            let right: &<R as Scalar>::VectorType = unsafe { VectorHelper::static_cast(r) };

            let it = left
                .iter_data()
                .zip(right.iter_data())
                .map(|(a, b)| f(a, b, ctx));
            <O as Scalar>::VectorType::from_owned_iterator(it)
        }

        (true, false) => {
            let lc: &ConstantVector = unsafe { VectorHelper::static_cast(l) };
            let left: &<L as Scalar>::VectorType = unsafe { VectorHelper::static_cast(lc.inner()) };

            let a = left.get_data(0);

            let right: &<R as Scalar>::VectorType = unsafe { VectorHelper::static_cast(r) };
            let it = right.iter_data().map(|b| f(a, b, ctx));
            <O as Scalar>::VectorType::from_owned_iterator(it)
        }

        // True True ?
        (true, true) => unimplemented!(),
    };

    if let Some(error) = ctx.error.take() {
        return Err(error);
    }
    Ok(result)
}
