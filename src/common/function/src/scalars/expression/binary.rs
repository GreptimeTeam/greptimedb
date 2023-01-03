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

use std::iter;

use common_query::error::Result;
use datatypes::prelude::*;
use datatypes::vectors::{ConstantVector, Helper};

use crate::scalars::expression::ctx::EvalContext;

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
            let left: &<L as Scalar>::VectorType = unsafe { Helper::static_cast(l) };
            let right: &ConstantVector = unsafe { Helper::static_cast(r) };
            let right: &<R as Scalar>::VectorType = unsafe { Helper::static_cast(right.inner()) };
            let b = right.get_data(0);

            let it = left.iter_data().map(|a| f(a, b, ctx));
            <O as Scalar>::VectorType::from_owned_iterator(it)
        }

        (false, false) => {
            let left: &<L as Scalar>::VectorType = unsafe { Helper::static_cast(l) };
            let right: &<R as Scalar>::VectorType = unsafe { Helper::static_cast(r) };

            let it = left
                .iter_data()
                .zip(right.iter_data())
                .map(|(a, b)| f(a, b, ctx));
            <O as Scalar>::VectorType::from_owned_iterator(it)
        }

        (true, false) => {
            let left: &ConstantVector = unsafe { Helper::static_cast(l) };
            let left: &<L as Scalar>::VectorType = unsafe { Helper::static_cast(left.inner()) };
            let a = left.get_data(0);

            let right: &<R as Scalar>::VectorType = unsafe { Helper::static_cast(r) };
            let it = right.iter_data().map(|b| f(a, b, ctx));
            <O as Scalar>::VectorType::from_owned_iterator(it)
        }

        (true, true) => {
            let left: &ConstantVector = unsafe { Helper::static_cast(l) };
            let left: &<L as Scalar>::VectorType = unsafe { Helper::static_cast(left.inner()) };
            let a = left.get_data(0);

            let right: &ConstantVector = unsafe { Helper::static_cast(r) };
            let right: &<R as Scalar>::VectorType = unsafe { Helper::static_cast(right.inner()) };
            let b = right.get_data(0);

            let it = iter::repeat(a)
                .zip(iter::repeat(b))
                .map(|(a, b)| f(a, b, ctx))
                .take(left.len());
            <O as Scalar>::VectorType::from_owned_iterator(it)
        }
    };

    if let Some(error) = ctx.error.take() {
        return Err(error);
    }
    Ok(result)
}
