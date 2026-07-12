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

use common_query::error::{self, Result};
use datatypes::prelude::*;
use datatypes::vectors::Helper;
use snafu::ResultExt;

use crate::scalars::expression::ctx::EvalContext;

pub fn scalar_unary_op<L: Scalar, O: Scalar, F>(
    l: &VectorRef,
    f: F,
    ctx: &mut EvalContext,
) -> Result<<O as Scalar>::VectorType>
where
    F: Fn(Option<L::RefType<'_>>, &mut EvalContext) -> Option<O>,
{
    let left = Helper::check_get_scalar::<L>(l).context(error::GetScalarVectorSnafu)?;
    let it = left.iter_data().map(|a| f(a, ctx));
    let result = <O as Scalar>::VectorType::from_owned_iterator(it);

    if let Some(error) = ctx.error.take() {
        return Err(error);
    }

    Ok(result)
}
