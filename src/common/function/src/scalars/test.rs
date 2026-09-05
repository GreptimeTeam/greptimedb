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

use std::fmt;

use common_query::error::Result;
use datafusion::logical_expr::ColumnarValue;
use datafusion_expr::{ScalarFunctionArgs, Signature, Volatility};
use datatypes::arrow::datatypes::DataType;
use datatypes::prelude::{Scalar, ScalarVector, VectorRef};
use datatypes::vectors::{Helper, Vector};

use crate::function::{Function, extract_args};
use crate::scalars::expression::EvalContext;

fn scalar_binary_op<L: Scalar, R: Scalar, O: Scalar, F>(
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

    let left: &<L as Scalar>::VectorType = unsafe { Helper::static_cast(l) };
    let right: &<R as Scalar>::VectorType = unsafe { Helper::static_cast(r) };
    let result = <O as Scalar>::VectorType::from_owned_iterator(
        left.iter_data()
            .zip(right.iter_data())
            .map(|(a, b)| f(a, b, ctx)),
    );

    if let Some(error) = ctx.error.take() {
        return Err(error);
    }
    Ok(result)
}

#[derive(Clone)]
pub(crate) struct TestAndFunction {
    signature: Signature,
}

impl Default for TestAndFunction {
    fn default() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Boolean, DataType::Boolean],
                Volatility::Immutable,
            ),
        }
    }
}

impl Function for TestAndFunction {
    fn name(&self) -> &str {
        "test_and"
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [arg0, arg1] = extract_args(self.name(), &args)?;
        let columns = Helper::try_into_vectors(&[arg0, arg1]).unwrap();
        let col = scalar_binary_op::<bool, bool, bool, _>(
            &columns[0],
            &columns[1],
            scalar_and,
            &mut EvalContext::default(),
        )?;
        Ok(ColumnarValue::Array(col.to_arrow_array()))
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
