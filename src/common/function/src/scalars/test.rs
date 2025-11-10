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

use datafusion::logical_expr::ColumnarValue;
use datafusion_expr::{ScalarFunctionArgs, Signature, Volatility};
use datatypes::arrow::datatypes::DataType;
use datatypes::vectors::{Helper, Vector};

use crate::function::{Function, extract_args};
use crate::scalars::expression::{EvalContext, scalar_binary_op};

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
