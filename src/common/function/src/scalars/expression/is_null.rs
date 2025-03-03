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
use std::fmt::Display;
use std::sync::Arc;

use common_query::error;
use common_query::error::{ArrowComputeSnafu, InvalidFuncArgsSnafu};
use common_query::prelude::{Signature, Volatility};
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::compute::is_null;
use datatypes::data_type::ConcreteDataType;
use datatypes::prelude::VectorRef;
use datatypes::vectors::Helper;
use snafu::{ensure, ResultExt};

use crate::function::{Function, FunctionContext};

const NAME: &str = "isnull";

/// The function to check whether an expression is NULL
#[derive(Clone, Debug, Default)]
pub struct IsNullFunction;

impl Display for IsNullFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", NAME.to_ascii_uppercase())
    }
}

impl Function for IsNullFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _: &[ConcreteDataType]) -> common_query::error::Result<ConcreteDataType> {
        Ok(ConcreteDataType::boolean_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::any(1, Volatility::Immutable)
    }

    fn eval(
        &self,
        _func_ctx: &FunctionContext,
        columns: &[VectorRef],
    ) -> common_query::error::Result<VectorRef> {
        ensure!(
            columns.len() == 1,
            InvalidFuncArgsSnafu {
                err_msg: format!(
                    "The length of the args is not correct, expect exactly one, have: {}",
                    columns.len()
                ),
            }
        );
        let values = &columns[0];
        let arrow_array = &values.to_arrow_array();
        let result = is_null(arrow_array).context(ArrowComputeSnafu)?;

        Helper::try_into_vector(Arc::new(result) as ArrayRef).context(error::FromArrowArraySnafu)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_query::prelude::TypeSignature;
    use datatypes::scalars::ScalarVector;
    use datatypes::vectors::{BooleanVector, Float32Vector};

    use super::*;
    #[test]
    fn test_is_null_function() {
        let is_null = IsNullFunction;
        assert_eq!("isnull", is_null.name());
        assert_eq!(
            ConcreteDataType::boolean_datatype(),
            is_null.return_type(&[]).unwrap()
        );
        assert_eq!(
            is_null.signature(),
            Signature {
                type_signature: TypeSignature::Any(1),
                volatility: Volatility::Immutable
            }
        );
        let values = vec![None, Some(3.0), None];

        let args: Vec<VectorRef> = vec![Arc::new(Float32Vector::from(values))];
        let vector = is_null.eval(&FunctionContext::default(), &args).unwrap();
        let expect: VectorRef = Arc::new(BooleanVector::from_vec(vec![true, false, true]));
        assert_eq!(expect, vector);
    }
}
