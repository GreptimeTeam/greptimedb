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

use std::fmt::{self};
use std::sync::Arc;

use common_query::error::Result;
use common_query::prelude::{Signature, Volatility};
use datatypes::prelude::{ConcreteDataType, DataType, VectorRef};
use datatypes::types::LogicalPrimitiveType;
use datatypes::with_match_primitive_type_id;
use num_traits::AsPrimitive;

use crate::function::{Function, FunctionContext};
use crate::scalars::expression::{scalar_unary_op, EvalContext};

#[derive(Clone, Debug, Default)]
pub struct PGGetUserByIdFunction;

const NAME: &str = crate::pg_catalog_func_fullname!("pg_get_userbyid");

impl fmt::Display for PGGetUserByIdFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, crate::pg_catalog_func_fullname!("PG_GET_USERBYID"))
    }
}

impl Function for PGGetUserByIdFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::string_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::uniform(
            1,
            vec![ConcreteDataType::uint32_datatype()],
            Volatility::Immutable,
        )
    }

    fn eval(&self, _func_ctx: FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        with_match_primitive_type_id!(columns[0].data_type().logical_type_id(), |$T| {
            let col = scalar_unary_op::<<$T as LogicalPrimitiveType>::Native, String, _>(&columns[0], pg_get_user_by_id, &mut EvalContext::default())?;
            Ok(Arc::new(col))
        }, {
            unreachable!()
        })
    }
}

fn pg_get_user_by_id<I>(table_oid: Option<I>, _ctx: &mut EvalContext) -> Option<String>
where
    I: AsPrimitive<u32>,
{
    // TODO(J0HN50N133): We lack way to get the user_info by a numeric value. Once we have it, we can implement this function.
    table_oid.map(|_| "".to_string())
}
