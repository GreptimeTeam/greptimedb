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

use common_macro::admin_fn;
use common_query::error::{
    InvalidFuncArgsSnafu, MissingTableMutationHandlerSnafu, Result, UnsupportedInputDataTypeSnafu,
};
use common_query::prelude::{Signature, Volatility};
use datatypes::prelude::*;
use session::context::QueryContextRef;
use snafu::ensure;
use store_api::storage::RegionId;

use crate::handlers::TableMutationHandlerRef;
use crate::helper::cast_u64;

macro_rules! define_region_function {
    ($name: expr, $display_name_str: expr, $display_name: ident) => {
        /// A function to $display_name
        #[admin_fn(name = $name, display_name = $display_name_str, sig_fn = signature, ret = uint64)]
        pub(crate) async fn $display_name(
            table_mutation_handler: &TableMutationHandlerRef,
            query_ctx: &QueryContextRef,
            params: &[ValueRef<'_>],
        ) -> Result<Value> {
            ensure!(
                params.len() == 1,
                InvalidFuncArgsSnafu {
                    err_msg: format!(
                        "The length of the args is not correct, expect 1, have: {}",
                        params.len()
                    ),
                }
            );

            let Some(region_id) = cast_u64(&params[0])? else {
                return UnsupportedInputDataTypeSnafu {
                    function: stringify!($display_name_str),
                    datatypes: params.iter().map(|v| v.data_type()).collect::<Vec<_>>(),
                }
                .fail();
            };

            let affected_rows = table_mutation_handler
                .$display_name(RegionId::from_u64(region_id), query_ctx.clone())
                .await?;

            Ok(Value::from(affected_rows as u64))
        }
    };
}

define_region_function!(FlushRegionFunction, flush_region, flush_region);

define_region_function!(CompactRegionFunction, compact_region, compact_region);

fn signature() -> Signature {
    Signature::uniform(1, ConcreteDataType::numerics(), Volatility::Immutable)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_query::prelude::TypeSignature;
    use datatypes::vectors::UInt64Vector;

    use super::*;
    use crate::function::{Function, FunctionContext};

    macro_rules! define_region_function_test {
        ($name: ident, $func: ident) => {
            paste::paste! {
                #[test]
                fn [<test_ $name _misc>]() {
                    let f = $func;
                    assert_eq!(stringify!($name), f.name());
                    assert_eq!(
                        ConcreteDataType::uint64_datatype(),
                        f.return_type(&[]).unwrap()
                    );
                    assert!(matches!(f.signature(),
                                     Signature {
                                         type_signature: TypeSignature::Uniform(1, valid_types),
                                         volatility: Volatility::Immutable
                                     } if valid_types == ConcreteDataType::numerics()));
                }

                #[test]
                fn [<test_ $name _missing_table_mutation>]() {
                    let f = $func;

                    let args = vec![99];

                    let args = args
                        .into_iter()
                        .map(|arg| Arc::new(UInt64Vector::from_slice([arg])) as _)
                        .collect::<Vec<_>>();

                    let result = f.eval(FunctionContext::default(), &args).unwrap_err();
                    assert_eq!(
                        "Missing TableMutationHandler, not expected",
                        result.to_string()
                    );
                }

                #[test]
                fn [<test_ $name>]() {
                    let f = $func;


                    let args = vec![99];

                    let args = args
                        .into_iter()
                        .map(|arg| Arc::new(UInt64Vector::from_slice([arg])) as _)
                        .collect::<Vec<_>>();

                    let result = f.eval(FunctionContext::mock(), &args).unwrap();

                    let expect: VectorRef = Arc::new(UInt64Vector::from_slice([42]));
                    assert_eq!(expect, result);
                }
            }
        };
    }

    define_region_function_test!(flush_region, FlushRegionFunction);

    define_region_function_test!(compact_region, CompactRegionFunction);
}
