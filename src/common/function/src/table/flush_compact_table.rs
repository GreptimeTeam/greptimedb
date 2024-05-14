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
use std::str::FromStr;

use common_error::ext::BoxedError;
use common_macro::admin_fn;
use common_query::error::Error::ThreadJoin;
use common_query::error::{
    InvalidFuncArgsSnafu, MissingTableMutationHandlerSnafu, Result, TableMutationSnafu,
    UnsupportedInputDataTypeSnafu,
};
use common_query::prelude::{Signature, Volatility};
use common_telemetry::{error, info};
use datatypes::prelude::*;
use datatypes::vectors::VectorRef;
use session::context::QueryContextRef;
use session::table_name::table_name_to_full_name;
use snafu::{ensure, Location, OptionExt, ResultExt};
use table::requests::{CompactTableRequest, CompactType, FlushTableRequest, StrictWindowOptions};

use crate::ensure_greptime;
use crate::function::{Function, FunctionContext};
use crate::handlers::TableMutationHandlerRef;

#[admin_fn(
    name = "FlushTableFunction",
    display_name = "flush_table",
    sig_fn = "flush_signature",
    ret = "uint64"
)]
pub(crate) async fn flush_table(
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

    let ValueRef::String(table_name) = params[0] else {
        return UnsupportedInputDataTypeSnafu {
            function: "flush_table",
            datatypes: params.iter().map(|v| v.data_type()).collect::<Vec<_>>(),
        }
        .fail();
    };

    let (catalog_name, schema_name, table_name) = table_name_to_full_name(table_name, &query_ctx)
        .map_err(BoxedError::new)
        .context(TableMutationSnafu)?;

    let affected_rows = table_mutation_handler
        .flush(
            FlushTableRequest {
                catalog_name,
                schema_name,
                table_name,
            },
            query_ctx.clone(),
        )
        .await?;

    Ok(Value::from(affected_rows as u64))
}

#[admin_fn(
    name = "CompactTableFunction",
    display_name = "compact_table",
    sig_fn = "compact_signature",
    ret = "uint64"
)]
pub(crate) async fn compact_table(
    table_mutation_handler: &TableMutationHandlerRef,
    query_ctx: &QueryContextRef,
    params: &[ValueRef<'_>],
) -> Result<Value> {
    let request = parse_compact_params(params, query_ctx)?;
    info!("Compact table request: {:?}", request);

    let affected_rows = table_mutation_handler
        .compact(request, query_ctx.clone())
        .await?;

    Ok(Value::from(affected_rows as u64))
}

fn flush_signature() -> Signature {
    Signature::uniform(
        1,
        vec![ConcreteDataType::string_datatype()],
        Volatility::Immutable,
    )
}

fn compact_signature() -> Signature {
    Signature::variadic(
        vec![ConcreteDataType::string_datatype()],
        Volatility::Immutable,
    )
}

fn parse_compact_params(
    params: &[ValueRef<'_>],
    query_ctx: &QueryContextRef,
) -> Result<CompactTableRequest> {
    ensure!(
        params.len() >= 1,
        InvalidFuncArgsSnafu {
            err_msg: "The length of the args is not correct, expect at least 1",
        }
    );

    let ValueRef::String(table_name) = params[0] else {
        return UnsupportedInputDataTypeSnafu {
            function: "compact_table",
            datatypes: params.iter().map(|v| v.data_type()).collect::<Vec<_>>(),
        }
        .fail();
    };

    let (catalog_name, schema_name, table_name) = table_name_to_full_name(table_name, &query_ctx)
        .map_err(BoxedError::new)
        .context(TableMutationSnafu)?;

    let ty = if params.len() > 1 {
        let ValueRef::String(compact_ty_str) = params[1] else {
            return UnsupportedInputDataTypeSnafu {
                function: "compact_table",
                datatypes: params.iter().map(|v| v.data_type()).collect::<Vec<_>>(),
            }
            .fail();
        };
        if compact_ty_str.eq_ignore_ascii_case("strict_window") {
            let window = params.get(2).and_then(|v| {
                if let ValueRef::String(window_str) = v {
                    i64::from_str(window_str).ok()
                } else {
                    None
                }
            });
            CompactType::StrictWindow(StrictWindowOptions {
                window_size: window,
            })
        } else {
            CompactType::Regular
        }
    } else {
        CompactType::Regular
    };

    Ok(CompactTableRequest {
        catalog_name,
        schema_name,
        table_name,
        compact_type: ty,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_query::prelude::TypeSignature;
    use datatypes::vectors::{StringVector, UInt64Vector};

    use super::*;

    macro_rules! define_table_function_test {
        ($name: ident, $func: ident) => {
            paste::paste!{
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
                                     } if valid_types == vec![ConcreteDataType::string_datatype()]));
                }

                #[test]
                fn [<test_ $name _missing_table_mutation>]() {
                    let f = $func;

                    let args = vec!["test"];

                    let args = args
                        .into_iter()
                        .map(|arg| Arc::new(StringVector::from(vec![arg])) as _)
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


                    let args = vec!["test"];

                    let args = args
                        .into_iter()
                        .map(|arg| Arc::new(StringVector::from(vec![arg])) as _)
                        .collect::<Vec<_>>();

                    let result = f.eval(FunctionContext::mock(), &args).unwrap();

                    let expect: VectorRef = Arc::new(UInt64Vector::from_slice([42]));
                    assert_eq!(expect, result);
                }
            }
        }
    }

    define_table_function_test!(flush_table, FlushTableFunction);

    define_table_function_test!(compact_table, CompactTableFunction);
}
