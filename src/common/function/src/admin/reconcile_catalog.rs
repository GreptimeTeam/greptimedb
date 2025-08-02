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

use api::v1::meta::reconcile_request::Target;
use api::v1::meta::{ReconcileCatalog, ReconcileRequest};
use common_macro::admin_fn;
use common_query::error::{
    InvalidFuncArgsSnafu, MissingProcedureServiceHandlerSnafu, Result,
    UnsupportedInputDataTypeSnafu,
};
use common_query::prelude::{Signature, TypeSignature, Volatility};
use common_telemetry::debug;
use datatypes::prelude::*;
use session::context::QueryContextRef;

use crate::handlers::ProcedureServiceHandlerRef;
use crate::helper::{
    cast_u32, default_parallelism, default_resolve_strategy, parse_resolve_strategy,
};

const FN_NAME: &str = "reconcile_catalog";

/// Get the string value from the params.
fn get_string<'a>(params: &'a [ValueRef<'a>], index: usize) -> Result<&'a str> {
    let ValueRef::String(s) = &params[index] else {
        return UnsupportedInputDataTypeSnafu {
            function: FN_NAME,
            datatypes: params.iter().map(|v| v.data_type()).collect::<Vec<_>>(),
        }
        .fail();
    };
    Ok(s)
}

/// A function to reconcile a catalog.
/// Returns the procedure id if success.
///
/// - `reconcile_catalog(resolve_strategy)`.
/// - `reconcile_catalog(resolve_strategy, parallelism)`.
///
/// - `reconcile_catalog()`.
#[admin_fn(
    name = ReconcileCatalogFunction,
    display_name = reconcile_catalog,
    sig_fn = signature,
    ret = string
)]
pub(crate) async fn reconcile_catalog(
    procedure_service_handler: &ProcedureServiceHandlerRef,
    query_ctx: &QueryContextRef,
    params: &[ValueRef<'_>],
) -> Result<Value> {
    let (resolve_strategy, parallelism) = match params.len() {
        0 => (default_resolve_strategy(), default_parallelism()),
        1 => (
            parse_resolve_strategy(get_string(params, 0)?)?,
            default_parallelism(),
        ),
        2 => {
            let Some(parallelism) = cast_u32(&params[1])? else {
                return UnsupportedInputDataTypeSnafu {
                    function: FN_NAME,
                    datatypes: params.iter().map(|v| v.data_type()).collect::<Vec<_>>(),
                }
                .fail();
            };
            (parse_resolve_strategy(get_string(params, 0)?)?, parallelism)
        }
        size => {
            return InvalidFuncArgsSnafu {
                err_msg: format!(
                    "The length of the args is not correct, expect 0, 1 or 2, have: {}",
                    size
                ),
            }
            .fail();
        }
    };
    debug!(
        "reconcile catalog with resolve_strategy: {:?}, parallelism: {}",
        resolve_strategy, parallelism
    );
    let pid = procedure_service_handler
        .reconcile(ReconcileRequest {
            target: Some(Target::ReconcileCatalog(ReconcileCatalog {
                catalog_name: query_ctx.current_catalog().to_string(),
                parallelism,
                resolve_strategy: resolve_strategy as i32,
            })),
            ..Default::default()
        })
        .await?;
    match pid {
        Some(pid) => Ok(Value::from(pid)),
        None => Ok(Value::Null),
    }
}

fn signature() -> Signature {
    let nums = ConcreteDataType::numerics();
    let mut signs = Vec::with_capacity(2 + nums.len());
    signs.extend([
        // reconcile_catalog()
        TypeSignature::NullAry,
        // reconcile_catalog(resolve_strategy)
        TypeSignature::Exact(vec![ConcreteDataType::string_datatype()]),
    ]);
    for sign in nums {
        // reconcile_catalog(resolve_strategy, parallelism)
        signs.push(TypeSignature::Exact(vec![
            ConcreteDataType::string_datatype(),
            sign,
        ]));
    }
    Signature::one_of(signs, Volatility::Immutable)
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::sync::Arc;

    use common_query::error::Error;
    use datatypes::vectors::{StringVector, UInt64Vector, VectorRef};

    use crate::admin::reconcile_catalog::ReconcileCatalogFunction;
    use crate::function::{AsyncFunction, FunctionContext};

    #[tokio::test]
    async fn test_reconcile_catalog() {
        common_telemetry::init_default_ut_logging();

        // reconcile_catalog()
        let f = ReconcileCatalogFunction;
        let args = vec![];
        let result = f.eval(FunctionContext::mock(), &args).await.unwrap();
        let expect: VectorRef = Arc::new(StringVector::from(vec!["test_pid"]));
        assert_eq!(expect, result);

        // reconcile_catalog(resolve_strategy)
        let f = ReconcileCatalogFunction;
        let args = vec![Arc::new(StringVector::from(vec!["UseMetasrv"])) as _];
        let result = f.eval(FunctionContext::mock(), &args).await.unwrap();
        let expect: VectorRef = Arc::new(StringVector::from(vec!["test_pid"]));
        assert_eq!(expect, result);

        // reconcile_catalog(resolve_strategy, parallelism)
        let f = ReconcileCatalogFunction;
        let args = vec![
            Arc::new(StringVector::from(vec!["UseLatest"])) as _,
            Arc::new(UInt64Vector::from_slice([10])) as _,
        ];
        let result = f.eval(FunctionContext::mock(), &args).await.unwrap();
        let expect: VectorRef = Arc::new(StringVector::from(vec!["test_pid"]));
        assert_eq!(expect, result);

        // unsupported input data type
        let f = ReconcileCatalogFunction;
        let args = vec![
            Arc::new(StringVector::from(vec!["UseLatest"])) as _,
            Arc::new(StringVector::from(vec!["test"])) as _,
        ];
        let err = f.eval(FunctionContext::mock(), &args).await.unwrap_err();
        assert_matches!(err, Error::UnsupportedInputDataType { .. });

        // invalid function args
        let f = ReconcileCatalogFunction;
        let args = vec![
            Arc::new(StringVector::from(vec!["UseLatest"])) as _,
            Arc::new(UInt64Vector::from_slice([10])) as _,
            Arc::new(StringVector::from(vec!["10"])) as _,
        ];
        let err = f.eval(FunctionContext::mock(), &args).await.unwrap_err();
        assert_matches!(err, Error::InvalidFuncArgs { .. });
    }
}
