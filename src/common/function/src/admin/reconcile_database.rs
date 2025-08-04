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
use api::v1::meta::{ReconcileDatabase, ReconcileRequest};
use common_macro::admin_fn;
use common_query::error::{
    InvalidFuncArgsSnafu, MissingProcedureServiceHandlerSnafu, Result,
    UnsupportedInputDataTypeSnafu,
};
use common_query::prelude::{Signature, TypeSignature, Volatility};
use common_telemetry::info;
use datatypes::prelude::*;
use session::context::QueryContextRef;

use crate::handlers::ProcedureServiceHandlerRef;
use crate::helper::{
    cast_u32, default_parallelism, default_resolve_strategy, get_string_from_params,
    parse_resolve_strategy,
};

const FN_NAME: &str = "reconcile_database";

/// A function to reconcile a database.
/// Returns the procedure id if success.
///
/// - `reconcile_database(database_name)`.
/// - `reconcile_database(database_name, resolve_strategy)`.
/// - `reconcile_database(database_name, resolve_strategy, parallelism)`.
///
/// The parameters:
/// - `database_name`:  the database name
#[admin_fn(
    name = ReconcileDatabaseFunction,
    display_name = reconcile_database,
    sig_fn = signature,
    ret = string
)]
pub(crate) async fn reconcile_database(
    procedure_service_handler: &ProcedureServiceHandlerRef,
    query_ctx: &QueryContextRef,
    params: &[ValueRef<'_>],
) -> Result<Value> {
    let (database_name, resolve_strategy, parallelism) = match params.len() {
        1 => (
            get_string_from_params(params, 0, FN_NAME)?,
            default_resolve_strategy(),
            default_parallelism(),
        ),
        2 => (
            get_string_from_params(params, 0, FN_NAME)?,
            parse_resolve_strategy(get_string_from_params(params, 1, FN_NAME)?)?,
            default_parallelism(),
        ),
        3 => {
            let Some(parallelism) = cast_u32(&params[2])? else {
                return UnsupportedInputDataTypeSnafu {
                    function: FN_NAME,
                    datatypes: params.iter().map(|v| v.data_type()).collect::<Vec<_>>(),
                }
                .fail();
            };
            (
                get_string_from_params(params, 0, FN_NAME)?,
                parse_resolve_strategy(get_string_from_params(params, 1, FN_NAME)?)?,
                parallelism,
            )
        }
        size => {
            return InvalidFuncArgsSnafu {
                err_msg: format!(
                    "The length of the args is not correct, expect 1, 2 or 3, have: {}",
                    size
                ),
            }
            .fail();
        }
    };
    info!(
        "Reconciling database: {}, resolve_strategy: {:?}, parallelism: {}",
        database_name, resolve_strategy, parallelism
    );
    let pid = procedure_service_handler
        .reconcile(ReconcileRequest {
            target: Some(Target::ReconcileDatabase(ReconcileDatabase {
                catalog_name: query_ctx.current_catalog().to_string(),
                database_name: database_name.to_string(),
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
        // reconcile_database(datanode_name)
        TypeSignature::Exact(vec![ConcreteDataType::string_datatype()]),
        // reconcile_database(database_name, resolve_strategy)
        TypeSignature::Exact(vec![
            ConcreteDataType::string_datatype(),
            ConcreteDataType::string_datatype(),
        ]),
    ]);
    for sign in nums {
        // reconcile_database(database_name, resolve_strategy, parallelism)
        signs.push(TypeSignature::Exact(vec![
            ConcreteDataType::string_datatype(),
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
    use datatypes::vectors::{StringVector, UInt32Vector, VectorRef};

    use crate::admin::reconcile_database::ReconcileDatabaseFunction;
    use crate::function::{AsyncFunction, FunctionContext};

    #[tokio::test]
    async fn test_reconcile_catalog() {
        common_telemetry::init_default_ut_logging();

        // reconcile_database(database_name)
        let f = ReconcileDatabaseFunction;
        let args = vec![Arc::new(StringVector::from(vec!["test"])) as _];
        let result = f.eval(FunctionContext::mock(), &args).await.unwrap();
        let expect: VectorRef = Arc::new(StringVector::from(vec!["test_pid"]));
        assert_eq!(expect, result);

        // reconcile_database(database_name, resolve_strategy)
        let f = ReconcileDatabaseFunction;
        let args = vec![
            Arc::new(StringVector::from(vec!["test"])) as _,
            Arc::new(StringVector::from(vec!["UseLatest"])) as _,
        ];
        let result = f.eval(FunctionContext::mock(), &args).await.unwrap();
        let expect: VectorRef = Arc::new(StringVector::from(vec!["test_pid"]));
        assert_eq!(expect, result);

        // reconcile_database(database_name, resolve_strategy, parallelism)
        let f = ReconcileDatabaseFunction;
        let args = vec![
            Arc::new(StringVector::from(vec!["test"])) as _,
            Arc::new(StringVector::from(vec!["UseLatest"])) as _,
            Arc::new(UInt32Vector::from_slice([10])) as _,
        ];
        let result = f.eval(FunctionContext::mock(), &args).await.unwrap();
        let expect: VectorRef = Arc::new(StringVector::from(vec!["test_pid"]));
        assert_eq!(expect, result);

        // invalid function args
        let f = ReconcileDatabaseFunction;
        let args = vec![
            Arc::new(StringVector::from(vec!["UseLatest"])) as _,
            Arc::new(UInt32Vector::from_slice([10])) as _,
            Arc::new(StringVector::from(vec!["v1"])) as _,
            Arc::new(StringVector::from(vec!["v2"])) as _,
        ];
        let err = f.eval(FunctionContext::mock(), &args).await.unwrap_err();
        assert_matches!(err, Error::InvalidFuncArgs { .. });

        // unsupported input data type
        let f = ReconcileDatabaseFunction;
        let args = vec![
            Arc::new(StringVector::from(vec!["UseLatest"])) as _,
            Arc::new(UInt32Vector::from_slice([10])) as _,
            Arc::new(StringVector::from(vec!["v1"])) as _,
        ];
        let err = f.eval(FunctionContext::mock(), &args).await.unwrap_err();
        assert_matches!(err, Error::UnsupportedInputDataType { .. });
    }
}
