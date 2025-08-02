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
use api::v1::meta::{ReconcileRequest, ReconcileTable, ResolveStrategy};
use common_catalog::format_full_table_name;
use common_error::ext::BoxedError;
use common_macro::admin_fn;
use common_query::error::{
    MissingProcedureServiceHandlerSnafu, Result, TableMutationSnafu, UnsupportedInputDataTypeSnafu,
};
use common_query::prelude::{Signature, TypeSignature, Volatility};
use common_telemetry::debug;
use datatypes::prelude::*;
use session::context::QueryContextRef;
use session::table_name::table_name_to_full_name;
use snafu::ResultExt;

use crate::handlers::ProcedureServiceHandlerRef;
use crate::helper::parse_resolve_strategy;

const FN_NAME: &str = "reconcile_table";

/// A function to reconcile a table.
/// Returns the procedure id if success.
///
/// - `reconcile_table(table_name)`.
/// - `reconcile_table(table_name, resolve_strategy)`.
///
/// The parameters:
/// - `table_name`:  the table name
#[admin_fn(
    name = ReconcileTableFunction,
    display_name = reconcile_table,
    sig_fn = signature,
    ret = string
)]
pub(crate) async fn reconcile_table(
    procedure_service_handler: &ProcedureServiceHandlerRef,
    query_ctx: &QueryContextRef,
    params: &[ValueRef<'_>],
) -> Result<Value> {
    let (table_name, resolve_strategy) = match params {
        [ValueRef::String(table_name)] => (table_name, ResolveStrategy::UseLatest),
        [ValueRef::String(table_name), ValueRef::String(resolve_strategy)] => {
            (table_name, parse_resolve_strategy(resolve_strategy)?)
        }
        _ => {
            return UnsupportedInputDataTypeSnafu {
                function: FN_NAME,
                datatypes: params.iter().map(|v| v.data_type()).collect::<Vec<_>>(),
            }
            .fail()
        }
    };
    let (catalog_name, schema_name, table_name) = table_name_to_full_name(table_name, query_ctx)
        .map_err(BoxedError::new)
        .context(TableMutationSnafu)?;
    debug!(
        "reconcile table: {} with resolve_strategy: {:?}",
        format_full_table_name(&catalog_name, &schema_name, &table_name),
        resolve_strategy
    );
    let pid = procedure_service_handler
        .reconcile(ReconcileRequest {
            target: Some(Target::ReconcileTable(ReconcileTable {
                catalog_name,
                schema_name,
                table_name,
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
    Signature::one_of(
        vec![
            // reconcile_table(table_name)
            TypeSignature::Exact(vec![ConcreteDataType::string_datatype()]),
            // reconcile_table(table_name, resolve_strategy)
            TypeSignature::Exact(vec![
                ConcreteDataType::string_datatype(),
                ConcreteDataType::string_datatype(),
            ]),
        ],
        Volatility::Immutable,
    )
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::sync::Arc;

    use common_query::error::Error;
    use datatypes::vectors::{StringVector, VectorRef};

    use crate::admin::reconcile_table::ReconcileTableFunction;
    use crate::function::{AsyncFunction, FunctionContext};

    #[tokio::test]
    async fn test_reconcile_table() {
        common_telemetry::init_default_ut_logging();

        // reconcile_table(table_name)
        let f = ReconcileTableFunction;
        let args = vec![Arc::new(StringVector::from(vec!["test"])) as _];
        let result = f.eval(FunctionContext::mock(), &args).await.unwrap();
        let expect: VectorRef = Arc::new(StringVector::from(vec!["test_pid"]));
        assert_eq!(expect, result);

        // reconcile_table(table_name, resolve_strategy)
        let f = ReconcileTableFunction;
        let args = vec![
            Arc::new(StringVector::from(vec!["test"])) as _,
            Arc::new(StringVector::from(vec!["UseMetasrv"])) as _,
        ];
        let result = f.eval(FunctionContext::mock(), &args).await.unwrap();
        let expect: VectorRef = Arc::new(StringVector::from(vec!["test_pid"]));
        assert_eq!(expect, result);

        // unsupported input data type
        let f = ReconcileTableFunction;
        let args = vec![
            Arc::new(StringVector::from(vec!["test"])) as _,
            Arc::new(StringVector::from(vec!["UseMetasrv"])) as _,
            Arc::new(StringVector::from(vec!["10"])) as _,
        ];
        let err = f.eval(FunctionContext::mock(), &args).await.unwrap_err();
        assert_matches!(err, Error::UnsupportedInputDataType { .. });
    }
}
