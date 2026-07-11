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

use arrow::datatypes::DataType as ArrowDataType;
use common_error::ext::BoxedError;
use common_macro::admin_fn;
use common_query::error::{
    InvalidFuncArgsSnafu, MissingProcedureServiceHandlerSnafu, ProcedureServiceSnafu, Result,
    UnsupportedInputDataTypeSnafu,
};
use datafusion_expr::{Signature, Volatility};
use datatypes::prelude::{Value, ValueRef};
use session::context::QueryContextRef;
use session::table_name::table_name_to_full_name;
use snafu::{ResultExt, ensure};
use table::table_name::TableName;

use crate::handlers::ProcedureServiceHandlerRef;

/// Purges a dropped table after the ADMIN statement layer authorizes the request.
/// Name validation retains the session helper's existing catalog-only permission check.
#[admin_fn(
    name = PurgeTableFunction,
    display_name = purge_table,
    sig_fn = purge_table_signature,
    ret = uint64,
    single_row
)]
pub(crate) async fn purge_table(
    procedure_service_handler: &ProcedureServiceHandlerRef,
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
            function: "purge_table",
            datatypes: params
                .iter()
                .map(|value| value.data_type())
                .collect::<Vec<_>>(),
        }
        .fail();
    };
    let (catalog_name, schema_name, table_name) = table_name_to_full_name(table_name, query_ctx)
        .map_err(BoxedError::new)
        .context(ProcedureServiceSnafu)?;

    procedure_service_handler
        .purge_table(
            TableName::new(catalog_name, schema_name, table_name),
            query_ctx.clone(),
        )
        .await?;
    Ok(Value::from(0_u64))
}

fn purge_table_signature() -> Signature {
    Signature::uniform(1, vec![ArrowDataType::Utf8], Volatility::Immutable)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use api::v1::meta::ReconcileRequest;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType as ArrowDataType, Field};
    use async_trait::async_trait;
    use catalog::CatalogManagerRef;
    use common_meta::rpc::procedure::{
        GcRegionsRequest, GcResponse, GcTableRequest, ManageRegionFollowerRequest,
        MigrateRegionRequest, ProcedureStateResponse,
    };
    use common_query::error::{InvalidFuncArgsSnafu, Result};
    use datafusion_expr::{ColumnarValue, Signature, TypeSignature, Volatility};
    use datatypes::prelude::{Value, ValueRef};
    use session::context::{QueryContext, QueryContextBuilder, QueryContextRef};
    use table::table_name::TableName;

    use super::{PurgeTableFunction, purge_table};
    use crate::function_factory::ScalarFunctionFactory;
    use crate::function_registry::{FUNCTION_REGISTRY, get_admin_function};
    use crate::handlers::{ProcedureServiceHandler, ProcedureServiceHandlerRef};
    use crate::state::FunctionState;

    #[derive(Default)]
    struct RecordingHandler {
        calls: Mutex<Vec<(TableName, QueryContextRef)>>,
        fail: bool,
    }

    #[async_trait]
    impl ProcedureServiceHandler for RecordingHandler {
        async fn purge_table(
            &self,
            table_name: TableName,
            query_ctx: QueryContextRef,
        ) -> Result<()> {
            if self.fail {
                return InvalidFuncArgsSnafu {
                    err_msg: "purge failed",
                }
                .fail();
            }
            self.calls.lock().unwrap().push((table_name, query_ctx));
            Ok(())
        }

        async fn migrate_region(&self, _: MigrateRegionRequest) -> Result<Option<String>> {
            unreachable!()
        }
        async fn reconcile(&self, _: ReconcileRequest) -> Result<Option<String>> {
            unreachable!()
        }
        async fn query_procedure_state(&self, _: &str) -> Result<ProcedureStateResponse> {
            unreachable!()
        }
        async fn manage_region_follower(&self, _: ManageRegionFollowerRequest) -> Result<()> {
            unreachable!()
        }
        fn catalog_manager(&self) -> &CatalogManagerRef {
            unreachable!()
        }
        async fn gc_regions(&self, _: GcRegionsRequest) -> Result<GcResponse> {
            unreachable!()
        }
        async fn gc_table(&self, _: GcTableRequest) -> Result<GcResponse> {
            unreachable!()
        }
    }

    #[test]
    fn test_purge_table_is_admin_only() {
        assert!(get_admin_function("purge_table").is_some());
        assert!(FUNCTION_REGISTRY.get_function("purge_table").is_none());
    }

    #[tokio::test]
    async fn test_admin_only_factory_executes_without_registry_leak() {
        let handler = Arc::new(RecordingHandler::default());
        let state = FunctionState {
            procedure_service_handler: Some(handler.clone()),
            ..Default::default()
        };
        let function =
            get_admin_function("purge_table")
                .unwrap()
                .provide(crate::function::FunctionContext {
                    query_ctx: QueryContext::arc(),
                    state: Arc::new(state),
                });
        let args = datafusion_expr::ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(StringArray::from(vec![
                "foo",
            ])))],
            arg_fields: vec![Arc::new(Field::new("arg_0", ArrowDataType::Utf8, false))],
            return_field: Arc::new(Field::new("result", ArrowDataType::UInt64, false)),
            number_rows: 1,
            config_options: Arc::new(datafusion_common::config::ConfigOptions::default()),
        };

        function
            .as_async()
            .unwrap()
            .invoke_async_with_args(args)
            .await
            .unwrap();

        assert_eq!(1, handler.calls.lock().unwrap().len());
        assert!(FUNCTION_REGISTRY.get_function("purge_table").is_none());
    }

    #[test]
    fn test_purge_table_signature() {
        let factory: ScalarFunctionFactory = PurgeTableFunction::factory().into();
        let function = factory.provide(crate::function::FunctionContext::mock());
        assert_eq!("purge_table", function.name());
        assert_eq!(ArrowDataType::UInt64, function.return_type(&[]).unwrap());
        assert!(matches!(
            function.signature(),
            Signature { type_signature: TypeSignature::Uniform(1, types), volatility: Volatility::Immutable, .. }
                if types == &vec![ArrowDataType::Utf8]
        ));
    }

    #[tokio::test]
    async fn test_purge_table_rejects_multi_row_before_handler_call() {
        let handler = Arc::new(RecordingHandler::default());
        let state = FunctionState {
            procedure_service_handler: Some(handler.clone()),
            ..Default::default()
        };
        let factory: ScalarFunctionFactory = PurgeTableFunction::factory().into();
        let function = factory.provide(crate::function::FunctionContext {
            query_ctx: QueryContext::arc(),
            state: Arc::new(state),
        });
        let args = datafusion_expr::ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(StringArray::from(vec![
                Some("foo"),
                None,
            ])))],
            arg_fields: vec![Arc::new(Field::new("arg_0", ArrowDataType::Utf8, true))],
            return_field: Arc::new(Field::new("result", ArrowDataType::UInt64, true)),
            number_rows: 2,
            config_options: Arc::new(datafusion_common::config::ConfigOptions::default()),
        };

        assert!(
            function
                .as_async()
                .unwrap()
                .invoke_async_with_args(args)
                .await
                .is_err()
        );
        assert!(handler.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_purge_table_resolves_names_and_returns_zero() {
        let handler = Arc::new(RecordingHandler::default());
        let handler_ref: ProcedureServiceHandlerRef = handler.clone();
        let query_ctx = QueryContextBuilder::default()
            .current_catalog("catalog".to_string())
            .current_schema("schema".to_string())
            .build()
            .into();

        for (input, expected) in [
            ("foo", TableName::new("catalog", "schema", "foo")),
            ("other.foo", TableName::new("catalog", "other", "foo")),
            (
                "catalog.other.foo",
                TableName::new("catalog", "other", "foo"),
            ),
        ] {
            assert_eq!(
                Value::from(0_u64),
                purge_table(&handler_ref, &query_ctx, &[ValueRef::String(input)])
                    .await
                    .unwrap()
            );
            let calls = handler.calls.lock().unwrap();
            assert_eq!(expected, calls.last().unwrap().0);
            assert!(Arc::ptr_eq(&query_ctx, &calls.last().unwrap().1));
        }
    }

    #[tokio::test]
    async fn test_purge_table_rejects_invalid_arguments() {
        let recording_handler = Arc::new(RecordingHandler::default());
        let handler: ProcedureServiceHandlerRef = recording_handler.clone();
        for params in [vec![], vec![ValueRef::String("a"), ValueRef::String("b")]] {
            assert!(
                purge_table(&handler, &QueryContext::arc(), &params)
                    .await
                    .is_err()
            );
        }
        assert!(
            purge_table(&handler, &QueryContext::arc(), &[ValueRef::Int64(1)])
                .await
                .is_err()
        );
        assert!(
            purge_table(&handler, &QueryContext::arc(), &[ValueRef::Null])
                .await
                .is_err()
        );
        assert!(
            purge_table(
                &handler,
                &QueryContext::arc(),
                &[ValueRef::String("a.b.c.d")]
            )
            .await
            .is_err()
        );
        assert!(recording_handler.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_purge_table_propagates_handler_error() {
        let handler: ProcedureServiceHandlerRef = Arc::new(RecordingHandler {
            fail: true,
            ..Default::default()
        });
        let error = purge_table(&handler, &QueryContext::arc(), &[ValueRef::String("foo")])
            .await
            .unwrap_err();
        assert!(error.to_string().contains("purge failed"));
    }
}
