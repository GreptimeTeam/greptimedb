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

use common_error::ext::BoxedError;
use common_macro::admin_fn;
use common_query::error::{
    ExecuteSnafu, InvalidFuncArgsSnafu, MissingFlowServiceHandlerSnafu, Result,
    UnsupportedInputDataTypeSnafu,
};
use datafusion_expr::{Signature, Volatility};
use datatypes::data_type::DataType;
use datatypes::value::{Value, ValueRef};
use session::context::QueryContextRef;
use snafu::{ensure, ResultExt};
use sql::ast::ObjectNamePartExt;
use sql::parser::ParserContext;
use store_api::storage::ConcreteDataType;

use crate::handlers::FlowServiceHandlerRef;

fn flush_signature() -> Signature {
    Signature::uniform(
        1,
        vec![ConcreteDataType::string_datatype().as_arrow_type()],
        Volatility::Immutable,
    )
}

#[admin_fn(
    name = FlushFlowFunction,
    display_name = flush_flow,
    sig_fn = flush_signature,
    ret = uint64
)]
pub(crate) async fn flush_flow(
    flow_service_handler: &FlowServiceHandlerRef,
    query_ctx: &QueryContextRef,
    params: &[ValueRef<'_>],
) -> Result<Value> {
    let (catalog_name, flow_name) = parse_flush_flow(params, query_ctx)?;

    let res = flow_service_handler
        .flush(&catalog_name, &flow_name, query_ctx.clone())
        .await?;
    let affected_rows = res.affected_rows;

    Ok(Value::from(affected_rows))
}

fn parse_flush_flow(
    params: &[ValueRef<'_>],
    query_ctx: &QueryContextRef,
) -> Result<(String, String)> {
    ensure!(
        params.len() == 1,
        InvalidFuncArgsSnafu {
            err_msg: format!(
                "The length of the args is not correct, expect 1, have: {}",
                params.len()
            ),
        }
    );

    let ValueRef::String(flow_name) = params[0] else {
        return UnsupportedInputDataTypeSnafu {
            function: "flush_flow",
            datatypes: params.iter().map(|v| v.data_type()).collect::<Vec<_>>(),
        }
        .fail();
    };
    let obj_name = ParserContext::parse_table_name(flow_name, query_ctx.sql_dialect())
        .map_err(BoxedError::new)
        .context(ExecuteSnafu)?;

    let (catalog_name, flow_name) = match &obj_name.0[..] {
        [flow_name] => (
            query_ctx.current_catalog().to_string(),
            flow_name.to_string_unquoted(),
        ),
        [catalog, flow_name] => (catalog.to_string_unquoted(), flow_name.to_string_unquoted()),
        _ => {
            return InvalidFuncArgsSnafu {
                err_msg: format!(
                    "expect flow name to be <catalog>.<flow-name> or <flow-name>, actual: {}",
                    obj_name
                ),
            }
            .fail()
        }
    };
    Ok((catalog_name, flow_name))
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use session::context::QueryContext;

    use super::*;
    use crate::function::FunctionContext;
    use crate::function_factory::ScalarFunctionFactory;

    #[test]
    fn test_flush_flow_metadata() {
        let factory: ScalarFunctionFactory = FlushFlowFunction::factory().into();
        let f = factory.provide(FunctionContext::mock());
        assert_eq!("flush_flow", f.name());
        assert_eq!(
            arrow::datatypes::DataType::UInt64,
            f.return_type(&[]).unwrap()
        );
        let expected_signature = datafusion_expr::Signature::uniform(
            1,
            vec![ConcreteDataType::string_datatype().as_arrow_type()],
            datafusion_expr::Volatility::Immutable,
        );
        assert_eq!(*f.signature(), expected_signature);
    }

    #[tokio::test]
    async fn test_missing_flow_service() {
        let factory: ScalarFunctionFactory = FlushFlowFunction::factory().into();
        let binding = factory.provide(FunctionContext::default());
        let f = binding.as_async().unwrap();

        let flow_name_array = Arc::new(arrow::array::StringArray::from(vec!["flow_name"]));

        let columnar_args = vec![datafusion_expr::ColumnarValue::Array(flow_name_array as _)];

        let func_args = datafusion::logical_expr::ScalarFunctionArgs {
            args: columnar_args,
            arg_fields: vec![Arc::new(arrow::datatypes::Field::new(
                "arg_0",
                arrow::datatypes::DataType::Utf8,
                false,
            ))],
            return_field: Arc::new(arrow::datatypes::Field::new(
                "result",
                arrow::datatypes::DataType::UInt64,
                true,
            )),
            number_rows: 1,
            config_options: Arc::new(datafusion_common::config::ConfigOptions::default()),
        };

        let result = f.invoke_async_with_args(func_args).await.unwrap_err();
        assert_eq!(
            "Execution error: Handler error: Missing FlowServiceHandler, not expected",
            result.to_string()
        );
    }

    #[test]
    fn test_parse_flow_args() {
        let testcases = [
            ("flow_name", ("greptime", "flow_name")),
            ("catalog.flow_name", ("catalog", "flow_name")),
        ];
        for (input, expected) in testcases.iter() {
            let args = vec![*input];
            let args = args.into_iter().map(ValueRef::String).collect::<Vec<_>>();

            let result = parse_flush_flow(&args, &QueryContext::arc()).unwrap();
            assert_eq!(*expected, (result.0.as_str(), result.1.as_str()));
        }
    }
}
