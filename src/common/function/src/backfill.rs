// Copyright 2023-2026 GrepTime Inc.
//
// This file is part of the GreptimeDB Enterprise Edition and is licensed under
// the GreptimeDB Enterprise License. You may not use this file except in
// compliance with that license. A copy of the license is available at the root
// of this repository in the file LICENSE-ENTERPRISE.
//
// Unless required by applicable law or agreed to in writing, this software is
// distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied.

//! `ADMIN flow_backfill(...)` and `ADMIN flow_backfill_status(...)`.
//!
//! Both are routed operator tools shaped exactly like `ADMIN flush_flow`:
//! they resolve `[<catalog>.]<flow>` and ask the flow service handler, which
//! forwards the request to the flownode(s) that own the flow. Nothing is
//! decided here: the answer comes back through the response extensions.

use arrow::datatypes::DataType as ArrowDataType;
use chrono::DateTime;
use common_error::ext::{BoxedError, PlainError};
use common_error::status_code::StatusCode;
use common_macro::admin_fn;
use common_query::error::{
    ExecuteSnafu, InvalidFuncArgsSnafu, MissingFlowServiceHandlerSnafu, Result,
    UnsupportedInputDataTypeSnafu,
};
use datafusion_expr::{Signature, TypeSignature, Volatility};
use datatypes::value::{Value, ValueRef};
use session::context::QueryContextRef;
use snafu::{ResultExt, ensure};
use sql::ast::ObjectNamePartExt;
use sql::parser::ParserContext;

use crate::handlers::FlowServiceHandlerRef;

/// Extension key of the attempt id a backfill request answers with.
pub(crate) const FLOW_BACKFILL_ATTEMPT_ID_KEY: &str = "flow_backfill_attempt_id";
/// Extension key of the status JSON a backfill status request answers with.
pub(crate) const FLOW_BACKFILL_STATUS_KEY: &str = "flow_backfill_status";

/// `ADMIN flow_backfill(<flow>, <startMillis>, <endMillis>)`.
///
/// Returns the attempt id, which is meaningful only to the process that
/// answered and survives no restart.
///
/// The two endpoints are literals of exactly two forms, and may differ from
/// each other: a signed Int64 of epoch milliseconds of source event time, or a
/// single-quoted RFC 3339 timestamp with an explicit offset
/// (`'2025-01-01T00:00:00Z'`, `'2025-01-01 08:00:00+08:00'`). A timestamp is
/// converted to exact epoch milliseconds: a value finer than one millisecond
/// is refused instead of being truncated, because rounding would move a range
/// boundary.
#[admin_fn(
    name = FlowBackfillFunction,
    display_name = flow_backfill,
    sig_fn = flow_backfill_signature,
    ret = string
)]
pub(crate) async fn flow_backfill(
    flow_service_handler: &FlowServiceHandlerRef,
    query_ctx: &QueryContextRef,
    params: &[ValueRef<'_>],
) -> Result<Value> {
    ensure!(
        params.len() == 3,
        InvalidFuncArgsSnafu {
            err_msg: format!(
                "flow_backfill expects 3 arguments (<flow>, <startMillis>, <endMillis>), received {}",
                params.len()
            ),
        }
    );
    let (catalog_name, flow_name) = parse_flow_name(&params[0], query_ctx, "flow_backfill")?;
    let start_ms = parse_millis(&params[1], "startMillis")?;
    let end_ms = parse_millis(&params[2], "endMillis")?;

    let res = flow_service_handler
        .backfill(
            &catalog_name,
            &flow_name,
            start_ms,
            end_ms,
            query_ctx.clone(),
        )
        .await?;

    Ok(Value::from(
        response_extension(&res, FLOW_BACKFILL_ATTEMPT_ID_KEY)?.as_str(),
    ))
}

/// `ADMIN flow_backfill_status(<flow>)`.
///
/// Returns the JSON status of the attempt the answering process currently
/// knows about; `null` when it knows none.
#[admin_fn(
    name = FlowBackfillStatusFunction,
    display_name = flow_backfill_status,
    sig_fn = flow_backfill_status_signature,
    ret = string
)]
pub(crate) async fn flow_backfill_status(
    flow_service_handler: &FlowServiceHandlerRef,
    query_ctx: &QueryContextRef,
    params: &[ValueRef<'_>],
) -> Result<Value> {
    ensure!(
        params.len() == 1,
        InvalidFuncArgsSnafu {
            err_msg: format!(
                "flow_backfill_status expects 1 argument (<flow>), received {}",
                params.len()
            ),
        }
    );
    let (catalog_name, flow_name) = parse_flow_name(&params[0], query_ctx, "flow_backfill_status")?;

    let res = flow_service_handler
        .backfill_status(&catalog_name, &flow_name, query_ctx.clone())
        .await?;

    Ok(Value::from(
        response_extension(&res, FLOW_BACKFILL_STATUS_KEY)?.as_str(),
    ))
}

/// Parses `<flow>` or `<catalog>.<flow>`, defaulting to the current catalog.
fn parse_flow_name(
    param: &ValueRef<'_>,
    query_ctx: &QueryContextRef,
    function: &'static str,
) -> Result<(String, String)> {
    let ValueRef::String(flow_name) = param else {
        return UnsupportedInputDataTypeSnafu {
            function,
            datatypes: vec![param.data_type()],
        }
        .fail();
    };
    let obj_name = ParserContext::parse_table_name(flow_name, query_ctx.sql_dialect())
        .map_err(BoxedError::new)
        .context(ExecuteSnafu)?;

    match &obj_name.0[..] {
        [flow_name] => Ok((
            query_ctx.current_catalog().to_string(),
            flow_name.to_string_unquoted(),
        )),
        [catalog, flow_name] => Ok((catalog.to_string_unquoted(), flow_name.to_string_unquoted())),
        _ => InvalidFuncArgsSnafu {
            err_msg: format!(
                "expect flow name to be <catalog>.<flow-name> or <flow-name>, actual: {}",
                obj_name
            ),
        }
        .fail(),
    }
}

/// Parses one range endpoint: epoch milliseconds as an Int64, or an RFC 3339
/// timestamp string with an explicit offset.
fn parse_millis(param: &ValueRef<'_>, name: &str) -> Result<i64> {
    match param {
        ValueRef::Int64(millis) => Ok(*millis),
        ValueRef::String(timestamp) => parse_rfc3339_millis(timestamp, name),
        other => InvalidFuncArgsSnafu {
            err_msg: format!(
                "{name} must be an Int64 epoch-millisecond value or a single-quoted RFC 3339 \
                 timestamp, actual type: {:?}",
                other.data_type()
            ),
        }
        .fail(),
    }
}

/// Converts an RFC 3339 timestamp with an explicit offset to epoch milliseconds.
fn parse_rfc3339_millis(timestamp: &str, name: &str) -> Result<i64> {
    // The engine's standard RFC 3339 parser: fixed-width zero-padded fields, `T`
    // (or `t`, or one space) and an explicit offset. A timestamp without an
    // offset names no instant, and no session time zone is applied here.
    let parsed = DateTime::parse_from_rfc3339(timestamp).map_err(|err| {
        InvalidFuncArgsSnafu {
            err_msg: format!(
                "{name} is not an RFC 3339 timestamp with an explicit offset: {timestamp}, {err}"
            ),
        }
        .build()
    })?;
    // A boundary is never rounded: a fractional second finer than one
    // millisecond has no exact epoch-millisecond value.
    ensure!(
        parsed.timestamp_subsec_nanos() % 1_000_000 == 0,
        InvalidFuncArgsSnafu {
            err_msg: format!(
                "{name} is finer than one millisecond: {timestamp}; truncating it would move the \
                 range boundary"
            ),
        }
    );
    // The offset decides the instant, so a non-UTC offset is converted rather
    // than reinterpreted as UTC.
    Ok(parsed.timestamp_millis())
}

/// Extracts a UTF-8 extension an answering flownode must have set.
fn response_extension(response: &api::v1::flow::FlowResponse, key: &str) -> Result<String> {
    let bytes = response
        .extensions
        .get(key)
        .ok_or_else(|| {
            BoxedError::new(PlainError::new(
                format!("flow response does not carry the `{key}` extension"),
                StatusCode::Internal,
            ))
        })
        .context(ExecuteSnafu)?;

    String::from_utf8(bytes.clone())
        .map_err(|err| {
            BoxedError::new(PlainError::new(
                format!("`{key}` extension of the flow response is not UTF-8: {err}"),
                StatusCode::Internal,
            ))
        })
        .context(ExecuteSnafu)
}

fn flow_backfill_signature() -> Signature {
    // Int64 epoch milliseconds or a quoted RFC 3339 timestamp for each endpoint;
    // either form may be used for either endpoint. Keep exact types so DataFusion
    // never coerces the integer literals to Float64.
    let endpoints = [ArrowDataType::Int64, ArrowDataType::Utf8];
    let mut sigs = Vec::with_capacity(endpoints.len() * endpoints.len());
    for start in &endpoints {
        for end in &endpoints {
            sigs.push(TypeSignature::Exact(vec![
                ArrowDataType::Utf8,
                start.clone(),
                end.clone(),
            ]));
        }
    }

    Signature::one_of(sigs, Volatility::Immutable)
}

fn flow_backfill_status_signature() -> Signature {
    Signature::uniform(1, vec![ArrowDataType::Utf8], Volatility::Immutable)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::value::OrderedFloat;
    use session::context::QueryContext;

    use super::*;
    use crate::function::FunctionContext;
    use crate::function_factory::ScalarFunctionFactory;

    const ZERO_UTC_MILLIS: i64 = 1_735_689_600_000; // 2025-01-01T00:00:00Z

    #[test]
    fn test_flow_backfill_metadata() {
        let factory: ScalarFunctionFactory = FlowBackfillFunction::factory().into();
        let f = factory.provide(FunctionContext::mock());
        assert_eq!("flow_backfill", f.name());
        assert_eq!(ArrowDataType::Utf8, f.return_type(&[]).unwrap());
        assert_eq!(*f.signature(), flow_backfill_signature());

        let factory: ScalarFunctionFactory = FlowBackfillStatusFunction::factory().into();
        let f = factory.provide(FunctionContext::mock());
        assert_eq!("flow_backfill_status", f.name());
        assert_eq!(ArrowDataType::Utf8, f.return_type(&[]).unwrap());
        assert_eq!(*f.signature(), flow_backfill_status_signature());
    }

    #[test]
    fn test_flow_backfill_registration() {
        for name in ["flow_backfill", "flow_backfill_status"] {
            let factory = crate::function_registry::get_admin_function(name)
                .unwrap_or_else(|| panic!("{name} is not registered as an ADMIN function"));
            assert!(
                crate::function_registry::FUNCTION_REGISTRY
                    .get_function(name)
                    .is_none(),
                "{name} must not resolve through the ordinary scalar registry"
            );
            let function = factory.provide(FunctionContext::mock());
            assert_eq!(name, function.name());
            assert_eq!(ArrowDataType::Utf8, function.return_type(&[]).unwrap());
        }
    }

    #[test]
    fn test_parse_flow_name_defaults_to_current_catalog() {
        let testcases = [
            ("flow_name", ("greptime", "flow_name")),
            ("catalog.flow_name", ("catalog", "flow_name")),
        ];
        for (input, expected) in testcases.iter() {
            let result = parse_flow_name(
                &ValueRef::String(input),
                &QueryContext::arc(),
                "flow_backfill",
            )
            .unwrap();
            assert_eq!(*expected, (result.0.as_str(), result.1.as_str()));
        }
    }

    #[test]
    fn test_parse_flow_name_rejects_non_string() {
        let err = parse_flow_name(&ValueRef::Int64(1), &QueryContext::arc(), "flow_backfill")
            .unwrap_err();
        assert!(matches!(
            err,
            common_query::error::Error::UnsupportedInputDataType { .. }
        ));
    }

    #[test]
    fn test_parse_millis_integer() {
        for millis in [-3_600_000, 0, 1000, i64::MAX, i64::MIN] {
            assert_eq!(
                millis,
                parse_millis(&ValueRef::Int64(millis), "startMillis").unwrap()
            );
        }
    }

    #[test]
    fn test_parse_millis_timestamp_with_offset() {
        // `Z`, an explicit zero offset, a non-UTC offset that names the same
        // instant, and a space instead of `T` are all the same boundary.
        for timestamp in [
            "2025-01-01T00:00:00Z",
            "2025-01-01T00:00:00+00:00",
            "2025-01-01T08:00:00+08:00",
            "2025-01-01 00:00:00+00:00",
            "2025-01-01 08:00:00+08:00",
        ] {
            assert_eq!(
                ZERO_UTC_MILLIS,
                parse_millis(&ValueRef::String(timestamp), "endMillis").unwrap(),
                "{timestamp}"
            );
        }
        // A whole-millisecond fraction is exact.
        assert_eq!(
            ZERO_UTC_MILLIS + 123,
            parse_millis(&ValueRef::String("2025-01-01T00:00:00.123Z"), "startMillis").unwrap()
        );
        // Below the epoch the boundary is floored by the parser, not shifted.
        assert_eq!(
            -1,
            parse_millis(&ValueRef::String("1969-12-31T23:59:59.999Z"), "startMillis").unwrap()
        );
    }

    #[test]
    fn test_parse_millis_rejects_sub_millisecond() {
        for timestamp in [
            "2025-01-01T00:00:00.0001Z",
            "2025-01-01T00:00:00.123456Z",
            "2025-01-01T00:00:00.000000001Z",
        ] {
            let err = parse_millis(&ValueRef::String(timestamp), "startMillis").unwrap_err();
            assert!(
                err.to_string().contains("finer than one millisecond"),
                "{timestamp}: {err}"
            );
        }
    }

    #[test]
    fn test_parse_millis_rejects_missing_offset_and_other_types() {
        for timestamp in [
            "2025-01-01T00:00:00",
            "2025-01-01 00:00:00",
            "not a timestamp",
        ] {
            let err = parse_millis(&ValueRef::String(timestamp), "startMillis").unwrap_err();
            assert!(
                err.to_string()
                    .contains("not an RFC 3339 timestamp with an explicit offset"),
                "{timestamp}: {err}"
            );
        }
        for value in [
            ValueRef::UInt64(1),
            ValueRef::Int32(1),
            ValueRef::Float64(OrderedFloat(1.0)),
            ValueRef::Null,
            ValueRef::Boolean(true),
        ] {
            let err = parse_millis(&value, "startMillis").unwrap_err();
            assert!(
                matches!(err, common_query::error::Error::InvalidFuncArgs { .. }),
                "{value:?}: {err}"
            );
        }
    }

    #[tokio::test]
    async fn test_flow_backfill_rejects_wrong_arity_and_types() {
        let ctx = FunctionContext::mock();
        let handler = ctx.state.flow_service_handler.as_ref().unwrap();

        for params in [
            vec![],
            vec![ValueRef::String("flow")],
            vec![ValueRef::String("flow"), ValueRef::Int64(0)],
            vec![
                ValueRef::String("flow"),
                ValueRef::Int64(0),
                ValueRef::Int64(1),
                ValueRef::Int64(2),
            ],
        ] {
            let err = flow_backfill(handler, &ctx.query_ctx, &params)
                .await
                .unwrap_err();
            assert!(
                err.to_string().contains("expects 3 arguments"),
                "{params:?}: {err}"
            );
        }

        // A Float64 or Null endpoint is not an Int64 millisecond value nor a
        // quoted timestamp.
        for endpoint in [ValueRef::Float64(OrderedFloat(1.0)), ValueRef::Null] {
            let err = flow_backfill(
                handler,
                &ctx.query_ctx,
                &[ValueRef::String("flow"), endpoint, ValueRef::Int64(1)],
            )
            .await
            .unwrap_err();
            assert!(
                matches!(err, common_query::error::Error::InvalidFuncArgs { .. }),
                "{err}"
            );
        }

        for params in [vec![], vec![ValueRef::String("a"), ValueRef::String("b")]] {
            let err = flow_backfill_status(handler, &ctx.query_ctx, &params)
                .await
                .unwrap_err();
            assert!(
                err.to_string().contains("expects 1 argument"),
                "{params:?}: {err}"
            );
        }
        let err = flow_backfill_status(handler, &ctx.query_ctx, &[ValueRef::Int64(1)])
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            common_query::error::Error::UnsupportedInputDataType { .. }
        ));
    }

    #[tokio::test]
    async fn test_missing_extension_is_execute_error() {
        let ctx = FunctionContext::mock();
        let handler = ctx.state.flow_service_handler.as_ref().unwrap();

        // The mock handler answers without extensions.
        let err = flow_backfill(
            handler,
            &ctx.query_ctx,
            &[
                ValueRef::String("catalog.flow"),
                ValueRef::Int64(-1000),
                ValueRef::Int64(1000),
            ],
        )
        .await
        .unwrap_err();
        assert!(
            matches!(err, common_query::error::Error::Execute { .. }),
            "{err}"
        );
        assert!(
            err.to_string().contains(FLOW_BACKFILL_ATTEMPT_ID_KEY),
            "{err}"
        );

        let err = flow_backfill_status(handler, &ctx.query_ctx, &[ValueRef::String("flow")])
            .await
            .unwrap_err();
        assert!(
            matches!(err, common_query::error::Error::Execute { .. }),
            "{err}"
        );
        assert!(err.to_string().contains(FLOW_BACKFILL_STATUS_KEY), "{err}");
    }

    #[tokio::test]
    async fn test_missing_flow_service() {
        let factory: ScalarFunctionFactory = FlowBackfillFunction::factory().into();
        let binding = factory.provide(FunctionContext::default());
        let f = binding.as_async().unwrap();

        let args = vec![
            datafusion_expr::ColumnarValue::Scalar(datafusion_common::ScalarValue::Utf8(Some(
                "flow".to_string(),
            ))),
            datafusion_expr::ColumnarValue::Scalar(datafusion_common::ScalarValue::Int64(Some(0))),
            datafusion_expr::ColumnarValue::Scalar(datafusion_common::ScalarValue::Int64(Some(1))),
        ];
        let arg_fields = vec![
            Arc::new(arrow::datatypes::Field::new(
                "arg_0",
                ArrowDataType::Utf8,
                false,
            )),
            Arc::new(arrow::datatypes::Field::new(
                "arg_1",
                ArrowDataType::Int64,
                false,
            )),
            Arc::new(arrow::datatypes::Field::new(
                "arg_2",
                ArrowDataType::Int64,
                false,
            )),
        ];

        let result = f
            .invoke_async_with_args(datafusion::logical_expr::ScalarFunctionArgs {
                args,
                arg_fields,
                return_field: Arc::new(arrow::datatypes::Field::new(
                    "result",
                    ArrowDataType::Utf8,
                    true,
                )),
                number_rows: 1,
                config_options: Arc::new(datafusion_common::config::ConfigOptions::default()),
            })
            .await
            .unwrap_err();
        assert_eq!(
            "Execution error: Missing FlowServiceHandler, not expected",
            result.to_string()
        );
    }
}
