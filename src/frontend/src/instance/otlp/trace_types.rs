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

use api::v1::value::ValueData;
use api::v1::{ColumnDataType, Row, Rows};
use servers::error::{self, Result as ServerResult};
use servers::otlp::coerce::{
    coerce_value_data, is_supported_trace_coercion, resolve_new_trace_column_type,
    trace_value_datatype,
};

use crate::instance::otlp::trace_semconv::trace_semconv_fixed_type;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TraceReconcileDecision {
    UseExisting(ColumnDataType),
    UseRequestLocal(ColumnDataType),
    AlterExistingTo(ColumnDataType),
}

impl TraceReconcileDecision {
    pub(super) fn target_type(self) -> ColumnDataType {
        match self {
            Self::UseExisting(target_type)
            | Self::UseRequestLocal(target_type)
            | Self::AlterExistingTo(target_type) => target_type,
        }
    }

    pub(super) fn requires_alter(self) -> bool {
        matches!(self, Self::AlterExistingTo(_))
    }
}

/// Describes a column rewrite before its row values have been validated.
#[derive(Debug)]
pub(super) struct PendingTraceColumnRewrite {
    pub(super) col_idx: usize,
    pub(super) target_type: ColumnDataType,
    pub(super) column_name: String,
}

/// Holds the schema and value rewrites prepared for atomic application.
#[derive(Debug)]
pub(super) struct PreparedTraceColumnRewrites {
    columns: Vec<PreparedTraceColumnRewrite>,
    values: Vec<PreparedTraceValueRewrite>,
}

/// Reports the column whose trace value could not be rewritten.
#[derive(Debug)]
pub(super) struct TraceColumnRewriteError {
    pub(super) error: servers::error::Error,
    pub(super) column_name: String,
}

/// Updates one request column to its reconciled datatype.
#[derive(Debug)]
struct PreparedTraceColumnRewrite {
    col_idx: usize,
    target_type: ColumnDataType,
}

/// Replaces one trace value with its precomputed coerced value.
#[derive(Debug)]
struct PreparedTraceValueRewrite {
    row_idx: usize,
    col_idx: usize,
    value_data: Option<ValueData>,
}

impl PreparedTraceColumnRewrites {
    pub(super) fn apply(self, rows: &mut Rows) {
        for column in self.columns {
            rows.schema[column.col_idx].datatype = column.target_type as i32;
        }
        for value in self.values {
            rows.rows[value.row_idx].values[value.col_idx].value_data = value.value_data;
        }
    }
}

/// Picks the reconciliation action for one trace column.
///
/// Existing table schema is authoritative unless the only incompatible case is
/// widening an existing Int64 column to Float64 for incoming Int64/Float64 data.
pub(super) fn choose_trace_reconcile_decision(
    column_name: &str,
    observed_types: &[ColumnDataType],
    existing_type: Option<ColumnDataType>,
) -> ServerResult<Option<TraceReconcileDecision>> {
    if let Some(fixed_type) = trace_semconv_fixed_type(column_name) {
        return choose_fixed_trace_reconcile_decision(fixed_type, observed_types, existing_type);
    }

    let Some(existing_type) = existing_type else {
        return resolve_new_trace_column_type(observed_types.iter().copied())
            .map(|target_type| target_type.map(TraceReconcileDecision::UseRequestLocal))
            .map_err(|_| {
                error::InvalidParameterSnafu {
                    reason: "unsupported trace type mix".to_string(),
                }
                .build()
            });
    };

    if observed_types.iter().all(|&request_type| {
        request_type == existing_type || is_supported_trace_coercion(request_type, existing_type)
    }) {
        return Ok(Some(TraceReconcileDecision::UseExisting(existing_type)));
    }

    if existing_type == ColumnDataType::Int64
        && observed_types.contains(&ColumnDataType::Float64)
        && observed_types.iter().all(|observed_type| {
            matches!(
                observed_type,
                ColumnDataType::Int64 | ColumnDataType::Float64
            )
        })
    {
        return Ok(Some(TraceReconcileDecision::AlterExistingTo(
            ColumnDataType::Float64,
        )));
    }

    error::InvalidParameterSnafu {
        reason: "unsupported trace type mix".to_string(),
    }
    .fail()
}

fn choose_fixed_trace_reconcile_decision(
    fixed_type: ColumnDataType,
    observed_types: &[ColumnDataType],
    existing_type: Option<ColumnDataType>,
) -> ServerResult<Option<TraceReconcileDecision>> {
    let Some(existing_type) = existing_type else {
        return Ok(Some(TraceReconcileDecision::UseRequestLocal(fixed_type)));
    };

    if existing_type == fixed_type {
        return Ok(Some(TraceReconcileDecision::UseExisting(fixed_type)));
    }

    if fixed_type == ColumnDataType::Float64
        && existing_type == ColumnDataType::Int64
        && observed_types.iter().all(|observed_type| {
            matches!(
                observed_type,
                ColumnDataType::Int64 | ColumnDataType::Float64
            )
        })
    {
        return Ok(Some(TraceReconcileDecision::AlterExistingTo(fixed_type)));
    }

    error::InvalidParameterSnafu {
        reason: "unsupported trace type mix".to_string(),
    }
    .fail()
}

/// Prepares an atomic rewrite plan without mutating the input rows.
///
/// For each pending column rewrite, this precomputes every required value
/// coercion. Missing, null, and already-correct values are skipped. If any
/// coercion fails, it returns the failing column and leaves all rows unchanged.
///
/// Target types must already have been selected by `TraceRequestSchema`.
/// Call [`PreparedTraceColumnRewrites::apply`] to update the schema and values.
pub(super) fn prepare_trace_column_rewrites(
    rows: &[Row],
    pending_rewrites: Vec<PendingTraceColumnRewrite>,
    table_name: &str,
) -> Result<PreparedTraceColumnRewrites, TraceColumnRewriteError> {
    let mut values = Vec::new();
    for (row_idx, row) in rows.iter().enumerate() {
        for pending_rewrite in &pending_rewrites {
            let Some(value) = row.values.get(pending_rewrite.col_idx) else {
                continue;
            };
            let Some(request_type) = value.value_data.as_ref().and_then(trace_value_datatype)
            else {
                continue;
            };
            if request_type == pending_rewrite.target_type {
                continue;
            }

            let value_data =
                coerce_value_data(&value.value_data, pending_rewrite.target_type, request_type)
                    .map_err(|_| {
                        TraceColumnRewriteError {
                error: error::InvalidParameterSnafu {
                    reason: format!(
                        "failed to coerce trace column '{}' in table '{}' from {:?} to {:?}",
                        pending_rewrite.column_name,
                        table_name,
                        request_type,
                        pending_rewrite.target_type
                    ),
                }
                .build(),
                column_name: pending_rewrite.column_name.clone(),
            }
                    })?;
            values.push(PreparedTraceValueRewrite {
                row_idx,
                col_idx: pending_rewrite.col_idx,
                value_data,
            });
        }
    }

    let columns = pending_rewrites
        .into_iter()
        .map(|rewrite| PreparedTraceColumnRewrite {
            col_idx: rewrite.col_idx,
            target_type: rewrite.target_type,
        })
        .collect();
    Ok(PreparedTraceColumnRewrites { columns, values })
}

pub(super) fn enrich_trace_reconcile_error(
    table_name: &str,
    column_name: &str,
    observed_types: &[ColumnDataType],
    existing_type: Option<ColumnDataType>,
    fixed_type: Option<ColumnDataType>,
) -> servers::error::Error {
    let observed_types = observed_types
        .iter()
        .map(|datatype| format!("{datatype:?}"))
        .collect::<Vec<_>>()
        .join(", ");

    error::InvalidParameterSnafu {
        reason: match (existing_type, fixed_type) {
            (Some(existing_type), Some(fixed_type)) => format!(
                "failed to reconcile trace column '{}' in table '{}' with observed types [{}] against existing {:?} and fixed semconv {:?}",
                column_name, table_name, observed_types, existing_type, fixed_type
            ),
            (Some(existing_type), None) => format!(
                "failed to reconcile trace column '{}' in table '{}' with observed types [{}] against existing {:?}",
                column_name, table_name, observed_types, existing_type
            ),
            (None, Some(fixed_type)) => format!(
                "failed to reconcile trace column '{}' in table '{}' with observed types [{}] and fixed semconv {:?}",
                column_name, table_name, observed_types, fixed_type
            ),
            (None, None) => format!(
                "failed to reconcile trace column '{}' in table '{}' with observed types [{}]",
                column_name, table_name, observed_types
            ),
        },
    }
    .build()
}

/// Only these trace scalar types participate in reconciliation. Other column kinds
/// such as JSON and binary keep their original write path and schema checks.
pub(super) fn is_trace_reconcile_candidate_type(datatype: ColumnDataType) -> bool {
    matches!(
        datatype,
        ColumnDataType::String
            | ColumnDataType::Boolean
            | ColumnDataType::Int64
            | ColumnDataType::Float64
    )
}

/// Keeps the observed type list small without depending on enum ordering.
pub(super) fn push_observed_trace_type(
    observed_types: &mut Vec<ColumnDataType>,
    datatype: ColumnDataType,
) {
    if !observed_types.contains(&datatype) {
        observed_types.push(datatype);
    }
}

#[cfg(test)]
mod tests {
    use api::v1::value::ValueData;
    use api::v1::{ColumnDataType, ColumnSchema, Row, Rows, Value};
    use common_error::ext::ErrorExt;
    use common_error::status_code::StatusCode;

    use super::{
        PendingTraceColumnRewrite, TraceReconcileDecision, choose_trace_reconcile_decision,
        enrich_trace_reconcile_error, is_trace_reconcile_candidate_type,
        prepare_trace_column_rewrites, push_observed_trace_type,
    };

    #[test]
    fn test_choose_trace_reconcile_decision_existing_int64_keeps_int64() {
        assert_eq!(
            choose_trace_reconcile_decision(
                "span_attributes.attr_int",
                &[ColumnDataType::Int64],
                Some(ColumnDataType::Int64)
            )
            .unwrap(),
            Some(TraceReconcileDecision::UseExisting(ColumnDataType::Int64))
        );
    }

    #[test]
    fn test_choose_trace_reconcile_decision_existing_int64_widens_to_float64() {
        assert_eq!(
            choose_trace_reconcile_decision(
                "span_attributes.attr_double",
                &[ColumnDataType::Int64, ColumnDataType::Float64],
                Some(ColumnDataType::Int64)
            )
            .unwrap(),
            Some(TraceReconcileDecision::AlterExistingTo(
                ColumnDataType::Float64
            ))
        );
    }

    #[test]
    fn test_choose_trace_reconcile_decision_existing_float64_stays_authoritative() {
        assert_eq!(
            choose_trace_reconcile_decision(
                "span_attributes.attr_double",
                &[ColumnDataType::Int64, ColumnDataType::Float64],
                Some(ColumnDataType::Float64)
            )
            .unwrap(),
            Some(TraceReconcileDecision::UseExisting(ColumnDataType::Float64))
        );
    }

    #[test]
    fn test_choose_trace_reconcile_decision_existing_int64_with_boolean_is_error() {
        let err = choose_trace_reconcile_decision(
            "span_attributes.attr_numeric",
            &[ColumnDataType::Boolean, ColumnDataType::Int64],
            Some(ColumnDataType::Int64),
        )
        .unwrap_err();
        assert_eq!(err.status_code(), StatusCode::InvalidArguments);
    }

    #[test]
    fn test_choose_trace_reconcile_decision_request_local_prefers_float64() {
        assert_eq!(
            choose_trace_reconcile_decision(
                "span_attributes.attr_numeric",
                &[ColumnDataType::Int64, ColumnDataType::Float64],
                None
            )
            .unwrap(),
            Some(TraceReconcileDecision::UseRequestLocal(
                ColumnDataType::Float64
            ))
        );
    }

    #[test]
    fn test_choose_trace_reconcile_decision_whitelisted_new_int64_column_uses_fixed_type() {
        assert_eq!(
            choose_trace_reconcile_decision(
                "span_attributes.http.response.status_code",
                &[ColumnDataType::String, ColumnDataType::Int64],
                None
            )
            .unwrap(),
            Some(TraceReconcileDecision::UseRequestLocal(
                ColumnDataType::Int64
            ))
        );
    }

    #[test]
    fn test_choose_trace_reconcile_decision_new_boolean_column_uses_dynamic_resolution() {
        assert_eq!(
            choose_trace_reconcile_decision(
                "span_attributes.messaging.destination.temporary",
                &[ColumnDataType::String, ColumnDataType::Boolean],
                None
            )
            .unwrap(),
            Some(TraceReconcileDecision::UseRequestLocal(
                ColumnDataType::Boolean
            ))
        );
    }

    #[test]
    fn test_choose_trace_reconcile_decision_whitelisted_existing_matching_type_uses_fixed_type() {
        assert_eq!(
            choose_trace_reconcile_decision(
                "resource_attributes.service.name",
                &[ColumnDataType::String],
                Some(ColumnDataType::String)
            )
            .unwrap(),
            Some(TraceReconcileDecision::UseExisting(ColumnDataType::String))
        );
    }

    #[test]
    fn test_choose_trace_reconcile_decision_whitelisted_existing_conflicting_type_is_error() {
        let err = choose_trace_reconcile_decision(
            "span_attributes.server.port",
            &[ColumnDataType::Int64],
            Some(ColumnDataType::String),
        )
        .unwrap_err();
        assert_eq!(err.status_code(), StatusCode::InvalidArguments);
    }

    #[test]
    fn test_choose_trace_reconcile_decision_non_whitelisted_retains_dynamic_behavior() {
        assert_eq!(
            choose_trace_reconcile_decision(
                "span_attributes.attr_numeric",
                &[ColumnDataType::Int64, ColumnDataType::Float64],
                None
            )
            .unwrap(),
            Some(TraceReconcileDecision::UseRequestLocal(
                ColumnDataType::Float64
            ))
        );
    }

    #[test]
    fn test_prepare_trace_column_rewrites_rejects_invalid_string_parse() {
        let rows = vec![Row {
            values: vec![Value {
                value_data: Some(ValueData::StringValue("not_a_number".to_string())),
            }],
        }];
        let pending_rewrites = vec![PendingTraceColumnRewrite {
            col_idx: 0,
            target_type: ColumnDataType::Int64,
            column_name: "span_attributes.attr_int".to_string(),
        }];

        let err = prepare_trace_column_rewrites(&rows, pending_rewrites, "trace_type_atomicity")
            .unwrap_err();
        assert_eq!(err.error.status_code(), StatusCode::InvalidArguments);
        assert_eq!(err.column_name, "span_attributes.attr_int");
    }

    #[test]
    fn test_prepare_trace_column_rewrites_applies_prepared_values() {
        let mut rows = Rows {
            schema: vec![ColumnSchema {
                datatype: ColumnDataType::String as i32,
                ..Default::default()
            }],
            rows: vec![Row {
                values: vec![Value {
                    value_data: Some(ValueData::StringValue("503".to_string())),
                }],
            }],
        };
        let pending_rewrites = vec![PendingTraceColumnRewrite {
            col_idx: 0,
            target_type: ColumnDataType::Int64,
            column_name: "span_attributes.http.response.status_code".to_string(),
        }];

        let prepared =
            prepare_trace_column_rewrites(&rows.rows, pending_rewrites, "trace_type_atomicity")
                .unwrap();
        assert_eq!(
            rows.rows[0].values[0].value_data,
            Some(ValueData::StringValue("503".to_string()))
        );

        prepared.apply(&mut rows);
        assert_eq!(rows.schema[0].datatype, ColumnDataType::Int64 as i32);
        assert_eq!(
            rows.rows[0].values[0].value_data,
            Some(ValueData::I64Value(503))
        );
    }

    #[test]
    fn test_prepare_trace_column_rewrites_boolean_rejects_invalid_string_parse() {
        let rows = vec![Row {
            values: vec![Value {
                value_data: Some(ValueData::StringValue("not_a_bool".to_string())),
            }],
        }];
        let pending_rewrites = vec![PendingTraceColumnRewrite {
            col_idx: 0,
            target_type: ColumnDataType::Boolean,
            column_name: "span_attributes.messaging.destination.temporary".to_string(),
        }];

        let err = prepare_trace_column_rewrites(&rows, pending_rewrites, "trace_type_atomicity")
            .unwrap_err();
        assert_eq!(err.error.status_code(), StatusCode::InvalidArguments);
        assert_eq!(
            err.column_name,
            "span_attributes.messaging.destination.temporary"
        );
    }

    #[test]
    fn test_enrich_trace_reconcile_error_includes_existing_type() {
        let err = enrich_trace_reconcile_error(
            "trace_type_atomicity",
            "span_attributes.attr_int",
            &[ColumnDataType::String, ColumnDataType::Int64],
            Some(ColumnDataType::Boolean),
            None,
        );

        assert_eq!(err.status_code(), StatusCode::InvalidArguments);
        assert!(err.to_string().contains("span_attributes.attr_int"));
        assert!(err.to_string().contains("Boolean"));
    }

    #[test]
    fn test_enrich_trace_reconcile_error_includes_fixed_semconv_type() {
        let err = enrich_trace_reconcile_error(
            "trace_type_atomicity",
            "span_attributes.server.port",
            &[ColumnDataType::String, ColumnDataType::Int64],
            Some(ColumnDataType::String),
            Some(ColumnDataType::Int64),
        );

        assert_eq!(err.status_code(), StatusCode::InvalidArguments);
        assert!(err.to_string().contains("span_attributes.server.port"));
        assert!(err.to_string().contains("fixed semconv Int64"));
    }

    #[test]
    fn test_is_trace_reconcile_candidate_type_filters_non_scalar_types() {
        assert!(is_trace_reconcile_candidate_type(ColumnDataType::String));
        assert!(is_trace_reconcile_candidate_type(ColumnDataType::Boolean));
        assert!(!is_trace_reconcile_candidate_type(ColumnDataType::Binary));
        assert!(!is_trace_reconcile_candidate_type(
            ColumnDataType::TimestampMillisecond
        ));
    }

    #[test]
    fn test_push_observed_trace_type_deduplicates_types() {
        let mut observed_types = Vec::new();

        push_observed_trace_type(&mut observed_types, ColumnDataType::Int64);
        push_observed_trace_type(&mut observed_types, ColumnDataType::Int64);
        push_observed_trace_type(&mut observed_types, ColumnDataType::Float64);

        assert_eq!(
            observed_types,
            vec![ColumnDataType::Int64, ColumnDataType::Float64]
        );
    }
}
