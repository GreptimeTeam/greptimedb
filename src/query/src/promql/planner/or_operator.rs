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

//! Planning of the PromQL `or` binary operator.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::logical_expr::{Cast, Extension, LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::{Column, Expr as DfExpr};
use datafusion::scalar::ScalarValue;
use datafusion_common::TableReference;
use datatypes::arrow::datatypes::DataType as ArrowDataType;
use promql::extension_plan::UnionDistinctOn;
use promql_parser::parser::{BinModifier, LabelModifier};
use snafu::{OptionExt, ResultExt, ensure};
use store_api::metric_engine_consts::DATA_SCHEMA_TSID_COLUMN_NAME;

use super::{OR_FLOAT_FIELD_PREFIX, OR_HISTOGRAM_FIELD_PREFIX, PromPlanner, PromPlannerContext};
use crate::promql::error::{
    ColumnNotFoundSnafu, DataFusionPlanningSnafu, MultiFieldsNotSupportedSnafu, Result,
    TimeIndexNotFoundSnafu, UnexpectedPlanExprSnafu,
};

impl PromPlanner {
    // TODO(ruihang): change function name
    #[allow(clippy::too_many_arguments)]
    pub(super) fn or_operator(
        &mut self,
        left: LogicalPlan,
        right: LogicalPlan,
        left_tag_cols_set: HashSet<String>,
        right_tag_cols_set: HashSet<String>,
        left_context: PromPlannerContext,
        right_context: PromPlannerContext,
        modifier: &Option<BinModifier>,
    ) -> Result<LogicalPlan> {
        let left_is_empty = Self::is_zero_row_empty_relation(&left);
        let right_is_empty = Self::is_zero_row_empty_relation(&right);
        match (left_is_empty, right_is_empty) {
            (true, false) => {
                self.ctx = right_context;
                return Ok(right);
            }
            (false, true) => {
                self.ctx = left_context;
                return Ok(left);
            }
            (true, true) => {
                self.ctx = left_context;
                return Ok(left);
            }
            (false, false) => {}
        }

        ensure!(
            !left.schema().fields().is_empty() && !right.schema().fields().is_empty(),
            UnexpectedPlanExprSnafu {
                desc: "OR operator input has zero columns",
            }
        );
        let left_has_alternative_samples =
            Self::field_columns_are_alternative_samples(left.schema(), &left_context.field_columns);
        let right_has_alternative_samples = Self::field_columns_are_alternative_samples(
            right.schema(),
            &right_context.field_columns,
        );
        ensure!(
            left_context.field_columns.len() == 1 || left_has_alternative_samples,
            MultiFieldsNotSupportedSnafu {
                operator: "OR operator"
            }
        );
        ensure!(
            right_context.field_columns.len() == 1 || right_has_alternative_samples,
            MultiFieldsNotSupportedSnafu {
                operator: "OR operator"
            }
        );

        // prepare hash sets
        let all_tags = left_tag_cols_set
            .union(&right_tag_cols_set)
            .cloned()
            .collect::<HashSet<_>>();
        let left_qualifier = left.schema().qualified_field(0).0.cloned();
        let right_qualifier = right.schema().qualified_field(0).0.cloned();
        let left_qualifier_string = left_qualifier
            .as_ref()
            .map(|l| l.to_string())
            .unwrap_or_default();
        let right_qualifier_string = right_qualifier
            .as_ref()
            .map(|r| r.to_string())
            .unwrap_or_default();
        let left_time_index_column =
            left_context
                .time_index_column
                .clone()
                .with_context(|| TimeIndexNotFoundSnafu {
                    table: left_qualifier_string.clone(),
                })?;
        let right_time_index_column =
            right_context
                .time_index_column
                .clone()
                .with_context(|| TimeIndexNotFoundSnafu {
                    table: right_qualifier_string.clone(),
                })?;
        let native_histogram_type = Self::native_histogram_arrow_type();
        let is_numeric = |data_type: &ArrowDataType| {
            matches!(
                data_type,
                ArrowDataType::Int8
                    | ArrowDataType::Int16
                    | ArrowDataType::Int32
                    | ArrowDataType::Int64
                    | ArrowDataType::UInt8
                    | ArrowDataType::UInt16
                    | ArrowDataType::UInt32
                    | ArrowDataType::UInt64
                    | ArrowDataType::Float32
                    | ArrowDataType::Float64
            )
        };
        let left_fields = left_context
            .field_columns
            .iter()
            .map(|name| {
                left.schema()
                    .iter()
                    .find(|(_, field)| field.name() == name)
                    .map(|(qualifier, field)| {
                        (name.clone(), qualifier.cloned(), field.data_type().clone())
                    })
                    .with_context(|| ColumnNotFoundSnafu { col: name.clone() })
            })
            .collect::<Result<Vec<_>>>()?;
        let right_fields = right_context
            .field_columns
            .iter()
            .map(|name| {
                right
                    .schema()
                    .iter()
                    .find(|(_, field)| field.name() == name)
                    .map(|(qualifier, field)| {
                        (name.clone(), qualifier.cloned(), field.data_type().clone())
                    })
                    .with_context(|| ColumnNotFoundSnafu { col: name.clone() })
            })
            .collect::<Result<Vec<_>>>()?;
        let left_field = &left_fields[0];
        let right_field = &right_fields[0];
        let left_field_col = &left_field.0;
        let right_field_col = &right_field.0;
        let fields_are_samples = |fields: &[(String, Option<TableReference>, ArrowDataType)]| {
            fields.iter().all(|(_, _, data_type)| {
                is_numeric(data_type) || data_type == &native_histogram_type
            })
        };
        let mixed_sample_types = if left_has_alternative_samples || right_has_alternative_samples {
            if !fields_are_samples(&left_fields) || !fields_are_samples(&right_fields) {
                return UnexpectedPlanExprSnafu {
                    desc: format!(
                        "OR value fields have incompatible types: {:?} and {:?}",
                        left_fields
                            .iter()
                            .map(|(_, _, data_type)| data_type)
                            .collect::<Vec<_>>(),
                        right_fields
                            .iter()
                            .map(|(_, _, data_type)| data_type)
                            .collect::<Vec<_>>()
                    ),
                }
                .fail();
            }
            true
        } else {
            (left_field.2 == native_histogram_type && is_numeric(&right_field.2))
                || (right_field.2 == native_histogram_type && is_numeric(&left_field.2))
        };
        let target_field_type = if mixed_sample_types {
            // Mixed vectors use the existing response representation: one nullable float column
            // and one nullable native-histogram column.
            ArrowDataType::Float64
        } else if left_field.2 == right_field.2 {
            left_field.2.clone()
        } else if is_numeric(&left_field.2) && is_numeric(&right_field.2) {
            ArrowDataType::Float64
        } else {
            return UnexpectedPlanExprSnafu {
                desc: format!(
                    "OR value fields have incompatible types: {:?} and {:?}",
                    left_field.2, right_field.2
                ),
            }
            .fail();
        };
        let (mixed_float_field_col, mixed_histogram_field_col) = if mixed_sample_types {
            let mut reserved_names = left
                .schema()
                .fields()
                .iter()
                .chain(right.schema().fields().iter())
                .map(|field| field.name().clone())
                .collect::<HashSet<_>>();
            for (name, _, _) in left_fields.iter().chain(&right_fields) {
                reserved_names.remove(name);
            }
            reserved_names.extend(all_tags.iter().cloned());
            let unique_name = |prefix: &str, reserved_names: &mut HashSet<String>| {
                let mut index = 0;
                loop {
                    let name = format!("{prefix}{index}");
                    index += 1;
                    if reserved_names.insert(name.clone()) {
                        break name;
                    }
                }
            };
            let float_field = unique_name(OR_FLOAT_FIELD_PREFIX, &mut reserved_names);
            let histogram_field = unique_name(OR_HISTOGRAM_FIELD_PREFIX, &mut reserved_names);
            (float_field, histogram_field)
        } else {
            (left_field_col.clone(), String::new())
        };
        let left_tag_types = left_tag_cols_set
            .iter()
            .map(|label| {
                left.schema()
                    .fields()
                    .iter()
                    .find(|field| field.name() == label)
                    .map(|field| (label.clone(), field.data_type().clone()))
                    .with_context(|| ColumnNotFoundSnafu { col: label.clone() })
            })
            .collect::<Result<HashMap<_, _>>>()?;
        let right_tag_types = right_tag_cols_set
            .iter()
            .map(|label| {
                right
                    .schema()
                    .fields()
                    .iter()
                    .find(|field| field.name() == label)
                    .map(|field| (label.clone(), field.data_type().clone()))
                    .with_context(|| ColumnNotFoundSnafu { col: label.clone() })
            })
            .collect::<Result<HashMap<_, _>>>()?;
        let mut target_tag_types = HashMap::with_capacity(all_tags.len());
        for label in &all_tags {
            let Some(data_type) =
                Self::common_label_data_type(left_tag_types.get(label), right_tag_types.get(label))
            else {
                return UnexpectedPlanExprSnafu {
                    desc: format!(
                        "OR label {label} has incompatible types: {:?} and {:?}",
                        left_tag_types.get(label),
                        right_tag_types.get(label)
                    ),
                }
                .fail();
            };
            target_tag_types.insert(label.clone(), data_type);
        }
        let left_has_tsid = left
            .schema()
            .fields()
            .iter()
            .any(|field| field.name() == DATA_SCHEMA_TSID_COLUMN_NAME);
        let right_has_tsid = right
            .schema()
            .fields()
            .iter()
            .any(|field| field.name() == DATA_SCHEMA_TSID_COLUMN_NAME);

        // step 0: fill all columns in output schema
        let mut all_columns_set = left
            .schema()
            .fields()
            .iter()
            .chain(right.schema().fields().iter())
            .map(|field| field.name().clone())
            .collect::<HashSet<_>>();
        // Keep `__tsid` only when both sides contain it, otherwise it may break schema alignment
        // (e.g. `unknown_metric or some_metric`).
        if !(left_has_tsid && right_has_tsid) {
            all_columns_set.remove(DATA_SCHEMA_TSID_COLUMN_NAME);
        }
        // remove time index column
        all_columns_set.remove(&left_time_index_column);
        all_columns_set.remove(&right_time_index_column);
        if mixed_sample_types {
            for (name, _, _) in left_fields.iter().chain(&right_fields) {
                all_columns_set.remove(name);
            }
            all_columns_set.extend(all_tags.iter().cloned());
            all_columns_set.insert(mixed_float_field_col.clone());
            all_columns_set.insert(mixed_histogram_field_col.clone());
        } else if left_field_col != right_field_col {
            // remove field column in the right
            all_columns_set.remove(right_field_col);
        }
        let mut all_columns = all_columns_set.into_iter().collect::<Vec<_>>();
        // sort to ensure the generated schema is not volatile
        all_columns.sort_unstable();
        // use left time index column name as the result time index column name
        all_columns.insert(0, left_time_index_column.clone());
        let mut occupied_column_names = left
            .schema()
            .fields()
            .iter()
            .chain(right.schema().fields().iter())
            .map(|field| field.name().clone())
            .collect::<HashSet<_>>();

        // step 1: align schema using project, fill non-exist columns with null
        let aligned_label_expr = |col: &String, source_types: &HashMap<String, ArrowDataType>| {
            let target_type = &target_tag_types[col];
            if let Some(source_type) = source_types.get(col) {
                let expr = DfExpr::Column(Column::new(None::<String>, col));
                if source_type == target_type {
                    expr
                } else {
                    DfExpr::Cast(Cast::new(Box::new(expr), target_type.clone())).alias(col.clone())
                }
            } else {
                DfExpr::Literal(
                    Self::string_scalar_value(target_type, None)
                        .expect("target label type is a string"),
                    None,
                )
                .alias(col.clone())
            }
        };
        let null_histogram =
            ScalarValue::try_new_null(&native_histogram_type).context(DataFusionPlanningSnafu)?;
        let mixed_value_expr = |fields: &[(String, Option<TableReference>, ArrowDataType)],
                                output_col: &String| {
            if output_col == &mixed_float_field_col {
                if let Some((name, qualifier, data_type)) = fields
                    .iter()
                    .find(|(_, _, data_type)| is_numeric(data_type))
                {
                    let expr = DfExpr::Column(Column::new(qualifier.clone(), name));
                    if data_type == &ArrowDataType::Float64 {
                        expr.alias(output_col)
                    } else {
                        DfExpr::Cast(Cast::new(Box::new(expr), ArrowDataType::Float64))
                            .alias(output_col)
                    }
                } else {
                    DfExpr::Literal(ScalarValue::Float64(None), None).alias(output_col)
                }
            } else {
                fields
                    .iter()
                    .find(|(_, _, data_type)| data_type == &native_histogram_type)
                    .map(|(name, qualifier, _)| {
                        DfExpr::Column(Column::new(qualifier.clone(), name)).alias(output_col)
                    })
                    .unwrap_or_else(|| {
                        DfExpr::Literal(null_histogram.clone(), None).alias(output_col)
                    })
            }
        };
        let left_proj_exprs = all_columns.iter().map(|col| {
            if mixed_sample_types
                && (col == &mixed_float_field_col || col == &mixed_histogram_field_col)
            {
                mixed_value_expr(&left_fields, col)
            } else if !mixed_sample_types
                && col == left_field_col
                && left_field.2 != target_field_type
            {
                DfExpr::Cast(Cast::new(
                    Box::new(DfExpr::Column(Column::new(
                        left_field.1.clone(),
                        left_field_col,
                    ))),
                    target_field_type.clone(),
                ))
                .alias(left_field_col.clone())
            } else if target_tag_types.contains_key(col) {
                aligned_label_expr(col, &left_tag_types)
            } else {
                DfExpr::Column(Column::new(None::<String>, col))
            }
        });
        let right_time_index_expr = DfExpr::Column(Column::new(
            right_qualifier.clone(),
            right_time_index_column,
        ))
        .alias(left_time_index_column.clone());
        // The field column in right side may not have qualifier (it may be removed by join operation),
        // so we need to find it from the schema.
        // `skip（1)` to skip the time index column
        let right_proj_exprs_without_time_index = all_columns.iter().skip(1).map(|col| {
            // expr
            if mixed_sample_types
                && (col == &mixed_float_field_col || col == &mixed_histogram_field_col)
            {
                mixed_value_expr(&right_fields, col)
            } else if !mixed_sample_types && col == left_field_col {
                let expr = DfExpr::Column(Column::new(right_field.1.clone(), right_field_col));
                if right_field.2 != target_field_type {
                    DfExpr::Cast(Cast::new(Box::new(expr), target_field_type.clone()))
                        .alias(left_field_col.clone())
                } else if left_field_col != right_field_col {
                    expr.alias(left_field_col.clone())
                } else {
                    expr
                }
            } else if target_tag_types.contains_key(col) {
                aligned_label_expr(col, &right_tag_types)
            } else {
                DfExpr::Column(Column::new(None::<String>, col))
            }
        });
        let right_proj_exprs = [right_time_index_expr]
            .into_iter()
            .chain(right_proj_exprs_without_time_index);

        let left_projected = LogicalPlanBuilder::from(left)
            .project(left_proj_exprs)
            .context(DataFusionPlanningSnafu)?
            .alias(left_qualifier_string.clone())
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)?;
        let right_projected = LogicalPlanBuilder::from(right)
            .project(right_proj_exprs)
            .context(DataFusionPlanningSnafu)?
            .alias(right_qualifier_string.clone())
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)?;

        // step 2: compute match columns
        let mut match_columns = if let Some(modifier) = modifier
            && let Some(matching) = &modifier.matching
        {
            match matching {
                // keeps columns mentioned in `on`
                LabelModifier::Include(on) => on.labels.clone(),
                // removes columns memtioned in `ignoring`
                LabelModifier::Exclude(ignoring) => {
                    let ignoring = ignoring.labels.iter().cloned().collect::<HashSet<_>>();
                    all_tags.difference(&ignoring).cloned().collect()
                }
            }
        } else {
            all_tags.iter().cloned().collect()
        };
        // sort to ensure the generated plan is not volatile
        match_columns.sort_unstable();
        match_columns.dedup();
        occupied_column_names.extend(
            left_projected
                .schema()
                .fields()
                .iter()
                .chain(right_projected.schema().fields().iter())
                .map(|field| field.name().clone()),
        );

        let visible_schema = left_projected.schema().clone();
        let visible_left_exprs = left_projected
            .schema()
            .iter()
            .map(|(qualifier, field)| {
                DfExpr::Column(Column::new(qualifier.cloned(), field.name().clone()))
            })
            .collect::<Vec<_>>();
        let visible_right_exprs = right_projected
            .schema()
            .iter()
            .map(|(qualifier, field)| {
                DfExpr::Column(Column::new(qualifier.cloned(), field.name().clone()))
            })
            .collect::<Vec<_>>();
        let mut left_match_exprs = Vec::with_capacity(match_columns.len());
        let mut right_match_exprs = Vec::with_capacity(match_columns.len());
        let mut next_internal_column = 0;

        for label in &match_columns {
            let left_field = if left_tag_cols_set.contains(label) {
                Some(
                    left_projected
                        .schema()
                        .iter()
                        .find(|(_, field)| field.name() == label)
                        .map(|(qualifier, field)| (qualifier.cloned(), field.data_type().clone()))
                        .with_context(|| ColumnNotFoundSnafu { col: label.clone() })?,
                )
            } else {
                None
            };
            let right_field = if right_tag_cols_set.contains(label) {
                Some(
                    right_projected
                        .schema()
                        .iter()
                        .find(|(_, field)| field.name() == label)
                        .map(|(qualifier, field)| (qualifier.cloned(), field.data_type().clone()))
                        .with_context(|| ColumnNotFoundSnafu { col: label.clone() })?,
                )
            } else {
                None
            };
            let data_type = match (left_field.as_ref(), right_field.as_ref()) {
                (Some((_, left_type)), Some((_, right_type))) if left_type == right_type => {
                    left_type.clone()
                }
                (Some((_, left_type)), Some((_, right_type))) => {
                    return UnexpectedPlanExprSnafu {
                        desc: format!(
                            "OR match label {label} has incompatible types: {left_type:?} and {right_type:?}"
                        ),
                    }
                    .fail();
                }
                (Some((_, data_type)), None) | (None, Some((_, data_type))) => data_type.clone(),
                (None, None) => ArrowDataType::Utf8,
            };
            let Some(value_type) = Self::string_value_data_type(&data_type).cloned() else {
                return UnexpectedPlanExprSnafu {
                    desc: format!("OR match label {label} must be a string"),
                }
                .fail();
            };
            let internal_name = loop {
                let name = format!("__promql_or_match_{next_internal_column}");
                next_internal_column += 1;
                if occupied_column_names.insert(name.clone()) {
                    break name;
                }
            };
            left_match_exprs.push(Self::normalized_match_key_expr(
                label,
                left_field,
                &value_type,
                &internal_name,
            ));
            right_match_exprs.push(Self::normalized_match_key_expr(
                label,
                right_field,
                &value_type,
                &internal_name,
            ));
        }

        let left_augmented = LogicalPlanBuilder::from(left_projected)
            .project(visible_left_exprs.into_iter().chain(left_match_exprs))
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)?;
        let right_augmented = LogicalPlanBuilder::from(right_projected)
            .project(visible_right_exprs.into_iter().chain(right_match_exprs))
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)?;

        // step 3: build `UnionDistinctOn` with normalized internal match keys.
        let visible_field_count = visible_schema.fields().len();
        let compare_key_indices =
            (visible_field_count..visible_field_count + match_columns.len()).collect::<Vec<_>>();
        let (time_qualifier, _) = visible_schema
            .iter()
            .find(|(_, field)| field.name() == &left_time_index_column)
            .with_context(|| TimeIndexNotFoundSnafu {
                table: left_qualifier_string.clone(),
            })?;
        let ts_col_idx = left_augmented
            .schema()
            .iter()
            .position(|(qualifier, field)| {
                qualifier == time_qualifier && field.name() == &left_time_index_column
            })
            .with_context(|| TimeIndexNotFoundSnafu {
                table: left_qualifier_string.clone(),
            })?;
        let union_distinct_on = UnionDistinctOn::try_new(
            left_augmented,
            right_augmented,
            compare_key_indices,
            ts_col_idx,
        )
        .context(DataFusionPlanningSnafu)?;
        let augmented_result = LogicalPlan::Extension(Extension {
            node: Arc::new(union_distinct_on),
        });
        let result = LogicalPlanBuilder::from(augmented_result)
            .project(visible_schema.iter().map(|(qualifier, field)| {
                DfExpr::Column(Column::new(qualifier.cloned(), field.name().clone()))
            }))
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)?;

        // step 4: update context
        let output_field_col = left_field_col.clone();
        let mut output_context = left_context;
        let mut visible_tags = all_tags.into_iter().collect::<Vec<_>>();
        visible_tags.sort_unstable();
        output_context.time_index_column = Some(left_time_index_column);
        output_context.tag_columns = visible_tags;
        output_context.field_columns = if mixed_sample_types {
            vec![mixed_float_field_col, mixed_histogram_field_col]
        } else {
            vec![output_field_col]
        };
        output_context.use_tsid = left_has_tsid && right_has_tsid;
        self.ctx = output_context;

        Ok(result)
    }
}
