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

//! Dedicated helper plans for PromQL functions that cannot be expressed as a
//! plain scalar function over the input plan: the histogram helpers, `vector()`,
//! `scalar()`, and `absent()`.

use std::sync::Arc;

use common_query::prelude::greptime_value;
use datafusion::functions_aggregate::expr_fn::first_value;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{Extension, LogicalPlan, LogicalPlanBuilder};
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion::prelude::{Column, Expr as DfExpr};
use datafusion::scalar::ScalarValue;
use datafusion_common::DFSchema;
use datafusion_expr::expr_fn::when;
use datafusion_expr::simplify::SimplifyContext;
use datafusion_expr::{col, lit};
use datafusion_functions::core::coalesce;
use datatypes::arrow::datatypes::DataType as ArrowDataType;
use promql::extension_plan::{
    Absent, EmptyMetric, HistogramFold, HistogramFoldOperation, ScalarCalculate,
};
use promql::functions::{NativeHistogramDrop, NativeHistogramFraction, NativeHistogramQuantile};
use promql_parser::label::MatchOp;
use promql_parser::parser::{Expr as PromExpr, FunctionArgs as PromFunctionArgs};
use snafu::{OptionExt, ResultExt, ensure};

use super::{
    LE_COLUMN_NAME, PromPlanner, SCALAR_FUNCTION, SPECIAL_ABSENT_FUNCTION,
    SPECIAL_HISTOGRAM_FRACTION, SPECIAL_HISTOGRAM_QUANTILE, SPECIAL_TIME_FUNCTION,
    SPECIAL_VECTOR_FUNCTION,
};
use crate::promql::error::{
    DataFusionPlanningSnafu, FunctionInvalidArgumentSnafu, MultiFieldsNotSupportedSnafu,
    PromqlPlanNodeSnafu, Result, TimeIndexNotFoundSnafu, ValueNotFoundSnafu,
};
use crate::query_engine::QueryEngineState;

impl PromPlanner {
    /// Create a classic, native, or mixed histogram helper plan.
    pub(super) async fn create_histogram_plan(
        &mut self,
        function_name: &str,
        args: &PromFunctionArgs,
        query_engine_state: &QueryEngineState,
    ) -> Result<LogicalPlan> {
        let float_literal = |param: &PromExpr| -> Result<f64> {
            let value = (|| {
                let expr = Self::get_param_as_literal_expr(
                    Some(param),
                    None,
                    Some(ArrowDataType::Float64),
                )
                .ok()?;
                let simplifier = ExprSimplifier::new(SimplifyContext::default());
                let expr = simplifier.coerce(expr, &DFSchema::empty()).ok()?;
                let DfExpr::Literal(value, _) = simplifier.simplify(expr).ok()? else {
                    return None;
                };
                let ScalarValue::Float64(Some(value)) =
                    value.cast_to(&ArrowDataType::Float64).ok()?
                else {
                    return None;
                };
                Some(value)
            })()
            .with_context(|| FunctionInvalidArgumentSnafu {
                fn_name: function_name.to_string(),
            })?;
            Ok(value)
        };
        let (function, input) = match (function_name, args.args.as_slice()) {
            (SPECIAL_HISTOGRAM_QUANTILE, [quantile, input]) => (
                HistogramFoldOperation::Quantile(float_literal(quantile)?.into()),
                input.as_ref().clone(),
            ),
            (SPECIAL_HISTOGRAM_FRACTION, [lower, upper, input]) => (
                HistogramFoldOperation::Fraction {
                    lower: float_literal(lower)?.into(),
                    upper: float_literal(upper)?.into(),
                },
                input.as_ref().clone(),
            ),
            _ => {
                return FunctionInvalidArgumentSnafu {
                    fn_name: function_name.to_string(),
                }
                .fail();
            }
        };

        let input_plan = self.prom_expr_to_plan(&input, query_engine_state).await?;
        // Histogram helpers fold buckets across `le`, so `__tsid` (which includes `le`) is not a
        // stable series identifier anymore. HistogramFold must not treat it as a label column.
        let input_plan = self.strip_tsid_column(input_plan)?;
        self.ctx.use_tsid = false;

        if let Some((float_field, histogram_field)) =
            Self::alternative_sample_columns(input_plan.schema(), &self.ctx.field_columns)
                .map(|(float, histogram)| (float.to_string(), histogram.to_string()))
        {
            if self.ctx.has_le_tag() {
                return self.create_mixed_histogram_plan(
                    function,
                    input_plan,
                    float_field,
                    histogram_field,
                );
            }
            self.ctx.field_columns = vec![histogram_field];
        }
        if self.all_field_columns_are_native_histograms(input_plan.schema()) {
            return self.create_native_histogram_plan(function, input_plan);
        }

        if !self.ctx.has_le_tag() {
            // Return empty result instead of error when 'le' column is not found
            // This handles the case when histogram metrics don't exist
            return Ok(LogicalPlan::EmptyRelation(
                datafusion::logical_expr::EmptyRelation {
                    produce_one_row: false,
                    schema: input_plan.schema().clone(),
                },
            ));
        }
        let time_index_column =
            self.ctx
                .time_index_column
                .clone()
                .with_context(|| TimeIndexNotFoundSnafu {
                    table: self.ctx.table_name.clone().unwrap_or_default(),
                })?;
        // FIXME(ruihang): support multi fields
        let field_column = self
            .ctx
            .field_columns
            .first()
            .with_context(|| FunctionInvalidArgumentSnafu {
                fn_name: function.function_name().to_string(),
            })?
            .clone();
        // remove le column from tag columns
        self.ctx.tag_columns.retain(|col| col != LE_COLUMN_NAME);

        let fold = HistogramFold::new_with_operation(
            LE_COLUMN_NAME.to_string(),
            field_column,
            time_index_column,
            function,
            None,
            input_plan,
        )
        .context(DataFusionPlanningSnafu)?;
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(fold),
        }))
    }

    fn create_native_histogram_expr(
        &self,
        function: HistogramFoldOperation,
        field_column: &str,
    ) -> DfExpr {
        let field = DfExpr::Column(Column::from_name(field_column));
        let (func, args) = match function {
            HistogramFoldOperation::Quantile(quantile) => (
                Arc::new(NativeHistogramQuantile::scalar_udf_with_collector(
                    self.promql_annotations.clone(),
                )),
                vec![field, lit(f64::from(quantile))],
            ),
            HistogramFoldOperation::Fraction { lower, upper } => (
                Arc::new(NativeHistogramFraction::scalar_udf_with_collector(
                    self.promql_annotations.clone(),
                )),
                vec![field, lit(f64::from(lower)), lit(f64::from(upper))],
            ),
        };
        DfExpr::ScalarFunction(ScalarFunction { func, args })
    }

    fn create_native_histogram_plan(
        &mut self,
        function: HistogramFoldOperation,
        input_plan: LogicalPlan,
    ) -> Result<LogicalPlan> {
        ensure!(
            self.ctx.field_columns.len() == 1,
            MultiFieldsNotSupportedSnafu {
                operator: function.function_name()
            },
        );

        let field_column = self.ctx.field_columns[0].clone();
        let function_expr = self.create_native_histogram_expr(function, &field_column);
        let display_name = function_expr.schema_name().to_string();
        self.ctx.field_columns = vec![display_name.clone()];

        let project_exprs = std::iter::once(self.create_time_index_column_expr()?)
            .chain(std::iter::once(function_expr.alias(display_name)))
            .chain(self.create_tag_column_exprs()?)
            .collect::<Vec<_>>();

        LogicalPlanBuilder::from(input_plan)
            .project(project_exprs)
            .context(DataFusionPlanningSnafu)?
            .filter(self.create_empty_values_filter_expr(false)?)
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)
    }

    fn create_mixed_histogram_plan(
        &mut self,
        function: HistogramFoldOperation,
        input_plan: LogicalPlan,
        float_field: String,
        histogram_field: String,
    ) -> Result<LogicalPlan> {
        let time_index_column =
            self.ctx
                .time_index_column
                .clone()
                .with_context(|| TimeIndexNotFoundSnafu {
                    table: self.ctx.table_name.clone().unwrap_or_default(),
                })?;
        let tag_columns = self.ctx.tag_columns.clone();
        let folded = HistogramFold::new_with_operation(
            LE_COLUMN_NAME.to_string(),
            float_field.clone(),
            time_index_column.clone(),
            function,
            Some(histogram_field.clone()),
            input_plan,
        )
        .context(DataFusionPlanningSnafu)?;
        let record_collision = DfExpr::ScalarFunction(ScalarFunction {
            func: Arc::new(NativeHistogramDrop::warning_bool_false_udf(
                "vector contains a mix of classic and native histograms".to_string(),
                self.promql_annotations.clone(),
            )),
            args: vec![col(&float_field), col(&histogram_field)],
        });
        let keep = when(
            col(&float_field)
                .is_not_null()
                .and(col(&histogram_field).is_not_null()),
            record_collision,
        )
        .otherwise(lit(true))
        .context(DataFusionPlanningSnafu)?;

        let native_expr = self.create_native_histogram_expr(function, &histogram_field);
        let output_field = native_expr.schema_name().to_string();
        let value = DfExpr::ScalarFunction(ScalarFunction {
            func: coalesce(),
            args: vec![col(&float_field), native_expr],
        });
        self.ctx.field_columns = vec![output_field.clone()];
        LogicalPlanBuilder::from(LogicalPlan::Extension(Extension {
            node: Arc::new(folded),
        }))
        .filter(keep)
        .context(DataFusionPlanningSnafu)?
        .project(
            std::iter::once(col(&time_index_column))
                .chain(std::iter::once(value.alias(output_field)))
                .chain(tag_columns.iter().map(col)),
        )
        .context(DataFusionPlanningSnafu)?
        .build()
        .context(DataFusionPlanningSnafu)
    }

    /// Create a [SPECIAL_VECTOR_FUNCTION] plan
    pub(super) async fn create_vector_plan(
        &mut self,
        args: &PromFunctionArgs,
    ) -> Result<LogicalPlan> {
        if args.args.len() != 1 {
            return FunctionInvalidArgumentSnafu {
                fn_name: SPECIAL_VECTOR_FUNCTION.to_string(),
            }
            .fail();
        }
        let lit = Self::get_param_as_literal_expr(Some(args.args[0].as_ref()), None, None)?;

        // reuse `SPECIAL_TIME_FUNCTION` as name of time index column
        self.ctx.time_index_column = Some(SPECIAL_TIME_FUNCTION.to_string());
        self.ctx.reset_table_name_and_schema();
        self.ctx.tag_columns = vec![];
        self.ctx.aggregation_field_labels.clear();
        self.ctx.field_columns = vec![greptime_value().to_string()];
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(
                EmptyMetric::new(
                    self.ctx.start,
                    self.ctx.end,
                    self.ctx.interval,
                    SPECIAL_TIME_FUNCTION.to_string(),
                    greptime_value().to_string(),
                    Some(lit),
                )
                .context(DataFusionPlanningSnafu)?,
            ),
        }))
    }

    /// Create a [SCALAR_FUNCTION] plan
    pub(super) async fn create_scalar_plan(
        &mut self,
        args: &PromFunctionArgs,
        query_engine_state: &QueryEngineState,
    ) -> Result<LogicalPlan> {
        ensure!(
            args.len() == 1,
            FunctionInvalidArgumentSnafu {
                fn_name: SCALAR_FUNCTION
            }
        );
        let input = self
            .prom_expr_to_plan(&args.args[0], query_engine_state)
            .await?;
        let input_schema = input.schema().clone();
        let alternative_samples =
            Self::field_columns_are_alternative_samples(&input_schema, &self.ctx.field_columns);
        let histogram_fields = self
            .ctx
            .field_columns
            .iter()
            .filter(|field| Self::field_column_is_native_histogram(&input_schema, field))
            .count();
        ensure!(
            self.ctx.field_columns.len() == 1 || alternative_samples,
            MultiFieldsNotSupportedSnafu {
                operator: SCALAR_FUNCTION
            },
        );
        let scalar_field = self
            .ctx
            .field_columns
            .iter()
            .find(|field| !Self::field_column_is_native_histogram(&input_schema, field))
            .or_else(|| self.ctx.field_columns.first())
            .cloned()
            .with_context(|| FunctionInvalidArgumentSnafu {
                fn_name: SCALAR_FUNCTION,
            })?;
        let input = if histogram_fields == self.ctx.field_columns.len() {
            // scalar() ignores histogram samples. An empty input makes ScalarCalculate emit NaN
            // for every evaluation timestamp without attempting a Struct-to-Float64 cast.
            LogicalPlanBuilder::from(input)
                .filter(lit(false))
                .context(DataFusionPlanningSnafu)?
                .build()
                .context(DataFusionPlanningSnafu)?
        } else if histogram_fields > 0 {
            // A mixed vector contributes only its float samples to scalar().
            LogicalPlanBuilder::from(input)
                .filter(DfExpr::Column(Column::from_name(&scalar_field)).is_not_null())
                .context(DataFusionPlanningSnafu)?
                .build()
                .context(DataFusionPlanningSnafu)?
        } else {
            input
        };
        let scalar_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(
                ScalarCalculate::new(
                    self.ctx.start,
                    self.ctx.end,
                    self.ctx.interval,
                    input,
                    self.ctx.time_index_column.as_ref().unwrap(),
                    &self.ctx.tag_columns,
                    &scalar_field,
                    self.ctx.table_name.as_deref(),
                )
                .context(PromqlPlanNodeSnafu)?,
            ),
        });
        // scalar plan have no tag columns
        self.ctx.tag_columns.clear();
        self.ctx.aggregation_field_labels.clear();
        self.ctx.field_columns.clear();
        self.ctx
            .field_columns
            .push(scalar_plan.schema().field(1).name().clone());
        Ok(scalar_plan)
    }

    /// Create a [SPECIAL_ABSENT_FUNCTION] plan
    pub(super) async fn create_absent_plan(
        &mut self,
        args: &PromFunctionArgs,
        query_engine_state: &QueryEngineState,
    ) -> Result<LogicalPlan> {
        if args.args.len() != 1 {
            return FunctionInvalidArgumentSnafu {
                fn_name: SPECIAL_ABSENT_FUNCTION.to_string(),
            }
            .fail();
        }
        let input = self
            .prom_expr_to_plan(&args.args[0], query_engine_state)
            .await?;

        let time_index_expr = self.create_time_index_column_expr()?;
        let first_field_expr =
            self.create_field_column_exprs()?
                .pop()
                .with_context(|| ValueNotFoundSnafu {
                    table: self.ctx.table_name.clone().unwrap_or_default(),
                })?;
        let first_value_expr = first_value(first_field_expr, vec![]);

        let ordered_aggregated_input = LogicalPlanBuilder::from(input)
            .aggregate(
                vec![time_index_expr.clone()],
                vec![first_value_expr.clone()],
            )
            .context(DataFusionPlanningSnafu)?
            .sort(vec![time_index_expr.sort(true, false)])
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)?;

        let fake_labels = self
            .ctx
            .selector_matcher
            .iter()
            .filter_map(|matcher| match matcher.op {
                MatchOp::Equal => Some((matcher.name.clone(), matcher.value.clone())),
                _ => None,
            })
            .collect::<Vec<_>>();

        // Create the absent plan
        let absent_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(
                Absent::try_new(
                    self.ctx.start,
                    self.ctx.end,
                    self.ctx.interval,
                    self.ctx.time_index_column.as_ref().unwrap().clone(),
                    self.ctx.field_columns[0].clone(),
                    fake_labels,
                    ordered_aggregated_input,
                )
                .context(DataFusionPlanningSnafu)?,
            ),
        });

        // The absent series carries the equality matchers as labels, not the input's
        // tags or value fields, so the input's field grouping labels no longer apply.
        self.ctx.aggregation_field_labels.clear();
        Ok(absent_plan)
    }
}
