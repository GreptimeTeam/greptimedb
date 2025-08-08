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

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use arrow_schema::DataType;
use async_recursion::async_recursion;
use catalog::table_source::DfTableSourceProvider;
use chrono::Utc;
use common_time::interval::{MS_PER_DAY, NANOS_PER_MILLI};
use common_time::timestamp::TimeUnit;
use common_time::{IntervalDayTime, IntervalMonthDayNano, IntervalYearMonth, Timestamp, Timezone};
use datafusion::datasource::DefaultTableSource;
use datafusion::prelude::Column;
use datafusion::scalar::ScalarValue;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter};
use datafusion_common::{DFSchema, DataFusionError, Result as DFResult};
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::expr::WildcardOptions;
use datafusion_expr::simplify::SimplifyContext;
use datafusion_expr::{
    Aggregate, Analyze, Cast, Distinct, DistinctOn, Explain, Expr, ExprSchemable, Extension,
    Literal, LogicalPlan, LogicalPlanBuilder, Projection,
};
use datafusion_optimizer::simplify_expressions::ExprSimplifier;
use datatypes::prelude::ConcreteDataType;
use promql_parser::util::parse_duration;
use session::context::QueryContextRef;
use snafu::{ensure, OptionExt, ResultExt};
use table::table::adapter::DfTableProviderAdapter;

use crate::error::{
    CatalogSnafu, RangeQuerySnafu, Result, TimeIndexNotFoundSnafu, UnknownTableSnafu,
};
use crate::plan::ExtractExpr;
use crate::range_select::plan::{Fill, RangeFn, RangeSelect};

/// `RangeExprRewriter` will recursively search certain `Expr`, find all `range_fn` scalar udf contained in `Expr`,
/// and collect the information required by the RangeSelect query,
/// and finally modify the `range_fn` scalar udf to an ordinary column field.
pub struct RangeExprRewriter<'a> {
    input_plan: &'a Arc<LogicalPlan>,
    align: Duration,
    align_to: i64,
    by: Vec<Expr>,
    /// Use `BTreeSet` to avoid in case like `avg(a) RANGE '5m' + avg(a) RANGE '5m'`, duplicate range expr `avg(a) RANGE '5m'` be calculate twice
    range_fn: BTreeSet<RangeFn>,
    sub_aggr: &'a Aggregate,
    query_ctx: &'a QueryContextRef,
}

impl RangeExprRewriter<'_> {
    pub fn get_range_expr(&self, args: &[Expr], i: usize) -> DFResult<Expr> {
        match args.get(i) {
            Some(Expr::Column(column)) => {
                let index = self.sub_aggr.schema.index_of_column(column)?;
                let len = self.sub_aggr.group_expr.len();
                self.sub_aggr
                    .aggr_expr
                    .get(index - len)
                    .cloned()
                    .ok_or(DataFusionError::Plan(
                        "Range expr not found in underlying Aggregate Plan".into(),
                    ))
            }
            other => Err(dispose_parse_error(other)),
        }
    }
}

#[inline]
fn dispose_parse_error(expr: Option<&Expr>) -> DataFusionError {
    DataFusionError::Plan(
        expr.map(|x| {
            format!(
                "Illegal argument `{}` in range select query",
                x.schema_name()
            )
        })
        .unwrap_or("Missing argument in range select query".into()),
    )
}

fn parse_str_expr(args: &[Expr], i: usize) -> DFResult<&str> {
    match args.get(i) {
        Some(Expr::Literal(ScalarValue::Utf8(Some(str)), _)) => Ok(str.as_str()),
        other => Err(dispose_parse_error(other)),
    }
}

fn parse_expr_to_string(args: &[Expr], i: usize) -> DFResult<String> {
    match args.get(i) {
        Some(Expr::Literal(ScalarValue::Utf8(Some(str)), _)) => Ok(str.to_string()),
        Some(expr) => Ok(expr.schema_name().to_string()),
        None => Err(dispose_parse_error(None)),
    }
}

/// Parse a duraion expr:
/// 1. duration string (e.g. `'1h'`)
/// 2. Interval expr (e.g. `INTERVAL '1 year 3 hours 20 minutes'`)
/// 3. An interval expr can be evaluated at the logical plan stage (e.g. `INTERVAL '2' day - INTERVAL '1' day`)
fn parse_duration_expr(args: &[Expr], i: usize) -> DFResult<Duration> {
    match args.get(i) {
        Some(Expr::Literal(ScalarValue::Utf8(Some(str)), _)) => {
            parse_duration(str).map_err(DataFusionError::Plan)
        }
        Some(expr) => {
            let ms = evaluate_expr_to_millisecond(args, i, true)?;
            if ms <= 0 {
                return Err(dispose_parse_error(Some(expr)));
            }
            Ok(Duration::from_millis(ms as u64))
        }
        None => Err(dispose_parse_error(None)),
    }
}

/// Evaluate a time calculation expr, case like:
/// 1. `INTERVAL '1' day + INTERVAL '1 year 2 hours 3 minutes'`
/// 2. `now() - INTERVAL '1' day` (when `interval_only==false`)
///
/// Output a millisecond timestamp
///
/// if `interval_only==true`, only accept expr with all interval type (case 2 will return a error)
fn evaluate_expr_to_millisecond(args: &[Expr], i: usize, interval_only: bool) -> DFResult<i64> {
    let Some(expr) = args.get(i) else {
        return Err(dispose_parse_error(None));
    };
    if interval_only && !interval_only_in_expr(expr) {
        return Err(dispose_parse_error(Some(expr)));
    }
    let execution_props = ExecutionProps::new().with_query_execution_start_time(Utc::now());
    let info = SimplifyContext::new(&execution_props).with_schema(Arc::new(DFSchema::empty()));
    let simplify_expr = ExprSimplifier::new(info).simplify(expr.clone())?;
    match simplify_expr {
        Expr::Literal(ScalarValue::TimestampNanosecond(ts_nanos, _), _)
        | Expr::Literal(ScalarValue::DurationNanosecond(ts_nanos), _) => {
            ts_nanos.map(|v| v / 1_000_000)
        }
        Expr::Literal(ScalarValue::TimestampMicrosecond(ts_micros, _), _)
        | Expr::Literal(ScalarValue::DurationMicrosecond(ts_micros), _) => {
            ts_micros.map(|v| v / 1_000)
        }
        Expr::Literal(ScalarValue::TimestampMillisecond(ts_millis, _), _)
        | Expr::Literal(ScalarValue::DurationMillisecond(ts_millis), _) => ts_millis,
        Expr::Literal(ScalarValue::TimestampSecond(ts_secs, _), _)
        | Expr::Literal(ScalarValue::DurationSecond(ts_secs), _) => ts_secs.map(|v| v * 1_000),
        // We don't support interval with months as days in a month is unclear.
        Expr::Literal(ScalarValue::IntervalYearMonth(interval), _) => interval
            .map(|v| {
                let interval = IntervalYearMonth::from_i32(v);
                if interval.months != 0 {
                    return Err(DataFusionError::Plan(format!(
                        "Year or month interval is not allowed in range query: {}",
                        expr.schema_name()
                    )));
                }

                Ok(0)
            })
            .transpose()?,
        Expr::Literal(ScalarValue::IntervalDayTime(interval), _) => interval.map(|v| {
            let interval = IntervalDayTime::from(v);
            interval.as_millis()
        }),
        Expr::Literal(ScalarValue::IntervalMonthDayNano(interval), _) => interval
            .map(|v| {
                let interval = IntervalMonthDayNano::from(v);
                if interval.months != 0 {
                    return Err(DataFusionError::Plan(format!(
                        "Year or month interval is not allowed in range query: {}",
                        expr.schema_name()
                    )));
                }

                Ok(interval.days as i64 * MS_PER_DAY + interval.nanoseconds / NANOS_PER_MILLI)
            })
            .transpose()?,
        _ => None,
    }
    .ok_or_else(|| {
        DataFusionError::Plan(format!(
            "{} is not a expr can be evaluate and use in range query",
            expr.schema_name()
        ))
    })
}

/// Parse the `align to` clause and return a UTC timestamp with unit of millisecond,
/// which is used as the basis for dividing time slot during the align operation.
/// 1. NOW: align to current execute time
/// 2. Timestamp string: align to specific timestamp
/// 3. An expr can be evaluated at the logical plan stage (e.g. `now() - INTERVAL '1' day`)
/// 4. leave empty (as Default Option): align to unix epoch 0 (timezone aware)
fn parse_align_to(args: &[Expr], i: usize, timezone: Option<&Timezone>) -> DFResult<i64> {
    let Ok(s) = parse_str_expr(args, i) else {
        return evaluate_expr_to_millisecond(args, i, false);
    };
    let upper = s.to_uppercase();
    match upper.as_str() {
        "NOW" => return Ok(Timestamp::current_millis().value()),
        // default align to unix epoch 0 (timezone aware)
        "" => return Ok(timezone.map(|tz| tz.local_minus_utc() * 1000).unwrap_or(0)),
        _ => (),
    }

    Timestamp::from_str(s, timezone)
        .map_err(|e| {
            DataFusionError::Plan(format!(
                "Illegal `align to` argument `{}` in range select query, can't be parse as NOW/CALENDAR/Timestamp, error: {}",
                s, e
            ))
        })?.convert_to(TimeUnit::Millisecond).map(|x|x.value()).ok_or(DataFusionError::Plan(format!(
            "Illegal `align to` argument `{}` in range select query, can't be convert to a valid Timestamp",
            s
        ))
        )
}

fn parse_expr_list(args: &[Expr], start: usize, len: usize) -> DFResult<Vec<Expr>> {
    let mut outs = Vec::with_capacity(len);
    for i in start..start + len {
        outs.push(match &args.get(i) {
            Some(
                Expr::Column(_)
                | Expr::Literal(_, _)
                | Expr::BinaryExpr(_)
                | Expr::ScalarFunction(_),
            ) => args[i].clone(),
            other => {
                return Err(dispose_parse_error(*other));
            }
        });
    }
    Ok(outs)
}

macro_rules! inconsistent_check {
    ($self: ident.$name: ident, $cond: expr) => {
        if $cond && $self.$name != $name {
            return Err(DataFusionError::Plan(
                concat!(
                    "Inconsistent ",
                    stringify!($name),
                    " given in Range Function Rewrite"
                )
                .into(),
            ));
        } else {
            $self.$name = $name;
        }
    };
}

impl TreeNodeRewriter for RangeExprRewriter<'_> {
    type Node = Expr;

    fn f_down(&mut self, node: Expr) -> DFResult<Transformed<Expr>> {
        if let Expr::ScalarFunction(func) = &node {
            if func.name() == "range_fn" {
                // `range_fn(func, range, fill, byc, [byv], align, to)`
                // `[byv]` are variadic arguments, byc indicate the length of arguments
                let range_expr = self.get_range_expr(&func.args, 0)?;
                let range = parse_duration_expr(&func.args, 1)?;
                let byc = str::parse::<usize>(parse_str_expr(&func.args, 3)?)
                    .map_err(|e| DataFusionError::Plan(e.to_string()))?;
                let by = parse_expr_list(&func.args, 4, byc)?;
                let align = parse_duration_expr(&func.args, byc + 4)?;
                let align_to =
                    parse_align_to(&func.args, byc + 5, Some(&self.query_ctx.timezone()))?;
                let mut data_type = range_expr.get_type(self.input_plan.schema())?;
                let mut need_cast = false;
                let fill = Fill::try_from_str(parse_str_expr(&func.args, 2)?, &data_type)?;
                if matches!(fill, Some(Fill::Linear)) && data_type.is_integer() {
                    data_type = DataType::Float64;
                    need_cast = true;
                }
                inconsistent_check!(self.by, !self.by.is_empty());
                inconsistent_check!(self.align, self.align != Duration::default());
                inconsistent_check!(self.align_to, self.align_to != 0);
                let range_fn = RangeFn {
                    name: if let Some(fill) = &fill {
                        format!(
                            "{} RANGE {} FILL {}",
                            range_expr.schema_name(),
                            parse_expr_to_string(&func.args, 1)?,
                            fill
                        )
                    } else {
                        format!(
                            "{} RANGE {}",
                            range_expr.schema_name(),
                            parse_expr_to_string(&func.args, 1)?,
                        )
                    },
                    data_type,
                    expr: range_expr,
                    range,
                    fill,
                    need_cast,
                };
                let alias = Expr::Column(Column::from_name(range_fn.name.clone()));
                self.range_fn.insert(range_fn);
                return Ok(Transformed::yes(alias));
            }
        }
        Ok(Transformed::no(node))
    }
}

/// In order to implement RangeSelect query like `avg(field_0) RANGE '5m' FILL NULL`,
/// All RangeSelect query items are converted into udf scalar function in sql parse stage, with format like `range_fn(avg(field_0), .....)`.
/// `range_fn` contains all the parameters we need to execute RangeSelect.
/// In order to correctly execute the query process of range select, we need to modify the query plan generated by datafusion.
/// We need to recursively find the entire LogicalPlan, and find all `range_fn` scalar udf contained in the project plan,
/// collecting info we need to generate RangeSelect Query LogicalPlan and rewrite th original LogicalPlan.
pub struct RangePlanRewriter {
    table_provider: DfTableSourceProvider,
    query_ctx: QueryContextRef,
}

impl RangePlanRewriter {
    pub fn new(table_provider: DfTableSourceProvider, query_ctx: QueryContextRef) -> Self {
        Self {
            table_provider,
            query_ctx,
        }
    }

    pub async fn rewrite(&mut self, plan: LogicalPlan) -> Result<LogicalPlan> {
        match self.rewrite_logical_plan(&plan).await? {
            Some(new_plan) => Ok(new_plan),
            None => Ok(plan),
        }
    }

    #[async_recursion]
    async fn rewrite_logical_plan(&mut self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        let inputs = plan.inputs();
        let mut new_inputs = Vec::with_capacity(inputs.len());
        for input in &inputs {
            new_inputs.push(self.rewrite_logical_plan(input).await?)
        }
        match plan {
            LogicalPlan::Projection(Projection { expr, input, .. })
                if have_range_in_exprs(expr) =>
            {
                let (aggr_plan, input) = if let LogicalPlan::Aggregate(aggr) = input.as_ref() {
                    // Expr like `rate(max(a) RANGE '6m') RANGE '6m'` have legal syntax but illegal semantic.
                    if have_range_in_exprs(&aggr.aggr_expr) {
                        return RangeQuerySnafu {
                            msg: "Nest Range Query is not allowed",
                        }
                        .fail();
                    }
                    (aggr, aggr.input.clone())
                } else {
                    return RangeQuerySnafu {
                        msg: "Window functions is not allowed in Range Query",
                    }
                    .fail();
                };
                let (time_index, default_by) = self.get_index_by(input.schema()).await?;
                let mut range_rewriter = RangeExprRewriter {
                    input_plan: &input,
                    align: Duration::default(),
                    align_to: 0,
                    by: vec![],
                    range_fn: BTreeSet::new(),
                    sub_aggr: aggr_plan,
                    query_ctx: &self.query_ctx,
                };
                let new_expr = expr
                    .iter()
                    .map(|expr| expr.clone().rewrite(&mut range_rewriter).map(|x| x.data))
                    .collect::<DFResult<Vec<_>>>()?;
                if range_rewriter.by.is_empty() {
                    range_rewriter.by = default_by;
                }
                let range_select = RangeSelect::try_new(
                    input.clone(),
                    range_rewriter.range_fn.into_iter().collect(),
                    range_rewriter.align,
                    range_rewriter.align_to,
                    time_index,
                    range_rewriter.by,
                    &new_expr,
                )?;
                let no_additional_project = range_select.schema_project.is_some();
                let range_plan = LogicalPlan::Extension(Extension {
                    node: Arc::new(range_select),
                });
                if no_additional_project {
                    Ok(Some(range_plan))
                } else {
                    let project_plan = LogicalPlanBuilder::from(range_plan)
                        .project(new_expr)
                        .and_then(|x| x.build())?;
                    Ok(Some(project_plan))
                }
            }
            _ => {
                if new_inputs.iter().any(|x| x.is_some()) {
                    let inputs: Vec<LogicalPlan> = new_inputs
                        .into_iter()
                        .zip(inputs)
                        .map(|(x, y)| match x {
                            Some(plan) => plan,
                            None => y.clone(),
                        })
                        .collect();
                    // Due to the limitations of Datafusion, for `LogicalPlan::Analyze` and `LogicalPlan::Explain`,
                    // directly using the method `with_new_inputs` to rebuild a new `LogicalPlan` will cause an error,
                    // so here we directly use the `LogicalPlanBuilder` to build a new plan.
                    let plan = match plan {
                        LogicalPlan::Analyze(Analyze { verbose, .. }) => {
                            ensure!(
                                inputs.len() == 1,
                                RangeQuerySnafu {
                                    msg: "Illegal subplan nums when rewrite Analyze logical plan",
                                }
                            );
                            LogicalPlanBuilder::from(inputs[0].clone())
                                .explain(*verbose, true)?
                                .build()
                        }
                        LogicalPlan::Explain(Explain { verbose, .. }) => {
                            ensure!(
                                inputs.len() == 1,
                                RangeQuerySnafu {
                                    msg: "Illegal subplan nums when rewrite Explain logical plan",
                                }
                            );
                            LogicalPlanBuilder::from(inputs[0].clone())
                                .explain(*verbose, false)?
                                .build()
                        }
                        LogicalPlan::Distinct(Distinct::On(DistinctOn {
                            on_expr,
                            select_expr,
                            sort_expr,
                            ..
                        })) => {
                            ensure!(
                                inputs.len() == 1,
                                RangeQuerySnafu {
                                    msg:
                                        "Illegal subplan nums when rewrite DistinctOn logical plan",
                                }
                            );
                            LogicalPlanBuilder::from(inputs[0].clone())
                                .distinct_on(
                                    on_expr.clone(),
                                    select_expr.clone(),
                                    sort_expr.clone(),
                                )?
                                .build()
                        }
                        _ => plan.with_new_exprs(plan.expressions_consider_join(), inputs),
                    }?;
                    Ok(Some(plan))
                } else {
                    Ok(None)
                }
            }
        }
    }

    /// this function use to find the time_index column and row columns from input schema,
    /// return `(time_index, [row_columns])` to the rewriter.
    /// If the user does not explicitly use the `by` keyword to indicate time series,
    /// `[row_columns]` will be use as default time series
    async fn get_index_by(&mut self, schema: &Arc<DFSchema>) -> Result<(Expr, Vec<Expr>)> {
        #[allow(deprecated)]
        let mut time_index_expr = Expr::Wildcard {
            qualifier: None,
            options: Box::new(WildcardOptions::default()),
        };
        let mut default_by = vec![];
        for i in 0..schema.fields().len() {
            let (qualifier, _) = schema.qualified_field(i);
            if let Some(table_ref) = qualifier {
                let table = self
                    .table_provider
                    .resolve_table(table_ref.clone())
                    .await
                    .context(CatalogSnafu)?
                    .as_any()
                    .downcast_ref::<DefaultTableSource>()
                    .context(UnknownTableSnafu)?
                    .table_provider
                    .as_any()
                    .downcast_ref::<DfTableProviderAdapter>()
                    .context(UnknownTableSnafu)?
                    .table();
                let schema = table.schema();
                let time_index_column =
                    schema
                        .timestamp_column()
                        .with_context(|| TimeIndexNotFoundSnafu {
                            table: table_ref.to_string(),
                        })?;
                // assert time_index's datatype is timestamp
                if let ConcreteDataType::Timestamp(_) = time_index_column.data_type {
                    default_by = table
                        .table_info()
                        .meta
                        .row_key_column_names()
                        .map(|key| Expr::Column(Column::new(Some(table_ref.clone()), key)))
                        .collect();
                    // If the user does not specify a primary key when creating a table,
                    // then by default all data will be aggregated into one time series,
                    // which is equivalent to using `by(1)` in SQL
                    if default_by.is_empty() {
                        default_by = vec![1.lit()];
                    }
                    time_index_expr = Expr::Column(Column::new(
                        Some(table_ref.clone()),
                        time_index_column.name.clone(),
                    ));
                }
            }
        }
        #[allow(deprecated)]
        if matches!(time_index_expr, Expr::Wildcard { .. }) {
            TimeIndexNotFoundSnafu {
                table: schema.to_string(),
            }
            .fail()
        } else {
            Ok((time_index_expr, default_by))
        }
    }
}

fn have_range_in_exprs(exprs: &[Expr]) -> bool {
    exprs.iter().any(|expr| {
        let mut find_range = false;
        let _ = expr.apply(|expr| {
            Ok(match expr {
                Expr::ScalarFunction(func) if func.name() == "range_fn" => {
                    find_range = true;
                    TreeNodeRecursion::Stop
                }
                _ => TreeNodeRecursion::Continue,
            })
        });
        find_range
    })
}

fn interval_only_in_expr(expr: &Expr) -> bool {
    let mut all_interval = true;
    let _ = expr.apply(|expr| {
        // A cast expression for an interval.
        if matches!(
            expr,
            Expr::Cast(Cast{
                expr,
                data_type: DataType::Interval(_)
            }) if matches!(&**expr, Expr::Literal(ScalarValue::Utf8(_), _))
        ) {
            // Stop checking the sub `expr`,
            // which is a `Utf8` type and has already been tested above.
            return Ok(TreeNodeRecursion::Stop);
        }

        if !matches!(
            expr,
            Expr::Literal(ScalarValue::IntervalDayTime(_), _)
                | Expr::Literal(ScalarValue::IntervalMonthDayNano(_), _)
                | Expr::Literal(ScalarValue::IntervalYearMonth(_), _)
                | Expr::BinaryExpr(_)
                | Expr::Cast(Cast {
                    data_type: DataType::Interval(_),
                    ..
                })
        ) {
            all_interval = false;
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    });

    all_interval
}

#[cfg(test)]
mod test {

    use arrow::datatypes::IntervalUnit;
    use catalog::memory::MemoryCatalogManager;
    use catalog::RegisterTableRequest;
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_time::IntervalYearMonth;
    use datafusion_expr::{BinaryExpr, Literal, Operator};
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use session::context::QueryContext;
    use table::metadata::{TableInfoBuilder, TableMetaBuilder};
    use table::test_util::EmptyTable;

    use super::*;
    use crate::options::QueryOptions;
    use crate::parser::QueryLanguageParser;
    use crate::{QueryEngineFactory, QueryEngineRef};

    async fn create_test_engine() -> QueryEngineRef {
        let table_name = "test".to_string();
        let mut columns = vec![];
        for i in 0..5 {
            columns.push(ColumnSchema::new(
                format!("tag_{i}"),
                ConcreteDataType::string_datatype(),
                false,
            ));
        }
        columns.push(
            ColumnSchema::new(
                "timestamp".to_string(),
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
        );
        for i in 0..5 {
            columns.push(ColumnSchema::new(
                format!("field_{i}"),
                ConcreteDataType::float64_datatype(),
                true,
            ));
        }
        let schema = Arc::new(Schema::new(columns));
        let table_meta = TableMetaBuilder::empty()
            .schema(schema)
            .primary_key_indices((0..5).collect())
            .value_indices((6..11).collect())
            .next_column_id(1024)
            .build()
            .unwrap();
        let table_info = TableInfoBuilder::default()
            .name(&table_name)
            .meta(table_meta)
            .build()
            .unwrap();
        let table = EmptyTable::from_table_info(&table_info);
        let catalog_list = MemoryCatalogManager::with_default_setup();
        assert!(catalog_list
            .register_table_sync(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name,
                table_id: 1024,
                table,
            })
            .is_ok());
        QueryEngineFactory::new(
            catalog_list,
            None,
            None,
            None,
            None,
            false,
            QueryOptions::default(),
        )
        .query_engine()
    }

    async fn do_query(sql: &str) -> Result<LogicalPlan> {
        let stmt = QueryLanguageParser::parse_sql(sql, &QueryContext::arc()).unwrap();
        let engine = create_test_engine().await;
        engine.planner().plan(&stmt, QueryContext::arc()).await
    }

    async fn query_plan_compare(sql: &str, expected: String) {
        let plan = do_query(sql).await.unwrap();
        assert_eq!(plan.display_indent_schema().to_string(), expected);
    }

    #[tokio::test]
    async fn range_no_project() {
        let query = r#"SELECT timestamp, tag_0, tag_1, avg(field_0 + field_1) RANGE '5m' FROM test ALIGN '1h' by (tag_0,tag_1);"#;
        let expected = String::from(
            "RangeSelect: range_exprs=[avg(test.field_0 + test.field_1) RANGE 5m], align=3600000ms, align_to=0ms, align_by=[test.tag_0, test.tag_1], time_index=timestamp [timestamp:Timestamp(Millisecond, None), tag_0:Utf8, tag_1:Utf8, avg(test.field_0 + test.field_1) RANGE 5m:Float64;N]\
            \n  TableScan: test [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, tag_4:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N, field_3:Float64;N, field_4:Float64;N]"
        );
        query_plan_compare(query, expected).await;
    }

    #[tokio::test]
    async fn range_expr_calculation() {
        let query = r#"SELECT (avg(field_0 + field_1)/4) RANGE '5m' FROM test ALIGN '1h' by (tag_0,tag_1);"#;
        let expected = String::from(
            "Projection: avg(test.field_0 + test.field_1) RANGE 5m / Int64(4) [avg(test.field_0 + test.field_1) RANGE 5m / Int64(4):Float64;N]\
            \n  RangeSelect: range_exprs=[avg(test.field_0 + test.field_1) RANGE 5m], align=3600000ms, align_to=0ms, align_by=[test.tag_0, test.tag_1], time_index=timestamp [avg(test.field_0 + test.field_1) RANGE 5m:Float64;N, timestamp:Timestamp(Millisecond, None), tag_0:Utf8, tag_1:Utf8]\
            \n    TableScan: test [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, tag_4:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N, field_3:Float64;N, field_4:Float64;N]"
        );
        query_plan_compare(query, expected).await;
    }

    #[tokio::test]
    async fn range_multi_args() {
        let query =
            r#"SELECT (covar(field_0 + field_1, field_1)/4) RANGE '5m' FROM test ALIGN '1h';"#;
        let expected = String::from(
            "Projection: covar_samp(test.field_0 + test.field_1,test.field_1) RANGE 5m / Int64(4) [covar_samp(test.field_0 + test.field_1,test.field_1) RANGE 5m / Int64(4):Float64;N]\
            \n  RangeSelect: range_exprs=[covar_samp(test.field_0 + test.field_1,test.field_1) RANGE 5m], align=3600000ms, align_to=0ms, align_by=[test.tag_0, test.tag_1, test.tag_2, test.tag_3, test.tag_4], time_index=timestamp [covar_samp(test.field_0 + test.field_1,test.field_1) RANGE 5m:Float64;N, timestamp:Timestamp(Millisecond, None), tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, tag_4:Utf8]\
            \n    TableScan: test [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, tag_4:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N, field_3:Float64;N, field_4:Float64;N]"
        );
        query_plan_compare(query, expected).await;
    }

    #[tokio::test]
    async fn range_calculation() {
        let query = r#"SELECT ((avg(field_0)+sum(field_1))/4) RANGE '5m' FROM test ALIGN '1h' by (tag_0,tag_1) FILL NULL;"#;
        let expected = String::from(
            "Projection: (avg(test.field_0) RANGE 5m FILL NULL + sum(test.field_1) RANGE 5m FILL NULL) / Int64(4) [avg(test.field_0) RANGE 5m FILL NULL + sum(test.field_1) RANGE 5m FILL NULL / Int64(4):Float64;N]\
            \n  RangeSelect: range_exprs=[avg(test.field_0) RANGE 5m FILL NULL, sum(test.field_1) RANGE 5m FILL NULL], align=3600000ms, align_to=0ms, align_by=[test.tag_0, test.tag_1], time_index=timestamp [avg(test.field_0) RANGE 5m FILL NULL:Float64;N, sum(test.field_1) RANGE 5m FILL NULL:Float64;N, timestamp:Timestamp(Millisecond, None), tag_0:Utf8, tag_1:Utf8]\
            \n    TableScan: test [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, tag_4:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N, field_3:Float64;N, field_4:Float64;N]"
        );
        query_plan_compare(query, expected).await;
    }

    #[tokio::test]
    async fn range_as_sub_query() {
        let query = r#"SELECT foo + 1 from (SELECT ((avg(field_0)+sum(field_1))/4) RANGE '5m' as foo FROM test ALIGN '1h' by (tag_0,tag_1) FILL NULL) where foo > 1;"#;
        let expected = String::from(
            "Projection: foo + Int64(1) [foo + Int64(1):Float64;N]\
            \n  Filter: foo > Int64(1) [foo:Float64;N]\
            \n    Projection: (avg(test.field_0) RANGE 5m FILL NULL + sum(test.field_1) RANGE 5m FILL NULL) / Int64(4) AS foo [foo:Float64;N]\
            \n      RangeSelect: range_exprs=[avg(test.field_0) RANGE 5m FILL NULL, sum(test.field_1) RANGE 5m FILL NULL], align=3600000ms, align_to=0ms, align_by=[test.tag_0, test.tag_1], time_index=timestamp [avg(test.field_0) RANGE 5m FILL NULL:Float64;N, sum(test.field_1) RANGE 5m FILL NULL:Float64;N, timestamp:Timestamp(Millisecond, None), tag_0:Utf8, tag_1:Utf8]\
            \n        TableScan: test [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, tag_4:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N, field_3:Float64;N, field_4:Float64;N]"
        );
        query_plan_compare(query, expected).await;
    }

    #[tokio::test]
    async fn range_from_nest_query() {
        let query = r#"SELECT ((avg(a)+sum(b))/4) RANGE '5m' FROM (SELECT field_0 as a, field_1 as b, tag_0 as c, tag_1 as d, timestamp from test where field_0 > 1.0) ALIGN '1h' by (c, d) FILL NULL;"#;
        let expected = String::from(
            "Projection: (avg(a) RANGE 5m FILL NULL + sum(b) RANGE 5m FILL NULL) / Int64(4) [avg(a) RANGE 5m FILL NULL + sum(b) RANGE 5m FILL NULL / Int64(4):Float64;N]\
            \n  RangeSelect: range_exprs=[avg(a) RANGE 5m FILL NULL, sum(b) RANGE 5m FILL NULL], align=3600000ms, align_to=0ms, align_by=[c, d], time_index=timestamp [avg(a) RANGE 5m FILL NULL:Float64;N, sum(b) RANGE 5m FILL NULL:Float64;N, timestamp:Timestamp(Millisecond, None), c:Utf8, d:Utf8]\
            \n    Projection: test.field_0 AS a, test.field_1 AS b, test.tag_0 AS c, test.tag_1 AS d, test.timestamp [a:Float64;N, b:Float64;N, c:Utf8, d:Utf8, timestamp:Timestamp(Millisecond, None)]\
            \n      Filter: test.field_0 > Float64(1) [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, tag_4:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N, field_3:Float64;N, field_4:Float64;N]\
            \n        TableScan: test [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, tag_4:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N, field_3:Float64;N, field_4:Float64;N]"
         );
        query_plan_compare(query, expected).await;
    }

    #[tokio::test]
    async fn range_in_expr() {
        let query = r#"SELECT sin(avg(field_0 + field_1) RANGE '5m' + 1) FROM test ALIGN '1h' by (tag_0,tag_1);"#;
        let expected = String::from(
            "Projection: sin(avg(test.field_0 + test.field_1) RANGE 5m + Int64(1)) [sin(avg(test.field_0 + test.field_1) RANGE 5m + Int64(1)):Float64;N]\
            \n  RangeSelect: range_exprs=[avg(test.field_0 + test.field_1) RANGE 5m], align=3600000ms, align_to=0ms, align_by=[test.tag_0, test.tag_1], time_index=timestamp [avg(test.field_0 + test.field_1) RANGE 5m:Float64;N, timestamp:Timestamp(Millisecond, None), tag_0:Utf8, tag_1:Utf8]\
            \n    TableScan: test [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, tag_4:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N, field_3:Float64;N, field_4:Float64;N]"
        );
        query_plan_compare(query, expected).await;
    }

    #[tokio::test]
    async fn duplicate_range_expr() {
        let query = r#"SELECT avg(field_0) RANGE '5m' FILL 6.0 + avg(field_0) RANGE '5m' FILL 6.0 FROM test ALIGN '1h' by (tag_0,tag_1);"#;
        let expected = String::from(
            "Projection: avg(test.field_0) RANGE 5m FILL 6 + avg(test.field_0) RANGE 5m FILL 6 [avg(test.field_0) RANGE 5m FILL 6 + avg(test.field_0) RANGE 5m FILL 6:Float64]\
            \n  RangeSelect: range_exprs=[avg(test.field_0) RANGE 5m FILL 6], align=3600000ms, align_to=0ms, align_by=[test.tag_0, test.tag_1], time_index=timestamp [avg(test.field_0) RANGE 5m FILL 6:Float64, timestamp:Timestamp(Millisecond, None), tag_0:Utf8, tag_1:Utf8]\
            \n    TableScan: test [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, tag_4:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N, field_3:Float64;N, field_4:Float64;N]"
        );
        query_plan_compare(query, expected).await;
    }

    #[tokio::test]
    async fn deep_nest_range_expr() {
        let query = r#"SELECT round(sin(avg(field_0 + field_1) RANGE '5m' + 1)) FROM test ALIGN '1h' by (tag_0,tag_1);"#;
        let expected = String::from(
            "Projection: round(sin(avg(test.field_0 + test.field_1) RANGE 5m + Int64(1))) [round(sin(avg(test.field_0 + test.field_1) RANGE 5m + Int64(1))):Float64;N]\
            \n  RangeSelect: range_exprs=[avg(test.field_0 + test.field_1) RANGE 5m], align=3600000ms, align_to=0ms, align_by=[test.tag_0, test.tag_1], time_index=timestamp [avg(test.field_0 + test.field_1) RANGE 5m:Float64;N, timestamp:Timestamp(Millisecond, None), tag_0:Utf8, tag_1:Utf8]\
            \n    TableScan: test [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, tag_4:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N, field_3:Float64;N, field_4:Float64;N]"
        );
        query_plan_compare(query, expected).await;
    }

    #[tokio::test]
    async fn complex_range_expr() {
        let query = r#"SELECT gcd(CAST(max(field_0 + 1) Range '5m' FILL NULL AS Int64), CAST(tag_0 AS Int64)) + round(max(field_2+1) Range '6m' FILL NULL + 1) + max(field_2+3) Range '10m' FILL NULL * CAST(tag_1 AS Float64) + 1 FROM test ALIGN '1h' by (tag_0, tag_1);"#;
        let expected = String::from(
            "Projection: gcd(arrow_cast(max(test.field_0 + Int64(1)) RANGE 5m FILL NULL, Utf8(\"Int64\")), arrow_cast(test.tag_0, Utf8(\"Int64\"))) + round(max(test.field_2 + Int64(1)) RANGE 6m FILL NULL + Int64(1)) + max(test.field_2 + Int64(3)) RANGE 10m FILL NULL * arrow_cast(test.tag_1, Utf8(\"Float64\")) + Int64(1) [gcd(arrow_cast(max(test.field_0 + Int64(1)) RANGE 5m FILL NULL,Utf8(\"Int64\")),arrow_cast(test.tag_0,Utf8(\"Int64\"))) + round(max(test.field_2 + Int64(1)) RANGE 6m FILL NULL + Int64(1)) + max(test.field_2 + Int64(3)) RANGE 10m FILL NULL * arrow_cast(test.tag_1,Utf8(\"Float64\")) + Int64(1):Float64;N]\
            \n  RangeSelect: range_exprs=[max(test.field_0 + Int64(1)) RANGE 5m FILL NULL, max(test.field_2 + Int64(1)) RANGE 6m FILL NULL, max(test.field_2 + Int64(3)) RANGE 10m FILL NULL], align=3600000ms, align_to=0ms, align_by=[test.tag_0, test.tag_1], time_index=timestamp [max(test.field_0 + Int64(1)) RANGE 5m FILL NULL:Float64;N, max(test.field_2 + Int64(1)) RANGE 6m FILL NULL:Float64;N, max(test.field_2 + Int64(3)) RANGE 10m FILL NULL:Float64;N, timestamp:Timestamp(Millisecond, None), tag_0:Utf8, tag_1:Utf8]\
            \n    TableScan: test [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, tag_4:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N, field_3:Float64;N, field_4:Float64;N]"
        );
        query_plan_compare(query, expected).await;
    }

    #[tokio::test]
    async fn range_linear_on_integer() {
        let query = r#"SELECT min(CAST(field_0 AS Int64) + CAST(field_1 AS Int64)) RANGE '5m' FILL LINEAR FROM test ALIGN '1h' by (tag_0,tag_1);"#;
        let expected = String::from(
            "RangeSelect: range_exprs=[min(arrow_cast(test.field_0,Utf8(\"Int64\")) + arrow_cast(test.field_1,Utf8(\"Int64\"))) RANGE 5m FILL LINEAR], align=3600000ms, align_to=0ms, align_by=[test.tag_0, test.tag_1], time_index=timestamp [min(arrow_cast(test.field_0,Utf8(\"Int64\")) + arrow_cast(test.field_1,Utf8(\"Int64\"))) RANGE 5m FILL LINEAR:Float64;N]\
            \n  TableScan: test [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, tag_4:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N, field_3:Float64;N, field_4:Float64;N]"
        );
        query_plan_compare(query, expected).await;
    }

    #[tokio::test]
    async fn range_nest_range_err() {
        let query = r#"SELECT sum(avg(field_0 + field_1) RANGE '5m' + 1) RANGE '5m' + 1 FROM test ALIGN '1h' by (tag_0,tag_1);"#;
        assert_eq!(
            do_query(query).await.unwrap_err().to_string(),
            "Range Query: Nest Range Query is not allowed"
        )
    }

    #[tokio::test]
    /// Start directly from the rewritten SQL and check whether the error reported by the range expression rewriting is as expected.
    /// the right argument is `range_fn(avg(field_0), '5m', 'NULL', '0', '1h')`
    async fn range_argument_err_1() {
        let query = r#"SELECT range_fn('5m', avg(field_0), 'NULL', '1', tag_0, '1h') FROM test group by tag_0;"#;
        let error = do_query(query).await.unwrap_err().to_string();
        assert_eq!(
            error,
            "Error during planning: Illegal argument `Utf8(\"5m\")` in range select query"
        )
    }

    #[tokio::test]
    async fn range_argument_err_2() {
        let query = r#"SELECT range_fn(avg(field_0), 5, 'NULL', '1', tag_0, '1h') FROM test group by tag_0;"#;
        let error = do_query(query).await.unwrap_err().to_string();
        assert_eq!(
            error,
            "Error during planning: Illegal argument `Int64(5)` in range select query"
        )
    }

    #[test]
    fn test_parse_duration_expr() {
        // test IntervalYearMonth
        let interval = IntervalYearMonth::new(10);
        let args = vec![ScalarValue::IntervalYearMonth(Some(interval.to_i32())).lit()];
        assert!(parse_duration_expr(&args, 0).is_err(),);
        // test IntervalDayTime
        let interval = IntervalDayTime::new(10, 10);
        let args = vec![ScalarValue::IntervalDayTime(Some(interval.into())).lit()];
        assert_eq!(
            parse_duration_expr(&args, 0).unwrap().as_millis() as i64,
            interval.as_millis()
        );
        // test IntervalMonthDayNano
        let interval = IntervalMonthDayNano::new(0, 10, 10);
        let args = vec![ScalarValue::IntervalMonthDayNano(Some(interval.into())).lit()];
        assert_eq!(
            parse_duration_expr(&args, 0).unwrap().as_millis() as i64,
            interval.days as i64 * MS_PER_DAY + interval.nanoseconds / NANOS_PER_MILLI,
        );
        // test Duration
        let args = vec!["1y4w".lit()];
        assert_eq!(
            parse_duration_expr(&args, 0).unwrap(),
            parse_duration("1y4w").unwrap()
        );
        // test cast expression
        let args = vec![Expr::Cast(Cast {
            expr: Box::new("15 minutes".lit()),
            data_type: DataType::Interval(IntervalUnit::MonthDayNano),
        })];
        assert_eq!(
            parse_duration_expr(&args, 0).unwrap(),
            parse_duration("15m").unwrap()
        );
        // test index err
        assert!(parse_duration_expr(&args, 10).is_err());
        // test evaluate expr
        let args = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(
                ScalarValue::IntervalDayTime(Some(IntervalDayTime::new(0, 10).into())).lit(),
            ),
            op: Operator::Plus,
            right: Box::new(
                ScalarValue::IntervalDayTime(Some(IntervalDayTime::new(0, 10).into())).lit(),
            ),
        })];
        assert_eq!(
            parse_duration_expr(&args, 0).unwrap(),
            Duration::from_millis(20)
        );
        let args = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(
                ScalarValue::IntervalDayTime(Some(IntervalDayTime::new(0, 10).into())).lit(),
            ),
            op: Operator::Minus,
            right: Box::new(
                ScalarValue::IntervalDayTime(Some(IntervalDayTime::new(0, 10).into())).lit(),
            ),
        })];
        // test zero interval error
        assert!(parse_duration_expr(&args, 0).is_err());
        // test must all be interval
        let args = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(
                ScalarValue::IntervalYearMonth(Some(IntervalYearMonth::new(10).to_i32())).lit(),
            ),
            op: Operator::Minus,
            right: Box::new(ScalarValue::Time64Microsecond(Some(0)).lit()),
        })];
        assert!(parse_duration_expr(&args, 0).is_err());
    }

    #[test]
    fn test_parse_align_to() {
        // test NOW
        let args = vec!["NOW".lit()];
        let epsinon = parse_align_to(&args, 0, None).unwrap() - Timestamp::current_millis().value();
        assert!(epsinon.abs() < 100);
        // test default
        let args = vec!["".lit()];
        assert_eq!(0, parse_align_to(&args, 0, None).unwrap());
        // test default with timezone
        let args = vec!["".lit()];
        assert_eq!(
            -36000 * 1000,
            parse_align_to(&args, 0, Some(&Timezone::from_tz_string("HST").unwrap())).unwrap()
        );
        assert_eq!(
            28800 * 1000,
            parse_align_to(
                &args,
                0,
                Some(&Timezone::from_tz_string("Asia/Shanghai").unwrap())
            )
            .unwrap()
        );

        // test Timestamp
        let args = vec!["1970-01-01T00:00:00+08:00".lit()];
        assert_eq!(parse_align_to(&args, 0, None).unwrap(), -8 * 60 * 60 * 1000);
        // timezone
        let args = vec!["1970-01-01T00:00:00".lit()];
        assert_eq!(
            parse_align_to(
                &args,
                0,
                Some(&Timezone::from_tz_string("Asia/Shanghai").unwrap())
            )
            .unwrap(),
            -8 * 60 * 60 * 1000
        );
        // test evaluate expr
        let args = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(
                ScalarValue::IntervalDayTime(Some(IntervalDayTime::new(0, 10).into())).lit(),
            ),
            op: Operator::Plus,
            right: Box::new(
                ScalarValue::IntervalDayTime(Some(IntervalDayTime::new(0, 10).into())).lit(),
            ),
        })];
        assert_eq!(parse_align_to(&args, 0, None).unwrap(), 20);
    }

    #[test]
    fn test_interval_only() {
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(ScalarValue::DurationMillisecond(Some(20)).lit()),
            op: Operator::Minus,
            right: Box::new(
                ScalarValue::IntervalDayTime(Some(IntervalDayTime::new(10, 0).into())).lit(),
            ),
        });
        assert!(!interval_only_in_expr(&expr));
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(
                ScalarValue::IntervalDayTime(Some(IntervalDayTime::new(10, 0).into())).lit(),
            ),
            op: Operator::Minus,
            right: Box::new(
                ScalarValue::IntervalDayTime(Some(IntervalDayTime::new(10, 0).into())).lit(),
            ),
        });
        assert!(interval_only_in_expr(&expr));

        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Cast(Cast {
                expr: Box::new("15 minute".lit()),
                data_type: DataType::Interval(IntervalUnit::MonthDayNano),
            })),
            op: Operator::Minus,
            right: Box::new(
                ScalarValue::IntervalDayTime(Some(IntervalDayTime::new(10, 0).into())).lit(),
            ),
        });
        assert!(interval_only_in_expr(&expr));

        let expr = Expr::Cast(Cast {
            expr: Box::new(Expr::BinaryExpr(BinaryExpr {
                left: Box::new(Expr::Cast(Cast {
                    expr: Box::new("15 minute".lit()),
                    data_type: DataType::Interval(IntervalUnit::MonthDayNano),
                })),
                op: Operator::Minus,
                right: Box::new(
                    ScalarValue::IntervalDayTime(Some(IntervalDayTime::new(10, 0).into())).lit(),
                ),
            })),
            data_type: DataType::Interval(IntervalUnit::MonthDayNano),
        });

        assert!(interval_only_in_expr(&expr));
    }
}
