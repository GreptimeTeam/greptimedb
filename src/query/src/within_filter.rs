use chrono::NaiveDate;
use common_time::timestamp::{TimeUnit, Timestamp};
use datafusion::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{BinaryExpr, Expr, Filter, LogicalPlan, Operator};

use crate::optimizer::ExtensionAnalyzerRule;
use crate::QueryEngineContext;

pub struct WithinFilterRule;

impl ExtensionAnalyzerRule for WithinFilterRule {
    fn analyze(
        &self,
        plan: LogicalPlan,
        _ctx: &QueryEngineContext,
        _config: &ConfigOptions,
    ) -> Result<LogicalPlan> {
        plan.transform(|plan| match plan.clone() {
            LogicalPlan::Filter(filter) => {
                if let Expr::ScalarFunction(func) = &filter.predicate
                    && func.func.name() == "within_filter"
                {
                    let column_name = func.args[0].clone();
                    let time_arg = func.args[1].clone();
                    if let Expr::Literal(literal) = time_arg
                        && let ScalarValue::Utf8(Some(s)) = literal
                    {
                        if let Ok(year) = s.parse::<i32>() {
                            let timestamp = NaiveDate::from_ymd_opt(year, 1, 1).unwrap();
                            let timestamp = Timestamp::from_chrono_date(timestamp).unwrap();
                            let value = Some(timestamp.value());
                            let timestamp = match timestamp.unit() {
                                TimeUnit::Second => ScalarValue::TimestampSecond(value, None),
                                TimeUnit::Millisecond => {
                                    ScalarValue::TimestampMillisecond(value, None)
                                }
                                TimeUnit::Microsecond => {
                                    ScalarValue::TimestampMicrosecond(value, None)
                                }
                                TimeUnit::Nanosecond => {
                                    ScalarValue::TimestampNanosecond(value, None)
                                }
                            };
                            let next_timestamp = NaiveDate::from_ymd_opt(year + 1, 1, 1).unwrap();
                            let next_timestamp =
                                Timestamp::from_chrono_date(next_timestamp).unwrap();
                            let value = Some(next_timestamp.value());
                            let next_timestamp = match next_timestamp.unit() {
                                TimeUnit::Second => ScalarValue::TimestampSecond(value, None),
                                TimeUnit::Millisecond => {
                                    ScalarValue::TimestampMillisecond(value, None)
                                }
                                TimeUnit::Microsecond => {
                                    ScalarValue::TimestampMicrosecond(value, None)
                                }
                                TimeUnit::Nanosecond => {
                                    ScalarValue::TimestampNanosecond(value, None)
                                }
                            };
                            let left = Expr::BinaryExpr(BinaryExpr {
                                left: Box::new(column_name.clone()),
                                op: Operator::GtEq,
                                right: Box::new(Expr::Literal(timestamp)),
                            });
                            let right = Expr::BinaryExpr(BinaryExpr {
                                left: Box::new(column_name),
                                op: Operator::Lt,
                                right: Box::new(Expr::Literal(next_timestamp)),
                            });
                            let new_expr = Expr::BinaryExpr(BinaryExpr::new(
                                Box::new(left),
                                Operator::And,
                                Box::new(right),
                            ));
                            let new_plan =
                                LogicalPlan::Filter(Filter::try_new(new_expr, filter.input)?);
                            Ok(Transformed::yes(new_plan))
                        } else {
                            Err(DataFusionError::NotImplemented(
                                "add more formats".to_string(),
                            ))
                        }
                    } else {
                        todo!();
                    }
                } else {
                    Ok(Transformed::no(plan))
                }
            }
            _ => Ok(Transformed::no(plan)),
        })
        .map(|t| t.data)
    }
}
