use std::sync::Arc;

use chrono::format::{Parsed, StrftimeItems};
use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime};
use common_function::scalars::math::WITHIN_FILTER_NAME;
use common_time::timestamp::{TimeUnit, Timestamp};
use datafusion::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DataFusionError, Result as DFResult, ScalarValue};
use datafusion_expr::{BinaryExpr, Expr, Filter, LogicalPlan, Operator};
use snafu::ensure;

use crate::error::WithinFilterInternalSnafu;
use crate::optimizer::ExtensionAnalyzerRule;
use crate::QueryEngineContext;

/// `WithinFilterRule` is an analyzer rule for DataFusion that converts the `within(ts, timestamp)`
/// function to a range
/// expression like `ts >= start AND ts < end`.
///
/// # Purpose
/// This rule simplifies time-based queries for users by letting them specify
/// a single timestamp like `'2025-04-19'` while the filter
/// is converted to:
/// ```sql
/// ts >= '2025-04-19 00:00:00' AND ts < '2025-04-20 00:00:00'
/// ```
/// Instead of writing these conditions manually, users can simply use the
/// `WITHIN` syntax.
///
/// # How It Works
/// 1. The rule analyzer detects `within(ts, timestamp)` functions in a `LogicalPlan`.
/// 2. It infers the precision of the given timestamp (year, month, day, second, etc.).
/// 3. Based on that precision, the rule calculates the appropriate start and end timestamps (e.g., `start = 2025-04-19 23:50:00`, `end = 2025-04-19 23:51:00`).
/// 4. The `within` function is converted to a range expression like `ts >= start AND ts < end`.
///
/// # Examples
/// - convert `ts WITHIN '2025'`
///   to:
///   ```sql
///   ts >= '2025-01-01 00:00:00' AND ts < '2026-01-01 00:00:00'
///   ```
/// - convert `ts WITHIN '2025-04-19'`
///   to:
///   ```sql
///   ts >= '2025-04-19 00:00:00' AND ts < '2025-04-20 00:00:00'
///   ```
/// - convert WITHIN '2025-04-19 23:50'`
///   to:
///   ```sql
///   ts >= '2025-04-19 23:50:00' AND ts < '2025-04-19 23:51:00'
///   ```
pub struct WithinFilterRule {}

impl WithinFilterRule {
    pub fn new() -> Self {
        WithinFilterRule {}
    }
}

impl ExtensionAnalyzerRule for WithinFilterRule {
    fn analyze(
        &self,
        plan: LogicalPlan,
        _ctx: &QueryEngineContext,
        _config: &ConfigOptions,
    ) -> DFResult<LogicalPlan> {
        plan.transform(|plan| match plan.clone() {
            LogicalPlan::Filter(filter) => {
                if let Expr::ScalarFunction(func) = &filter.predicate
                    && func.func.name() == WITHIN_FILTER_NAME
                {
                    ensure!(
                        func.args.len() == 2,
                        WithinFilterInternalSnafu {
                            message: "expected 2 arguments",
                        }
                    );
                    let column_name = func.args[0].clone();
                    let time_arg = func.args[1].clone();
                    if let Expr::Literal(literal) = time_arg
                        && let ScalarValue::Utf8(Some(s)) = literal
                    {
                        if let Some((start, end)) = try_to_infer_time_range(&s) {
                            return Ok(Transformed::yes(convert_plan(
                                filter.input,
                                &column_name,
                                start,
                                end,
                            )?));
                        }
                    }
                    Err(DataFusionError::Plan(
                        "Failed to convert within filter to normal filter.".to_string(),
                    ))
                } else {
                    Ok(Transformed::no(plan))
                }
            }
            _ => Ok(Transformed::no(plan)),
        })
        .map(|t| t.data)
    }
}

/// Infers the time range from a given timestamp string.
fn try_to_infer_time_range(timestamp: &str) -> Option<(Timestamp, Timestamp)> {
    fn try_parse_year(s: &str) -> Option<NaiveDate> {
        let mut parsed = Parsed::new();
        if chrono::format::parse(&mut parsed, s, StrftimeItems::new("%Y")).is_err() {
            return None;
        }
        parsed.set_month(1).unwrap();
        parsed.set_day(1).unwrap();
        Some(parsed.to_naive_date().unwrap())
    }
    fn try_parse_month(s: &str) -> Option<NaiveDate> {
        let mut parsed = Parsed::new();
        if chrono::format::parse(&mut parsed, s, StrftimeItems::new("%Y-%m")).is_err() {
            return None;
        }
        parsed.set_day(1).unwrap();
        Some(parsed.to_naive_date().unwrap())
    }
    fn try_parse_hour(s: &str) -> Option<NaiveDateTime> {
        let mut parsed = Parsed::new();
        if chrono::format::parse(&mut parsed, s, StrftimeItems::new("%Y-%m-%dT%H")).is_err() {
            return None;
        }
        parsed.set_minute(0).unwrap();
        Some(parsed.to_naive_datetime_with_offset(0).unwrap())
    }
    if let Some(naive_date) = try_parse_year(timestamp) {
        let start = Timestamp::from_chrono_date(naive_date).unwrap();
        let end = NaiveDate::from_ymd_opt(naive_date.year() + 1, 1, 1).unwrap();
        let end = Timestamp::from_chrono_date(end).unwrap();
        return Some((start, end));
    }
    if let Ok(naive_date) = NaiveDate::parse_from_str(timestamp, "%Y-%m-%d") {
        let start = Timestamp::from_chrono_date(naive_date).unwrap();
        let end = naive_date + Duration::days(1);
        let end = Timestamp::from_chrono_date(end).unwrap();
        return Some((start, end));
    }
    if let Some(naive_date) = try_parse_month(timestamp) {
        let start = Timestamp::from_chrono_date(naive_date).unwrap();
        let end = if naive_date.month() == 12 {
            NaiveDate::from_ymd_opt(naive_date.year() + 1, 1, 1).unwrap()
        } else {
            NaiveDate::from_ymd_opt(naive_date.year(), naive_date.month() + 1, 1).unwrap()
        };
        let end = Timestamp::from_chrono_date(end).unwrap();
        return Some((start, end));
    }
    if let Some(naive_date) = try_parse_hour(timestamp) {
        let start = Timestamp::from_chrono_datetime(naive_date).unwrap();
        let end = naive_date + Duration::hours(1);
        let end = Timestamp::from_chrono_datetime(end).unwrap();
        return Some((start, end));
    }
    if let Ok(naive_date) = NaiveDateTime::parse_from_str(timestamp, "%Y-%m-%dT%H:%M") {
        let end = naive_date + Duration::minutes(1);
        let end = Timestamp::from_chrono_datetime(end).unwrap();
        let start = Timestamp::from_chrono_datetime(naive_date).unwrap();
        return Some((start, end));
    }
    if let Ok(naive_date) = NaiveDateTime::parse_from_str(timestamp, "%Y-%m-%dT%H:%M:%S") {
        let end = naive_date + Duration::seconds(1);
        let end = Timestamp::from_chrono_datetime(end).unwrap();
        let start = Timestamp::from_chrono_datetime(naive_date).unwrap();
        return Some((start, end));
    }
    None
}

fn convert_plan(
    input_plan: Arc<LogicalPlan>,
    column_name: &Expr,
    start: Timestamp,
    end: Timestamp,
) -> DFResult<LogicalPlan> {
    let value = Some(start.value());
    let start = match start.unit() {
        TimeUnit::Second => ScalarValue::TimestampSecond(value, None),
        TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(value, None),
        TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(value, None),
        TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(value, None),
    };
    let value = Some(end.value());
    let end = match end.unit() {
        TimeUnit::Second => ScalarValue::TimestampSecond(value, None),
        TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(value, None),
        TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(value, None),
        TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(value, None),
    };
    let left = Expr::BinaryExpr(BinaryExpr {
        left: Box::new(column_name.clone()),
        op: Operator::GtEq,
        right: Box::new(Expr::Literal(start)),
    });
    let right = Expr::BinaryExpr(BinaryExpr {
        left: Box::new(column_name.clone()),
        op: Operator::Lt,
        right: Box::new(Expr::Literal(end)),
    });
    let new_expr = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(left),
        Operator::And,
        Box::new(right),
    ));
    Ok(LogicalPlan::Filter(Filter::try_new(new_expr, input_plan)?))
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use catalog::memory::MemoryCatalogManager;
    use catalog::RegisterTableRequest;
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use session::context::QueryContext;
    use table::metadata::{TableInfoBuilder, TableMetaBuilder};
    use table::test_util::EmptyTable;

    use super::*;
    use crate::error::Result;
    use crate::parser::QueryLanguageParser;
    use crate::{QueryEngineFactory, QueryEngineRef};

    async fn create_test_engine() -> QueryEngineRef {
        let table_name = "test".to_string();
        let columns = vec![
            ColumnSchema::new(
                "tag_1".to_string(),
                ConcreteDataType::string_datatype(),
                false,
            ),
            ColumnSchema::new(
                "ts".to_string(),
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new(
                "field_1".to_string(),
                ConcreteDataType::float64_datatype(),
                true,
            ),
        ];
        let schema = Arc::new(Schema::new(columns));
        let table_meta = TableMetaBuilder::default()
            .schema(schema)
            .primary_key_indices(vec![0])
            .value_indices(vec![6])
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
        QueryEngineFactory::new(catalog_list, None, None, None, None, false).query_engine()
    }

    async fn do_query(sql: &str) -> Result<LogicalPlan> {
        let stmt = QueryLanguageParser::parse_sql(sql, &QueryContext::arc()).unwrap();
        let engine = create_test_engine().await;
        engine.planner().plan(&stmt, QueryContext::arc()).await
    }

    #[tokio::test]
    async fn test_within_filter() {
        // TODO: test within filter with time zone
        // TODO: verify within filter is pushed down by optimizer.

        // 2015-01-01T00:00:00 <= timestamp < 2016-01-01T00:00:00
        let sql = "SELECT * FROM test WHERE ts WITHIN '2015'";
        let plan = do_query(sql).await.unwrap();
        let expected = "Projection: test.tag_1, test.ts, test.field_1\
        \n  Filter: test.ts >= TimestampSecond(1420070400, None) AND test.ts < TimestampSecond(1451606400, None)\
        \n    TableScan: test";
        assert_eq!(expected, format!("{plan:?}"));

        // 2025-03-01T00:00:00 <= timestamp < 2025-04-01T00:00:00
        let sql = "SELECT * FROM test WHERE ts WITHIN '2025-3'";
        let plan = do_query(sql).await.unwrap();
        let expected = "Projection: test.tag_1, test.ts, test.field_1\
        \n  Filter: test.ts >= TimestampSecond(1740787200, None) AND test.ts < TimestampSecond(1743465600, None)\
        \n    TableScan: test";
        assert_eq!(expected, format!("{plan:?}"));

        // 2025-12-1T00:00:00 <= timestamp < 2026-01-01T00:00:00
        let sql = "SELECT * FROM test WHERE ts WITHIN '2025-12'";
        let plan = do_query(sql).await.unwrap();
        let expected = "Projection: test.tag_1, test.ts, test.field_1\
        \n  Filter: test.ts >= TimestampSecond(1764547200, None) AND test.ts < TimestampSecond(1767225600, None)\
        \n    TableScan: test";
        assert_eq!(expected, format!("{plan:?}"));

        // 2025-12-1T00:00:00 <= timestamp < 2025-12-2T00:00:00
        let sql = "SELECT * FROM test WHERE ts WITHIN '2015-12-1'";
        let plan = do_query(sql).await.unwrap();
        let expected = "Projection: test.tag_1, test.ts, test.field_1\
        \n  Filter: test.ts >= TimestampSecond(1448928000, None) AND test.ts < TimestampSecond(1449014400, None)\
        \n    TableScan: test";
        assert_eq!(expected, format!("{plan:?}"));

        // 2025-12-1T01:00:00 <= timestamp < 2025-12-1T02:00:00
        let sql = "SELECT * FROM test WHERE ts WITHIN '2025-12-1T01'";
        let plan = do_query(sql).await.unwrap();
        let expected = "Projection: test.tag_1, test.ts, test.field_1\
        \n  Filter: test.ts >= TimestampSecond(1764550800, None) AND test.ts < TimestampSecond(1764554400, None)\
        \n    TableScan: test";
        assert_eq!(expected, format!("{plan:?}"));

        // 2025-12-1T01:12:00 <= timestamp < 2025-12-1T01:13:00
        let sql = "SELECT * FROM test WHERE ts WITHIN '2025-12-1T01:12'";
        let plan = do_query(sql).await.unwrap();
        let expected = "Projection: test.tag_1, test.ts, test.field_1\
        \n  Filter: test.ts >= TimestampSecond(1764551520, None) AND test.ts < TimestampSecond(1764551580, None)\
        \n    TableScan: test";
        assert_eq!(expected, format!("{plan:?}"));

        // 2025-12-1T01:12:01 <= timestamp < 2025-12-1T01:12:02
        let sql = "SELECT * FROM test WHERE ts WITHIN '2025-12-1T01:12:01'";
        let plan = do_query(sql).await.unwrap();
        let expected = "Projection: test.tag_1, test.ts, test.field_1\
        \n  Filter: test.ts >= TimestampSecond(1764551521, None) AND test.ts < TimestampSecond(1764551522, None)\
        \n    TableScan: test";
        assert_eq!(expected, format!("{plan:?}"));
    }
}
