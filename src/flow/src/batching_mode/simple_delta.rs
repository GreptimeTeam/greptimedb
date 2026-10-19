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

//! Row-local source delta support for simple SQL flows.
//!
//! A flow whose plan is one source scan plus a projection/alias/filter can read only the source
//! rows written since the last checkpoint. That is correct only because the projection is a
//! deterministic, row-local function of a single source row, and because a delta round is written
//! as a keyed materialization: rows sharing the sink key collapse under ordinary sink semantics.

use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_expr::{Expr, LogicalPlan, Volatility};
use datatypes::schema::{ColumnSchema, SchemaRef};

use crate::adapter::output_column_schemas;

/// Source-key layout of a flow output, expressed as output positions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SinkKeys {
    /// Output position of the source time index.
    pub(crate) time_index: usize,
    /// Output positions of the source primary keys, in source order.
    pub(crate) primary_keys: Vec<usize>,
}

/// Returns whether `plan` can be replayed from a source delta: at most one projection on top of
/// zero or more filters over exactly one table scan, and every expression is row-local and
/// immutable. Any other node (aggregate, join, distinct, sort, limit, union, subquery) makes the
/// plan not row-local, hence unsupported.
pub(crate) fn is_simple_source_delta_plan(plan: &LogicalPlan) -> bool {
    let mut is_row_local = true;
    let mut node = plan;
    // The optimizer may fold the projection into `TableScan.projection`, so a bare scan is a
    // valid shape too.
    if let LogicalPlan::Projection(projection) = node {
        is_row_local &= projection.expr.iter().all(expr_is_row_local);
        node = projection.input.as_ref();
    }
    while let LogicalPlan::Filter(filter) = node {
        is_row_local &= expr_is_row_local(&filter.predicate);
        node = filter.input.as_ref();
    }
    let LogicalPlan::TableScan(scan) = node else {
        return false;
    };
    // `LIMIT n` pushdown folds into `TableScan.fetch`, which truncates rows
    // non-deterministically and is not a row-local delta.
    // Pushed-down scan filters, in contrast, are ordinary `WHERE` predicates over
    // the same source row, so they stay part of the accepted shape.
    scan.fetch.is_none() && is_row_local && scan.filters.iter().all(expr_is_row_local)
}

/// Derives the sink keys of `plan` from source-column lineage: the output column that carries the
/// source time index and the output columns that carry the source primary keys, in source order.
///
/// Returns `None` when the source time index or any source primary key is not a direct
/// `Column`/`Alias(Column)` of the source, or when one source key maps to more than one output
/// column.
pub(crate) fn derive_sink_keys(
    plan: &LogicalPlan,
    source_schema: &SchemaRef,
    source_pk_indices: &[usize],
) -> Option<SinkKeys> {
    let (_, lineage) = output_column_schemas(plan, source_schema).ok()?;
    let source_time_index = source_schema
        .column_schemas()
        .iter()
        .position(ColumnSchema::is_time_index)?;

    let time_index = output_position(&lineage, source_time_index)?;
    let primary_keys = source_pk_indices
        .iter()
        .map(|source_index| output_position(&lineage, *source_index))
        .collect::<Option<Vec<_>>>()?;
    Some(SinkKeys {
        time_index,
        primary_keys,
    })
}

/// Returns the single output position whose lineage is `source_index`. Two output columns sharing
/// one source key would make the sink key ambiguous, so that case returns `None`.
fn output_position(lineage: &[Option<usize>], source_index: usize) -> Option<usize> {
    let mut positions = lineage
        .iter()
        .enumerate()
        .filter(|(_, index)| **index == Some(source_index))
        .map(|(position, _)| position);
    let position = positions.next()?;
    positions.next().is_none().then_some(position)
}

/// Returns whether `expr` is a row-local, immutable function of its input row: no window
/// function, subquery, aggregate, scalar variable, or volatile/stable scalar function.
fn expr_is_row_local(expr: &Expr) -> bool {
    let mut is_row_local = true;
    let _ = expr.apply(|expr| match expr {
        Expr::WindowFunction(_)
        | Expr::Exists(_)
        | Expr::InSubquery(_)
        | Expr::ScalarSubquery(_)
        | Expr::AggregateFunction(_)
        | Expr::ScalarVariable(_, _) => {
            is_row_local = false;
            Ok(TreeNodeRecursion::Stop)
        }
        Expr::ScalarFunction(function)
            if function.func.signature().volatility != Volatility::Immutable =>
        {
            is_row_local = false;
            Ok(TreeNodeRecursion::Stop)
        }
        _ => Ok(TreeNodeRecursion::Continue),
    });
    is_row_local
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use catalog::RegisterTableRequest;
    use catalog::memory::MemoryCatalogManager;
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_recordbatch::RecordBatch;
    use datafusion_common::tree_node::Transformed;
    use datafusion_expr::{Expr, LogicalPlan, Projection};
    use datatypes::arrow::datatypes::DataType;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
    use datatypes::vectors::{Float64Vector, StringVector, TimestampMillisecondVector, VectorRef};
    use query::QueryEngineRef;
    use session::context::QueryContext;
    use table::test_util::MemTable;

    use super::*;
    use crate::batching_mode::utils::sql_to_df_plan;
    use crate::test_utils::create_test_query_engine;

    const SOURCE_TABLE: &str = "simple_delta_source";

    /// Registers `simple_delta_source` (`host` primary key, `ts` time index, one other timestamp
    /// column and one value column) and returns its schema with the source primary key positions.
    fn register_source_table(query_engine: &QueryEngineRef) -> (SchemaRef, Vec<usize>) {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new("host", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new(
                "other_ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            ColumnSchema::new("value", ConcreteDataType::float64_datatype(), true),
        ]));
        let columns: Vec<VectorRef> = vec![
            Arc::new(StringVector::from_slice(&["a"])),
            Arc::new(TimestampMillisecondVector::from_slice([1_i64])),
            Arc::new(TimestampMillisecondVector::from_slice([2_i64])),
            Arc::new(Float64Vector::from_slice([1.0])),
        ];
        let recordbatch = RecordBatch::new(schema.clone(), columns).unwrap();
        let table = MemTable::table(SOURCE_TABLE, recordbatch);
        let catalog_manager = query_engine.engine_state().catalog_manager();
        let memory_catalog = catalog_manager
            .as_any()
            .downcast_ref::<MemoryCatalogManager>()
            .unwrap();
        memory_catalog
            .register_table_sync(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: SOURCE_TABLE.to_string(),
                table_id: 9101,
                table,
            })
            .unwrap();
        (schema, vec![0])
    }

    async fn plan(query_engine: &QueryEngineRef, sql: &str, optimize: bool) -> LogicalPlan {
        sql_to_df_plan(QueryContext::arc(), query_engine.clone(), sql, optimize)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_simple_source_delta_plan_accepts_scan_projection_and_filter() {
        let query_engine = create_test_query_engine();
        for (case, sql, optimize) in [
            ("bare scan", "SELECT number, ts FROM numbers_with_ts", true),
            (
                "scan with where",
                "SELECT number, ts FROM numbers_with_ts WHERE number > 1",
                true,
            ),
            (
                "projection over filter",
                "SELECT number + 1 AS n, ts FROM numbers_with_ts WHERE number > 1",
                true,
            ),
            (
                "unoptimized projection",
                "SELECT number, ts FROM numbers_with_ts",
                false,
            ),
        ] {
            let plan = plan(&query_engine, sql, optimize).await;
            assert!(
                is_simple_source_delta_plan(&plan),
                "{case} should be accepted:\n{plan}"
            );
        }
    }

    #[tokio::test]
    async fn test_simple_source_delta_plan_rejects_other_shapes() {
        let query_engine = create_test_query_engine();
        for (case, sql, optimize) in [
            (
                "aggregate",
                "SELECT max(number) AS number, ts FROM numbers_with_ts GROUP BY ts",
                true,
            ),
            (
                "join",
                "SELECT a.number, a.ts FROM numbers_with_ts a JOIN numbers b ON a.number = b.number",
                true,
            ),
            (
                "distinct",
                "SELECT DISTINCT number, ts FROM numbers_with_ts",
                true,
            ),
            (
                "sort",
                "SELECT number, ts FROM numbers_with_ts ORDER BY ts",
                true,
            ),
            (
                "limit",
                "SELECT number, ts FROM numbers_with_ts LIMIT 1",
                true,
            ),
            (
                "union",
                "SELECT number, ts FROM numbers_with_ts UNION ALL SELECT number, ts FROM numbers_with_ts",
                true,
            ),
            (
                "nested projection",
                "SELECT number FROM (SELECT number, ts FROM numbers_with_ts) t",
                false,
            ),
            (
                "scalar subquery",
                "SELECT number, ts FROM numbers_with_ts WHERE number > (SELECT 1)",
                false,
            ),
            (
                "volatile function",
                "SELECT random() AS number, ts FROM numbers_with_ts",
                true,
            ),
        ] {
            let plan = plan(&query_engine, sql, optimize).await;
            assert!(
                !is_simple_source_delta_plan(&plan),
                "{case} should be rejected:\n{plan}"
            );
        }
    }

    #[tokio::test]
    async fn test_simple_source_delta_plan_rejects_scan_fetch() {
        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();

        // `LIMIT` may fold into `TableScan.fetch` (DataFusion limit pushdown), which leaves a
        // bare accepted scan that truncates rows. The scan is built directly because this
        // engine's optimizer does not always push the fetch down.
        for sql in [
            "SELECT number, ts FROM numbers_with_ts",
            "SELECT number FROM numbers_with_ts",
        ] {
            let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), sql, false)
                .await
                .unwrap();
            let with_fetch = plan
                .transform_down(|node| {
                    Ok(match node {
                        LogicalPlan::TableScan(mut scan) => {
                            scan.fetch = Some(1);
                            Transformed::yes(LogicalPlan::TableScan(scan))
                        }
                        other => Transformed::no(other),
                    })
                })
                .unwrap()
                .data;
            assert!(
                !is_simple_source_delta_plan(&with_fetch),
                "a scan carrying a pushed-down fetch must be rejected:\n{with_fetch}"
            );
        }
    }

    #[tokio::test]
    async fn test_derive_sink_keys_keeps_projected_source_keys() {
        let query_engine = create_test_query_engine();
        let (source_schema, source_pk_indices) = register_source_table(&query_engine);

        let plan = plan(
            &query_engine,
            "SELECT ts, host, value FROM simple_delta_source",
            true,
        )
        .await;
        let keys = derive_sink_keys(&plan, &source_schema, &source_pk_indices).unwrap();
        assert_eq!(
            SinkKeys {
                time_index: 0,
                primary_keys: vec![1],
            },
            keys
        );
    }

    #[tokio::test]
    async fn test_derive_sink_keys_uses_source_time_index_not_first_timestamp() {
        let query_engine = create_test_query_engine();
        let (source_schema, source_pk_indices) = register_source_table(&query_engine);

        // `other_ts` is projected before the real time index `ts`.
        let plan = plan(
            &query_engine,
            "SELECT host, other_ts, ts FROM simple_delta_source",
            true,
        )
        .await;
        let keys = derive_sink_keys(&plan, &source_schema, &source_pk_indices).unwrap();
        assert_eq!(
            SinkKeys {
                time_index: 2,
                primary_keys: vec![0],
            },
            keys
        );
    }

    /// Builds `Projection(exprs)` over a scan of the test source table.
    async fn projection_over_source(
        query_engine: &QueryEngineRef,
        exprs: Vec<Expr>,
    ) -> LogicalPlan {
        let scan = plan(
            query_engine,
            "SELECT host, ts, value FROM simple_delta_source",
            false,
        )
        .await;
        LogicalPlan::Projection(
            Projection::try_new(exprs, Arc::new(scan)).expect("test projection should be valid"),
        )
    }

    #[tokio::test]
    async fn test_derive_sink_keys_rejects_computed_source_key() {
        let query_engine = create_test_query_engine();
        let (source_schema, source_pk_indices) = register_source_table(&query_engine);

        // A computed column loses source lineage even when it is the only output column and
        // keeps the source key name, so name-based lineage would wrongly accept it.
        let shadowing = projection_over_source(
            &query_engine,
            vec![
                Expr::Cast(datafusion_expr::Cast::new(
                    Box::new(Expr::Column("host".into())),
                    DataType::Utf8,
                ))
                .alias("host"),
                Expr::Column("ts".into()),
            ],
        )
        .await;
        assert!(
            derive_sink_keys(&shadowing, &source_schema, &source_pk_indices).is_none(),
            "a computed source key must not be inherited by naming:\n{shadowing}"
        );

        // `upper(host)` projected into a differently named column also inherits nothing, so the
        // source primary key is missing from the output.
        let computed = plan(
            &query_engine,
            "SELECT upper(host) AS upper_host, ts FROM simple_delta_source",
            true,
        )
        .await;
        assert!(
            derive_sink_keys(&computed, &source_schema, &source_pk_indices).is_none(),
            "a computed source key must not be inherited:\n{computed}"
        );

        // A second output column carrying the same source key leaves the sink key ambiguous.
        let duplicated = plan(
            &query_engine,
            "SELECT host AS h1, host AS h2, ts FROM simple_delta_source",
            true,
        )
        .await;
        assert!(
            derive_sink_keys(&duplicated, &source_schema, &source_pk_indices).is_none(),
            "ambiguous source key lineage must be rejected:\n{duplicated}"
        );
    }

    #[tokio::test]
    async fn test_derive_sink_keys_returns_none_for_dropped_source_key() {
        let query_engine = create_test_query_engine();
        let (source_schema, source_pk_indices) = register_source_table(&query_engine);

        for sql in [
            // dropped primary key
            "SELECT ts, value FROM simple_delta_source",
            // dropped time index
            "SELECT host, value FROM simple_delta_source",
        ] {
            let plan = plan(&query_engine, sql, true).await;
            assert!(
                derive_sink_keys(&plan, &source_schema, &source_pk_indices).is_none(),
                "a plan that drops a source key has no derivable sink keys:\n{plan}"
            );
        }
    }
}
