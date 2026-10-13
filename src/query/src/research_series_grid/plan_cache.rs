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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use catalog::table_source::DfTableSourceProvider;
use datafusion::datasource::DefaultTableSource;
use datafusion::execution::SessionState;
use datafusion_common::{Column, TableReference};
use datafusion_expr::{LogicalPlan, TableSource};
use promql_parser::parser::{EvalStmt, Expr, Offset};
use session::context::{QueryContext, REMOTE_QUERY_ID_EXTENSION_KEY};
use table::metadata::TableInfoRef;
use table::table::adapter::DfTableProviderAdapter;

use crate::research_series_grid::online_input::selector_requests;
use crate::research_series_grid::plan::millis;
use crate::research_series_grid::plan_template::Template;

/// Metadata actually used by planning, including logical tables whose scans
/// are subsequently replaced by their physical metric table.
#[derive(Debug, Default)]
pub(crate) struct Dependencies {
    pub tables: HashMap<TableReference, TableDependency>,
    pub invalid: bool,
}

impl Dependencies {
    pub(crate) fn record(&mut self, name: TableReference, table: &table::Table) {
        self.insert(name, TableDependency::new(table));
    }

    fn insert(&mut self, name: TableReference, dependency: TableDependency) {
        if let Some(previous) = self.tables.insert(name, dependency.clone())
            && previous != dependency
        {
            self.invalid = true;
        }
    }

    pub(crate) fn merge(&mut self, other: Self) {
        self.invalid |= other.invalid;
        for (name, info) in other.tables {
            self.insert(name, info);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableDependency {
    info: TableInfoRef,
    extra_partition_columns: Vec<String>,
}

impl TableDependency {
    fn new(table: &table::Table) -> Self {
        Self {
            info: table.table_info(),
            extra_partition_columns: table
                .partition_rules()
                .map(|rules| rules.extra_phy_cols_not_in_logical_table.clone())
                .unwrap_or_default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Key {
    expression: String,
    span: i64,
    step: i64,
    lookback: i64,
    scope: Vec<(String, String)>,
    options: Vec<(String, Option<String>)>,
    flags: Vec<(String, String)>,
}

pub(crate) struct Request {
    key: Key,
    start: i64,
}

struct Entry {
    template: Template,
    dependencies: Dependencies,
    start: i64,
    weight: u32,
}

#[derive(Clone)]
pub(crate) struct PlanCache {
    entries: moka::future::Cache<Key, Arc<Entry>>,
}

impl Default for PlanCache {
    fn default() -> Self {
        Self {
            entries: moka::future::Cache::builder()
                .max_capacity(16 * 1024 * 1024)
                .weigher(|_: &Key, entry: &Arc<Entry>| entry.weight)
                .build(),
        }
    }
}

impl PlanCache {
    pub(crate) fn request(
        stmt: &EvalStmt,
        state: &SessionState,
        ctx: &QueryContext,
    ) -> Option<Request> {
        std::env::var_os("GT_RESEARCH_SERIES_PLAN_CACHE")?;
        Self::request_inner(stmt, state, ctx)
    }

    fn request_inner(stmt: &EvalStmt, state: &SessionState, ctx: &QueryContext) -> Option<Request> {
        if !ctx.snapshots().is_empty() || !ctx.sst_min_sequences().is_empty() {
            return None;
        }
        let start = millis(stmt.start).ok().filter(|start| *start >= 0)?;
        let end = millis(stmt.end).ok()?;
        let span = end.checked_sub(start).filter(|span| *span >= 0)?;
        let step = milliseconds(stmt.interval).filter(|step| *step > 0)?;
        let lookback = milliseconds(stmt.lookback_delta)?;
        if span / step > 8192 {
            return None;
        }
        // Clipped source predicates cannot be restored by shifting their literals.
        for expr in selector_requests(&stmt.expr).ok()?.values() {
            let (selector, width) = match expr {
                Expr::VectorSelector(selector) => (selector, lookback),
                Expr::Call(call) => {
                    let [argument] = call.args.args.as_slice() else {
                        return None;
                    };
                    let Expr::MatrixSelector(matrix) = argument.as_ref() else {
                        return None;
                    };
                    (&matrix.vs, milliseconds(matrix.range)?)
                }
                _ => return None,
            };
            let offset = match selector.offset {
                Some(Offset::Pos(value)) => milliseconds(value)?,
                Some(Offset::Neg(value)) => milliseconds(value)?.checked_neg()?,
                None => 0,
            };
            i64::try_from(i128::from(start) - i128::from(offset) - i128::from(width)).ok()?;
            i64::try_from(i128::from(end) - i128::from(offset)).ok()?;
        }
        let expression = format!("{:?}", stmt.expr);
        if expression.len() > 16 * 1024 {
            return None;
        }
        let mut scope = ctx
            .extensions()
            .into_iter()
            .filter(|(key, _)| key != REMOTE_QUERY_ID_EXTENSION_KEY)
            .map(|(key, value)| (format!("extension:{key}"), value))
            .collect::<Vec<_>>();
        scope.extend([
            ("catalog".into(), ctx.current_catalog().into()),
            ("schema".into(), ctx.current_schema()),
            ("timezone".into(), ctx.timezone().to_string()),
            ("user".into(), ctx.current_user().username().into()),
            ("channel".into(), format!("{:?}", ctx.channel())),
            (
                "read_preference".into(),
                format!("{:?}", ctx.read_preference()),
            ),
        ]);
        scope.sort();
        let mut options = state
            .config_options()
            .entries()
            .into_iter()
            .map(|entry| (entry.key, entry.value))
            .collect::<Vec<_>>();
        options.sort();
        let mut flags = std::env::vars()
            .filter(|(key, _)| key.starts_with("GT_RESEARCH_"))
            .collect::<Vec<_>>();
        flags.sort();
        let key = Key {
            expression,
            span,
            step,
            lookback,
            scope,
            options,
            flags,
        };
        (format!("{key:?}").len() <= 64 * 1024).then_some(Request { key, start })
    }

    pub(crate) async fn get(
        &self,
        request: &Request,
        mut provider: DfTableSourceProvider,
    ) -> Option<LogicalPlan> {
        let entry = self.entries.get(&request.key).await?;
        let mut sources = HashMap::<TableReference, Arc<dyn TableSource>>::new();
        for (name, expected) in &entry.dependencies.tables {
            let source = match provider.resolve_table(name.clone()).await {
                Ok(source) => source,
                Err(error) => {
                    common_telemetry::debug!(%error, "Series template dependency resolution declined");
                    return None;
                }
            };
            let table = source
                .downcast_ref::<DefaultTableSource>()?
                .table_provider
                .downcast_ref::<DfTableProviderAdapter>()?
                .table();
            if &TableDependency::new(&table) != expected {
                return None;
            }
            sources.insert(name.clone(), source);
        }
        let delta = request.start.checked_sub(entry.start)?;
        match entry.template.bind(delta, &sources) {
            Ok(plan) => {
                common_telemetry::debug!("Research series plan template hit");
                Some(plan)
            }
            Err(error) => {
                common_telemetry::debug!(%error, "Series template binding declined");
                None
            }
        }
    }

    pub(crate) async fn insert(
        &self,
        request: Request,
        plan: LogicalPlan,
        dependencies: Dependencies,
    ) -> Option<LogicalPlan> {
        if dependencies.invalid || dependencies.tables.is_empty() || dependencies.tables.len() > 16
        {
            return None;
        }
        let mut columns = HashSet::new();
        let mut bytes = format!("{:?}", request.key).len();
        for (name, dependency) in &dependencies.tables {
            let info = &dependency.info;
            bytes = bytes.saturating_add(
                dependency
                    .extra_partition_columns
                    .iter()
                    .map(String::len)
                    .sum::<usize>(),
            );
            let Some(time) = info.meta.schema.timestamp_column() else {
                return None;
            };
            if time.data_type
                != datatypes::data_type::ConcreteDataType::timestamp_millisecond_datatype()
            {
                return None;
            }
            columns.insert(Column::new(Some(name.clone()), time.name.clone()));
            columns.insert(Column::new_unqualified(time.name.clone()));
            let Ok(metadata) = serde_json::to_vec(info.as_ref()) else {
                return None;
            };
            bytes = bytes.saturating_add(metadata.len());
            if bytes > 256 * 1024 {
                return None;
            }
        }
        let initial_sources = if std::env::var_os("GT_RESEARCH_QUERY_PROGRAM").is_some() {
            Some(crate::research_series_grid::plan_template::current_sources(&plan).ok()?)
        } else {
            None
        };
        let template = match Template::new(plan, columns) {
            Ok(template) => template,
            Err(error) => {
                common_telemetry::debug!(%error, "Series template admission declined");
                return None;
            }
        };
        bytes = bytes.saturating_add(format!("{template:?}").len());
        if bytes > 512 * 1024 {
            return None;
        }
        let initial = initial_sources
            .as_ref()
            .and_then(|sources| template.bind(0, sources).ok());
        // Account for structural overhead in addition to variable-size payloads.
        let weight = (bytes + 64 * 1024) as u32;
        self.entries
            .insert(
                request.key,
                Arc::new(Entry {
                    template,
                    dependencies,
                    start: request.start,
                    weight,
                }),
            )
            .await;
        initial
    }
}

fn milliseconds(duration: Duration) -> Option<i64> {
    if duration.subsec_nanos() % 1_000_000 != 0 {
        return None;
    }
    i64::try_from(duration.as_millis()).ok()
}

#[cfg(test)]
mod tests {
    use std::time::UNIX_EPOCH;

    use catalog::RegisterTableRequest;
    use catalog::memory::MemoryCatalogManager;
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_query::test_util::DummyDecoder;
    use common_recordbatch::RecordBatch;
    use datafusion::arrow::array::TimestampMillisecondArray;
    use datafusion::prelude::SessionContext;
    use datafusion_common::ScalarValue;
    use datafusion_expr::{Expr as DfExpr, LogicalPlanBuilder, col};
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use session::context::QueryContextBuilder;
    use table::Table;
    use table::metadata::{FilterPushDownType, TableInfoBuilder, TableMetaBuilder};
    use table::test_util::MemTable;

    use super::*;

    fn stmt() -> EvalStmt {
        EvalStmt {
            expr: promql_parser::parser::parse("sum(rate(m[5m]))").unwrap(),
            start: UNIX_EPOCH + Duration::from_secs(1_000),
            end: UNIX_EPOCH + Duration::from_secs(2_000),
            interval: Duration::from_secs(10),
            lookback_delta: Duration::from_secs(300),
        }
    }

    fn provider(logical_id: u32) -> DfTableSourceProvider {
        partitioned_provider(logical_id, false)
    }

    fn partitioned_provider(logical_id: u32, extra: bool) -> DfTableSourceProvider {
        let catalog = MemoryCatalogManager::with_default_setup();
        for (name, id) in [("m", 1024), ("logical", logical_id)] {
            let schema = Arc::new(Schema::new(vec![
                ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                )
                .with_time_index(true),
            ]));
            let batch = datafusion::arrow::record_batch::RecordBatch::try_new(
                schema.arrow_schema().clone(),
                vec![Arc::new(TimestampMillisecondArray::from(vec![1_i64]))],
            )
            .unwrap();
            let backing = MemTable::new_with_catalog(
                name,
                RecordBatch::from_df_record_batch(schema.clone(), batch),
                id,
                DEFAULT_CATALOG_NAME.into(),
                DEFAULT_SCHEMA_NAME.into(),
            );
            let info = Arc::new(
                TableInfoBuilder::default()
                    .name(name)
                    .table_id(id)
                    .meta(
                        TableMetaBuilder::empty()
                            .schema(schema)
                            .primary_key_indices(vec![])
                            .value_indices(vec![])
                            .next_column_id(1)
                            .created_on(Default::default())
                            .build()
                            .unwrap(),
                    )
                    .build()
                    .unwrap(),
            );
            catalog
                .register_table_sync(RegisterTableRequest {
                    catalog: DEFAULT_CATALOG_NAME.into(),
                    schema: DEFAULT_SCHEMA_NAME.into(),
                    table_name: name.into(),
                    table_id: id,
                    table: Arc::new(Table::new_partitioned(
                        info,
                        FilterPushDownType::Unsupported,
                        backing.data_source(),
                        (extra && name == "m").then(|| table::table::PartitionRules {
                            extra_phy_cols_not_in_logical_table: vec!["hidden".into()],
                        }),
                    )),
                })
                .unwrap();
        }
        DfTableSourceProvider::new(
            catalog,
            false,
            QueryContext::arc(),
            DummyDecoder::arc(),
            false,
        )
    }

    #[tokio::test]
    async fn hit_rebinds_time_and_checks_logical_dependencies_absent_from_scan() {
        let cache = PlanCache::default();
        let state = SessionContext::new().state();
        let ctx = QueryContext::arc();
        let original = stmt();
        let request = PlanCache::request_inner(&original, &state, &ctx).unwrap();
        let mut sources = provider(1025);
        let mut dependencies = Dependencies::default();
        for name in ["m", "logical"] {
            let source = sources
                .resolve_table(TableReference::bare(name))
                .await
                .unwrap();
            let info = source
                .downcast_ref::<DefaultTableSource>()
                .unwrap()
                .table_provider
                .downcast_ref::<DfTableProviderAdapter>()
                .unwrap()
                .table();
            dependencies.record(TableReference::bare(name), &info);
        }
        let plan = LogicalPlanBuilder::scan(
            "m",
            sources
                .resolve_table(TableReference::bare("m"))
                .await
                .unwrap(),
            None,
        )
        .unwrap()
        .filter(col("m.ts").gt(DfExpr::Literal(
            ScalarValue::TimestampMillisecond(Some(700_000), None),
            None,
        )))
        .unwrap()
        .build()
        .unwrap();
        let _ = cache.insert(request, plan, dependencies).await;
        let mut moved = original.clone();
        moved.start += Duration::from_secs(10);
        moved.end += Duration::from_secs(10);
        let request = PlanCache::request_inner(&moved, &state, &ctx).unwrap();
        let hit = cache.get(&request, provider(1025)).await.unwrap();
        let LogicalPlan::Filter(filter) = hit else {
            unreachable!()
        };
        assert_eq!(
            filter.predicate,
            col("m.ts").gt(DfExpr::Literal(
                ScalarValue::TimestampMillisecond(Some(710_000), None),
                None
            ))
        );
        assert!(cache.get(&request, provider(2025)).await.is_none());
        assert!(
            cache
                .get(&request, partitioned_provider(1025, true))
                .await
                .is_none()
        );
        assert!(cache.get(&request, provider(1025)).await.is_some());
    }

    #[test]
    fn request_key_separates_scope_and_rejects_unbindable_time() {
        let state = SessionContext::new().state();
        let ctx = QueryContext::arc();
        let original = stmt();
        let key = PlanCache::request_inner(&original, &state, &ctx)
            .unwrap()
            .key;
        let other = QueryContextBuilder::default()
            .current_schema("other".into())
            .build();
        assert_ne!(
            key,
            PlanCache::request_inner(&original, &state, &other)
                .unwrap()
                .key
        );
        let mut changed = original.clone();
        changed.interval += Duration::from_nanos(1);
        assert!(PlanCache::request_inner(&changed, &state, &ctx).is_none());
        changed = original;
        changed.expr = promql_parser::parser::parse("m @ 1000").unwrap();
        assert!(PlanCache::request_inner(&changed, &state, &ctx).is_none());
        changed.expr = promql_parser::parser::parse("rate(m[5m])").unwrap();
        changed.lookback_delta = Duration::from_secs(u64::MAX);
        assert!(PlanCache::request_inner(&changed, &state, &ctx).is_none());
    }
}
