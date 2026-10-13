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

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use catalog::CatalogManagerRef;
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::catalog::Session;
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::streaming::StreamingTableExec;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion_common::{DFSchemaRef, DataFusionError, Result, ToDFSchema};
use datafusion_expr::{
    Expr as DfExpr, Extension, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use promql_parser::parser::{EvalStmt, Expr};
use promql_parser::util::{ExprVisitor, walk_expr};
use session::context::QueryContextRef;

use crate::research_series_grid::execution::GridPartition;
use crate::research_series_grid::grid::TimeGrid;
use crate::research_series_grid::source::SnapshotIndex;

static SOURCE: tokio::sync::OnceCell<(PathBuf, Arc<SnapshotIndex>)> =
    tokio::sync::OnceCell::const_new();

fn invalid(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

pub(crate) async fn plan(
    stmt: &EvalStmt,
    catalog: &CatalogManagerRef,
    context: &QueryContextRef,
) -> Result<LogicalPlan> {
    // The immutable dataset has one namespace; it cannot answer another tenant's tables.
    if context.current_catalog() != "greptime" || context.current_schema() != "public" {
        return Err(invalid("Research snapshot is bound to greptime.public"));
    }
    let path = PathBuf::from(
        std::env::var("GT_RESEARCH_SERIES_SNAPSHOT")
            .map_err(|_| invalid("Research snapshot path is required"))?,
    )
    .canonicalize()?;
    let stored = SOURCE
        .get_or_try_init(|| async {
            let read_path = path.clone();
            let index =
                common_runtime::spawn_blocking_query(move || SnapshotIndex::open(&read_path))
                    .await
                    .map_err(|error| invalid(error.to_string()))??;
            Ok::<_, DataFusionError>((path.clone(), Arc::new(index)))
        })
        .await?;
    if stored.0 != path {
        return Err(invalid("Research snapshot changed during process lifetime"));
    }
    let mut selectors = Selectors(BTreeSet::new());
    walk_expr(&mut selectors, &stmt.expr)?;
    let mut tag_orders = BTreeMap::new();
    let mut output_tags = BTreeSet::new();
    for metric in selectors.0 {
        let table = catalog
            .table(
                context.current_catalog(),
                &context.current_schema(),
                &metric,
                Some(context),
            )
            .await
            .map_err(|error| invalid(error.to_string()))?
            .ok_or_else(|| invalid(format!("Research metric does not exist: {metric}")))?;
        let tags: Vec<_> = table
            .table_info()
            .meta
            .row_key_column_names()
            .filter(|name| name.as_str() != "__table_id" && name.as_str() != "__tsid")
            .cloned()
            .collect();
        output_tags.extend(tags.iter().cloned());
        tag_orders.insert(metric, tags);
    }
    output_tags.remove("__name__");
    if output_tags.contains("__research_grid_time") || output_tags.contains("__research_grid_value")
    {
        return Err(invalid(
            "Research output column conflicts with source label",
        ));
    }
    let mut fields = vec![
        Field::new(
            "__research_grid_time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("__research_grid_value", DataType::Float64, false),
    ];
    fields.extend(
        output_tags
            .into_iter()
            .map(|name| Field::new(name, DataType::Utf8, true)),
    );
    let schema = Arc::new(Schema::new(fields));
    let duration = |duration: std::time::Duration| -> Result<i64> {
        if duration.subsec_nanos() % 1_000_000 != 0 {
            return Err(invalid("Research grid requires millisecond durations"));
        }
        i64::try_from(duration.as_millis()).map_err(|_| invalid("Research duration overflow"))
    };
    let grid = TimeGrid::new(
        millis(stmt.start)?,
        millis(stmt.end)?,
        duration(stmt.interval)?,
    )
    .map_err(invalid)?;
    let key = format!("{stmt:?}:{path:?}:{tag_orders:?}");
    let partition = GridPartition {
        expr: stmt.expr.clone(),
        grid: Arc::new(grid),
        source: stored.1.clone(),
        path,
        tag_orders: Arc::new(tag_orders),
        schema: schema.clone(),
        lookback: duration(stmt.lookback_delta)?,
    };
    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(GridNode {
            key,
            schema: schema.to_dfschema_ref()?,
            partition: Arc::new(partition),
        }),
    }))
}

pub(crate) fn millis(time: SystemTime) -> Result<i64> {
    let (negative, duration) = match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => (false, duration),
        Err(error) => (true, error.duration()),
    };
    if duration.subsec_nanos() % 1_000_000 != 0 {
        return Err(invalid("Research grid requires millisecond timestamps"));
    }
    let value = duration.as_millis() as i128;
    i64::try_from(if negative { -value } else { value })
        .map_err(|_| invalid("Research timestamp overflow"))
}

struct Selectors(BTreeSet<String>);

impl ExprVisitor for Selectors {
    type Error = DataFusionError;
    fn pre_visit(&mut self, expr: &Expr) -> Result<bool> {
        let selector = match expr {
            Expr::VectorSelector(selector) => Some(selector),
            Expr::MatrixSelector(matrix) => Some(&matrix.vs),
            _ => None,
        };
        if let Some(selector) = selector {
            self.0.insert(
                selector
                    .name
                    .clone()
                    .ok_or_else(|| invalid("Research selector requires metric name"))?,
            );
        }
        Ok(true)
    }
}

#[derive(Debug, Clone)]
struct GridNode {
    key: String,
    schema: DFSchemaRef,
    partition: Arc<GridPartition>,
}

impl PartialEq for GridNode {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}
impl Eq for GridNode {}
impl PartialOrd for GridNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.key.cmp(&other.key))
    }
}
impl Hash for GridNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key.hash(state);
    }
}

impl UserDefinedLogicalNodeCore for GridNode {
    fn name(&self) -> &str {
        "ResearchSeriesGrid"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        Vec::new()
    }
    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }
    fn expressions(&self) -> Vec<DfExpr> {
        Vec::new()
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ResearchSeriesGrid: {}", self.partition.expr)
    }
    fn with_exprs_and_inputs(
        &self,
        expressions: Vec<DfExpr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        if !expressions.is_empty() || !inputs.is_empty() {
            return Err(invalid("Research grid is a leaf"));
        }
        Ok(self.clone())
    }
}

pub(crate) struct GridPlanner;

#[async_trait]
impl ExtensionPlanner for GridPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session: &dyn Session,
        context: &PhysicalPlanningContext,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(node) = node
            .as_any()
            .downcast_ref::<crate::research_series_grid::SeriesInstantMax>()
        {
            let [input] = physical_inputs else {
                return Err(invalid("Instant max fragment requires one physical input"));
            };
            return node.physical(input.clone()).map(Some);
        }
        if let Some(node) = node
            .as_any()
            .downcast_ref::<crate::research_series_grid::SeriesFragment>()
        {
            let [input] = physical_inputs else {
                return Err(invalid("Series fragment requires one physical input"));
            };
            return node.physical(input.clone()).map(Some);
        }
        if let Some(node) = node
            .as_any()
            .downcast_ref::<crate::research_series_grid::SeriesRange>()
        {
            let [input] = physical_inputs else {
                return Err(invalid("Series range requires one physical input"));
            };
            return node.physical(input.clone()).map(Some);
        }
        if let Some(node) = node
            .as_any()
            .downcast_ref::<crate::research_series_grid::SeriesPack>()
        {
            let [input] = physical_inputs else {
                return Err(invalid("Series pack requires one physical input"));
            };
            return node
                .physical(input.clone(), planner, session, context)
                .map(Some);
        }
        if let Some(node) = node
            .as_any()
            .downcast_ref::<crate::research_series_grid::online_plan::OnlineNode>()
        {
            return Ok(Some(Arc::new(
                crate::research_series_grid::online_execution::OnlineExec::try_new(
                    node.query.clone(),
                    physical_inputs.to_vec(),
                )?,
            )));
        }
        let Some(node) = node.as_any().downcast_ref::<GridNode>() else {
            return Ok(None);
        };
        Ok(Some(Arc::new(StreamingTableExec::try_new(
            node.partition.schema.clone(),
            vec![node.partition.clone()],
            None,
            [],
            false,
            None,
        )?)))
    }
}
