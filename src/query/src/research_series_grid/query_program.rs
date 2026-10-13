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

mod execution;
mod top_one;

pub(crate) use execution::try_bind;

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use common_recordbatch::OrderOption;
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{
    Column, DFSchemaRef, DataFusionError, Result, ScalarValue, TableReference,
};
use datafusion_expr::{
    Expr, Extension, LogicalPlan, LogicalPlanBuilder, TableSource, UserDefinedLogicalNodeCore,
};

use crate::dist_plan::MergeScanLogicalPlan;
use crate::research_series_grid::direct_binding::{Source, remap_range_spec};
use crate::research_series_grid::grid::TimeGrid;
use crate::research_series_grid::online_execution::OnlineQuery;
use crate::research_series_grid::online_frontier::join_sum::JoinSum;
use crate::research_series_grid::online_plan::OnlineNode;
use crate::research_series_grid::plan_template::shift;
use crate::research_series_grid::scan_max::InstantMaxSpec;
use crate::research_series_grid::scan_range::RangeSpec;
use crate::research_series_grid::{SeriesFragment, SeriesInstantMax};

#[derive(Debug, Clone)]
pub(crate) enum SourceOperation {
    Range(Arc<RangeSpec>),
    Max(Arc<InstantMaxSpec>),
}

impl SourceOperation {
    fn shift_time(&self, delta: i64) -> Result<Self> {
        Ok(match self {
            Self::Range(spec) => {
                let mut spec = spec.as_ref().clone();
                spec.grid = shift_grid(&spec.grid, delta)?;
                Self::Range(Arc::new(spec))
            }
            Self::Max(spec) => {
                let mut spec = spec.as_ref().clone();
                spec.grid = shift_grid(&spec.grid, delta)?;
                Self::Max(Arc::new(spec))
            }
        })
    }
}

/// Contains only compiled layout, expressions and window operations. Every
/// request must separately bind catalog sources, routes and storage snapshots.
#[derive(Debug, Clone)]
pub(crate) struct QueryProgram {
    pub(crate) query: Arc<OnlineQuery>,
    pub(crate) sources: Vec<ProgramSource>,
    pub(crate) join_sum: Option<JoinSum>,
    top_one: Option<top_one::TopOne>,
}

#[derive(Debug, Clone)]
pub(crate) struct ProgramSource {
    pub(crate) name: TableReference,
    pub(crate) schema: SchemaRef,
    pub(crate) projection: Vec<usize>,
    pub(crate) filters: Vec<Expr>,
    pub(crate) ordering: Vec<OrderOption>,
    pub(crate) operation: SourceOperation,
}

impl QueryProgram {
    pub(crate) fn compile(plan: &LogicalPlan) -> Result<Option<Self>> {
        let LogicalPlan::Extension(root) = plan else {
            return Ok(None);
        };
        let Some(root) = root.node.as_any().downcast_ref::<OnlineNode>() else {
            return Ok(None);
        };
        let Some(fragments) = &root.query.fragments else {
            return Ok(None);
        };
        let inputs = root.inputs();
        if inputs.is_empty() || inputs.len() != fragments.len() {
            return Ok(None);
        }
        let mut sources = Vec::with_capacity(inputs.len());
        for input in inputs {
            let LogicalPlan::Extension(remote) = input else {
                return Ok(None);
            };
            let Some(remote) = remote.node.as_any().downcast_ref::<MergeScanLogicalPlan>() else {
                return Ok(None);
            };
            if remote.is_placeholder() || remote.remote_dyn_filter_producer_id().is_some() {
                return Ok(None);
            }
            let LogicalPlan::Extension(fragment) = remote.input() else {
                return Ok(None);
            };
            if remote.schema() != fragment.node.schema() {
                return Ok(None);
            }
            let children = fragment.node.inputs();
            let [child] = children.as_slice() else {
                return Ok(None);
            };
            let Some(source) = Source::compile(child) else {
                return Ok(None);
            };
            let schema = source.provider.schema();
            let Some(ordering) = source.ordering else {
                return Ok(None);
            };
            if ordering.len() != 2 || schema.field(ordering[0]).name() != "__tsid" {
                return Ok(None);
            }
            let mut projection = source.columns.clone();
            projection.sort_unstable();
            projection.dedup();
            let projected = Arc::new(schema.project(&projection)?);
            let columns = source
                .columns
                .iter()
                .map(|index| {
                    projection
                        .binary_search(index)
                        .map_err(|_| invalid("Missing program source column"))
                })
                .collect::<Result<Vec<_>>>()?;
            let input_schema = Arc::new(child.schema().as_arrow().clone());
            let operation =
                if let Some(range) = fragment.node.as_any().downcast_ref::<SeriesFragment>() {
                    let spec =
                        remap_range_spec(range.specification(input_schema)?, projected, &columns)?;
                    if projection[spec.timestamp] != ordering[1]
                        || projection[spec.tsid] != ordering[0]
                    {
                        return Ok(None);
                    }
                    SourceOperation::Range(Arc::new(spec))
                } else if let Some(max) = fragment.node.as_any().downcast_ref::<SeriesInstantMax>()
                {
                    let mut spec = max.specification(input_schema)?;
                    if columns.len() != spec.input_schema.fields().len()
                        || spec.input_schema.fields().iter().zip(&columns).any(
                            |(expected, &index)| {
                                projected.fields().get(index).is_none_or(|actual| {
                                    actual.data_type() != expected.data_type()
                                        || actual.is_nullable() != expected.is_nullable()
                                })
                            },
                        )
                    {
                        return Err(invalid("Program max input schema mismatch"));
                    }
                    let map = |index: usize| {
                        columns
                            .get(index)
                            .copied()
                            .ok_or_else(|| invalid("Program max column out of bounds"))
                    };
                    spec.timestamp = map(spec.timestamp)?;
                    spec.value = map(spec.value)?;
                    spec.tsid = map(spec.tsid)?;
                    spec.groups = spec
                        .groups
                        .into_iter()
                        .map(map)
                        .collect::<Result<Vec<_>>>()?;
                    spec.input_schema = projected;
                    if projection[spec.timestamp] != ordering[1]
                        || projection[spec.tsid] != ordering[0]
                    {
                        return Ok(None);
                    }
                    SourceOperation::Max(Arc::new(spec))
                } else {
                    return Ok(None);
                };
            sources.push(ProgramSource {
                name: source.name,
                schema: schema.clone(),
                projection,
                filters: source.filters,
                ordering: ordering
                    .into_iter()
                    .map(|index| OrderOption {
                        name: schema.field(index).name().clone(),
                        options: SortOptions {
                            descending: false,
                            nulls_first: true,
                        },
                    })
                    .collect(),
                operation,
            });
        }
        Ok(Some(Self {
            join_sum: JoinSum::recognize(&root.query),
            top_one: top_one::TopOne::recognize(&root.query, &sources),
            query: root.query.clone(),
            sources,
        }))
    }

    pub(crate) fn bind(
        &self,
        delta: i64,
        fresh: &HashMap<TableReference, Arc<dyn TableSource>>,
    ) -> Result<(Self, Vec<LogicalPlan>)> {
        let mut query = self.query.as_ref().clone();
        query.grid = shift_grid(&query.grid, delta)?;
        let mut sources = Vec::with_capacity(self.sources.len());
        let mut scans = Vec::with_capacity(self.sources.len());
        for source in &self.sources {
            let provider = fresh
                .get(&source.name)
                .ok_or_else(|| invalid("Missing fresh program source"))?;
            if provider.schema() != source.schema {
                return Err(invalid("Program source schema changed"));
            }
            let filters = source
                .filters
                .iter()
                .map(|filter| {
                    Ok(filter
                        .clone()
                        .transform_up(|expr| {
                            if let Expr::Literal(
                                ScalarValue::TimestampMillisecond(Some(time), None),
                                metadata,
                            ) = expr
                            {
                                Ok(Transformed::yes(Expr::Literal(
                                    ScalarValue::TimestampMillisecond(
                                        Some(shift(time, delta)?),
                                        None,
                                    ),
                                    metadata,
                                )))
                            } else {
                                Ok(Transformed::no(expr))
                            }
                        })?
                        .data)
                })
                .collect::<Result<Vec<_>>>()?;
            // Preserve the authorized provider in the routing plan. Re-resolving
            // the table name here could bind a dropped-and-recreated table.
            let scan = LogicalPlanBuilder::scan_with_filters(
                source.name.clone(),
                provider.clone(),
                None,
                filters.clone(),
            )?
            .build()?;
            scans.push(scan);
            sources.push(ProgramSource {
                filters,
                operation: source.operation.shift_time(delta)?,
                ..source.clone()
            });
        }
        Ok((
            Self {
                join_sum: self.join_sum.clone(),
                top_one: self.top_one.clone(),
                query: Arc::new(query),
                sources,
            },
            scans,
        ))
    }
}

fn shift_grid(grid: &TimeGrid, delta: i64) -> Result<Arc<TimeGrid>> {
    Ok(Arc::new(TimeGrid {
        instants: grid
            .instants
            .iter()
            .map(|&time| shift(time, delta))
            .collect::<Result<Vec<_>>>()?,
    }))
}

fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Plan(message.into())
}

#[derive(Clone)]
pub(crate) struct ProgramNode {
    key: Arc<str>,
    delta: i64,
    schema: DFSchemaRef,
    pub(crate) program: Arc<QueryProgram>,
    pub(crate) scans: Vec<LogicalPlan>,
    fallback: LogicalPlan,
    time_columns: Arc<HashSet<Column>>,
    fresh: HashMap<TableReference, Arc<dyn TableSource>>,
}

impl fmt::Debug for ProgramNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProgramNode")
            .field("query", &self.program.query.expr)
            .field("delta", &self.delta)
            .field("scans", &self.scans)
            .finish_non_exhaustive()
    }
}

impl ProgramNode {
    pub(crate) fn bind(
        program: &QueryProgram,
        key: Arc<str>,
        fallback: LogicalPlan,
        time_columns: Arc<HashSet<Column>>,
        delta: i64,
        fresh: &HashMap<TableReference, Arc<dyn TableSource>>,
    ) -> Result<LogicalPlan> {
        let (program, scans) = program.bind(delta, fresh)?;
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(Self {
                key,
                delta,
                schema: fallback.schema().clone(),
                program: Arc::new(program),
                scans,
                fallback,
                time_columns,
                fresh: fresh.clone(),
            }),
        }))
    }

    pub(crate) fn fallback(&self) -> Result<LogicalPlan> {
        crate::research_series_grid::plan_template::rebind(
            self.fallback.clone(),
            self.delta,
            &self.fresh,
            &self.time_columns,
        )
    }
}

impl PartialEq for ProgramNode {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.delta == other.delta && self.scans == other.scans
    }
}
impl Eq for ProgramNode {}
impl PartialOrd for ProgramNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (&self.key, self.delta).cmp(&(&other.key, other.delta)) {
            Ordering::Equal if self.scans != other.scans => None,
            order => Some(order),
        }
    }
}
impl Hash for ProgramNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key.hash(state);
        self.delta.hash(state);
        self.scans.hash(state);
    }
}
impl UserDefinedLogicalNodeCore for ProgramNode {
    fn name(&self) -> &str {
        "ResearchQueryProgram"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.scans.iter().collect()
    }
    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }
    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ResearchQueryProgram: {}", self.program.query.expr)
    }
    fn with_exprs_and_inputs(
        &self,
        expressions: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        // The source layout is already compiled. Rewriting only its routing
        // plans would desynchronize the predicates used by the native reader.
        if !expressions.is_empty() || inputs != self.scans
            || inputs.iter().zip(&self.scans).any(|(input, original)| {
                !matches!((input, original), (LogicalPlan::TableScan(input), LogicalPlan::TableScan(original))
                    if Arc::ptr_eq(&input.source, &original.source))
            }) {
            return Err(invalid("Cannot rewrite compiled query program inputs"));
        }
        Ok(self.clone())
    }
}

pub(crate) fn explain_plan(plan: LogicalPlan) -> Result<LogicalPlan> {
    if let LogicalPlan::Extension(extension) = &plan
        && let Some(node) = extension.node.as_any().downcast_ref::<ProgramNode>()
    {
        return node.fallback();
    }
    Ok(plan)
}
