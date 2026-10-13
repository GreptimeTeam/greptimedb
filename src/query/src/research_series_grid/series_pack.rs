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

use std::cmp::Ordering;
use std::fmt;
use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_planner::PhysicalPlanner;
use datafusion_common::{Column, DFSchema, DFSchemaRef, DataFusionError, Result};
use datafusion_expr::{
    EmptyRelation, Expr, ExprSchemable, Extension, LogicalPlan, UserDefinedLogicalNodeCore, lit,
};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::aggregate::AggregateExprBuilder;
use datafusion_physical_expr::expressions::Column as PhysicalColumn;
use datafusion_physical_expr::expressions::IsNotNullExpr;
use serde::{Deserialize, Serialize};

use crate::research_series_grid::online_frontier::PACKED_POINTS;

/// Packs points independently in each partition. Label tuples may occur in multiple
/// output rows; only the online grid collector may consume this representation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SeriesPack {
    input: LogicalPlan,
    columns: Vec<Column>,
    schema: DFSchemaRef,
    unresolved: Option<Vec<usize>>,
}

#[derive(Serialize, Deserialize)]
struct WirePack {
    version: u8,
    columns: Vec<usize>,
}

impl SeriesPack {
    pub(crate) const NAME: &str = "ResearchSeriesPack";

    pub(crate) fn plan(
        input: LogicalPlan,
        tags: &[String],
        time: &str,
        value: &str,
    ) -> Result<LogicalPlan> {
        let columns = tags
            .iter()
            .map(String::as_str)
            .chain([time, value])
            .map(|name| {
                input
                    .schema()
                    .index_of_column_by_name(None, name)
                    .map(|index| input.schema().qualified_field(index))
                    .map(|(qualifier, field)| Column::new(qualifier.cloned(), field.name()))
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!("Missing series pack column {name}"))
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(Self::new(input, columns)?),
        }))
    }

    fn new(input: LogicalPlan, columns: Vec<Column>) -> Result<Self> {
        if columns.len() < 2 {
            return Err(DataFusionError::Plan(
                "Series pack requires time and value columns".into(),
            ));
        }
        for column in &columns {
            input.schema().index_of_column(column)?;
        }
        let tags = columns[..columns.len() - 2]
            .iter()
            .cloned()
            .map(Expr::Column)
            .collect::<Vec<_>>();
        let points = datafusion::functions_aggregate::array_agg::array_agg_udaf()
            .call(vec![Self::point(&columns)])
            .alias(PACKED_POINTS);
        let fields = tags
            .iter()
            .chain(std::iter::once(&points))
            .map(|expr| expr.to_field(input.schema()))
            .collect::<Result<Vec<_>>>()?;
        // Partition-local label tuples are not unique across the stream. Do not
        // inherit the functional dependencies of a global aggregate.
        let schema = Arc::new(DFSchema::new_with_metadata(
            fields,
            input.schema().metadata().clone(),
        )?);
        Ok(Self {
            input,
            columns,
            schema,
            unresolved: None,
        })
    }

    fn point(columns: &[Column]) -> Expr {
        datafusion::functions::core::named_struct().call(vec![
            lit("time"),
            Expr::Column(columns[columns.len() - 2].clone()),
            lit("value"),
            Expr::Column(columns[columns.len() - 1].clone()),
        ])
    }

    pub(crate) fn serialize(&self) -> Result<Vec<u8>> {
        let columns = self
            .columns
            .iter()
            .map(|column| self.input.schema().index_of_column(column))
            .collect::<Result<_>>()?;
        serde_json::to_vec(&WirePack {
            version: 1,
            columns,
        })
        .map_err(|error| DataFusionError::External(Box::new(error)))
    }

    pub(crate) fn deserialize(bytes: &[u8]) -> Result<Self> {
        let wire: WirePack = serde_json::from_slice(bytes)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        if wire.version != 1 || wire.columns.len() < 2 {
            return Err(DataFusionError::Plan(
                "Invalid series pack descriptor".into(),
            ));
        }
        let schema = Arc::new(DFSchema::empty());
        Ok(Self {
            input: LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: schema.clone(),
            }),
            columns: Vec::new(),
            schema,
            unresolved: Some(wire.columns),
        })
    }

    pub(crate) fn physical(
        &self,
        input: Arc<dyn ExecutionPlan>,
        planner: &dyn PhysicalPlanner,
        session: &dyn Session,
        context: &PhysicalPlanningContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if self.unresolved.is_some() {
            return Err(DataFusionError::Plan(
                "Series pack inputs have not been rebound".into(),
            ));
        }
        let point = planner.create_physical_expr(
            &Self::point(&self.columns),
            self.input.schema(),
            session,
            context,
        )?;
        let function = AggregateExprBuilder::new(
            datafusion::functions_aggregate::array_agg::array_agg_udaf(),
            vec![point],
        )
        .schema(input.schema())
        .alias(PACKED_POINTS)
        .build()?;
        let groups = self.columns[..self.columns.len() - 2]
            .iter()
            .map(|column| {
                Ok((
                    planner.create_physical_expr(
                        &Expr::Column(column.clone()),
                        self.input.schema(),
                        session,
                        context,
                    )?,
                    column.name.clone(),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let schema = input.schema();
        let packed: Arc<dyn ExecutionPlan> = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::new_single(groups),
            vec![Arc::new(function)],
            vec![None],
            input,
            schema,
        )?);
        let packed: Arc<dyn ExecutionPlan> = if self.columns.len() == 2 {
            // A global partial aggregate emits a null list for an empty partition.
            // It represents no points, not a null series at the transport boundary.
            let points = Arc::new(PhysicalColumn::new(packed.schema().field(0).name(), 0));
            Arc::new(FilterExec::try_new(
                Arc::new(IsNotNullExpr::new(points)),
                packed,
            )?)
        } else {
            packed
        };
        // Partial state and final array_agg have identical point-list types, but
        // different field names. No final aggregation is required by our consumer.
        let projection = self
            .schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                (
                    Arc::new(PhysicalColumn::new(
                        packed.schema().field(index).name(),
                        index,
                    )) as Arc<dyn PhysicalExpr>,
                    field.name().clone(),
                )
            })
            .collect::<Vec<_>>();
        Ok(Arc::new(ProjectionExec::try_new(projection, packed)?))
    }
}

impl PartialOrd for SeriesPack {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (&self.columns, &self.unresolved, &self.input).partial_cmp(&(
            &other.columns,
            &other.unresolved,
            &other.input,
        ))
    }
}

impl UserDefinedLogicalNodeCore for SeriesPack {
    fn name(&self) -> &str {
        Self::NAME
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }
    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }
    fn expressions(&self) -> Vec<Expr> {
        self.columns.iter().cloned().map(Expr::Column).collect()
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", Self::NAME)
    }
    fn with_exprs_and_inputs(
        &self,
        expressions: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        let [input] = inputs.as_slice() else {
            return Err(DataFusionError::Plan(
                "Series pack requires one input".into(),
            ));
        };
        let columns = if let Some(indices) = &self.unresolved {
            indices
                .iter()
                .map(|&index| {
                    if index >= input.schema().fields().len() {
                        return Err(DataFusionError::Plan(
                            "Series pack column index out of bounds".into(),
                        ));
                    }
                    let (qualifier, field) = input.schema().qualified_field(index);
                    Ok(Column::new(qualifier.cloned(), field.name()))
                })
                .collect::<Result<Vec<_>>>()?
        } else {
            if expressions.len() != self.columns.len() {
                return Err(DataFusionError::Plan(
                    "Series pack column count changed".into(),
                ));
            }
            expressions
                .into_iter()
                .map(|expr| match expr {
                    Expr::Column(column) => Ok(column),
                    _ => Err(DataFusionError::Plan(
                        "Series pack requires column expressions".into(),
                    )),
                })
                .collect::<Result<Vec<_>>>()?
        };
        Self::new(input.clone(), columns)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion_common::ToDFSchema;

    use super::*;

    #[test]
    fn descriptor_rebinds_columns_after_remote_renaming_and_rejects_bad_indices() {
        let input = |names: [&str; 3]| {
            LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: Schema::new(vec![
                    Field::new(names[0], DataType::Utf8, true),
                    Field::new(
                        names[1],
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        false,
                    ),
                    Field::new(names[2], DataType::Float64, true),
                ])
                .to_dfschema_ref()
                .unwrap(),
            })
        };
        let plan = SeriesPack::plan(
            input(["host", "ts", "value"]),
            &["host".into()],
            "ts",
            "value",
        )
        .unwrap();
        let LogicalPlan::Extension(extension) = plan else {
            unreachable!()
        };
        let node = extension
            .node
            .as_any()
            .downcast_ref::<SeriesPack>()
            .unwrap();
        assert!(node.schema().functional_dependencies().is_empty());
        let restored = SeriesPack::deserialize(&node.serialize().unwrap())
            .unwrap()
            .with_exprs_and_inputs(
                vec![],
                vec![input(["remote_host", "remote_time", "remote_value"])],
            )
            .unwrap();
        assert!(restored.schema().functional_dependencies().is_empty());
        assert_eq!(
            restored
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["remote_host", "remote_time", "remote_value"]
        );
        let bad = SeriesPack::deserialize(br#"{"version":1,"columns":[0,1,3]}"#).unwrap();
        assert!(
            bad.with_exprs_and_inputs(vec![], vec![input(["host", "ts", "value"])])
                .is_err()
        );
        assert!(SeriesPack::deserialize(br#"{"version":2,"columns":[0,1,2]}"#).is_err());
    }
}
