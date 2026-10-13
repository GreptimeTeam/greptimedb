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
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use catalog::CatalogManagerRef;
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion_common::{DFSchemaRef, DataFusionError, Result, ToDFSchema};
use datafusion_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use promql_parser::parser::{EvalStmt, Expr as PromExpr, Offset, parse};
use session::context::QueryContextRef;

use crate::parser;
use crate::research_series_grid::evaluate::SelectorKey;
use crate::research_series_grid::grid::TimeGrid;
use crate::research_series_grid::online_execution::OnlineQuery;
use crate::research_series_grid::online_input::SeriesInput;
use crate::research_series_grid::plan::millis;

pub(crate) fn unwrap_explain(expr: &PromExpr) -> Result<(PromExpr, Option<(bool, bool)>)> {
    let PromExpr::Extension(extension) = expr else {
        return Ok((expr.clone(), None));
    };
    let options = match extension.expr.name() {
        parser::EXPLAIN_NODE_NAME => (false, false),
        parser::EXPLAIN_VERBOSE_NODE_NAME => (true, false),
        parser::ANALYZE_NODE_NAME => (false, true),
        parser::ANALYZE_VERBOSE_NODE_NAME => (true, true),
        _ => return Err(DataFusionError::Plan("Unsupported online wrapper".into())),
    };
    let [child] = extension.expr.children() else {
        return Err(DataFusionError::Plan("Invalid online explain input".into()));
    };
    Ok((child.clone(), Some(options)))
}

pub(crate) async fn build(
    stmt: &EvalStmt,
    catalog: &CatalogManagerRef,
    context: &QueryContextRef,
    planned: Vec<(SelectorKey, LogicalPlan)>,
) -> Result<LogicalPlan> {
    let invalid = |message: &str| DataFusionError::Plan(message.into());
    let duration = |value: std::time::Duration| -> Result<i64> {
        if value.subsec_nanos() % 1_000_000 != 0 {
            return Err(invalid("Online query requires millisecond precision"));
        }
        i64::try_from(value.as_millis()).map_err(|_| invalid("Online duration overflow"))
    };
    let mut specs = Vec::new();
    let mut inputs = Vec::new();
    let mut tag_orders = BTreeMap::new();
    let mut output_tags = BTreeSet::new();
    for (key, input) in planned {
        let PromExpr::VectorSelector(selector) =
            parse(&key.selector).map_err(|error| DataFusionError::Plan(error.to_string()))?
        else {
            return Err(invalid("Online input key is not a selector"));
        };
        if selector.matchers.matchers.iter().any(|matcher| {
            matches!(
                matcher.name.as_str(),
                "__schema__" | "__database__" | "__field__"
            )
        }) {
            return Err(invalid(
                "Online prototype does not support namespace or field selectors",
            ));
        }
        let metric = selector
            .name
            .ok_or_else(|| invalid("Online input requires a metric name"))?;
        let table = catalog
            .table(
                context.current_catalog(),
                &context.current_schema(),
                &metric,
                Some(context),
            )
            .await
            .map_err(|error| DataFusionError::Plan(error.to_string()))?
            .ok_or_else(|| invalid("Online input metric does not exist"))?;
        let info = table.table_info();
        let fields: Vec<_> = info.meta.field_column_names().cloned().collect();
        if fields.len() != 1 {
            return Err(invalid("Online prototype requires one value field"));
        }
        let time_column = info
            .meta
            .schema
            .timestamp_column()
            .ok_or_else(|| invalid("Online input has no timestamp column"))?
            .name
            .clone();
        let tags: Vec<_> = info
            .meta
            .row_key_column_names()
            .filter(|name| name.as_str() != "__table_id" && name.as_str() != "__tsid")
            .cloned()
            .collect();
        let offset = match selector.offset {
            None => 0,
            Some(Offset::Pos(value)) => duration(value)?,
            Some(Offset::Neg(value)) => -duration(value)?,
        };
        output_tags.extend(tags.iter().cloned());
        tag_orders.insert(metric.clone(), tags.clone());
        specs.push(SeriesInput {
            key,
            metric,
            time_column,
            value_column: fields[0].clone(),
            tags,
            offset,
        });
        inputs.push(input);
    }
    output_tags.remove("__name__");
    if output_tags.contains("__research_grid_time") || output_tags.contains("__research_grid_value")
    {
        return Err(invalid("Online output column conflicts with source label"));
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
    let grid = TimeGrid::new(
        millis(stmt.start)?,
        millis(stmt.end)?,
        duration(stmt.interval)?,
    )
    .map_err(|message| invalid(message))?;
    OnlineNode::plan(
        OnlineQuery {
            fragments: None,
            expr: stmt.expr.clone(),
            grid: Arc::new(grid),
            specs: Arc::new(specs),
            tag_orders: Arc::new(tag_orders),
            schema: Arc::new(Schema::new(fields)),
            lookback: duration(stmt.lookback_delta)?,
            workers: 8,
        },
        inputs,
    )
}

#[derive(Debug, Clone)]
pub(crate) struct OnlineNode {
    key: String,
    schema: DFSchemaRef,
    pub query: Arc<OnlineQuery>,
    inputs: Vec<LogicalPlan>,
}

impl OnlineNode {
    pub(crate) fn shift_time(&self, delta: i64) -> Result<LogicalPlan> {
        let mut query = self.query.as_ref().clone();
        query.grid = Arc::new(TimeGrid {
            instants: query
                .grid
                .instants
                .iter()
                .map(|&time| crate::research_series_grid::plan_template::shift(time, delta))
                .collect::<Result<Vec<_>>>()?,
        });
        Self::plan(query, self.inputs.clone())
    }

    pub(crate) fn plan(query: OnlineQuery, inputs: Vec<LogicalPlan>) -> Result<LogicalPlan> {
        if inputs.len()
            != query
                .fragments
                .as_ref()
                .map_or(query.specs.len(), |inputs| inputs.len())
        {
            return Err(DataFusionError::Plan(
                "Online selector plan count mismatch".into(),
            ));
        }
        let node = Self {
            key: format!("{query:?}"),
            schema: query.schema.clone().to_dfschema_ref()?,
            query: Arc::new(query),
            inputs,
        };
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        }))
    }
}

impl PartialEq for OnlineNode {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.inputs == other.inputs
    }
}
impl Eq for OnlineNode {}

impl PartialOrd for OnlineNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.key.cmp(&other.key) {
            Ordering::Equal if self.inputs != other.inputs => None,
            order => Some(order),
        }
    }
}

impl Hash for OnlineNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key.hash(state);
        self.inputs.hash(state);
    }
}

impl UserDefinedLogicalNodeCore for OnlineNode {
    fn name(&self) -> &str {
        "ResearchOnlineSeries"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.inputs.iter().collect()
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ResearchOnlineSeries: {}", self.query.expr)
    }

    fn with_exprs_and_inputs(
        &self,
        expressions: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        if !expressions.is_empty() || inputs.len() != self.inputs.len() {
            return Err(DataFusionError::Plan(
                "Invalid online series rewrite".into(),
            ));
        }
        Ok(Self {
            inputs,
            ..self.clone()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use promql_parser::parser::ast::{Extension as PromExtension, ExtensionExpr};

    #[test]
    fn frontend_binding_requires_placed_fragments_and_preserves_remote_identity() {
        use datafusion_expr::{LogicalPlanBuilder, LogicalTableSource, col};
        use promql::extension_plan::{InstantManipulate, SeriesDivide};

        use crate::dist_plan::{DistPlannerAnalyzer, MergeScanLogicalPlan};
        use crate::research_series_grid::SeriesInstantMax;

        for label_type in [
            DataType::Utf8,
            DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
        ] {
            let source = LogicalPlanBuilder::scan(
                "devices",
                Arc::new(LogicalTableSource::new(Arc::new(Schema::new(vec![
                    Field::new(
                        "ts",
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        false,
                    ),
                    Field::new("greptime_value", DataType::Float64, true),
                    Field::new("__tsid", DataType::UInt64, false),
                    Field::new("host", label_type, true),
                ])))),
                None,
            )
            .unwrap()
            .build()
            .unwrap();
            let divided = LogicalPlan::Extension(Extension {
                node: Arc::new(SeriesDivide::new(
                    vec!["__tsid".into()],
                    "ts".into(),
                    source.clone(),
                )),
            });
            let instant = LogicalPlan::Extension(Extension {
                node: Arc::new(InstantManipulate::new(
                    10_000,
                    30_000,
                    10_000,
                    10_000,
                    0,
                    "ts".into(),
                    vec!["host".into()],
                    Some("greptime_value".into()),
                    divided,
                )),
            });
            let aggregate = LogicalPlanBuilder::from(instant)
                .aggregate(
                    vec![col("host"), col("ts")],
                    vec![
                        datafusion::functions_aggregate::min_max::max_udaf()
                            .call(vec![col("greptime_value")])
                            .alias("value"),
                    ],
                )
                .unwrap()
                .build()
                .unwrap();
            let fragment = SeriesInstantMax::recognize(&aggregate, &["host".into()], "ts", "value")
                .unwrap()
                .unwrap()
                .plan();
            let spec = SeriesInput {
                key: SelectorKey {
                    selector: "devices".into(),
                    function: None,
                },
                metric: "devices".into(),
                time_column: "ts".into(),
                value_column: "value".into(),
                tags: vec!["host".into()],
                offset: 0,
            };
            let wrap = |inputs: Vec<LogicalPlan>| {
                OnlineNode::plan(
                    OnlineQuery {
                        fragments: None,
                        expr: parse("sum(devices)").unwrap(),
                        grid: Arc::new(TimeGrid::new(10_000, 30_000, 10_000).unwrap()),
                        specs: Arc::new(vec![spec.clone(); inputs.len()]),
                        tag_orders: Arc::new(BTreeMap::new()),
                        schema: Arc::new(Schema::empty()),
                        lookback: 10_000,
                        workers: 8,
                    },
                    inputs,
                )
                .unwrap()
            };
            let remote = MergeScanLogicalPlan::new(fragment.clone(), false, BTreeMap::new());
            let original = wrap(vec![
                remote.clone().into_logical_plan(),
                remote.clone().into_logical_plan(),
            ]);
            let bound = DistPlannerAnalyzer::prepare_series_frontend(&original)
                .unwrap()
                .unwrap();
            assert_eq!(bound.schema(), original.schema());
            let mut ids = BTreeSet::new();
            for input in bound.inputs() {
                let LogicalPlan::Extension(extension) = input else {
                    panic!("Missing remote input")
                };
                let remote = extension
                    .node
                    .as_any()
                    .downcast_ref::<MergeScanLogicalPlan>()
                    .unwrap();
                assert_eq!(remote.input(), &fragment);
                assert!(ids.insert(remote.remote_dyn_filter_producer_id().unwrap()));
            }
            assert_eq!(ids.len(), 2);
            assert_eq!(
                DistPlannerAnalyzer::prepare_series_frontend(&bound)
                    .unwrap()
                    .unwrap(),
                bound
            );
            let invalid_schema = remote
                .with_output_schema(source.schema().clone())
                .into_logical_plan();
            let placeholder = MergeScanLogicalPlan::new(fragment.clone(), true, BTreeMap::new())
                .into_logical_plan();
            let ordinary_remote = MergeScanLogicalPlan::new(source.clone(), false, BTreeMap::new())
                .into_logical_plan();
            for rejected in [
                source,
                wrap(vec![]),
                wrap(vec![fragment]),
                wrap(vec![placeholder]),
                wrap(vec![invalid_schema]),
                wrap(vec![ordinary_remote]),
            ] {
                assert!(
                    DistPlannerAnalyzer::prepare_series_frontend(&rejected)
                        .unwrap()
                        .is_none()
                );
            }
        }
    }

    #[test]
    fn template_shift_updates_online_identity_without_mutating_shared_grid() {
        let original = OnlineNode::plan(
            OnlineQuery {
                fragments: None,
                expr: parse("sum(x)").unwrap(),
                grid: Arc::new(TimeGrid::new(-5, 6, 4).unwrap()),
                specs: Arc::new(vec![]),
                tag_orders: Arc::new(BTreeMap::new()),
                schema: Arc::new(Schema::empty()),
                lookback: 300_000,
                workers: 8,
            },
            vec![],
        )
        .unwrap();
        let LogicalPlan::Extension(extension) = &original else {
            unreachable!()
        };
        let moved = crate::research_series_grid::plan_template::shift_extension(extension, 10)
            .unwrap()
            .unwrap();
        assert_ne!(original, moved);
        let LogicalPlan::Extension(extension) = &moved else {
            unreachable!()
        };
        let node = extension
            .node
            .as_any()
            .downcast_ref::<OnlineNode>()
            .unwrap();
        assert_eq!(node.query.grid.instants, [5, 9, 13]);
        assert_eq!(node.query.lookback, 300_000);
        assert_eq!(node.shift_time(-10).unwrap(), original);
        assert!(node.shift_time(i64::MAX - 9).is_err());
        let LogicalPlan::Extension(extension) = &original else {
            unreachable!()
        };
        let node = extension
            .node
            .as_any()
            .downcast_ref::<OnlineNode>()
            .unwrap();
        assert_eq!(node.query.grid.instants, [-5, -1, 3]);
    }

    #[test]
    fn explain_and_analyze_wrap_the_query_instead_of_becoming_selectors() {
        let expr = parse("sum(x)").unwrap();
        let wrappers: [(Arc<dyn ExtensionExpr>, (bool, bool)); 4] = [
            (
                Arc::new(parser::ExplainExpr { expr: expr.clone() }),
                (false, false),
            ),
            (
                Arc::new(parser::ExplainVerboseExpr { expr: expr.clone() }),
                (true, false),
            ),
            (
                Arc::new(parser::AnalyzeExpr { expr: expr.clone() }),
                (false, true),
            ),
            (
                Arc::new(parser::AnalyzeVerboseExpr { expr: expr.clone() }),
                (true, true),
            ),
        ];
        for (wrapper, expected) in wrappers {
            let (inner, options) =
                unwrap_explain(&PromExpr::Extension(PromExtension { expr: wrapper })).unwrap();
            assert_eq!(inner, expr);
            assert_eq!(options, Some(expected));
            assert_eq!(
                crate::research_series_grid::selector_requests(&inner)
                    .unwrap()
                    .len(),
                1
            );
        }
        assert_eq!(unwrap_explain(&expr).unwrap(), (expr, None));
    }
}
