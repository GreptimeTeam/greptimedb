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

//! The binary-island fast path of the PromQL planner.

use std::collections::{BTreeSet, HashMap};

use common_query::prelude::OTLP_AGGREGATION_TEMPORALITY_LABEL;
use datafusion::common::DFSchemaRef;
use datafusion::logical_expr::expr::Alias;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::{Column, Expr as DfExpr, JoinType};
use datafusion_common::{NullEquality, TableReference};
use datafusion_expr::lit;
use promql_parser::label::{METRIC_NAME, MatchOp, Matcher};
use promql_parser::parser::token::{self, TokenType};
use promql_parser::parser::{
    BinaryExpr as PromBinaryExpr, Expr as PromExpr, Offset, ParenExpr, UnaryExpr,
    VectorMatchCardinality, VectorSelector,
};
use snafu::ResultExt;

use super::{BINARY_ISLAND_LEAF_ALIAS_PREFIX, PromPlanner, PromPlannerContext};
use crate::promql::error::{DataFusionPlanningSnafu, Result};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct VectorLeafKey {
    metric_name: String,
    matchers: Vec<(String, String, String)>,
    or_matchers: Vec<Vec<(String, String, String)>>,
    offset_ms: i128,
    at: String,
}

#[derive(Debug, Clone)]
struct IslandLeaf {
    selector: VectorSelector,
    display_table: String,
}

#[derive(Debug, Clone)]
enum IslandExpr {
    VectorLeaf(usize),
    Scalar(DfExpr),
    Unary {
        input: Box<IslandExpr>,
    },
    Binary {
        op: TokenType,
        lhs: Box<IslandExpr>,
        rhs: Box<IslandExpr>,
    },
}

impl IslandExpr {
    fn try_new(expr: &PromExpr, env: &mut IslandCollectEnv) -> Option<Self> {
        if let Some(expr) = PromPlanner::try_build_literal_expr(expr) {
            return Some(Self::Scalar(expr));
        }

        match expr {
            PromExpr::Paren(ParenExpr { expr }) => Self::try_new(expr, env),
            PromExpr::VectorSelector(selector) => {
                let leaf = env.intern_leaf(selector)?;
                Some(Self::VectorLeaf(leaf))
            }
            PromExpr::Unary(UnaryExpr { expr }) => {
                let input = Self::try_new(expr, env)?;
                Some(Self::Unary {
                    input: Box::new(input),
                })
            }
            PromExpr::Binary(PromBinaryExpr {
                lhs,
                rhs,
                op,
                modifier,
            }) if matches!(
                op.id(),
                token::T_ADD
                    | token::T_SUB
                    | token::T_MUL
                    | token::T_DIV
                    | token::T_MOD
                    | token::T_POW
                    | token::T_ATAN2
            ) && modifier.as_ref().is_none_or(|modifier| {
                !modifier.return_bool
                    && modifier.matching.is_none()
                    && matches!(modifier.card, VectorMatchCardinality::OneToOne)
                    && modifier.fill_values.lhs.is_none()
                    && modifier.fill_values.rhs.is_none()
            }) =>
            {
                let lhs = Self::try_new(lhs, env)?;
                let rhs = Self::try_new(rhs, env)?;
                Some(Self::Binary {
                    op: *op,
                    lhs: Box::new(lhs),
                    rhs: Box::new(rhs),
                })
            }
            _ => None,
        }
    }
}

#[derive(Debug, Default)]
struct IslandCollectEnv {
    leaf_by_key: HashMap<VectorLeafKey, usize>,
    leaves: Vec<IslandLeaf>,
    vector_occurrences: usize,
}

#[derive(Debug)]
struct PlannedIslandLeaf {
    plan: LogicalPlan,
    ctx: PromPlannerContext,
    alias: TableReference,
    display_table: String,
}

#[derive(Debug)]
struct IslandFieldExprs {
    exprs: Vec<DfExpr>,
    names: Vec<String>,
    scalar: bool,
}

impl VectorLeafKey {
    fn from_selector(selector: &VectorSelector) -> Option<Self> {
        let mut metric_name = selector.name.clone();
        let mut matchers = Vec::with_capacity(selector.matchers.matchers.len());
        let matcher_key = |matcher: &Matcher| {
            (
                matcher.name.clone(),
                matcher.op.to_string(),
                matcher.value.clone(),
            )
        };

        for matcher in &selector.matchers.matchers {
            if matcher.name == METRIC_NAME {
                if matcher.op != MatchOp::Equal || metric_name.is_some() {
                    return None;
                }
                metric_name = Some(matcher.value.clone());
            } else {
                matchers.push(matcher_key(matcher));
            }
        }
        matchers.sort();

        let mut or_matchers = selector
            .matchers
            .or_matchers
            .iter()
            .map(|group| {
                let mut group = group.iter().map(matcher_key).collect::<Vec<_>>();
                group.sort();
                group
            })
            .collect::<Vec<_>>();
        or_matchers.sort();

        Some(Self {
            metric_name: metric_name?,
            matchers,
            or_matchers,
            offset_ms: match &selector.offset {
                Some(Offset::Pos(duration)) => duration.as_millis() as i128,
                Some(Offset::Neg(duration)) => -(duration.as_millis() as i128),
                None => 0,
            },
            at: format!("{:?}", selector.at),
        })
    }
}

impl IslandCollectEnv {
    fn intern_leaf(&mut self, selector: &VectorSelector) -> Option<usize> {
        self.vector_occurrences += 1;
        let key = VectorLeafKey::from_selector(selector)?;
        if let Some(id) = self.leaf_by_key.get(&key) {
            return Some(*id);
        }

        let id = self.leaves.len();
        self.leaves.push(IslandLeaf {
            selector: selector.clone(),
            display_table: key.metric_name.clone(),
        });
        self.leaf_by_key.insert(key, id);
        Some(id)
    }
}

impl PromPlanner {
    pub(super) async fn try_plan_binary_island(
        &mut self,
        binary_expr: &PromBinaryExpr,
    ) -> Result<Option<LogicalPlan>> {
        let original_ctx = self.ctx.clone();
        let mut collect_env = IslandCollectEnv::default();
        let Some(island_expr) =
            IslandExpr::try_new(&PromExpr::Binary(binary_expr.clone()), &mut collect_env)
        else {
            return Ok(None);
        };

        if collect_env.leaves.is_empty()
            || collect_env.vector_occurrences <= collect_env.leaves.len()
        {
            return Ok(None);
        }

        let mut planned_leaves = Vec::with_capacity(collect_env.leaves.len());
        for (idx, leaf) in collect_env.leaves.iter().enumerate() {
            let plan = self
                .prom_vector_selector_to_plan(&leaf.selector, false)
                .await?;
            let ctx = self.ctx.clone();
            let alias = TableReference::bare(format!("{BINARY_ISLAND_LEAF_ALIAS_PREFIX}{idx}"));
            let plan = LogicalPlanBuilder::from(plan)
                .alias(alias.clone())
                .context(DataFusionPlanningSnafu)?
                .build()
                .context(DataFusionPlanningSnafu)?;
            planned_leaves.push(PlannedIslandLeaf {
                plan,
                ctx,
                alias,
                display_table: leaf.display_table.clone(),
            });
        }

        if planned_leaves.iter().any(|leaf| {
            Self::field_columns_contain_native_histogram(
                leaf.plan.schema(),
                &leaf.ctx.field_columns,
            )
        }) {
            self.ctx = original_ctx;
            return Ok(None);
        }

        if !Self::binary_island_join_contexts_supported(&planned_leaves) {
            self.ctx = original_ctx;
            return Ok(None);
        }

        let mut input = planned_leaves[0].plan.clone();
        for right_idx in 1..planned_leaves.len() {
            input = self.join_binary_island_leaf(
                input,
                &planned_leaves[0],
                &planned_leaves[right_idx],
            )?;
        }

        let field_exprs =
            Self::build_binary_island_field_exprs(&island_expr, &planned_leaves, input.schema())?;
        if field_exprs.scalar || field_exprs.exprs.is_empty() {
            self.ctx = original_ctx;
            return Ok(None);
        }

        let plan = self.project_binary_island(
            input,
            &planned_leaves[0].alias,
            &planned_leaves[0].ctx,
            field_exprs,
        )?;
        Ok(Some(plan))
    }

    fn binary_island_join_contexts_supported(leaves: &[PlannedIslandLeaf]) -> bool {
        if leaves
            .iter()
            .any(|leaf| leaf.ctx.time_index_column.is_none())
        {
            return false;
        }

        if leaves.len() <= 1 {
            return true;
        }

        let first_tags = leaves[0].ctx.tag_columns.iter().collect::<BTreeSet<_>>();

        leaves.iter().skip(1).all(|leaf| {
            (Self::plan_has_tsid_column(&leaves[0].plan) && Self::plan_has_tsid_column(&leaf.plan))
                || leaf.ctx.tag_columns.iter().collect::<BTreeSet<_>>() == first_tags
        })
    }

    fn join_binary_island_leaf(
        &self,
        left: LogicalPlan,
        first_leaf: &PlannedIslandLeaf,
        right_leaf: &PlannedIslandLeaf,
    ) -> Result<LogicalPlan> {
        let only_join_time_index = (first_leaf.ctx.tag_columns.is_empty()
            || right_leaf.ctx.tag_columns.is_empty())
            && !first_leaf
                .ctx
                .tag_columns
                .iter()
                .chain(&right_leaf.ctx.tag_columns)
                .any(|tag| tag == OTLP_AGGREGATION_TEMPORALITY_LABEL);
        let (mut left_keys, mut right_keys, force_empty_join) = self.binary_join_key_columns(
            left.schema(),
            right_leaf.plan.schema(),
            &first_leaf.ctx,
            &right_leaf.ctx,
            only_join_time_index,
            &None,
        )?;

        if let (Some(left_time_index_column), Some(right_time_index_column)) = (
            first_leaf.ctx.time_index_column.clone(),
            right_leaf.ctx.time_index_column.clone(),
        ) {
            left_keys.insert(left_time_index_column);
            right_keys.insert(right_time_index_column);
        }

        LogicalPlanBuilder::from(left)
            .join_detailed(
                right_leaf.plan.clone(),
                JoinType::Inner,
                (
                    left_keys
                        .into_iter()
                        .map(|name| Column::new(Some(first_leaf.alias.clone()), name))
                        .collect::<Vec<_>>(),
                    right_keys
                        .into_iter()
                        .map(|name| Column::new(Some(right_leaf.alias.clone()), name))
                        .collect::<Vec<_>>(),
                ),
                force_empty_join.then_some(lit(false)),
                NullEquality::NullEqualsNull,
            )
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)
    }

    fn build_binary_island_field_exprs(
        expr: &IslandExpr,
        leaves: &[PlannedIslandLeaf],
        schema: &DFSchemaRef,
    ) -> Result<IslandFieldExprs> {
        match expr {
            IslandExpr::VectorLeaf(id) => {
                let leaf = &leaves[*id];
                let exprs = leaf
                    .ctx
                    .field_columns
                    .iter()
                    .map(|field| {
                        schema
                            .qualified_field_with_name(Some(&leaf.alias), field)
                            .context(DataFusionPlanningSnafu)
                            .map(|field| DfExpr::Column(field.into()))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let names = leaf
                    .ctx
                    .field_columns
                    .iter()
                    .map(|field| format!("{}.{}", leaf.display_table, field))
                    .collect();
                Ok(IslandFieldExprs {
                    exprs,
                    names,
                    scalar: false,
                })
            }
            IslandExpr::Scalar(expr) => Ok(IslandFieldExprs {
                exprs: vec![expr.clone()],
                names: vec![expr.schema_name().to_string()],
                scalar: true,
            }),
            IslandExpr::Unary { input } => {
                let input = Self::build_binary_island_field_exprs(input, leaves, schema)?;
                let mut exprs = Vec::with_capacity(input.exprs.len());
                let mut names = Vec::with_capacity(input.names.len());
                for (expr, name) in input.exprs.into_iter().zip(input.names) {
                    exprs.push(DfExpr::Negative(Box::new(expr)));
                    names.push(format!("-{name}"));
                }
                Ok(IslandFieldExprs {
                    exprs,
                    names,
                    scalar: input.scalar,
                })
            }
            IslandExpr::Binary { op, lhs, rhs } => {
                let same_leaf = match (&**lhs, &**rhs) {
                    (IslandExpr::VectorLeaf(left), IslandExpr::VectorLeaf(right))
                        if left == right =>
                    {
                        Some(*left)
                    }
                    _ => None,
                };
                let lhs = Self::build_binary_island_field_exprs(lhs, leaves, schema)?;
                let rhs = Self::build_binary_island_field_exprs(rhs, leaves, schema)?;
                let expr_builder = Self::prom_token_to_binary_expr_builder(*op)?;
                let scalar = lhs.scalar && rhs.scalar;
                let op = op.to_string();

                let (exprs, names) = match (lhs.scalar, rhs.scalar) {
                    (true, true) => {
                        let expr = expr_builder(lhs.exprs[0].clone(), rhs.exprs[0].clone())?;
                        let name = format!("{} {op} {}", lhs.names[0], rhs.names[0]);
                        (vec![expr], vec![name])
                    }
                    (true, false) => {
                        let mut exprs = Vec::with_capacity(rhs.exprs.len());
                        let mut names = Vec::with_capacity(rhs.names.len());
                        for (rhs_expr, rhs_name) in rhs.exprs.into_iter().zip(rhs.names) {
                            exprs.push(expr_builder(lhs.exprs[0].clone(), rhs_expr)?);
                            names.push(format!("{} {op} {rhs_name}", lhs.names[0]));
                        }
                        (exprs, names)
                    }
                    (false, true) => {
                        let mut exprs = Vec::with_capacity(lhs.exprs.len());
                        let mut names = Vec::with_capacity(lhs.names.len());
                        for (lhs_expr, lhs_name) in lhs.exprs.into_iter().zip(lhs.names) {
                            exprs.push(expr_builder(lhs_expr, rhs.exprs[0].clone())?);
                            names.push(format!("{lhs_name} {op} {}", rhs.names[0]));
                        }
                        (exprs, names)
                    }
                    (false, false) => {
                        let mut exprs = Vec::new();
                        let mut names = Vec::new();
                        for (idx, ((lhs_expr, rhs_expr), (mut lhs_name, mut rhs_name))) in lhs
                            .exprs
                            .into_iter()
                            .zip(rhs.exprs)
                            .zip(lhs.names.into_iter().zip(rhs.names))
                            .enumerate()
                        {
                            if let Some(leaf) = same_leaf {
                                let field = leaves[leaf]
                                    .ctx
                                    .field_columns
                                    .get(idx)
                                    .cloned()
                                    .unwrap_or_else(|| lhs_name.clone());
                                lhs_name = format!("lhs.{field}");
                                rhs_name = format!("rhs.{field}");
                            }
                            exprs.push(expr_builder(lhs_expr, rhs_expr)?);
                            names.push(format!("{lhs_name} {op} {rhs_name}"));
                        }
                        (exprs, names)
                    }
                };

                Ok(IslandFieldExprs {
                    exprs,
                    names,
                    scalar,
                })
            }
        }
    }

    fn project_binary_island(
        &mut self,
        input: LogicalPlan,
        base_alias: &TableReference,
        base_ctx: &PromPlannerContext,
        field_exprs: IslandFieldExprs,
    ) -> Result<LogicalPlan> {
        self.ctx = base_ctx.clone();

        let schema = input.schema();
        let non_field_exprs = base_ctx
            .tag_columns
            .iter()
            .chain(base_ctx.time_index_column.iter())
            .map(|column| {
                schema
                    .qualified_field_with_name(Some(base_alias), column)
                    .context(DataFusionPlanningSnafu)
                    .map(|field| DfExpr::Column(field.into()))
            });
        let tsid_expr = Self::optional_tsid_projection(schema, Some(base_alias), base_ctx.use_tsid)
            .into_iter()
            .map(Ok);

        self.ctx.field_columns = field_exprs.names;
        let field_exprs = field_exprs
            .exprs
            .into_iter()
            .zip(self.ctx.field_columns.iter())
            .map(|(expr, name)| Ok(DfExpr::Alias(Alias::new(expr, None::<String>, name))));

        let project_exprs = non_field_exprs
            .chain(tsid_expr)
            .chain(field_exprs)
            .collect::<Result<Vec<_>>>()?;

        let plan = LogicalPlanBuilder::from(input)
            .project(project_exprs)
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)?;

        self.ctx.table_name = None;
        self.ctx.schema_name = None;

        Ok(plan)
    }
}
