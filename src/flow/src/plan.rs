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

//! This module contain basic definition for dataflow's plan
//! that can be translate to hydro dataflow

mod join;
mod reduce;

use std::collections::BTreeSet;

use datatypes::arrow::ipc::Map;
use serde::{Deserialize, Serialize};

use crate::adapter::error::Error;
use crate::expr::{
    AggregateExpr, EvalError, GlobalId, Id, LocalId, MapFilterProject, SafeMfpPlan, ScalarExpr,
    TypedExpr,
};
use crate::plan::join::JoinPlan;
pub(crate) use crate::plan::reduce::{AccumulablePlan, AggrWithIndex, KeyValPlan, ReducePlan};
use crate::repr::{ColumnType, DiffRow, RelationType};

/// A plan for a dataflow component. But with type to indicate the output type of the relation.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Deserialize, Serialize)]
pub struct TypedPlan {
    /// output type of the relation
    pub typ: RelationType,
    /// The untyped plan.
    pub plan: Plan,
}

impl TypedPlan {
    /// directly apply a mfp to the plan
    pub fn mfp(self, mfp: MapFilterProject) -> Result<Self, Error> {
        let new_type = self.typ.apply_mfp(&mfp)?;
        let plan = match self.plan {
            Plan::Mfp {
                input,
                mfp: old_mfp,
            } => Plan::Mfp {
                input,
                mfp: MapFilterProject::compose(old_mfp, mfp)?,
            },
            _ => Plan::Mfp {
                input: Box::new(self),
                mfp,
            },
        };
        Ok(TypedPlan {
            typ: new_type,
            plan,
        })
    }

    /// project the plan to the given expressions
    pub fn projection(self, exprs: Vec<TypedExpr>) -> Result<Self, Error> {
        let input_arity = self.typ.column_types.len();
        let output_arity = exprs.len();
        let (exprs, expr_typs): (Vec<_>, Vec<_>) = exprs
            .into_iter()
            .map(|TypedExpr { expr, typ }| (expr, typ))
            .unzip();
        let mfp = MapFilterProject::new(input_arity)
            .map(exprs)?
            .project(input_arity..input_arity + output_arity)?;
        let out_typ = self.typ.apply_mfp(&mfp)?;
        // special case for mfp to compose when the plan is already mfp
        let plan = match self.plan {
            Plan::Mfp {
                input,
                mfp: old_mfp,
            } => Plan::Mfp {
                input,
                mfp: MapFilterProject::compose(old_mfp, mfp)?,
            },
            _ => Plan::Mfp {
                input: Box::new(self),
                mfp,
            },
        };
        Ok(TypedPlan { typ: out_typ, plan })
    }

    /// Add a new filter to the plan, will filter out the records that do not satisfy the filter
    pub fn filter(self, filter: TypedExpr) -> Result<Self, Error> {
        let typ = self.typ.clone();
        let plan = match self.plan {
            Plan::Mfp {
                input,
                mfp: old_mfp,
            } => Plan::Mfp {
                input,
                mfp: old_mfp.filter(vec![filter.expr])?,
            },
            _ => Plan::Mfp {
                input: Box::new(self),
                mfp: MapFilterProject::new(typ.column_types.len()).filter(vec![filter.expr])?,
            },
        };
        Ok(TypedPlan { typ, plan })
    }
}

/// TODO(discord9): support `TableFunc`（by define FlatMap that map 1 to n)
/// Plan describe how to transform data in dataflow
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Deserialize, Serialize)]
pub enum Plan {
    /// A constant collection of rows.
    Constant { rows: Vec<DiffRow> },
    /// Get CDC data from an source, be it external reference to an existing source or an internal
    /// reference to a `Let` identifier
    Get { id: Id },
    /// Create a temporary collection from given `value`, and make this bind only available
    /// in scope of `body`
    ///
    /// Similar to this rust code snippet:
    /// ```rust, ignore
    /// {
    ///    let id = value;
    ///     body
    /// }
    Let {
        id: LocalId,
        value: Box<TypedPlan>,
        body: Box<TypedPlan>,
    },
    /// Map, Filter, and Project operators. Chained together.
    Mfp {
        /// The input collection.
        input: Box<TypedPlan>,
        /// Linear operator to apply to each record.
        mfp: MapFilterProject,
    },
    /// Reduce operator, aggregation by key assembled from KeyValPlan
    Reduce {
        /// The input collection.
        input: Box<TypedPlan>,
        /// A plan for changing input records into key, value pairs.
        key_val_plan: KeyValPlan,
        /// A plan for performing the reduce.
        ///
        /// The implementation of reduction has several different strategies based
        /// on the properties of the reduction, and the input itself.
        reduce_plan: ReducePlan,
    },
    /// A multiway relational equijoin, with fused map, filter, and projection.
    ///
    /// This stage performs a multiway join among `inputs`, using the equality
    /// constraints expressed in `plan`. The plan also describes the implementation
    /// strategy we will use, and any pushed down per-record work.
    Join {
        /// An ordered list of inputs that will be joined.
        inputs: Vec<TypedPlan>,
        /// Detailed information about the implementation of the join.
        ///
        /// This includes information about the implementation strategy, but also
        /// any map, filter, project work that we might follow the join with, but
        /// potentially pushed down into the implementation of the join.
        plan: JoinPlan,
    },
    /// Adds the contents of the input collections.
    ///
    /// Importantly, this is *multiset* union, so the multiplicities of records will
    /// add. This is in contrast to *set* union, where the multiplicities would be
    /// capped at one. A set union can be formed with `Union` followed by `Reduce`
    /// implementing the "distinct" operator.
    Union {
        /// The input collections
        inputs: Vec<TypedPlan>,
        /// Whether to consolidate the output, e.g., cancel negated records.
        consolidate_output: bool,
    },
}

impl Plan {
    /// Find all the used collection in the plan
    pub fn find_used_collection(&self) -> BTreeSet<GlobalId> {
        fn recur_find_use(plan: &Plan, used: &mut BTreeSet<GlobalId>) {
            match plan {
                Plan::Get { id } => {
                    match id {
                        Id::Local(_) => (),
                        Id::Global(g) => {
                            used.insert(*g);
                        }
                    };
                }
                Plan::Let { value, body, .. } => {
                    recur_find_use(&value.plan, used);
                    recur_find_use(&body.plan, used);
                }
                Plan::Mfp { input, .. } => {
                    recur_find_use(&input.plan, used);
                }
                Plan::Reduce { input, .. } => {
                    recur_find_use(&input.plan, used);
                }
                Plan::Join { inputs, .. } => {
                    for input in inputs {
                        recur_find_use(&input.plan, used);
                    }
                }
                Plan::Union { inputs, .. } => {
                    for input in inputs {
                        recur_find_use(&input.plan, used);
                    }
                }
                _ => {}
            }
        }
        let mut ret = Default::default();
        recur_find_use(self, &mut ret);
        ret
    }
}

impl Plan {
    pub fn with_types(self, typ: RelationType) -> TypedPlan {
        TypedPlan { typ, plan: self }
    }
}
