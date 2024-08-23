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

//! define MapFilterProject which is a compound operator that can be applied row-by-row.

use std::collections::{BTreeMap, BTreeSet};

use arrow::array::BooleanArray;
use arrow::buffer::BooleanBuffer;
use arrow::compute::FilterBuilder;
use common_telemetry::trace;
use datatypes::prelude::ConcreteDataType;
use datatypes::value::Value;
use datatypes::vectors::{BooleanVector, Helper};
use itertools::Itertools;
use snafu::{ensure, OptionExt, ResultExt};

use crate::error::{Error, InvalidQuerySnafu};
use crate::expr::error::{ArrowSnafu, DataTypeSnafu, EvalError, InternalSnafu, TypeMismatchSnafu};
use crate::expr::{Batch, InvalidArgumentSnafu, ScalarExpr};
use crate::repr::{self, value_to_internal_ts, Diff, Row};

/// A compound operator that can be applied row-by-row.
///
/// In practice, this operator is a sequence of map, filter, and project in arbitrary order,
/// which can and is stored by reordering the sequence's
/// apply order to a `map` first, `filter` second and `project` third order.
///
/// input is a row(a sequence of values), which is also being used for store intermediate results,
/// like `map` operator can append new columns to the row according to it's expressions,
/// `filter` operator decide whether this entire row can even be output by decide whether the row satisfy the predicates,
/// `project` operator decide which columns of the row should be output.
///
/// This operator integrates the map, filter, and project operators.
/// It applies a sequences of map expressions, which are allowed to
/// refer to previous expressions, interleaved with predicates which
/// must be satisfied for an output to be produced. If all predicates
/// evaluate to `Value::Boolean(True)` the data at the identified columns are
/// collected and produced as output in a packed `Row`.
///
/// This operator is a "builder" and its contents may contain expressions
/// that are not yet executable. For example, it may contain temporal
/// expressions in `self.expressions`, even though this is not something
/// we can directly evaluate. The plan creation methods will defensively
/// ensure that the right thing happens.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct MapFilterProject {
    /// A sequence of expressions that should be appended to the row.
    ///
    /// Many of these expressions may not be produced in the output,
    /// and may only be present as common subexpressions.
    pub expressions: Vec<ScalarExpr>,
    /// Expressions that must evaluate to `Datum::True` for the output
    /// row to be produced.
    ///
    /// Each entry is prepended with a column identifier indicating
    /// the column *before* which the predicate should first be applied.
    /// Most commonly this would be one plus the largest column identifier
    /// in the predicate's referred columns, but it could be larger to implement
    /// guarded evaluation of predicates.
    /// Put it in another word, the first element of the tuple means
    /// the predicates can't be evaluated until that number of columns is formed.
    ///
    /// This list should be sorted by the first field.
    pub predicates: Vec<(usize, ScalarExpr)>,
    /// A sequence of column identifiers whose data form the output row.
    pub projection: Vec<usize>,
    /// The expected number of input columns.
    ///
    /// This is needed to ensure correct identification of newly formed
    /// columns in the output.
    pub input_arity: usize,
}

impl MapFilterProject {
    /// Create a no-op operator for an input of a supplied arity.
    pub fn new(input_arity: usize) -> Self {
        Self {
            expressions: Vec::new(),
            predicates: Vec::new(),
            projection: (0..input_arity).collect(),
            input_arity,
        }
    }

    /// The number of columns expected in the output row.
    pub fn output_arity(&self) -> usize {
        self.projection.len()
    }

    /// Given two mfps, return an mfp that applies one
    /// followed by the other.
    /// Note that the arguments are in the opposite order
    /// from how function composition is usually written in mathematics.
    pub fn compose(before: Self, after: Self) -> Result<Self, Error> {
        let (m, f, p) = after.into_map_filter_project();
        before.map(m)?.filter(f)?.project(p)
    }

    /// True if the operator describes the identity transformation.
    pub fn is_identity(&self) -> bool {
        self.expressions.is_empty()
            && self.predicates.is_empty()
            // identity if projection is the identity permutation
            && self.projection.len() == self.input_arity
            && self.projection.iter().enumerate().all(|(i, p)| i == *p)
    }

    /// Retain only the indicated columns in the presented order.
    ///
    /// i.e. before: `self.projection = [1, 2, 0], columns = [1, 0]`
    /// ```mermaid
    /// flowchart TD
    /// col-0
    /// col-1
    /// col-2
    /// projection --> |0|col-1
    /// projection --> |1|col-2
    /// projection --> |2|col-0
    /// ```
    ///
    /// after: `self.projection = [2, 1]`
    /// ```mermaid
    /// flowchart TD
    /// col-0
    /// col-1
    /// col-2
    /// project("project:[1,2,0]")
    /// project
    /// project -->|0| col-1
    /// project -->|1| col-2
    /// project -->|2| col-0
    /// new_project("apply new project:[1,0]")
    /// new_project -->|0| col-2
    /// new_project -->|1| col-1
    /// ```
    pub fn project<I>(mut self, columns: I) -> Result<Self, Error>
    where
        I: IntoIterator<Item = usize> + std::fmt::Debug,
    {
        self.projection = columns
            .into_iter()
            .map(|c| self.projection.get(c).cloned().ok_or(c))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|c| {
                InvalidQuerySnafu {
                    reason: format!(
                        "column index {} out of range, expected at most {} columns",
                        c,
                        self.projection.len()
                    ),
                }
                .build()
            })?;
        Ok(self)
    }

    /// Retain only rows satisfying these predicates.
    ///
    /// This method introduces predicates as eagerly as they can be evaluated,
    /// which may not be desired for predicates that may cause exceptions.
    /// If fine manipulation is required, the predicates can be added manually.
    ///
    /// simply added to the end of the predicates list
    ///
    /// while paying attention to column references maintained by `self.projection`
    ///
    /// so `self.projection = [1, 2, 0], filter = [0]+[1]>0`:
    /// becomes:
    /// ```mermaid
    /// flowchart TD
    /// col-0
    /// col-1
    /// col-2
    /// project("first project:[1,2,0]")
    /// project
    /// project -->|0| col-1
    /// project -->|1| col-2
    /// project -->|2| col-0
    /// filter("then filter:[0]+[1]>0")
    /// filter -->|0| col-1
    /// filter --> |1| col-2
    /// ```
    pub fn filter<I>(mut self, predicates: I) -> Result<Self, Error>
    where
        I: IntoIterator<Item = ScalarExpr>,
    {
        for mut predicate in predicates {
            // Correct column references.
            predicate.permute(&self.projection[..])?;

            // Validate column references.
            let referred_columns = predicate.get_all_ref_columns();
            for c in referred_columns.iter() {
                // current row len include input columns and previous number of expressions
                let cur_row_len = self.input_arity + self.expressions.len();
                ensure!(
                    *c < cur_row_len,
                    InvalidQuerySnafu {
                        reason: format!(
                            "column index {} out of range, expected at most {} columns",
                            c, cur_row_len
                        )
                    }
                );
            }

            // Insert predicate as eagerly as it can be evaluated:
            // just after the largest column in its support is formed.
            let max_support = referred_columns
                .into_iter()
                .max()
                .map(|c| c + 1)
                .unwrap_or(0);
            self.predicates.push((max_support, predicate))
        }
        // Stable sort predicates by position at which they take effect.
        self.predicates
            .sort_by_key(|(position, _predicate)| *position);
        Ok(self)
    }

    /// Append the result of evaluating expressions to each row.
    ///
    /// simply append `expressions` to `self.expressions`
    ///
    /// while paying attention to column references maintained by `self.projection`
    ///
    /// hence, before apply map with a previously non-trivial projection would be like:
    /// before:
    /// ```mermaid
    /// flowchart TD
    /// col-0
    /// col-1
    /// col-2
    /// projection --> |0|col-1
    /// projection --> |1|col-2
    /// projection --> |2|col-0
    /// ```
    /// after apply map:
    /// ```mermaid
    /// flowchart TD
    /// col-0
    /// col-1
    /// col-2
    /// project("project:[1,2,0]")
    /// project
    /// project -->|0| col-1
    /// project -->|1| col-2
    /// project -->|2| col-0
    /// map("map:[0]/[1]/[2]")
    /// map -->|0|col-1
    /// map -->|1|col-2
    /// map -->|2|col-0
    /// ```
    pub fn map<I>(mut self, expressions: I) -> Result<Self, Error>
    where
        I: IntoIterator<Item = ScalarExpr>,
    {
        for mut expression in expressions {
            // Correct column references.
            expression.permute(&self.projection[..])?;

            // Validate column references.
            for c in expression.get_all_ref_columns().into_iter() {
                // current row len include input columns and previous number of expressions
                let current_row_len = self.input_arity + self.expressions.len();
                ensure!(
                    c < current_row_len,
                    InvalidQuerySnafu {
                        reason: format!(
                            "column index {} out of range, expected at most {} columns",
                            c, current_row_len
                        )
                    }
                );
            }

            // Introduce expression and produce as output.
            self.expressions.push(expression);
            // Expression by default is projected to output.
            let cur_expr_col_num = self.input_arity + self.expressions.len() - 1;
            self.projection.push(cur_expr_col_num);
        }

        Ok(self)
    }

    /// Like [`MapFilterProject::as_map_filter_project`], but consumes `self` rather than cloning.
    pub fn into_map_filter_project(self) -> (Vec<ScalarExpr>, Vec<ScalarExpr>, Vec<usize>) {
        let predicates = self
            .predicates
            .into_iter()
            .map(|(_pos, predicate)| predicate)
            .collect();
        (self.expressions, predicates, self.projection)
    }

    /// As the arguments to `Map`, `Filter`, and `Project` operators.
    ///
    /// In principle, this operator can be implemented as a sequence of
    /// more elemental operators, likely less efficiently.
    pub fn as_map_filter_project(&self) -> (Vec<ScalarExpr>, Vec<ScalarExpr>, Vec<usize>) {
        self.clone().into_map_filter_project()
    }
}

impl MapFilterProject {
    /// Convert the `MapFilterProject` into a safe evaluation plan. Marking it safe to evaluate.
    pub fn into_safe(self) -> SafeMfpPlan {
        SafeMfpPlan { mfp: self }
    }

    /// Optimize the `MapFilterProject` in place.
    pub fn optimize(&mut self) {
        // TODO(discord9): optimize
    }
    /// get the mapping of old columns to new columns after the mfp
    pub fn get_old_to_new_mapping(&self) -> BTreeMap<usize, usize> {
        BTreeMap::from_iter(
            self.projection
                .clone()
                .into_iter()
                .enumerate()
                .map(|(new, old)| {
                    // `projection` give the new -> old mapping
                    let mut old = old;
                    // trace back to the original column
                    // since there maybe indirect ref to old columns like
                    // col 2 <- expr=col(2) at pos col 4 <- expr=col(4) at pos col 6
                    // ideally such indirect ref should be optimize away
                    // TODO(discord9): refactor this after impl `optimize()`
                    while let Some(ScalarExpr::Column(prev)) = if old >= self.input_arity {
                        // get the correspond expr if not a original column
                        self.expressions.get(old - self.input_arity)
                    } else {
                        // we don't care about non column ref case since only need old to new column mapping
                        // in which case, the old->new mapping remain the same
                        None
                    } {
                        old = *prev;
                        if old < self.input_arity {
                            break;
                        }
                    }
                    (old, new)
                }),
        )
    }

    /// Convert the `MapFilterProject` into a staged evaluation plan.
    ///
    /// The main behavior is extract temporal predicates, which cannot be evaluated
    /// using the standard machinery.
    pub fn into_plan(self) -> Result<MfpPlan, Error> {
        MfpPlan::create_from(self)
    }

    /// Lists input columns whose values are used in outputs.
    ///
    /// It is entirely appropriate to determine the demand of an instance
    /// and then both apply a projection to the subject of the instance and
    /// `self.permute` this instance.
    pub fn demand(&self) -> BTreeSet<usize> {
        let mut demanded = BTreeSet::new();
        // first, get all columns referenced by predicates
        for (_index, pred) in self.predicates.iter() {
            demanded.extend(pred.get_all_ref_columns());
        }
        // then, get columns referenced by projection which is direct output
        demanded.extend(self.projection.iter().cloned());

        // check every expressions, if a expression is contained in demanded, then all columns it referenced should be added to demanded
        for index in (0..self.expressions.len()).rev() {
            if demanded.contains(&(self.input_arity + index)) {
                demanded.extend(self.expressions[index].get_all_ref_columns());
            }
        }

        // only keep demanded columns that are in input
        demanded.retain(|col| col < &self.input_arity);
        demanded
    }

    /// Update input column references, due to an input projection or permutation.
    ///
    /// The `shuffle` argument remaps expected column identifiers to new locations,
    /// with the expectation that `shuffle` describes all input columns, and so the
    /// intermediate results will be able to start at position `shuffle.len()`.
    ///
    /// The supplied `shuffle` may not list columns that are not "demanded" by the
    /// instance, and so we should ensure that `self` is optimized to not reference
    /// columns that are not demanded.
    pub fn permute(
        &mut self,
        mut shuffle: BTreeMap<usize, usize>,
        new_input_arity: usize,
    ) -> Result<(), Error> {
        // check shuffle is valid
        let demand = self.demand();
        for d in demand {
            ensure!(
                shuffle.contains_key(&d),
                InvalidQuerySnafu {
                    reason: format!(
                        "Demanded column {} is not in shuffle's keys: {:?}",
                        d,
                        shuffle.keys()
                    )
                }
            );
        }
        ensure!(
            shuffle.len() <= new_input_arity,
            InvalidQuerySnafu {
                reason: format!(
                    "shuffle's length {} is greater than new_input_arity {}",
                    shuffle.len(),
                    self.input_arity
                )
            }
        );

        // decompose self into map, filter, project for ease of manipulation
        let (mut map, mut filter, mut project) = self.as_map_filter_project();
        for index in 0..map.len() {
            // Intermediate columns are just shifted.
            shuffle.insert(self.input_arity + index, new_input_arity + index);
        }

        for expr in map.iter_mut() {
            expr.permute_map(&shuffle)?;
        }
        for pred in filter.iter_mut() {
            pred.permute_map(&shuffle)?;
        }
        let new_row_len = new_input_arity + map.len();
        for proj in project.iter_mut() {
            ensure!(
                shuffle[proj] < new_row_len,
                InvalidQuerySnafu {
                    reason: format!(
                        "shuffled column index {} out of range, expected at most {} columns",
                        shuffle[proj], new_row_len
                    )
                }
            );
            *proj = shuffle[proj];
        }
        *self = Self::new(new_input_arity)
            .map(map)?
            .filter(filter)?
            .project(project)?;
        Ok(())
    }
}

/// A wrapper type which indicates it is safe to simply evaluate all expressions.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct SafeMfpPlan {
    /// the inner `MapFilterProject` that is safe to evaluate.
    pub(crate) mfp: MapFilterProject,
}

impl SafeMfpPlan {
    /// See [`MapFilterProject::permute`].
    pub fn permute(&mut self, map: BTreeMap<usize, usize>, new_arity: usize) -> Result<(), Error> {
        self.mfp.permute(map, new_arity)
    }

    /// similar to [`MapFilterProject::evaluate_into`], just in batch, and rows that don't pass the predicates are not included in the output.
    ///
    /// so it's not guaranteed that the output will have the same number of rows as the input.
    pub fn eval_batch_into(&self, batch: &mut Batch) -> Result<Batch, EvalError> {
        ensure!(
            batch.column_count() == self.mfp.input_arity,
            InvalidArgumentSnafu {
                reason: format!(
                    "batch column length {} is not equal to input_arity {}",
                    batch.column_count(),
                    self.mfp.input_arity
                ),
            }
        );

        let passed_predicates = self.eval_batch_inner(batch)?;
        let filter = FilterBuilder::new(passed_predicates.as_boolean_array());
        let pred = filter.build();
        let mut result = vec![];
        for col in batch.batch() {
            let filtered = pred
                .filter(col.to_arrow_array().as_ref())
                .with_context(|_| ArrowSnafu {
                    context: format!("failed to filter column for mfp operator {:?}", self),
                })?;
            result.push(Helper::try_into_vector(filtered).context(DataTypeSnafu {
                msg: "Failed to convert arrow array to vector",
            })?);
        }
        let projected = self
            .mfp
            .projection
            .iter()
            .map(|c| result[*c].clone())
            .collect_vec();
        let row_count = pred.count();

        Batch::try_new(projected, row_count)
    }

    /// similar to [`MapFilterProject::evaluate_into`], just in batch.
    pub fn eval_batch_inner(&self, batch: &mut Batch) -> Result<BooleanVector, EvalError> {
        // mark the columns that have been evaluated and appended to the `batch`
        let mut expression = 0;
        // preds default to true and will be updated as we evaluate each predicate
        let buf = BooleanBuffer::new_set(batch.row_count());
        let arr = BooleanArray::new(buf, None);
        let mut all_preds = BooleanVector::from(arr);

        // to compute predicate, need to first compute all expressions used in predicates
        for (support, predicate) in self.mfp.predicates.iter() {
            while self.mfp.input_arity + expression < *support {
                let expr_eval = self.mfp.expressions[expression].eval_batch(batch)?;
                batch.batch_mut().push(expr_eval);
                expression += 1;
            }
            let pred_vec = predicate.eval_batch(batch)?;
            let pred_arr = pred_vec.to_arrow_array();
            let pred_arr = pred_arr.as_any().downcast_ref::<BooleanArray>().context({
                TypeMismatchSnafu {
                    expected: ConcreteDataType::boolean_datatype(),
                    actual: pred_vec.data_type(),
                }
            })?;
            let all_arr = all_preds.as_boolean_array();
            let res_arr = arrow::compute::and(all_arr, pred_arr).context(ArrowSnafu {
                context: format!("failed to compute predicate for mfp operator {:?}", self),
            })?;
            all_preds = BooleanVector::from(res_arr);
        }

        // while evaluated expressions are less than total expressions, keep evaluating
        while expression < self.mfp.expressions.len() {
            let expr_eval = self.mfp.expressions[expression].eval_batch(batch)?;
            batch.batch_mut().push(expr_eval);
            expression += 1;
        }

        Ok(all_preds)
    }

    /// Evaluates the linear operator on a supplied list of datums.
    ///
    /// The arguments are the initial datums associated with the row,
    /// and an appropriately lifetimed arena for temporary allocations
    /// needed by scalar evaluation.
    ///
    /// An `Ok` result will either be `None` if any predicate did not
    /// evaluate to `Value::Boolean(true)`, or the values of the columns listed
    /// by `self.projection` if all predicates passed. If an error
    /// occurs in the evaluation it is returned as an `Err` variant.
    /// As the evaluation exits early with failed predicates, it may
    /// miss some errors that would occur later in evaluation.
    ///
    /// The `row` is not cleared first, but emptied if the function
    /// returns `Ok(Some(row)).
    #[inline(always)]
    pub fn evaluate_into(
        &self,
        values: &mut Vec<Value>,
        row_buf: &mut Row,
    ) -> Result<Option<Row>, EvalError> {
        ensure!(
            values.len() == self.mfp.input_arity,
            InvalidArgumentSnafu {
                reason: format!(
                    "values length {} is not equal to input_arity {}",
                    values.len(),
                    self.mfp.input_arity
                ),
            }
        );
        let passed_predicates = self.evaluate_inner(values)?;

        if !passed_predicates {
            Ok(None)
        } else {
            row_buf.clear();
            row_buf.extend(self.mfp.projection.iter().map(|c| values[*c].clone()));
            Ok(Some(row_buf.clone()))
        }
    }

    /// A version of `evaluate` which produces an iterator over `Datum`
    /// as output.
    ///
    /// This version can be useful when one wants to capture the resulting
    /// datums without packing and then unpacking a row.
    #[inline(always)]
    pub fn evaluate_iter<'a>(
        &'a self,
        datums: &'a mut Vec<Value>,
    ) -> Result<Option<impl Iterator<Item = Value> + 'a>, EvalError> {
        let passed_predicates = self.evaluate_inner(datums)?;
        if !passed_predicates {
            Ok(None)
        } else {
            Ok(Some(
                self.mfp.projection.iter().map(move |i| datums[*i].clone()),
            ))
        }
    }

    /// Populates `values` with `self.expressions` and tests `self.predicates`.
    ///
    /// This does not apply `self.projection`, which is up to the calling method.
    pub fn evaluate_inner(&self, values: &mut Vec<Value>) -> Result<bool, EvalError> {
        let mut expression = 0;
        for (support, predicate) in self.mfp.predicates.iter() {
            while self.mfp.input_arity + expression < *support {
                values.push(self.mfp.expressions[expression].eval(&values[..])?);
                expression += 1;
            }
            if predicate.eval(&values[..])? != Value::Boolean(true) {
                return Ok(false);
            }
        }
        // while evaluated expressions are less than total expressions, keep evaluating
        while expression < self.mfp.expressions.len() {
            values.push(self.mfp.expressions[expression].eval(&values[..])?);
            expression += 1;
        }
        Ok(true)
    }
}

impl std::ops::Deref for SafeMfpPlan {
    type Target = MapFilterProject;
    fn deref(&self) -> &Self::Target {
        &self.mfp
    }
}

/// Predicates partitioned into temporal and non-temporal.
///
/// Temporal predicates require some recognition to determine their
/// structure, and it is best to do that once and re-use the results.
///
/// There are restrictions on the temporal predicates we currently support.
/// They must directly constrain `MzNow` from below or above,
/// by expressions that do not themselves contain `MzNow`.
/// Conjunctions of such constraints are also ok.
#[derive(Clone, Debug, PartialEq)]
pub struct MfpPlan {
    /// Normal predicates to evaluate on `&[Datum]` and expect `Ok(Datum::True)`.
    pub(crate) mfp: SafeMfpPlan,
    /// TODO(discord9): impl temporal filter later
    /// Expressions that when evaluated lower-bound `MzNow`.
    pub(crate) lower_bounds: Vec<ScalarExpr>,
    /// Expressions that when evaluated upper-bound `MzNow`.
    pub(crate) upper_bounds: Vec<ScalarExpr>,
}

impl MfpPlan {
    /// Indicates if the `MfpPlan` contains temporal predicates. That is have outputs that may occur in future.
    pub fn is_temporal(&self) -> bool {
        !self.lower_bounds.is_empty() || !self.upper_bounds.is_empty()
    }
    /// find `now` in `predicates` and put them into lower/upper temporal bounds for temporal filter to use
    pub fn create_from(mut mfp: MapFilterProject) -> Result<Self, Error> {
        let mut lower_bounds = Vec::new();
        let mut upper_bounds = Vec::new();

        let mut temporal = Vec::new();

        // Optimize, to ensure that temporal predicates are move in to `mfp.predicates`.
        mfp.optimize();

        mfp.predicates.retain(|(_position, predicate)| {
            if predicate.contains_temporal() {
                temporal.push(predicate.clone());
                false
            } else {
                true
            }
        });

        for predicate in temporal {
            let (lower, upper) = predicate.extract_bound()?;
            lower_bounds.extend(lower);
            upper_bounds.extend(upper);
        }
        Ok(Self {
            mfp: SafeMfpPlan { mfp },
            lower_bounds,
            upper_bounds,
        })
    }

    /// Indicates if the planned `MapFilterProject` emits exactly its inputs as outputs.
    pub fn is_identity(&self) -> bool {
        self.mfp.mfp.is_identity() && self.lower_bounds.is_empty() && self.upper_bounds.is_empty()
    }

    /// if `lower_bound <= sys_time < upper_bound`, return `[(data, sys_time, +1), (data, min_upper_bound, -1)]`
    ///
    /// else if `sys_time < lower_bound`, return `[(data, lower_bound, +1), (data, min_upper_bound, -1)]`
    ///
    /// else if `sys_time >= upper_bound`, return `[None, None]`
    ///
    /// if eval error appeal in any of those process, corresponding result will be `Err`
    pub fn evaluate<E: From<EvalError>>(
        &self,
        values: &mut Vec<Value>,
        sys_time: repr::Timestamp,
        diff: Diff,
    ) -> impl Iterator<Item = Result<(Row, repr::Timestamp, Diff), (E, repr::Timestamp, Diff)>>
    {
        match self.mfp.evaluate_inner(values) {
            Err(e) => {
                return Some(Err((e.into(), sys_time, diff)))
                    .into_iter()
                    .chain(None);
            }
            Ok(true) => {}
            Ok(false) => {
                return None.into_iter().chain(None);
            }
        }

        let mut lower_bound = sys_time;
        let mut upper_bound = None;

        // Track whether we have seen a null in either bound, as this should
        // prevent the record from being produced at any time.
        let mut null_eval = false;
        let ret_err = |e: EvalError| {
            Some(Err((e.into(), sys_time, diff)))
                .into_iter()
                .chain(None)
        };
        for l in self.lower_bounds.iter() {
            match l.eval(values) {
                Ok(v) => {
                    if v.is_null() {
                        null_eval = true;
                        continue;
                    }
                    match value_to_internal_ts(v) {
                        Ok(ts) => lower_bound = lower_bound.max(ts),
                        Err(e) => return ret_err(e),
                    }
                }
                Err(e) => return ret_err(e),
            };
        }

        for u in self.upper_bounds.iter() {
            if upper_bound != Some(lower_bound) {
                match u.eval(values) {
                    Err(e) => return ret_err(e),
                    Ok(val) => {
                        if val.is_null() {
                            null_eval = true;
                            continue;
                        }
                        let ts = match value_to_internal_ts(val) {
                            Ok(ts) => ts,
                            Err(e) => return ret_err(e),
                        };
                        if let Some(upper) = upper_bound {
                            upper_bound = Some(upper.min(ts));
                        } else {
                            upper_bound = Some(ts);
                        }
                        // Force the upper bound to be at least the lower
                        // bound.
                        if upper_bound.is_some() && upper_bound < Some(lower_bound) {
                            upper_bound = Some(lower_bound);
                        }
                    }
                }
            }
        }

        if Some(lower_bound) != upper_bound && !null_eval {
            if self.mfp.mfp.projection.iter().any(|c| values.len() <= *c) {
                trace!("values={:?}, mfp={:?}", &values, &self.mfp.mfp);
                let err = InternalSnafu {
                    reason: format!(
                        "Index out of bound for mfp={:?} and values={:?}",
                        &self.mfp.mfp, &values
                    ),
                }
                .build();
                return ret_err(err);
            }
            // safety: already checked that `projection` is not out of bound
            let res_row = Row::pack(self.mfp.mfp.projection.iter().map(|c| values[*c].clone()));
            let upper_opt =
                upper_bound.map(|upper_bound| Ok((res_row.clone(), upper_bound, -diff)));
            // if diff==-1, the `upper_opt` will cancel the future `-1` inserted before by previous diff==1 row
            let lower = Some(Ok((res_row, lower_bound, diff)));

            lower.into_iter().chain(upper_opt)
        } else {
            None.into_iter().chain(None)
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use datatypes::data_type::ConcreteDataType;
    use datatypes::vectors::{Int32Vector, Int64Vector};
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::expr::{BinaryFunc, UnaryFunc, UnmaterializableFunc};

    #[test]
    fn test_mfp_with_time() {
        use crate::expr::func::BinaryFunc;
        let lte_now = ScalarExpr::Column(0).call_binary(
            ScalarExpr::CallUnmaterializable(UnmaterializableFunc::Now),
            BinaryFunc::Lte,
        );
        assert!(lte_now.contains_temporal());

        let gt_now_minus_two = ScalarExpr::Column(0)
            .call_binary(
                ScalarExpr::Literal(Value::from(2i64), ConcreteDataType::int64_datatype()),
                BinaryFunc::AddInt64,
            )
            .call_binary(
                ScalarExpr::CallUnmaterializable(UnmaterializableFunc::Now),
                BinaryFunc::Gt,
            );
        assert!(gt_now_minus_two.contains_temporal());

        let mfp = MapFilterProject::new(3)
            .filter(vec![
                // col(0) <= now()
                lte_now,
                // col(0) + 2 > now()
                gt_now_minus_two,
            ])
            .unwrap()
            .project(vec![0])
            .unwrap();

        let mfp = MfpPlan::create_from(mfp).unwrap();
        let expected = vec![
            (
                0,
                vec![
                    (Row::new(vec![Value::from(4i64)]), 4, 1),
                    (Row::new(vec![Value::from(4i64)]), 6, -1),
                ],
            ),
            (
                5,
                vec![
                    (Row::new(vec![Value::from(4i64)]), 5, 1),
                    (Row::new(vec![Value::from(4i64)]), 6, -1),
                ],
            ),
            (10, vec![]),
        ];
        for (sys_time, expected) in expected {
            let mut values = vec![Value::from(4i64), Value::from(2i64), Value::from(3i64)];
            let ret = mfp
                .evaluate::<EvalError>(&mut values, sys_time, 1)
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(ret, expected);
        }
    }

    #[test]
    fn test_mfp() {
        use crate::expr::func::BinaryFunc;
        let mfp = MapFilterProject::new(3)
            .map(vec![
                ScalarExpr::Column(0).call_binary(ScalarExpr::Column(1), BinaryFunc::Lt),
                ScalarExpr::Column(1).call_binary(ScalarExpr::Column(2), BinaryFunc::Lt),
            ])
            .unwrap()
            .project(vec![3, 4])
            .unwrap();
        assert!(!mfp.is_identity());
        let mfp = MapFilterProject::compose(mfp, MapFilterProject::new(2)).unwrap();
        {
            let mfp_0 = mfp.as_map_filter_project();
            let same = MapFilterProject::new(3)
                .map(mfp_0.0)
                .unwrap()
                .filter(mfp_0.1)
                .unwrap()
                .project(mfp_0.2)
                .unwrap();
            assert_eq!(mfp, same);
        }
        assert_eq!(mfp.demand().len(), 3);
        let mut mfp = mfp;
        mfp.permute(BTreeMap::from([(0, 2), (2, 0), (1, 1)]), 3)
            .unwrap();
        assert_eq!(
            mfp,
            MapFilterProject::new(3)
                .map(vec![
                    ScalarExpr::Column(2).call_binary(ScalarExpr::Column(1), BinaryFunc::Lt),
                    ScalarExpr::Column(1).call_binary(ScalarExpr::Column(0), BinaryFunc::Lt),
                ])
                .unwrap()
                .project(vec![3, 4])
                .unwrap()
        );
        let safe_mfp = SafeMfpPlan { mfp };
        let mut values = vec![Value::from(4), Value::from(2), Value::from(3)];
        let ret = safe_mfp
            .evaluate_into(&mut values, &mut Row::empty())
            .unwrap()
            .unwrap();
        assert_eq!(ret, Row::pack(vec![Value::from(false), Value::from(true)]));

        // batch mode
        let mut batch = Batch::try_from_rows(vec![Row::from(vec![
            Value::from(4),
            Value::from(2),
            Value::from(3),
        ])])
        .unwrap();
        let ret = safe_mfp.eval_batch_into(&mut batch).unwrap();

        assert_eq!(
            ret,
            Batch::try_from_rows(vec![Row::from(vec![Value::from(false), Value::from(true)])])
                .unwrap()
        );
    }

    #[test]
    fn manipulation_mfp() {
        // give a input of 4 columns
        let mfp = MapFilterProject::new(4);
        // append a expression to the mfp'input row that get the sum of the first 3 columns
        let mfp = mfp
            .map(vec![ScalarExpr::Column(0)
                .call_binary(ScalarExpr::Column(1), BinaryFunc::AddInt32)
                .call_binary(ScalarExpr::Column(2), BinaryFunc::AddInt32)])
            .unwrap();
        // only retain sum result
        let mfp = mfp.project(vec![4]).unwrap();
        // accept only if the sum is greater than 10
        let mfp = mfp
            .filter(vec![ScalarExpr::Column(0).call_binary(
                ScalarExpr::Literal(Value::from(10i32), ConcreteDataType::int32_datatype()),
                BinaryFunc::Gt,
            )])
            .unwrap();
        let input1 = vec![
            Value::from(4),
            Value::from(2),
            Value::from(3),
            Value::from("abc"),
        ];
        let safe_mfp = SafeMfpPlan { mfp };
        let ret = safe_mfp
            .evaluate_into(&mut input1.clone(), &mut Row::empty())
            .unwrap();
        assert_eq!(ret, None);

        let mut input1_batch = Batch::try_from_rows(vec![Row::new(input1)]).unwrap();
        let ret_batch = safe_mfp.eval_batch_into(&mut input1_batch).unwrap();
        assert_eq!(
            ret_batch,
            Batch::try_new(vec![Arc::new(Int32Vector::from_vec(vec![]))], 0).unwrap()
        );

        let input2 = vec![
            Value::from(5),
            Value::from(2),
            Value::from(4),
            Value::from("abc"),
        ];
        let ret = safe_mfp
            .evaluate_into(&mut input2.clone(), &mut Row::empty())
            .unwrap();
        assert_eq!(ret, Some(Row::pack(vec![Value::from(11)])));

        let mut input2_batch = Batch::try_from_rows(vec![Row::new(input2)]).unwrap();
        let ret_batch = safe_mfp.eval_batch_into(&mut input2_batch).unwrap();
        assert_eq!(
            ret_batch,
            Batch::try_new(vec![Arc::new(Int32Vector::from_vec(vec![11]))], 1).unwrap()
        );
    }

    #[test]
    fn test_permute() {
        let mfp = MapFilterProject::new(3)
            .map(vec![
                ScalarExpr::Column(0).call_binary(ScalarExpr::Column(1), BinaryFunc::Lt)
            ])
            .unwrap()
            .filter(vec![
                ScalarExpr::Column(0).call_binary(ScalarExpr::Column(1), BinaryFunc::Gt)
            ])
            .unwrap()
            .project(vec![0, 1])
            .unwrap();
        assert_eq!(mfp.demand(), BTreeSet::from([0, 1]));
        let mut less = mfp.clone();
        less.permute(BTreeMap::from([(1, 0), (0, 1)]), 2).unwrap();

        let mut more = mfp.clone();
        more.permute(BTreeMap::from([(0, 1), (1, 2), (2, 0)]), 4)
            .unwrap();
    }

    #[test]
    fn mfp_test_cast_and_filter() {
        let mfp = MapFilterProject::new(3)
            .map(vec![ScalarExpr::Column(0).call_unary(UnaryFunc::Cast(
                ConcreteDataType::int32_datatype(),
            ))])
            .unwrap()
            .filter(vec![
                ScalarExpr::Column(3).call_binary(ScalarExpr::Column(1), BinaryFunc::Gt)
            ])
            .unwrap()
            .project([0, 1, 2])
            .unwrap();
        let input1 = vec![
            Value::from(4i64),
            Value::from(2),
            Value::from(3),
            Value::from(53),
        ];
        let safe_mfp = SafeMfpPlan { mfp };
        let ret = safe_mfp.evaluate_into(&mut input1.clone(), &mut Row::empty());
        assert!(matches!(ret, Err(EvalError::InvalidArgument { .. })));

        let mut input1_batch = Batch::try_from_rows(vec![Row::new(input1)]).unwrap();
        let ret_batch = safe_mfp.eval_batch_into(&mut input1_batch);
        assert!(matches!(ret_batch, Err(EvalError::InvalidArgument { .. })));

        let input2 = vec![Value::from(4i64), Value::from(2), Value::from(3)];
        let ret = safe_mfp
            .evaluate_into(&mut input2.clone(), &mut Row::empty())
            .unwrap();
        assert_eq!(ret, Some(Row::new(input2.clone())));

        let input2_batch = Batch::try_from_rows(vec![Row::new(input2)]).unwrap();
        let ret_batch = safe_mfp.eval_batch_into(&mut input2_batch.clone()).unwrap();
        assert_eq!(ret_batch, input2_batch);

        let input3 = vec![Value::from(4i64), Value::from(5), Value::from(2)];
        let ret = safe_mfp
            .evaluate_into(&mut input3.clone(), &mut Row::empty())
            .unwrap();
        assert_eq!(ret, None);

        let input3_batch = Batch::try_from_rows(vec![Row::new(input3)]).unwrap();
        let ret_batch = safe_mfp.eval_batch_into(&mut input3_batch.clone()).unwrap();
        assert_eq!(
            ret_batch,
            Batch::try_new(
                vec![
                    Arc::new(Int64Vector::from_vec(Default::default())),
                    Arc::new(Int32Vector::from_vec(Default::default())),
                    Arc::new(Int32Vector::from_vec(Default::default()))
                ],
                0
            )
            .unwrap()
        );
    }

    #[test]
    fn test_mfp_out_of_order() {
        let mfp = MapFilterProject::new(3)
            .project(vec![2, 1, 0])
            .unwrap()
            .filter(vec![
                ScalarExpr::Column(0).call_binary(ScalarExpr::Column(1), BinaryFunc::Gt)
            ])
            .unwrap()
            .map(vec![
                ScalarExpr::Column(0).call_binary(ScalarExpr::Column(1), BinaryFunc::Lt)
            ])
            .unwrap()
            .project(vec![3])
            .unwrap();
        let input1 = vec![Value::from(2), Value::from(3), Value::from(4)];
        let safe_mfp = SafeMfpPlan { mfp };
        let ret = safe_mfp.evaluate_into(&mut input1.clone(), &mut Row::empty());
        assert_eq!(ret.unwrap(), Some(Row::new(vec![Value::from(false)])));

        let mut input1_batch = Batch::try_from_rows(vec![Row::new(input1)]).unwrap();
        let ret_batch = safe_mfp.eval_batch_into(&mut input1_batch).unwrap();

        assert_eq!(
            ret_batch,
            Batch::try_new(vec![Arc::new(BooleanVector::from(vec![false]))], 1).unwrap()
        );
    }
    #[test]
    fn test_mfp_chore() {
        // project keeps permute columns until it becomes the identity permutation
        let mfp = MapFilterProject::new(3)
            .project([1, 2, 0])
            .unwrap()
            .project([1, 2, 0])
            .unwrap()
            .project([1, 2, 0])
            .unwrap();
        assert_eq!(mfp, MapFilterProject::new(3));
    }
}
