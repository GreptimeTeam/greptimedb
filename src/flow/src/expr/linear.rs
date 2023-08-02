use std::collections::{BTreeMap, BTreeSet};

use datatypes::value::Value;
use serde::{Deserialize, Serialize};

use crate::expr::{Id, LocalId, ScalarExpr};
use crate::repr::{self, Diff, Row};
use crate::storage::errors::EvalError;

/// A compound operator that can be applied row-by-row.
///
/// This operator integrates the map, filter, and project operators.
/// It applies a sequences of map expressions, which are allowed to
/// refer to previous expressions, interleaved with predicates which
/// must be satisfied for an output to be produced. If all predicates
/// evaluate to `Datum::True` the data at the identified columns are
/// collected and produced as output in a packed `Row`.
///
/// This operator is a "builder" and its contents may contain expressions
/// that are not yet executable. For example, it may contain temporal
/// expressions in `self.expressions`, even though this is not something
/// we can directly evaluate. The plan creation methods will defensively
/// ensure that the right thing happens.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
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
    /// in the predicate's support, but it could be larger to implement
    /// guarded evaluation of predicates.
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

    /// Given two mfps, return an mfp that applies one
    /// followed by the other.
    /// Note that the arguments are in the opposite order
    /// from how function composition is usually written in mathematics.
    pub fn compose(before: Self, after: Self) -> Self {
        let (m, f, p) = after.into_map_filter_project();
        before.map(m).filter(f).project(p)
    }

    /// True if the operator describes the identity transformation.
    pub fn is_identity(&self) -> bool {
        self.expressions.is_empty()
            && self.predicates.is_empty()
            && self.projection.len() == self.input_arity
            && self.projection.iter().enumerate().all(|(i, p)| i == *p)
    }

    /// Retain only the indicated columns in the presented order.
    pub fn project<I>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = usize> + std::fmt::Debug,
    {
        self.projection = columns.into_iter().map(|c| self.projection[c]).collect();
        self
    }

    /// Retain only rows satisfying these predicates.
    ///
    /// This method introduces predicates as eagerly as they can be evaluated,
    /// which may not be desired for predicates that may cause exceptions.
    /// If fine manipulation is required, the predicates can be added manually.
    pub fn filter<I>(mut self, predicates: I) -> Self
    where
        I: IntoIterator<Item = ScalarExpr>,
    {
        for mut predicate in predicates {
            // Correct column references.
            predicate.permute(&self.projection[..]);

            // Validate column references.
            assert!(predicate
                .support()
                .into_iter()
                .all(|c| c < self.input_arity + self.expressions.len()));

            // Insert predicate as eagerly as it can be evaluated:
            // just after the largest column in its support is formed.
            let max_support = predicate
                .support()
                .into_iter()
                .max()
                .map(|c| c + 1)
                .unwrap_or(0);
            self.predicates.push((max_support, predicate))
        }
        // Stable sort predicates by position at which they take effect.
        // We put literal errors at the end as a stop-gap to avoid erroring
        // before we are able to evaluate any predicates that might prevent it.
        self.predicates
            .sort_by_key(|(position, predicate)| (predicate.is_literal_err(), *position));
        self
    }

    /// Append the result of evaluating expressions to each row.
    pub fn map<I>(mut self, expressions: I) -> Self
    where
        I: IntoIterator<Item = ScalarExpr>,
    {
        for mut expression in expressions {
            // Correct column references.
            expression.permute(&self.projection[..]);

            // Validate column references.
            assert!(expression
                .support()
                .into_iter()
                .all(|c| c < self.input_arity + self.expressions.len()));

            // Introduce expression and produce as output.
            self.expressions.push(expression);
            self.projection
                .push(self.input_arity + self.expressions.len() - 1);
        }

        self
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
    pub fn optimize(&mut self) {
        // TODO(discord9): optimize later
    }

    /// Convert the `MapFilterProject` into a staged evaluation plan.
    ///
    /// The main behavior is extract temporal predicates, which cannot be evaluated
    /// using the standard machinery.
    pub fn into_plan(self) -> Result<MfpPlan, String> {
        MfpPlan::create_from(self)
    }

    /// Lists input columns whose values are used in outputs.
    ///
    /// It is entirely appropriate to determine the demand of an instance
    /// and then both apply a projection to the subject of the instance and
    /// `self.permute` this instance.
    pub fn demand(&self) -> BTreeSet<usize> {
        let mut demanded = BTreeSet::new();
        for (_index, pred) in self.predicates.iter() {
            demanded.extend(pred.support());
        }
        demanded.extend(self.projection.iter().cloned());
        for index in (0..self.expressions.len()).rev() {
            if demanded.contains(&(self.input_arity + index)) {
                demanded.extend(self.expressions[index].support());
            }
        }
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
    pub fn permute(&mut self, mut shuffle: BTreeMap<usize, usize>, new_input_arity: usize) {
        let (mut map, mut filter, mut project) = self.as_map_filter_project();
        for index in 0..map.len() {
            // Intermediate columns are just shifted.
            shuffle.insert(self.input_arity + index, new_input_arity + index);
        }
        for expr in map.iter_mut() {
            expr.permute_map(&shuffle);
        }
        for pred in filter.iter_mut() {
            pred.permute_map(&shuffle);
        }
        for proj in project.iter_mut() {
            assert!(shuffle[proj] < new_input_arity + map.len());
            *proj = shuffle[proj];
        }
        *self = Self::new(new_input_arity)
            .map(map)
            .filter(filter)
            .project(project)
    }
}

/// A wrapper type which indicates it is safe to simply evaluate all expressions.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SafeMfpPlan {
    pub(crate) mfp: MapFilterProject,
}

impl SafeMfpPlan {
    pub fn permute(&mut self, map: BTreeMap<usize, usize>, new_arity: usize) {
        self.mfp.permute(map, new_arity);
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

    /// Populates `datums` with `self.expressions` and tests `self.predicates`.
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
    pub fn create_from(mut mfp: MapFilterProject) -> Result<Self, String> {
        Ok(Self {
            mfp: SafeMfpPlan { mfp },
            lower_bounds: Vec::new(),
            upper_bounds: Vec::new(),
        })
    }
    pub fn evaluate<E: From<EvalError>, V: Fn(&repr::Timestamp) -> bool>(
        &self,
        values: &mut Vec<Value>,
        time: repr::Timestamp,
        diff: Diff,
        valid_time: V,
    ) -> impl Iterator<Item = Result<(Row, repr::Timestamp, Diff), (E, repr::Timestamp, Diff)>>
    {
        match self.mfp.evaluate_inner(values) {
            Err(e) => {
                return Some(Err((e.into(), time, diff)))
                    .into_iter()
                    .chain(None.into_iter());
            }
            Ok(true) => {}
            Ok(false) => {
                return None.into_iter().chain(None.into_iter());
            }
        }
        // TODO(discord9): Temporal filter
        let ret = Row::pack(self.mfp.mfp.projection.iter().map(|c| values[*c].clone()));
        Some(Ok((ret, time, diff)))
            .into_iter()
            .chain(None.into_iter())
    }
    /// Indicates if the planned `MapFilterProject` emits exactly its inputs as outputs.
    pub fn is_identity(&self) -> bool {
        self.mfp.mfp.is_identity() && self.lower_bounds.is_empty() && self.upper_bounds.is_empty()
    }
}
