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

//! Predicate recognition, execution, and precompiled regular expressions.

use std::fmt;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use datafusion::logical_expr::{Expr, Operator};
use datafusion_common::ScalarValue;
use datafusion_common::arrow::array::{ArrayRef, Datum, Scalar};
use datafusion_common::arrow::buffer::BooleanBuffer;
use datafusion_common::arrow::compute::kernels::cmp;
use datafusion_common::cast::as_string_array;
use datatypes::arrow::array::{
    Array, ArrayAccessor, ArrayData, BooleanArray, BooleanBufferBuilder, DictionaryArray,
    StringArrayType,
};
use datatypes::arrow::datatypes::{DataType, UInt32Type};
use datatypes::arrow::error::ArrowError;
use datatypes::compute::or_kleene;
use regex_automata::Input;
use regex_automata::meta;
use snafu::ResultExt;

use crate::error::{ArrowComputeSnafu, Result, UnsupportedOperationSnafu};

#[derive(Debug, Clone)]
pub(crate) enum SimplePredicate {
    Comparison {
        literal: Scalar<ArrayRef>,
        op: Operator,
        regex: Option<Arc<CompiledRegex>>,
        regex_negative: bool,
    },
    OrEqChain {
        literals: Vec<Scalar<ArrayRef>>,
    },
    IsNull {
        negated: bool,
    },
    Constant(Option<bool>),
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
}

impl SimplePredicate {
    pub(crate) fn try_new(expr: &Expr, column_type: Option<&DataType>) -> Option<Self> {
        match expr {
            Expr::BinaryExpr(binary) if matches!(binary.op, Operator::And | Operator::Or) => {
                let left = Self::try_new(&binary.left, column_type)?;
                let right = Self::try_new(&binary.right, column_type)?;
                if binary.op == Operator::And {
                    return Some(Self::And(Box::new(left), Box::new(right)));
                }
                // Preserve the encoded-key IN-list fast path for pure equality disjunctions.
                if let (Some(mut lhs), Some(rhs)) =
                    (left.equality_literals(), right.equality_literals())
                {
                    lhs.extend(rhs);
                    return Some(Self::OrEqChain { literals: lhs });
                }
                Some(Self::Or(Box::new(left), Box::new(right)))
            }
            Expr::BinaryExpr(binary) => {
                let mut op = binary.op;
                if !matches!(
                    op,
                    Operator::Eq
                        | Operator::NotEq
                        | Operator::Lt
                        | Operator::LtEq
                        | Operator::Gt
                        | Operator::GtEq
                        | Operator::RegexMatch
                        | Operator::RegexIMatch
                        | Operator::RegexNotMatch
                        | Operator::RegexNotIMatch
                ) {
                    return None;
                }
                let literal = if let Expr::Literal(literal, _) = &*binary.right {
                    Self::check_column(&binary.left, column_type)?;
                    literal
                } else if let Expr::Literal(literal, _) = &*binary.left {
                    Self::check_column(&binary.right, column_type)?;
                    op = op.swap()?;
                    literal
                } else {
                    return None;
                };
                let (regex, regex_negative) = Self::maybe_build_regex(op, literal).ok()?;
                Some(Self::Comparison {
                    literal: datafusion_common::utils::normalize_float_zero_scalar(literal.clone())
                        .to_scalar()
                        .ok()?,
                    op,
                    regex,
                    regex_negative,
                })
            }
            Expr::IsNull(column) | Expr::IsNotNull(column) => {
                Self::check_column(column, column_type)?;
                Some(Self::IsNull {
                    negated: matches!(expr, Expr::IsNotNull(_)),
                })
            }
            Expr::Literal(ScalarValue::Boolean(value), _) => Some(Self::Constant(*value)),
            _ => None,
        }
    }

    fn check_column(expr: &Expr, column_type: Option<&DataType>) -> Option<()> {
        match expr {
            Expr::Column(_) => Some(()),
            Expr::Cast(cast) if cast.field.data_type() == &DataType::Utf8 => {
                let Expr::Column(_) = &*cast.expr else {
                    return None;
                };
                match column_type? {
                    DataType::Utf8 => Some(()),
                    DataType::Dictionary(key, value)
                        if **key == DataType::UInt32 && **value == DataType::Utf8 =>
                    {
                        Some(())
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }

    fn equality_literals(&self) -> Option<Vec<Scalar<ArrayRef>>> {
        match self {
            Self::Comparison {
                literal,
                op: Operator::Eq,
                ..
            } => Some(vec![literal.clone()]),
            Self::OrEqChain { literals } => Some(literals.clone()),
            _ => None,
        }
    }

    pub(crate) fn evaluate(&self, input: &impl Datum, len: usize) -> Result<BooleanArray> {
        match self {
            Self::Comparison {
                literal,
                op,
                regex,
                regex_negative,
            } => match op {
                Operator::Eq => cmp::eq(input, literal),
                Operator::NotEq => cmp::neq(input, literal),
                Operator::Lt => cmp::lt(input, literal),
                Operator::LtEq => cmp::lt_eq(input, literal),
                Operator::Gt => cmp::gt(input, literal),
                Operator::GtEq => cmp::gt_eq(input, literal),
                Operator::RegexMatch
                | Operator::RegexIMatch
                | Operator::RegexNotMatch
                | Operator::RegexNotIMatch => {
                    Self::regex_match(input, regex.as_deref(), *regex_negative)
                }
                _ => {
                    return UnsupportedOperationSnafu {
                        reason: format!("{op:?}"),
                    }
                    .fail();
                }
            }
            .context(ArrowComputeSnafu),
            Self::OrEqChain { literals } => {
                let mut result = BooleanArray::from(vec![false; len]);
                for literal in literals {
                    let rhs = cmp::eq(input, literal).context(ArrowComputeSnafu)?;
                    result = or_kleene(&result, &rhs).context(ArrowComputeSnafu)?;
                }
                Ok(result)
            }
            Self::IsNull { negated } => {
                let values = match input.get().0.logical_nulls() {
                    Some(nulls) if *negated => nulls.inner().clone(),
                    Some(nulls) => !nulls.inner(),
                    None if *negated => BooleanBuffer::new_set(len),
                    None => BooleanBuffer::new_unset(len),
                };
                Ok(BooleanArray::new(values, None))
            }
            Self::Constant(value) => Ok(BooleanArray::from(vec![*value; len])),
            Self::And(left, right) => datatypes::compute::and_kleene(
                &left.evaluate(input, len)?,
                &right.evaluate(input, len)?,
            )
            .context(ArrowComputeSnafu),
            Self::Or(left, right) => {
                or_kleene(&left.evaluate(input, len)?, &right.evaluate(input, len)?)
                    .context(ArrowComputeSnafu)
            }
        }
    }

    /// Builds a regex pattern from a scalar value and operator.
    /// Returns the `(regex, negative)` and if successful.
    ///
    /// Returns `Err` if
    /// - the value is not a string
    /// - the regex pattern is invalid
    ///
    /// The regex is `None` if
    /// - the operator is not a regex operator
    /// - the pattern is empty
    ///
    /// The pattern is compiled once here, together with the [`meta::Cache`] every search needs:
    /// evaluating an array of rows then only borrows that cache, instead of taking one from
    /// `regex`'s cache pool per row.
    fn maybe_build_regex(
        operator: Operator,
        value: &ScalarValue,
    ) -> Result<(Option<Arc<CompiledRegex>>, bool), ArrowError> {
        let (ignore_case, negative) = match operator {
            Operator::RegexMatch => (false, false),
            Operator::RegexIMatch => (true, false),
            Operator::RegexNotMatch => (false, true),
            Operator::RegexNotIMatch => (true, true),
            _ => return Ok((None, false)),
        };
        let flag = if ignore_case { Some("i") } else { None };
        let regex = value
            .try_as_str()
            .ok_or_else(|| ArrowError::CastError(format!("Cannot cast {:?} to str", value)))?
            .ok_or_else(|| ArrowError::CastError("Regex should not be null".to_string()))?;
        let pattern = match flag {
            Some(flag) => format!("(?{flag}){regex}"),
            None => regex.to_string(),
        };
        if pattern.is_empty() {
            Ok((None, negative))
        } else {
            CompiledRegex::new(&pattern).map(|regex| (Some(Arc::new(regex)), negative))
        }
    }

    fn regex_match(
        input: &impl Datum,
        regex: Option<&CompiledRegex>,
        negative: bool,
    ) -> std::result::Result<BooleanArray, ArrowError> {
        let array = input.get().0;

        // Try to cast to StringArray first
        if let Ok(string_array) = as_string_array(array) {
            let mut result = regexp_is_match_scalar(string_array, regex)?;
            if negative {
                result = datatypes::compute::not(&result)?;
            }
            return Ok(result);
        }

        // Try to cast to StringDictionaryArray
        if let Some(dict_array) = array.as_any().downcast_ref::<DictionaryArray<UInt32Type>>() {
            let mut result = regexp_is_match_dictionary(dict_array, regex)?;
            if negative {
                result = datatypes::compute::not(&result)?;
            }
            return Ok(result);
        }

        Err(ArrowError::CastError(format!(
            "Cannot cast {:?} to StringArray or StringDictionaryArray",
            array.data_type()
        )))
    }
}

/// A regular expression compiled once, holding the scratch space every search needs.
///
/// [`regex::Regex`] keeps a pool of [`meta::Cache`]s and borrows one for the duration of a single
/// `is_match`: a profile of a scan is dominated by that round trip (`Cache::get_slow`,
/// `put_value`) rather than by the search itself, because the scan matches one value per row.
/// This type owns the cache next to the compiled program instead, so
/// [`regexp_is_match_scalar`] acquires it once for a whole array and a row costs one
/// `search_half`.
///
/// The pattern is compiled with the configuration [`regex::Regex`] uses for the same pattern
/// (`MatchKind::LeftmostFirst`, UTF-8 only, a 10 MiB NFA limit and a 2 MiB hybrid cache, which are
/// the [`meta::Config`] defaults), so the answers are identical to `Regex::is_match`.
pub struct CompiledRegex {
    /// The compiled program.
    regex: meta::Regex,
    /// The pattern text, kept for diagnostics.
    pattern: String,
    /// The scratch space the program searches with. Serialized because the evaluator owning this
    /// regex is shared between threads (`PhysicalExpr` is `Send + Sync`).
    cache: Mutex<meta::Cache>,
    /// How many times [`Self::cache`] was acquired. A scan searches one array under one
    /// acquisition, so a test can prove the row loop does not pay for the lock per row.
    #[cfg(test)]
    cache_acquisitions: AtomicUsize,
}

impl CompiledRegex {
    /// Compiles `pattern`.
    pub fn new(pattern: &str) -> Result<Self, ArrowError> {
        let regex = meta::Regex::new(pattern).map_err(|e| {
            ArrowError::ComputeError(format!("Regular expression did not compile: {e:?}"))
        })?;
        // The one and only cache this pattern searches with.
        let cache = Mutex::new(regex.create_cache());

        Ok(Self {
            regex,
            pattern: pattern.to_string(),
            cache,
            #[cfg(test)]
            cache_acquisitions: AtomicUsize::new(0),
        })
    }

    /// Returns whether `value` matches.
    pub fn is_match(&self, value: &str) -> bool {
        let mut cache = self.lock_cache();
        self.is_match_with(&mut cache, value)
    }

    /// Returns whether `value` matches, searching with the caller's cache.
    fn is_match_with(&self, cache: &mut meta::Cache, value: &str) -> bool {
        // The input `Regex::is_match` builds for the same value: `earliest` asks for the first
        // match to end, not for the leftmost one, which is all an existence check needs.
        self.regex
            .search_half_with(cache, &Input::new(value).earliest(true))
            .is_some()
    }

    /// Acquires the one cache this pattern owns.
    fn lock_cache(&self) -> MutexGuard<'_, meta::Cache> {
        #[cfg(test)]
        self.cache_acquisitions.fetch_add(1, Ordering::Relaxed);

        // A panic while searching leaves scratch space behind, which is exactly what the regex
        // crate itself does when it returns a cache to its pool on unwind, so a poisoned cache is
        // still usable.
        self.cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Returns how many times this pattern acquired its cache.
    #[cfg(test)]
    fn cache_acquisitions(&self) -> usize {
        self.cache_acquisitions.load(Ordering::Relaxed)
    }
}

impl fmt::Debug for CompiledRegex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The compiled program and its cache are huge; the pattern identifies them.
        f.debug_struct("CompiledRegex")
            .field("pattern", &self.pattern)
            .finish_non_exhaustive()
    }
}

/// The same as arrow [regexp_is_match_scalar()](datatypes::compute::kernels::regexp::regexp_is_match_scalar())
/// with pre-compiled regex.
/// See <https://github.com/apache/arrow-rs/blob/54.2.0/arrow-string/src/regexp.rs#L204-L246> for the implementation details.
pub fn regexp_is_match_scalar<'a, S>(
    array: &'a S,
    regex: Option<&CompiledRegex>,
) -> Result<BooleanArray, ArrowError>
where
    &'a S: StringArrayType<'a>,
{
    let null_bit_buffer = array.nulls().map(|x| x.inner().sliced());
    let mut result = BooleanBufferBuilder::new(array.len());

    if let Some(re) = regex {
        // One cache acquisition for the whole array: the loop below must not pay for the mutex
        // per value.
        let mut cache = re.lock_cache();
        for i in 0..array.len() {
            result.append(re.is_match_with(&mut cache, array.value(i)));
        }
    } else {
        result.append_n(array.len(), true);
    }

    let buffer = result.into();
    let data = unsafe {
        ArrayData::new_unchecked(
            DataType::Boolean,
            array.len(),
            None,
            null_bit_buffer,
            0,
            vec![buffer],
            vec![],
        )
    };

    Ok(BooleanArray::from(data))
}

/// Similar to [regexp_is_match_scalar] but for StringDictionaryArray.
/// Iterates through dictionary keys to get string values and applies regex matching.
pub fn regexp_is_match_dictionary(
    dict_array: &DictionaryArray<UInt32Type>,
    regex: Option<&CompiledRegex>,
) -> Result<BooleanArray, ArrowError> {
    // Get the string values from the dictionary
    let string_values = dict_array
        .values()
        .as_any()
        .downcast_ref::<datatypes::arrow::array::StringArray>()
        .ok_or_else(|| {
            ArrowError::CastError("Dictionary values must be StringArray".to_string())
        })?;

    // Dictionary logical nulls include both null keys and keys whose dictionary value is null.
    let logical_nulls = dict_array.logical_nulls();
    let null_bit_buffer = logical_nulls.as_ref().map(|x| x.inner().sliced());
    let mut result = BooleanBufferBuilder::new(dict_array.len());

    if let Some(re) = regex {
        // One cache acquisition for the whole array, as in `regexp_is_match_scalar`.
        let mut cache = re.lock_cache();
        let keys = dict_array.keys().values();
        for i in 0..dict_array.len() {
            if logical_nulls.as_ref().is_some_and(|nulls| nulls.is_null(i)) {
                result.append(false);
            } else {
                let key = keys[i] as usize;
                let string_value = string_values.value(key);
                result.append(re.is_match_with(&mut cache, string_value));
            }
        }
    } else {
        result.append_n(dict_array.len(), true);
    }

    let buffer = result.into();
    let data = unsafe {
        ArrayData::new_unchecked(
            DataType::Boolean,
            dict_array.len(),
            None,
            null_bit_buffer,
            0,
            vec![buffer],
            vec![],
        )
    };

    Ok(BooleanArray::from(data))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::execution::context::ExecutionProps;
    use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
    use datafusion::logical_expr::{BinaryExpr, Expr, Literal, Operator, col, lit};
    use datafusion::physical_expr::create_physical_expr;
    use datafusion_common::arrow::array::ArrayRef;
    use datafusion_common::arrow::buffer::BooleanBuffer;
    use datafusion_common::{Column, DFSchema, ScalarValue};
    use datatypes::arrow::array::{
        Array, BooleanArray, DictionaryArray, LargeStringArray, RecordBatch, StringArray,
        StringArrayType, StringViewArray, UInt32Array,
    };
    use datatypes::arrow::datatypes::{DataType, Field, Schema, TimeUnit, UInt32Type};
    use datatypes::value::Value;
    use regex::Regex;

    use crate::filter::predicate::{
        CompiledRegex, SimplePredicate, regexp_is_match_dictionary, regexp_is_match_scalar,
    };
    use crate::filter::{SimpleFilterEvaluator, batch_filter};

    #[test]
    fn unsupported_filter_op() {
        // `+` is not supported
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("foo"))),
            op: Operator::Plus,
            right: Box::new(1.lit()),
        });
        assert!(SimpleFilterEvaluator::try_new(&expr).is_none());

        // two literal is not supported
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(1.lit()),
            op: Operator::Eq,
            right: Box::new(1.lit()),
        });
        assert!(SimpleFilterEvaluator::try_new(&expr).is_none());

        // two column is not supported
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("foo"))),
            op: Operator::Eq,
            right: Box::new(Expr::Column(Column::from_name("bar"))),
        });
        assert!(SimpleFilterEvaluator::try_new(&expr).is_none());

        // compound expr is not supported
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::BinaryExpr(BinaryExpr {
                left: Box::new(Expr::Column(Column::from_name("foo"))),
                op: Operator::Eq,
                right: Box::new(1.lit()),
            })),
            op: Operator::Eq,
            right: Box::new(1.lit()),
        });
        assert!(SimpleFilterEvaluator::try_new(&expr).is_none());
        for negated in [false, true] {
            let membership = col("label").in_list(vec![lit(""), lit("lo")], negated);
            assert!(SimpleFilterEvaluator::try_new(&membership).is_none());
            assert!(
                SimpleFilterEvaluator::try_new(&col("label").is_null().or(membership)).is_none()
            );
        }
        for expr in [
            col("other").is_null().or(col("label").not_eq(lit("lo"))),
            col("a.label")
                .is_null()
                .or(col("b.label").not_eq(lit("lo"))),
            col("label").is_null().or(col("label").eq(col("other"))),
            col("label")
                .is_null()
                .or(Expr::Cast(datafusion::logical_expr::Cast::new(
                    Box::new(col("label")),
                    DataType::Int64,
                ))
                .eq(lit("lo"))),
        ] {
            assert!(SimpleFilterEvaluator::try_new(&expr).is_none(), "{expr}");
        }
    }

    #[test]
    fn string_cast_requires_matching_column_type() {
        let cast = Expr::Cast(datafusion::logical_expr::Cast::new(
            Box::new(col("label")),
            DataType::Utf8,
        ));
        let expr = col("label").is_null().or(cast.not_eq(lit("lo")));
        for data_type in [
            DataType::Int64,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            DataType::Binary,
        ] {
            assert!(
                SimpleFilterEvaluator::try_new_with_column_type(&expr, &|_| Some(
                    data_type.clone()
                ))
                .is_none()
            );
        }
        assert!(SimpleFilterEvaluator::try_new(&expr).is_none());
    }

    #[tokio::test]
    async fn single_column_predicates_match_datafusion() {
        use datafusion::prelude::SessionContext;
        use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
        use datatypes::arrow::array::{Int32Array, StringArray, UInt32Array};

        let values = vec![
            None,
            Some(""),
            Some("lo"),
            Some("eth0"),
            Some("a\nb"),
            Some("服务"),
            None,
        ];
        let strings: ArrayRef = Arc::new(StringArray::from(values));
        let dictionary: ArrayRef = Arc::new(DictionaryArray::<UInt32Type>::new(
            UInt32Array::from(vec![
                None,
                Some(0),
                Some(1),
                Some(2),
                Some(3),
                Some(4),
                Some(5),
            ]),
            Arc::new(StringArray::from(vec![
                Some(""),
                Some("lo"),
                Some("eth0"),
                Some("a\nb"),
                Some("服务"),
                None,
            ])),
        ));
        let null = || lit(ScalarValue::Utf8(None));
        let mut predicates = vec![
            col("label").is_null(),
            col("label").is_not_null(),
            col("label")
                .is_null()
                .or(col("label").eq(lit("")))
                .and(col("label").is_not_null()),
            col("label")
                .not_eq(lit("lo"))
                .and(col("label").not_eq(lit("eth0"))),
            col("label")
                .eq(lit("lo"))
                .or(col("label").eq(lit("eth0")))
                .or(col("label").eq(lit(""))),
            col("label").eq(null()).or(col("label").eq(lit("eth0"))),
            col("label").gt(lit("a")).and(col("label").lt(lit("z"))),
            lit("lo").not_eq(col("label")),
        ];
        for (op, literal) in [
            (Operator::Eq, ""),
            (Operator::NotEq, "lo"),
            (Operator::RegexNotMatch, "^(?:lo|vfat)$"),
            (Operator::RegexMatch, "^(?:|eth0)$"),
        ] {
            let comparison = Expr::BinaryExpr(BinaryExpr::new(
                Box::new(col("label")),
                op,
                Box::new(lit(literal)),
            ));
            predicates.extend([
                col("label").is_null().or(comparison.clone()),
                comparison.or(col("label").is_null()),
            ]);
        }
        for input in [strings, dictionary] {
            let ctx = SessionContext::new();
            let schema = Arc::new(Schema::new(vec![
                Field::new("label", input.data_type().clone(), true),
                Field::new("id", DataType::Int32, false),
            ]));
            ctx.register_batch(
                "labels",
                RecordBatch::try_new(
                    schema,
                    vec![
                        input.clone(),
                        Arc::new(Int32Array::from_iter_values(0..input.len() as i32)),
                    ],
                )
                .unwrap(),
            )
            .unwrap();
            for expr in &predicates {
                let df = ctx
                    .table("labels")
                    .await
                    .unwrap()
                    .filter(expr.clone())
                    .unwrap();
                let plan = df.clone().into_optimized_plan().unwrap();
                let expected = df
                    .collect()
                    .await
                    .unwrap()
                    .iter()
                    .flat_map(|batch| {
                        batch
                            .column_by_name("id")
                            .unwrap()
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap()
                            .values()
                            .to_vec()
                    })
                    .collect::<Vec<_>>();
                let evaluate = |expr: &Expr| {
                    let evaluator =
                        SimpleFilterEvaluator::try_new_with_column_type(expr, &|name| {
                            (name == "label").then(|| input.data_type().clone())
                        })
                        .unwrap_or_else(|| panic!("unsupported: {expr}"));
                    let mask = evaluator.evaluate_array(&input).unwrap();
                    for (i, matched) in mask.iter().enumerate() {
                        let scalar = ScalarValue::try_from_array(&input, i).unwrap();
                        assert_eq!(
                            evaluator.evaluate_scalar(&scalar).unwrap(),
                            matched,
                            "{expr}, row={i}"
                        );
                    }
                    mask
                };
                let selected = |mask: BooleanBuffer| {
                    mask.iter()
                        .enumerate()
                        .filter_map(|(i, matched)| matched.then_some(i as i32))
                        .collect::<Vec<_>>()
                };
                assert_eq!(selected(evaluate(expr)), expected, "{expr}");
                let mut filters = Vec::new();
                plan.apply(|plan| {
                    if let datafusion::logical_expr::LogicalPlan::Filter(filter) = plan {
                        filters.push(filter.predicate.clone());
                    }
                    Ok(TreeNodeRecursion::Continue)
                })
                .unwrap();
                if !filters.is_empty() {
                    let mut mask = BooleanBuffer::new_set(input.len());
                    for filter in filters {
                        mask = &mask & &evaluate(&filter);
                    }
                    assert_eq!(selected(mask), expected, "optimized: {plan}");
                }
            }
        }
    }

    #[tokio::test]
    async fn float_zero_predicates_match_datafusion() {
        use datafusion::prelude::SessionContext;
        use datatypes::arrow::array::{Float32Array, Float64Array, Int32Array};

        let float64: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(-0.0),
            Some(0.0),
            Some(1.0),
            Some(f64::NAN),
            None,
        ]));
        let float32: ArrayRef = Arc::new(Float32Array::from(vec![
            Some(-0.0),
            Some(0.0),
            Some(1.0),
            Some(f32::NAN),
            None,
        ]));
        for input in [float64, float32] {
            let literal = |v: f64| {
                if input.data_type() == &DataType::Float32 {
                    lit(v as f32)
                } else {
                    lit(v)
                }
            };
            let ctx = SessionContext::new();
            let schema = Arc::new(Schema::new(vec![
                Field::new("v", input.data_type().clone(), true),
                Field::new("id", DataType::Int32, false),
            ]));
            ctx.register_batch(
                "floats",
                RecordBatch::try_new(
                    schema,
                    vec![
                        input.clone(),
                        Arc::new(Int32Array::from_iter_values(0..input.len() as i32)),
                    ],
                )
                .unwrap(),
            )
            .unwrap();
            for expr in [
                col("v").eq(literal(0.0)),
                col("v").eq(literal(-0.0)),
                col("v").not_eq(literal(-0.0)),
                col("v").lt(literal(0.0)),
                col("v").lt_eq(literal(-0.0)),
                col("v").gt(literal(-0.0)),
                col("v").gt_eq(literal(0.0)),
                literal(0.0).lt_eq(col("v")),
                col("v").eq(literal(0.0)).or(col("v").eq(literal(1.0))),
                col("v").eq(literal(f64::NAN)),
            ] {
                let expected = ctx
                    .table("floats")
                    .await
                    .unwrap()
                    .filter(expr.clone())
                    .unwrap()
                    .collect()
                    .await
                    .unwrap()
                    .iter()
                    .flat_map(|batch| {
                        batch
                            .column_by_name("id")
                            .unwrap()
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap()
                            .values()
                            .to_vec()
                    })
                    .collect::<Vec<_>>();
                let evaluator = SimpleFilterEvaluator::try_new(&expr).unwrap();
                let mask = evaluator.evaluate_array(&input).unwrap();
                let selected = mask
                    .iter()
                    .enumerate()
                    .filter_map(|(i, matched)| matched.then_some(i as i32))
                    .collect::<Vec<_>>();
                assert_eq!(selected, expected, "{expr}");
                for (i, matched) in mask.iter().enumerate() {
                    let scalar = ScalarValue::try_from_array(&input, i).unwrap();
                    assert_eq!(
                        evaluator.evaluate_scalar(&scalar).unwrap(),
                        matched,
                        "{expr}, row={i}"
                    );
                }
            }
        }
    }

    #[test]
    fn supported_filter_op() {
        // equal
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("foo"))),
            op: Operator::Eq,
            right: Box::new(1.lit()),
        });
        let _ = SimpleFilterEvaluator::try_new(&expr).unwrap();

        // swap operands
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(1.lit()),
            op: Operator::Lt,
            right: Box::new(Expr::Column(Column::from_name("foo"))),
        });
        let evaluator = SimpleFilterEvaluator::try_new(&expr).unwrap();
        assert!(evaluator.is_gt());
        assert_eq!(evaluator.column_name, "foo".to_string());
    }

    #[test]
    fn run_on_array() {
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("foo"))),
            op: Operator::Eq,
            right: Box::new(1i64.lit()),
        });
        let evaluator = SimpleFilterEvaluator::try_new(&expr).unwrap();

        let input_1 = Arc::new(datatypes::arrow::array::Int64Array::from(vec![1, 2, 3])) as _;
        let result = evaluator.evaluate_array(&input_1).unwrap();
        assert_eq!(result, BooleanBuffer::from(vec![true, false, false]));

        let input_2 = Arc::new(datatypes::arrow::array::Int64Array::from(vec![1, 1, 1])) as _;
        let result = evaluator.evaluate_array(&input_2).unwrap();
        assert_eq!(result, BooleanBuffer::from(vec![true, true, true]));

        let input_3 = Arc::new(datatypes::arrow::array::Int64Array::new_null(0)) as _;
        let result = evaluator.evaluate_array(&input_3).unwrap();
        assert_eq!(result, BooleanBuffer::from(vec![]));
    }

    #[test]
    fn run_on_scalar() {
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("foo"))),
            op: Operator::Lt,
            right: Box::new(1i64.lit()),
        });
        let evaluator = SimpleFilterEvaluator::try_new(&expr).unwrap();

        let input_1 = ScalarValue::Int64(Some(1));
        let result = evaluator.evaluate_scalar(&input_1).unwrap();
        assert!(!result);

        let input_2 = ScalarValue::Int64(Some(0));
        let result = evaluator.evaluate_scalar(&input_2).unwrap();
        assert!(result);

        let input_3 = ScalarValue::Int64(None);
        let result = evaluator.evaluate_scalar(&input_3).unwrap();
        assert!(!result);
    }

    #[test]
    fn test_complex_filter_expression() {
        // Create an expression tree for: col = 'B' OR col = 'C' OR col = 'D'
        let col_eq_b = col("col").eq(lit("B"));
        let col_eq_c = col("col").eq(lit("C"));
        let col_eq_d = col("col").eq(lit("D"));

        // Build the OR chain
        let col_or_expr = col_eq_b.or(col_eq_c).or(col_eq_d);

        // Check that SimpleFilterEvaluator can handle OR chain
        let or_evaluator = SimpleFilterEvaluator::try_new(&col_or_expr).unwrap();
        assert_eq!(or_evaluator.column_name, "col");
        assert!(or_evaluator.is_or_eq_chain());
        assert_eq!(
            or_evaluator.literal_list_values().unwrap(),
            vec![Value::from("B"), Value::from("C"), Value::from("D")]
        );

        // Create a schema and batch for testing
        let schema = Schema::new(vec![Field::new("col", DataType::Utf8, false)]);
        let df_schema = DFSchema::try_from(schema.clone()).unwrap();
        let props = ExecutionProps::new();
        let physical_expr = create_physical_expr(
            &col_or_expr,
            &df_schema,
            &props,
            &PhysicalPlanningContext::default(),
        )
        .unwrap();

        // Create test data
        let col_data = Arc::new(datatypes::arrow::array::StringArray::from(vec![
            "B", "C", "E", "B", "C", "D", "F",
        ]));
        let batch = RecordBatch::try_new(Arc::new(schema), vec![col_data]).unwrap();
        let expected = datatypes::arrow::array::StringArray::from(vec!["B", "C", "B", "C", "D"]);

        // Filter the batch
        let filtered_batch = batch_filter(&batch, &physical_expr).unwrap();

        // Expected: rows with col in ("B", "C", "D")
        // That would be rows 0, 1, 3, 4, 5
        assert_eq!(filtered_batch.num_rows(), 5);

        let col_filtered = filtered_batch
            .column(0)
            .as_any()
            .downcast_ref::<datatypes::arrow::array::StringArray>()
            .unwrap();
        assert_eq!(col_filtered, &expected);
    }

    #[test]
    fn test_maybe_build_regex() {
        // Test case for RegexMatch (case sensitive, non-negative)
        let (regex, negative) = SimplePredicate::maybe_build_regex(
            Operator::RegexMatch,
            &ScalarValue::Utf8(Some("a.*b".to_string())),
        )
        .unwrap();
        assert!(regex.is_some());
        assert!(!negative);
        assert!(regex.unwrap().is_match("axxb"));

        // Test case for RegexIMatch (case insensitive, non-negative)
        let (regex, negative) = SimplePredicate::maybe_build_regex(
            Operator::RegexIMatch,
            &ScalarValue::Utf8(Some("a.*b".to_string())),
        )
        .unwrap();
        assert!(regex.is_some());
        assert!(!negative);
        assert!(regex.unwrap().is_match("AxxB"));

        // Test case for RegexNotMatch (case sensitive, negative)
        let (regex, negative) = SimplePredicate::maybe_build_regex(
            Operator::RegexNotMatch,
            &ScalarValue::Utf8(Some("a.*b".to_string())),
        )
        .unwrap();
        assert!(regex.is_some());
        assert!(negative);

        // Test case for RegexNotIMatch (case insensitive, negative)
        let (regex, negative) = SimplePredicate::maybe_build_regex(
            Operator::RegexNotIMatch,
            &ScalarValue::Utf8(Some("a.*b".to_string())),
        )
        .unwrap();
        assert!(regex.is_some());
        assert!(negative);

        // Test with empty regex pattern
        let (regex, negative) = SimplePredicate::maybe_build_regex(
            Operator::RegexMatch,
            &ScalarValue::Utf8(Some("".to_string())),
        )
        .unwrap();
        assert!(regex.is_none());
        assert!(!negative);

        // Test with non-regex operator
        let (regex, negative) = SimplePredicate::maybe_build_regex(
            Operator::Eq,
            &ScalarValue::Utf8(Some("a.*b".to_string())),
        )
        .unwrap();
        assert!(regex.is_none());
        assert!(!negative);

        // Test with invalid regex pattern
        let result = SimplePredicate::maybe_build_regex(
            Operator::RegexMatch,
            &ScalarValue::Utf8(Some("a(b".to_string())),
        );
        assert!(result.is_err());

        // Test with non-string value
        let result = SimplePredicate::maybe_build_regex(
            Operator::RegexMatch,
            &ScalarValue::Int64(Some(123)),
        );
        assert!(result.is_err());

        // Test with null value
        let result =
            SimplePredicate::maybe_build_regex(Operator::RegexMatch, &ScalarValue::Utf8(None));
        assert!(result.is_err());
    }

    #[test]
    fn test_regex_match_dictionary_array() {
        use datatypes::arrow::array::StringDictionaryBuilder;

        // Create a StringDictionaryArray
        let mut builder = StringDictionaryBuilder::<UInt32Type>::new();
        builder.append("apple").unwrap();
        builder.append("banana").unwrap();
        builder.append("apple").unwrap();
        builder.append("cherry").unwrap();
        let dict_array = builder.finish();

        // Test regex that matches "apple"
        let regex = CompiledRegex::new(r"app.*").unwrap();
        let result = regexp_is_match_dictionary(&dict_array, Some(&regex)).unwrap();

        // Should match indices 0 and 2 (both "apple")
        assert_eq!(result.len(), 4);
        assert!(result.value(0)); // "apple"
        assert!(!result.value(1)); // "banana"
        assert!(result.value(2)); // "apple"
        assert!(!result.value(3)); // "cherry"

        // Test regex that matches "banana"
        let regex2 = CompiledRegex::new(r"ban.*").unwrap();
        let result2 = regexp_is_match_dictionary(&dict_array, Some(&regex2)).unwrap();

        assert!(!result2.value(0)); // "apple"
        assert!(result2.value(1)); // "banana"
        assert!(!result2.value(2)); // "apple"
        assert!(!result2.value(3)); // "cherry"

        // Test with no regex (should match all)
        let result3 = regexp_is_match_dictionary(&dict_array, None).unwrap();
        assert!(result3.value(0));
        assert!(result3.value(1));
        assert!(result3.value(2));
        assert!(result3.value(3));
    }

    #[test]
    fn test_regex_scan_masks_preserve_sql_null_semantics() {
        let plain = Arc::new(datatypes::arrow::array::StringArray::from(vec![
            Some("api"),
            Some("API"),
            Some("db"),
            None,
        ])) as ArrayRef;
        assert_regex_scan_masks(
            &plain,
            [
                vec![true, false, false, false],
                vec![true, true, false, false],
                vec![false, true, true, false],
                vec![false, false, true, false],
            ],
        );

        let dictionary = DictionaryArray::new(
            datatypes::arrow::array::UInt32Array::from(vec![
                Some(0),
                Some(1),
                Some(2),
                None,
                Some(3),
            ]),
            Arc::new(datatypes::arrow::array::StringArray::from(vec![
                Some("api"),
                Some("API"),
                Some("db"),
                None,
            ])),
        );
        let raw =
            regexp_is_match_dictionary(&dictionary, Some(&CompiledRegex::new("^api$").unwrap()))
                .unwrap();
        assert!(raw.is_null(3)); // null dictionary key
        assert!(raw.is_null(4)); // non-null key referencing a null dictionary value

        let dictionary = Arc::new(dictionary) as ArrayRef;
        assert_regex_scan_masks(
            &dictionary,
            [
                vec![true, false, false, false, false],
                vec![true, true, false, false, false],
                vec![false, true, true, false, false],
                vec![false, false, true, false, false],
            ],
        );

        let negative = regex_evaluator(Operator::RegexNotMatch);
        assert!(!negative.evaluate_scalar(&ScalarValue::Utf8(None)).unwrap());
    }

    fn assert_regex_scan_masks(input: &ArrayRef, expected: [Vec<bool>; 4]) {
        for (op, expected) in [
            Operator::RegexMatch,
            Operator::RegexIMatch,
            Operator::RegexNotMatch,
            Operator::RegexNotIMatch,
        ]
        .into_iter()
        .zip(expected)
        {
            assert_eq!(
                BooleanBuffer::from(expected),
                regex_evaluator(op).evaluate_array(input).unwrap(),
                "{op:?}"
            );
        }
    }

    fn regex_evaluator(op: Operator) -> SimpleFilterEvaluator {
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("host"))),
            op,
            right: Box::new("^api$".lit()),
        });
        SimpleFilterEvaluator::try_new(&expr).unwrap()
    }

    /// The sql expr `col =~ pattern` as the scan sees it, so the kernels run through the same
    /// predicate an evaluator builds.
    fn regex_column_evaluator(pattern: &str, op: Operator) -> SimpleFilterEvaluator {
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("host"))),
            op,
            right: Box::new(pattern.lit()),
        });
        SimpleFilterEvaluator::try_new(&expr).unwrap()
    }

    /// Reusing one cache must not change an answer: compare with `regex::Regex::is_match` over
    /// anchors, flags, empty matches, alternations and multi-byte UTF-8.
    #[test]
    fn test_compiled_regex_agrees_with_regex_crate() {
        let patterns = [
            "^",
            "$",
            "^$",
            ".*",
            "host.*",
            "(?i)host",
            "(?i)HOST-?A",
            "a|ab",
            "(?:ab)+",
            "a{2,3}",
            "\\bdb\\b",
            "[[:alpha:]]+",
            "\\p{Greek}",
            "日",
            "^.$",
            "(?s).",
            "(?m)^db$",
            "x?",
            "[^a-z]",
            "\\d+",
            "^\\s*$",
        ];
        let values = [
            "",
            "host-a",
            "HOST-A",
            "Host-a",
            "db",
            "a",
            "aa",
            "aaa",
            "ab",
            "abab",
            " ",
            "\t",
            "42",
            "host-42",
            "日",
            "日本語",
            "αβ",
            "x",
            "\n",
            "a\ndb\n",
            "-",
            "A",
        ];

        for pattern in patterns {
            let compiled = CompiledRegex::new(pattern)
                .unwrap_or_else(|e| panic!("{pattern:?} failed to compile: {e}"));
            let reference = Regex::new(pattern).unwrap();
            for value in values {
                assert_eq!(
                    reference.is_match(value),
                    compiled.is_match(value),
                    "{pattern:?} on {value:?}"
                );
            }
        }
    }

    /// A compiled regex and the evaluator holding it are shared by the threads evaluating one
    /// plan: both have to be `Send + Sync` without `unsafe`.
    #[test]
    fn test_compiled_regex_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}

        assert_send_sync::<CompiledRegex>();
        assert_send_sync::<SimpleFilterEvaluator>();
    }

    /// Asserts the kernel answers exactly what arrow's own kernel answers for `array`.
    fn assert_kernel_agrees_with_arrow<'a, S>(array: &'a S, pattern: &str)
    where
        &'a S: StringArrayType<'a>,
    {
        let expected =
            datatypes::compute::kernels::regexp::regexp_is_match_scalar(array, pattern, None)
                .unwrap();
        let compiled = compiled_or_match_all(pattern);
        let actual = regexp_is_match_scalar(array, compiled.as_ref()).unwrap();

        assert_eq!(expected, actual, "{pattern:?}");
    }

    /// Returns the compiled pattern, or `None` for the empty pattern, which matches every row and
    /// is how [`SimplePredicate::maybe_build_regex`] represents it.
    fn compiled_or_match_all(pattern: &str) -> Option<CompiledRegex> {
        (!pattern.is_empty()).then(|| CompiledRegex::new(pattern).unwrap())
    }

    /// The array kernels agree with arrow's kernel row by row, including for `NULL`s and for
    /// sliced arrays whose values do not start at offset 0.
    #[test]
    fn test_regex_array_kernels_agree_with_arrow() {
        let values = vec![Some("host-a"), None, Some(""), Some("db"), Some("host-b")];
        let patterns = ["host.*", "^host-a$", "(?i)HOST.*", "^$", "日", ""];

        let plain = StringArray::from(values.clone());
        let large = LargeStringArray::from(values.clone());
        let view = StringViewArray::from(values.clone());
        for pattern in patterns {
            assert_kernel_agrees_with_arrow(&plain, pattern);
            assert_kernel_agrees_with_arrow(&large, pattern);
            assert_kernel_agrees_with_arrow(&view, pattern);

            // A slice: row 0 is `NULL` and the first value starts at a non-zero offset in the
            // values buffer, which the kernel must honour instead of assuming row 0 is offset 0.
            let sliced = plain.slice(1, 3);
            assert!(sliced.value_offsets()[0] > 0);
            assert!(sliced.is_null(0));
            assert_kernel_agrees_with_arrow(&sliced, pattern);
        }
    }

    /// The dictionary kernel sees the same values as the scalar kernel, and therefore as arrow:
    /// a `NULL` key, a key pointing at a `NULL` dictionary value, and a sliced dictionary array.
    #[test]
    fn test_dictionary_kernel_agrees_with_the_scalar_kernel() {
        let values = Arc::new(StringArray::from(vec![
            Some("api"),
            Some("API"),
            Some("db"),
            None,
        ]));
        let keys = UInt32Array::from(vec![Some(0), Some(1), Some(3), None, Some(2)]);
        let dictionary = DictionaryArray::new(keys, values);
        let patterns = ["^api$", "(?i)^api$", "db", "日", ""];

        for pattern in patterns {
            assert_dictionary_kernel_agrees_with_the_scalar_kernel(&dictionary, pattern);
            // A slice whose rows are a key pointing at a `NULL` dictionary value and then a
            // `NULL` key. Both are SQL `NULL`, which only the logical nulls of a dictionary show.
            let sliced = dictionary.slice(1, 3);
            let logical_nulls = sliced.logical_nulls().unwrap();
            assert!(!sliced.is_null(1));
            assert!(logical_nulls.is_null(1));
            assert!(logical_nulls.is_null(2));
            assert_dictionary_kernel_agrees_with_the_scalar_kernel(&sliced, pattern);
        }
    }

    /// Asserts the dictionary kernel answers what the scalar kernel answers on the values the
    /// dictionary stands for. Arrow's own kernel takes no dictionary, but
    /// [`test_regex_array_kernels_agree_with_arrow`] pins the scalar kernel to it.
    fn assert_dictionary_kernel_agrees_with_the_scalar_kernel(
        dictionary: &DictionaryArray<UInt32Type>,
        pattern: &str,
    ) {
        let values = dictionary
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let nulls = dictionary.logical_nulls();
        let materialized: StringArray = (0..dictionary.len())
            .map(|i: usize| {
                let is_null = nulls.as_ref().is_some_and(|nulls| nulls.is_null(i));
                (!is_null).then(|| {
                    values
                        .value(dictionary.keys().values()[i] as usize)
                        .to_string()
                })
            })
            .collect();

        let compiled = compiled_or_match_all(pattern);
        let expected = regexp_is_match_scalar(&materialized, compiled.as_ref()).unwrap();
        let actual = regexp_is_match_dictionary(dictionary, compiled.as_ref()).unwrap();

        assert_same_boolean_rows(&expected, &actual, pattern);
    }

    /// Asserts two boolean arrays hold the same value in every row, `NULL` included.
    fn assert_same_boolean_rows(expected: &BooleanArray, actual: &BooleanArray, pattern: &str) {
        assert_eq!(expected.len(), actual.len(), "{pattern:?}");
        for i in 0..expected.len() {
            assert_eq!(
                (expected.is_valid(i), expected.value(i)),
                (actual.is_valid(i), actual.value(i)),
                "{pattern:?} row {i}"
            );
        }
    }

    /// A scan evaluates one expression per array, so the cache is acquired once per array and not
    /// once per row: 100 arrays of 3 rows cost 100 acquisitions, each searching 3 values.
    #[test]
    fn test_evaluator_borrows_the_cache_once_per_array() {
        let evaluator = regex_column_evaluator("api|db", Operator::RegexMatch);
        let SimplePredicate::Comparison {
            regex: Some(compiled),
            ..
        } = &evaluator.predicate
        else {
            panic!("the evaluator holds a compiled regex");
        };
        assert_eq!(0, compiled.cache_acquisitions());

        let array = Arc::new(StringArray::from(vec![Some("api"), None, Some("db")])) as ArrayRef;
        for arrays in 1..=100 {
            assert_eq!(
                BooleanBuffer::from(vec![true, false, true]),
                evaluator.evaluate_array(&array).unwrap()
            );
            assert_eq!(
                arrays,
                compiled.cache_acquisitions(),
                "{arrays} arrays must cost {arrays} cache acquisitions, not one per row"
            );
        }
    }

    /// One evaluator is evaluated by several threads at once during a scan, each taking the one
    /// cache in turn.
    #[test]
    fn test_evaluator_matches_from_several_threads() {
        let evaluator = Arc::new(regex_column_evaluator("^api$", Operator::RegexMatch));
        let array = Arc::new(StringArray::from(vec![
            Some("api"),
            Some("API"),
            None,
            Some("db"),
        ])) as ArrayRef;
        let expected = BooleanBuffer::from(vec![true, false, false, false]);

        std::thread::scope(|scope| {
            for _ in 0..8 {
                let evaluator = Arc::clone(&evaluator);
                let array = Arc::clone(&array);
                let expected = expected.clone();
                scope.spawn(move || {
                    for _ in 0..100 {
                        assert_eq!(expected, evaluator.evaluate_array(&array).unwrap());
                    }
                });
            }
        });
    }
}
