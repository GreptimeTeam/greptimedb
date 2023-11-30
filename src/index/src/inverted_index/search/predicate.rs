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

use std::collections::HashSet;

use crate::inverted_index::Bytes;

/// Enumerates types of predicates for value filtering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Predicate {
    /// Predicate for matching values in a list.
    InList(InListPredicate),

    /// Predicate for matching values within a range.
    Range(RangePredicate),

    /// Predicate for matching values against a regex pattern.
    RegexMatch(RegexMatchPredicate),
}

/// `InListPredicate` contains a list of acceptable values. A value needs to match at least
/// one of the elements (logical OR semantic) for the predicate to be satisfied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InListPredicate {
    /// List of acceptable values.
    pub list: HashSet<Bytes>,
}

/// `Bound` is a sub-component of a range, representing a single-sided limit that could be inclusive or exclusive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bound {
    /// Whether the bound is inclusive or exclusive.
    pub inclusive: bool,
    /// The value of the bound.
    pub value: Bytes,
}

/// `Range` defines a single continuous range which can optionally have a lower and/or upper limit.
/// Both the lower and upper bounds must be satisfied for the range condition to be true.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Range {
    /// The lower bound of the range.
    pub lower: Option<Bound>,
    /// The upper bound of the range.
    pub upper: Option<Bound>,
}

/// `RangePredicate` encapsulates a range condition that must be satisfied
/// for the predicate to hold true (logical AND semantic between the bounds).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangePredicate {
    /// The range condition.
    pub range: Range,
}

/// `RegexMatchPredicate` encapsulates a single regex pattern. A value must match
/// the pattern for the predicate to be satisfied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegexMatchPredicate {
    /// The regex pattern.
    pub pattern: String,
}
