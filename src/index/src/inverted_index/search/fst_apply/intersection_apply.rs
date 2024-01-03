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

use std::mem::size_of;

use fst::map::OpBuilder;
use fst::{IntoStreamer, Streamer};
use regex_automata::dfa::dense::DFA;
use snafu::{ensure, ResultExt};

use crate::inverted_index::error::{
    EmptyPredicatesSnafu, IntersectionApplierWithInListSnafu, ParseDFASnafu, Result,
};
use crate::inverted_index::search::fst_apply::FstApplier;
use crate::inverted_index::search::predicate::{Predicate, Range};
use crate::inverted_index::FstMap;

/// `IntersectionFstApplier` applies intersection operations on an FstMap using specified ranges and regex patterns.
pub struct IntersectionFstApplier {
    /// A list of `Range` which define inclusive or exclusive ranges for keys to be queried in the FstMap.
    ranges: Vec<Range>,

    /// A list of `Dfa` compiled from regular expression patterns.
    dfas: Vec<DFA<Vec<u32>>>,
}

impl FstApplier for IntersectionFstApplier {
    fn apply(&self, fst: &FstMap) -> Vec<u64> {
        let mut op = OpBuilder::new();

        for range in &self.ranges {
            match (range.lower.as_ref(), range.upper.as_ref()) {
                (Some(lower), Some(upper)) => match (lower.inclusive, upper.inclusive) {
                    (true, true) => op.push(fst.range().ge(&lower.value).le(&upper.value)),
                    (true, false) => op.push(fst.range().ge(&lower.value).lt(&upper.value)),
                    (false, true) => op.push(fst.range().gt(&lower.value).le(&upper.value)),
                    (false, false) => op.push(fst.range().gt(&lower.value).lt(&upper.value)),
                },
                (Some(lower), None) => match lower.inclusive {
                    true => op.push(fst.range().ge(&lower.value)),
                    false => op.push(fst.range().gt(&lower.value)),
                },
                (None, Some(upper)) => match upper.inclusive {
                    true => op.push(fst.range().le(&upper.value)),
                    false => op.push(fst.range().lt(&upper.value)),
                },
                (None, None) => op.push(fst),
            }
        }

        for dfa in &self.dfas {
            op.push(fst.search(dfa));
        }

        let mut stream = op.intersection().into_stream();
        let mut values = Vec::new();
        while let Some((_, v)) = stream.next() {
            values.push(v[0].value)
        }
        values
    }

    fn memory_usage(&self) -> usize {
        let mut size = self.ranges.capacity() * size_of::<Range>();
        for range in &self.ranges {
            size += range
                .lower
                .as_ref()
                .map_or(0, |bound| bound.value.capacity());
            size += range
                .upper
                .as_ref()
                .map_or(0, |bound| bound.value.capacity());
        }

        size += self.dfas.capacity() * size_of::<DFA<Vec<u32>>>();
        for dfa in &self.dfas {
            size += dfa.memory_usage();
        }
        size
    }
}

impl IntersectionFstApplier {
    /// Attempts to create an `IntersectionFstApplier` from a list of `Predicate`.
    ///
    /// This function only accepts predicates of the variants `Range` and `RegexMatch`.
    /// It does not accept `InList` predicates and will return an error if any are found.
    /// `InList` predicates are handled by `KeysFstApplier`.
    pub fn try_from(predicates: Vec<Predicate>) -> Result<Self> {
        ensure!(!predicates.is_empty(), EmptyPredicatesSnafu);

        let mut dfas = Vec::with_capacity(predicates.len());
        let mut ranges = Vec::with_capacity(predicates.len());

        for predicate in predicates {
            match predicate {
                Predicate::Range(range) => ranges.push(range.range),
                Predicate::RegexMatch(regex) => {
                    let dfa = DFA::new(&regex.pattern);
                    let dfa = dfa.map_err(Box::new).context(ParseDFASnafu)?;
                    dfas.push(dfa);
                }
                // Rejection of `InList` predicates is enforced here.
                Predicate::InList(_) => {
                    return IntersectionApplierWithInListSnafu.fail();
                }
            }
        }

        Ok(Self { dfas, ranges })
    }
}

impl TryFrom<Vec<Predicate>> for IntersectionFstApplier {
    type Error = crate::inverted_index::error::Error;

    fn try_from(predicates: Vec<Predicate>) -> Result<Self> {
        Self::try_from(predicates)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::inverted_index::error::Error;
    use crate::inverted_index::search::predicate::{
        Bound, InListPredicate, RangePredicate, RegexMatchPredicate,
    };

    fn create_applier_from_range(range: Range) -> Result<IntersectionFstApplier> {
        IntersectionFstApplier::try_from(vec![Predicate::Range(RangePredicate { range })])
    }

    fn create_applier_from_pattern(pattern: &str) -> Result<IntersectionFstApplier> {
        IntersectionFstApplier::try_from(vec![Predicate::RegexMatch(RegexMatchPredicate {
            pattern: pattern.to_string(),
        })])
    }

    #[test]
    fn test_intersection_fst_applier_with_ranges() {
        let test_fst = FstMap::from_iter([("aa", 1), ("bb", 2), ("cc", 3)]).unwrap();

        let applier_inclusive_lower = create_applier_from_range(Range {
            lower: Some(Bound {
                value: b"bb".to_vec(),
                inclusive: true,
            }),
            upper: None,
        })
        .unwrap();
        let results = applier_inclusive_lower.apply(&test_fst);
        assert_eq!(results, vec![2, 3]);

        let applier_exclusive_lower = create_applier_from_range(Range {
            lower: Some(Bound {
                value: b"bb".to_vec(),
                inclusive: false,
            }),
            upper: None,
        })
        .unwrap();
        let results = applier_exclusive_lower.apply(&test_fst);
        assert_eq!(results, vec![3]);

        let applier_inclusive_upper = create_applier_from_range(Range {
            lower: None,
            upper: Some(Bound {
                value: b"bb".to_vec(),
                inclusive: true,
            }),
        })
        .unwrap();
        let results = applier_inclusive_upper.apply(&test_fst);
        assert_eq!(results, vec![1, 2]);

        let applier_exclusive_upper = create_applier_from_range(Range {
            lower: None,
            upper: Some(Bound {
                value: b"bb".to_vec(),
                inclusive: false,
            }),
        })
        .unwrap();
        let results = applier_exclusive_upper.apply(&test_fst);
        assert_eq!(results, vec![1]);

        let applier_inclusive_bounds = create_applier_from_range(Range {
            lower: Some(Bound {
                value: b"aa".to_vec(),
                inclusive: true,
            }),
            upper: Some(Bound {
                value: b"cc".to_vec(),
                inclusive: true,
            }),
        })
        .unwrap();
        let results = applier_inclusive_bounds.apply(&test_fst);
        assert_eq!(results, vec![1, 2, 3]);

        let applier_exclusive_bounds = create_applier_from_range(Range {
            lower: Some(Bound {
                value: b"aa".to_vec(),
                inclusive: false,
            }),
            upper: Some(Bound {
                value: b"cc".to_vec(),
                inclusive: false,
            }),
        })
        .unwrap();
        let results = applier_exclusive_bounds.apply(&test_fst);
        assert_eq!(results, vec![2]);
    }

    #[test]
    fn test_intersection_fst_applier_with_valid_pattern() {
        let test_fst = FstMap::from_iter([("123", 1), ("abc", 2)]).unwrap();

        let cases = vec![
            ("1", vec![1]),
            ("2", vec![1]),
            ("3", vec![1]),
            ("^1", vec![1]),
            ("^2", vec![]),
            ("^3", vec![]),
            ("^1.*", vec![1]),
            ("^.*2", vec![1]),
            ("^.*3", vec![1]),
            ("1$", vec![]),
            ("2$", vec![]),
            ("3$", vec![1]),
            ("1.*$", vec![1]),
            ("2.*$", vec![1]),
            ("3.*$", vec![1]),
            ("^1..$", vec![1]),
            ("^.2.$", vec![1]),
            ("^..3$", vec![1]),
            ("^[0-9]", vec![1]),
            ("^[0-9]+$", vec![1]),
            ("^[0-9][0-9]$", vec![]),
            ("^[0-9][0-9][0-9]$", vec![1]),
            ("^123$", vec![1]),
            ("a", vec![2]),
            ("b", vec![2]),
            ("c", vec![2]),
            ("^a", vec![2]),
            ("^b", vec![]),
            ("^c", vec![]),
            ("^a.*", vec![2]),
            ("^.*b", vec![2]),
            ("^.*c", vec![2]),
            ("a$", vec![]),
            ("b$", vec![]),
            ("c$", vec![2]),
            ("a.*$", vec![2]),
            ("b.*$", vec![2]),
            ("c.*$", vec![2]),
            ("^.[a-z]", vec![2]),
            ("^abc$", vec![2]),
            ("^ab$", vec![]),
            ("abc$", vec![2]),
            ("^a.c$", vec![2]),
            ("^..c$", vec![2]),
            ("ab", vec![2]),
            (".*", vec![1, 2]),
            ("", vec![1, 2]),
            ("^$", vec![]),
            ("1|a", vec![1, 2]),
            ("^123$|^abc$", vec![1, 2]),
            ("^123$|d", vec![1]),
        ];

        for (pattern, expected) in cases {
            let applier = create_applier_from_pattern(pattern).unwrap();
            let results = applier.apply(&test_fst);
            assert_eq!(results, expected);
        }
    }

    #[test]
    fn test_intersection_fst_applier_with_composite_predicates() {
        let test_fst = FstMap::from_iter([("aa", 1), ("bb", 2), ("cc", 3)]).unwrap();

        let applier = IntersectionFstApplier::try_from(vec![
            Predicate::Range(RangePredicate {
                range: Range {
                    lower: Some(Bound {
                        value: b"aa".to_vec(),
                        inclusive: true,
                    }),
                    upper: Some(Bound {
                        value: b"cc".to_vec(),
                        inclusive: true,
                    }),
                },
            }),
            Predicate::RegexMatch(RegexMatchPredicate {
                pattern: "a.?".to_string(),
            }),
        ])
        .unwrap();
        let results = applier.apply(&test_fst);
        assert_eq!(results, vec![1]);

        let applier = IntersectionFstApplier::try_from(vec![
            Predicate::Range(RangePredicate {
                range: Range {
                    lower: Some(Bound {
                        value: b"aa".to_vec(),
                        inclusive: false,
                    }),
                    upper: Some(Bound {
                        value: b"cc".to_vec(),
                        inclusive: true,
                    }),
                },
            }),
            Predicate::RegexMatch(RegexMatchPredicate {
                pattern: "a.?".to_string(),
            }),
        ])
        .unwrap();
        let results = applier.apply(&test_fst);
        assert!(results.is_empty());
    }

    #[test]
    fn test_intersection_fst_applier_with_invalid_pattern() {
        let result = create_applier_from_pattern("a(");
        assert!(matches!(result, Err(Error::ParseDFA { .. })));
    }

    #[test]
    fn test_intersection_fst_applier_with_empty_predicates() {
        let result = IntersectionFstApplier::try_from(vec![]);
        assert!(matches!(result, Err(Error::EmptyPredicates { .. })));
    }

    #[test]
    fn test_intersection_fst_applier_with_in_list_predicate() {
        let result = IntersectionFstApplier::try_from(vec![Predicate::InList(InListPredicate {
            list: HashSet::from_iter([b"one".to_vec(), b"two".to_vec()]),
        })]);
        assert!(matches!(
            result,
            Err(Error::IntersectionApplierWithInList { .. })
        ));
    }

    #[test]
    fn test_intersection_fst_applier_memory_usage() {
        let applier = IntersectionFstApplier {
            ranges: vec![],
            dfas: vec![],
        };

        assert_eq!(applier.memory_usage(), 0);

        let dfa = DFA::new("^abc$").unwrap();
        assert_eq!(dfa.memory_usage(), 320);

        let applier = IntersectionFstApplier {
            ranges: vec![Range {
                lower: Some(Bound {
                    value: b"aa".to_vec(),
                    inclusive: true,
                }),
                upper: Some(Bound {
                    value: b"cc".to_vec(),
                    inclusive: true,
                }),
            }],
            dfas: vec![dfa],
        };

        assert_eq!(
            applier.memory_usage(),
            size_of::<Range>() + 4 + size_of::<DFA<Vec<u32>>>() + 320
        );
    }
}
