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
use std::mem::size_of;

use snafu::{ensure, ResultExt};

use crate::inverted_index::error::{
    EmptyPredicatesSnafu, KeysApplierUnexpectedPredicatesSnafu, KeysApplierWithoutInListSnafu,
    ParseRegexSnafu, Result,
};
use crate::inverted_index::search::fst_apply::FstApplier;
use crate::inverted_index::search::predicate::Predicate;
use crate::inverted_index::{Bytes, FstMap};

/// `KeysFstApplier` is responsible for applying a search using a set of predefined keys
/// against an FstMap to fetch associated values.
pub struct KeysFstApplier {
    /// A list of keys to be fetched directly from the FstMap.
    keys: Vec<Bytes>,
}

impl FstApplier for KeysFstApplier {
    fn apply(&self, fst: &FstMap) -> Vec<u64> {
        self.keys.iter().filter_map(|k| fst.get(k)).collect()
    }

    fn memory_usage(&self) -> usize {
        self.keys.capacity() * size_of::<Bytes>()
            + self.keys.iter().map(|k| k.capacity()).sum::<usize>()
    }
}

impl KeysFstApplier {
    /// Tries to create a `KeysFstApplier` from a list of predicates.
    ///
    /// This function constructs the applier by intersecting keys from one or more `InList` predicates,
    /// which are required. It then optionally refines this set using any additional `Range` and `RegexMatch`
    /// predicates provided.
    pub fn try_from(mut predicates: Vec<Predicate>) -> Result<Self> {
        ensure!(!predicates.is_empty(), EmptyPredicatesSnafu);

        let (in_lists, others) = Self::split_at_in_lists(&mut predicates);
        let (ranges, regexes) = Self::split_at_ranges(others);
        Self::ensure_all_regexes(regexes)?;

        ensure!(!in_lists.is_empty(), KeysApplierWithoutInListSnafu);
        let intersected_keys = Self::intersect_with_lists(in_lists);
        let range_matched_keys = Self::filter_by_ranges(intersected_keys, ranges);
        let regex_matched_keys = Self::filter_by_regexes(range_matched_keys, regexes)?;

        Ok(Self {
            keys: regex_matched_keys,
        })
    }

    fn split_at_in_lists(predicates: &mut [Predicate]) -> (&mut [Predicate], &mut [Predicate]) {
        let in_list_index = predicates
            .iter_mut()
            .partition_in_place(|p| matches!(p, Predicate::InList(_)));
        predicates.split_at_mut(in_list_index)
    }

    fn split_at_ranges(predicates: &mut [Predicate]) -> (&mut [Predicate], &mut [Predicate]) {
        let range_index = predicates
            .iter_mut()
            .partition_in_place(|p| matches!(p, Predicate::Range(_)));
        predicates.split_at_mut(range_index)
    }

    fn ensure_all_regexes(ps: &[Predicate]) -> Result<()> {
        ensure!(
            ps.iter().all(|p| matches!(p, Predicate::RegexMatch(_))),
            KeysApplierUnexpectedPredicatesSnafu {
                predicates: ps.to_vec()
            }
        );
        Ok(())
    }

    fn intersect_with_lists(in_lists: &mut [Predicate]) -> Vec<Bytes> {
        #[inline]
        fn get_list(p: &Predicate) -> &HashSet<Bytes> {
            match p {
                Predicate::InList(i) => &i.list,
                _ => unreachable!(), // `in_lists` is filtered by `split_at_in_lists`
            }
        }

        in_lists.sort_unstable_by_key(|p| get_list(p).len());
        get_list(&in_lists[0])
            .iter()
            .filter(|c| in_lists[1..].iter().all(|s| get_list(s).contains(*c)))
            .cloned()
            .collect()
    }

    fn filter_by_ranges(mut keys: Vec<Bytes>, ranges: &[Predicate]) -> Vec<Bytes> {
        #[inline]
        fn range_contains(p: &Predicate, key: &Bytes) -> bool {
            let (lower, upper) = match p {
                Predicate::Range(r) => (&r.range.lower, &r.range.upper),
                _ => unreachable!(), // `ranges` is filtered by `split_at_ranges`
            };

            match (lower, upper) {
                (Some(lower), Some(upper)) => match (lower.inclusive, upper.inclusive) {
                    (true, true) => &lower.value <= key && key <= &upper.value,
                    (true, false) => &lower.value <= key && key < &upper.value,
                    (false, true) => &lower.value < key && key <= &upper.value,
                    (false, false) => &lower.value < key && key < &upper.value,
                },
                (Some(lower), None) => match lower.inclusive {
                    true => &lower.value <= key,
                    false => &lower.value < key,
                },
                (None, Some(upper)) => match upper.inclusive {
                    true => key <= &upper.value,
                    false => key < &upper.value,
                },
                (None, None) => true,
            }
        }

        keys.retain(|k| ranges.iter().all(|r| range_contains(r, k)));
        keys
    }

    fn filter_by_regexes(mut keys: Vec<Bytes>, regexes: &[Predicate]) -> Result<Vec<Bytes>> {
        for p in regexes {
            let pattern = match p {
                Predicate::RegexMatch(r) => &r.pattern,
                _ => unreachable!(), // checked by `ensure_all_regexes`
            };

            let regex = regex::Regex::new(pattern).with_context(|_| ParseRegexSnafu {
                pattern: pattern.to_owned(),
            })?;

            keys.retain(|k| {
                std::str::from_utf8(k)
                    .map(|k| regex.is_match(k))
                    .unwrap_or_default()
            });
            if keys.is_empty() {
                return Ok(keys);
            }
        }

        Ok(keys)
    }
}

impl TryFrom<Vec<Predicate>> for KeysFstApplier {
    type Error = crate::inverted_index::error::Error;
    fn try_from(predicates: Vec<Predicate>) -> Result<Self> {
        Self::try_from(predicates)
    }
}

#[cfg(test)]
mod tests {
    use fst::Map as FstMap;

    use super::*;
    use crate::inverted_index::error::Error;
    use crate::inverted_index::search::predicate::{
        Bound, InListPredicate, Predicate, Range, RangePredicate, RegexMatchPredicate,
    };

    fn create_fst_map(items: &[(&[u8], u64)]) -> FstMap<Vec<u8>> {
        let mut items = items
            .iter()
            .map(|(k, v)| (k.to_vec(), *v))
            .collect::<Vec<_>>();
        items.sort();
        FstMap::from_iter(items).unwrap()
    }

    fn b(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }

    #[test]
    fn test_keys_fst_applier_apply() {
        let test_fst = create_fst_map(&[(b"foo", 1), (b"bar", 2), (b"baz", 3)]);
        let applier = KeysFstApplier {
            keys: vec![b("foo"), b("baz")],
        };

        let results = applier.apply(&test_fst);
        assert_eq!(results, vec![1, 3]);
    }

    #[test]
    fn test_keys_fst_applier_with_empty_keys() {
        let test_fst = create_fst_map(&[(b"foo", 1), (b"bar", 2), (b"baz", 3)]);
        let applier = KeysFstApplier { keys: vec![] };

        let results = applier.apply(&test_fst);
        assert!(results.is_empty());
    }

    #[test]
    fn test_keys_fst_applier_with_unmatched_keys() {
        let test_fst = create_fst_map(&[(b"foo", 1), (b"bar", 2), (b"baz", 3)]);
        let applier = KeysFstApplier {
            keys: vec![b("qux"), b("quux")],
        };

        let results = applier.apply(&test_fst);
        assert!(results.is_empty());
    }

    #[test]
    fn test_keys_fst_applier_try_from() {
        let predicates = vec![
            Predicate::InList(InListPredicate {
                list: HashSet::from_iter(vec![b("foo"), b("bar")]),
            }),
            Predicate::Range(RangePredicate {
                range: Range {
                    lower: Some(Bound {
                        value: b("bar"),
                        inclusive: true,
                    }),
                    upper: None,
                },
            }),
            Predicate::RegexMatch(RegexMatchPredicate {
                pattern: ".*r".to_string(),
            }),
        ];
        let applier = KeysFstApplier::try_from(predicates).unwrap();
        assert_eq!(applier.keys, vec![b("bar")]);
    }

    #[test]
    fn test_keys_fst_applier_try_from_filter_out_unmatched_keys() {
        let predicates = vec![
            Predicate::InList(InListPredicate {
                list: HashSet::from_iter(vec![b("foo"), b("bar")]),
            }),
            Predicate::Range(RangePredicate {
                range: Range {
                    lower: Some(Bound {
                        value: b("f"),
                        inclusive: true,
                    }),
                    upper: None,
                },
            }),
            Predicate::RegexMatch(RegexMatchPredicate {
                pattern: ".*o".to_string(),
            }),
        ];
        let applier = KeysFstApplier::try_from(predicates).unwrap();
        assert_eq!(applier.keys, vec![b("foo")]);
    }

    #[test]
    fn test_keys_fst_applier_try_from_empty_predicates() {
        let predicates = vec![];
        let result = KeysFstApplier::try_from(predicates);
        assert!(matches!(result, Err(Error::EmptyPredicates { .. })));
    }

    #[test]
    fn test_keys_fst_applier_try_from_without_in_list() {
        let predicates = vec![Predicate::Range(RangePredicate {
            range: Range {
                lower: Some(Bound {
                    value: b("bar"),
                    inclusive: true,
                }),
                upper: None,
            },
        })];
        let result = KeysFstApplier::try_from(predicates);
        assert!(matches!(
            result,
            Err(Error::KeysApplierWithoutInList { .. })
        ));
    }

    #[test]
    fn test_keys_fst_applier_try_from_with_invalid_regex() {
        let predicates = vec![
            Predicate::InList(InListPredicate {
                list: HashSet::from_iter(vec![b("foo"), b("bar")]),
            }),
            Predicate::RegexMatch(RegexMatchPredicate {
                pattern: "*invalid regex".to_string(),
            }),
        ];
        let result = KeysFstApplier::try_from(predicates);
        assert!(matches!(result, Err(Error::ParseRegex { .. })));
    }

    #[test]
    fn test_keys_fst_applier_memory_usage() {
        let applier = KeysFstApplier { keys: vec![] };
        assert_eq!(applier.memory_usage(), 0);

        let applier = KeysFstApplier {
            keys: vec![b("foo"), b("bar")],
        };
        assert_eq!(applier.memory_usage(), 2 * size_of::<Bytes>() + 6);
    }
}
