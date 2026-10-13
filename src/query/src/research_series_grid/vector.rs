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

use std::collections::BTreeMap;

pub(crate) type Labels = BTreeMap<String, String>;

/// One label set with values at the query's shared evaluation instants.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SeriesVector {
    pub labels: Labels,
    pub values: Vec<Option<f64>>,
}

impl SeriesVector {
    pub(crate) fn memory_size(&self) -> usize {
        // Include spare capacity and a conservative allowance for BTreeMap nodes.
        self.labels.iter().fold(
            self.values
                .capacity()
                .saturating_mul(size_of::<Option<f64>>())
                .saturating_add(2 * size_of::<Self>()),
            |bytes, (name, value)| {
                bytes
                    .saturating_add(name.capacity())
                    .saturating_add(value.capacity())
                    .saturating_add(256)
            },
        )
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum Aggregate {
    Sum,
    Max,
    Count,
}

/// `by` retains these names; `without` removes them and the metric name.
pub(crate) fn group_labels(labels: &Labels, names: &[String], without: bool) -> Labels {
    labels
        .iter()
        .filter(|(name, _)| {
            if without {
                name.as_str() != "__name__" && !names.contains(name)
            } else {
                names.contains(name)
            }
        })
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

pub(crate) fn aggregate(
    input: Vec<SeriesVector>,
    names: &[String],
    without: bool,
    operation: Aggregate,
    points: usize,
) -> Result<Vec<SeriesVector>, &'static str> {
    let mut groups: BTreeMap<Labels, Vec<Option<f64>>> = BTreeMap::new();
    for series in input {
        if series.values.len() != points {
            return Err("series does not match evaluation grid");
        }
        let values = groups
            .entry(group_labels(&series.labels, names, without))
            .or_insert_with(|| vec![None; points]);
        for (output, value) in values.iter_mut().zip(series.values) {
            if let Some(value) = value {
                let value = if matches!(operation, Aggregate::Count) {
                    1.0
                } else {
                    value
                };
                *output = Some(match *output {
                    None => value,
                    Some(previous) => match operation {
                        Aggregate::Sum | Aggregate::Count => previous + value,
                        Aggregate::Max => match value.partial_cmp(&previous) {
                            Some(std::cmp::Ordering::Greater) | None => value,
                            _ => previous,
                        },
                    },
                });
            }
        }
    }
    Ok(groups
        .into_iter()
        .filter(|(_, values)| values.iter().any(Option::is_some))
        .map(|(labels, values)| SeriesVector { labels, values })
        .collect())
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum Arithmetic {
    Multiply,
    Divide,
}

impl Arithmetic {
    pub fn apply(self, left: f64, right: f64) -> f64 {
        match self {
            Self::Multiply => left * right,
            Self::Divide => left / right,
        }
    }
}

pub(crate) fn broadcast(
    left: &[SeriesVector],
    right: &[SeriesVector],
    labels_from_left: bool,
    operation: Arithmetic,
    points: usize,
) -> Result<Vec<SeriesVector>, &'static str> {
    if left
        .iter()
        .chain(right)
        .any(|series| series.values.len() != points)
    {
        return Err("series does not match evaluation grid");
    }
    let mut result: BTreeMap<Labels, Vec<Option<f64>>> = BTreeMap::new();
    for lhs in left {
        for rhs in right {
            let mut labels = if labels_from_left {
                &lhs.labels
            } else {
                &rhs.labels
            }
            .clone();
            labels.remove("__name__");
            let values = result.entry(labels).or_insert_with(|| vec![None; points]);
            for ((output, a), b) in values.iter_mut().zip(&lhs.values).zip(&rhs.values) {
                if let (Some(a), Some(b)) = (a, b) {
                    if output.is_some() {
                        return Err("duplicate result label set at an evaluation instant");
                    }
                    *output = Some(operation.apply(*a, *b));
                }
            }
        }
    }
    Ok(result
        .into_iter()
        .filter(|(_, values)| values.iter().any(Option::is_some))
        .map(|(labels, values)| SeriesVector { labels, values })
        .collect())
}

/// Matching is indexed by series labels; uniqueness is checked at each instant.
pub(crate) fn binary(
    left: &[SeriesVector],
    right: &[SeriesVector],
    on: Option<&[String]>,
    group_left: Option<&[String]>,
    operation: Arithmetic,
    points: usize,
) -> Result<Vec<SeriesVector>, &'static str> {
    if left
        .iter()
        .chain(right)
        .any(|series| series.values.len() != points)
    {
        return Err("series does not match evaluation grid");
    }
    let key = |labels: &Labels| match on {
        Some(names) => group_labels(labels, names, false),
        None => group_labels(labels, &[], true),
    };
    let mut rhs: BTreeMap<Labels, (Vec<&SeriesVector>, Vec<u8>)> = BTreeMap::new();
    for series in right {
        let (series_group, active_counts) = rhs
            .entry(key(&series.labels))
            .or_insert_with(|| (Vec::new(), vec![0; points]));
        series_group.push(series);
        for (count, value) in active_counts.iter_mut().zip(&series.values) {
            if value.is_some() {
                *count = (*count + 1).min(2);
            }
        }
    }
    let mut matched_left: BTreeMap<Labels, Vec<bool>> = BTreeMap::new();
    let mut result: BTreeMap<Labels, Vec<Option<f64>>> = BTreeMap::new();
    for lhs in left {
        let match_key = key(&lhs.labels);
        let Some((candidates, active_counts)) = rhs.get(&match_key) else {
            continue;
        };
        let seen = matched_left
            .entry(match_key)
            .or_insert_with(|| vec![false; points]);
        for rhs in candidates {
            let mut labels = if group_left.is_none() {
                match on {
                    Some(names) => group_labels(&lhs.labels, names, false),
                    None => lhs.labels.clone(),
                }
            } else {
                lhs.labels.clone()
            };
            labels.remove("__name__");
            if let Some(names) = group_left {
                for name in names {
                    if let Some(value) = rhs.labels.get(name) {
                        labels.insert(name.clone(), value.clone());
                    } else {
                        labels.remove(name);
                    }
                }
            }
            let values = result.entry(labels).or_insert_with(|| vec![None; points]);
            for index in 0..points {
                if let (Some(a), Some(b)) = (lhs.values[index], rhs.values[index]) {
                    if group_left.is_none() && seen[index] {
                        return Err("multiple series match at an evaluation instant");
                    }
                    // A group-left match must also have exactly one active RHS.
                    if active_counts[index] != 1 {
                        return Err("multiple right series match at an evaluation instant");
                    }
                    if values[index].is_some() {
                        return Err("duplicate result label set at an evaluation instant");
                    }
                    seen[index] = true;
                    values[index] = Some(operation.apply(a, b));
                }
            }
        }
    }
    Ok(result
        .into_iter()
        .filter(|(_, values)| values.iter().any(Option::is_some))
        .map(|(labels, values)| SeriesVector { labels, values })
        .collect())
}

/// Selects top-k at each instant, with GT's descending tag tie-break order.
/// `tag_order` is the input schema order, not the map's label-name order.
pub(crate) fn topk(
    input: &[SeriesVector],
    names: &[String],
    without: bool,
    k: usize,
    points: usize,
    tag_order: &[String],
) -> Result<Vec<SeriesVector>, &'static str> {
    use std::cmp::Ordering;

    if input.iter().any(|series| series.values.len() != points) {
        return Err("series does not match evaluation grid");
    }
    if k == 0 {
        return Ok(Vec::new());
    }
    let mut groups: BTreeMap<Labels, Vec<usize>> = BTreeMap::new();
    for (index, series) in input.iter().enumerate() {
        groups
            .entry(group_labels(&series.labels, names, without))
            .or_default()
            .push(index);
    }
    let mut output: BTreeMap<usize, Vec<Option<f64>>> = BTreeMap::new();
    let mut candidates = Vec::new();
    for members in groups.values() {
        for instant in 0..points {
            candidates.clear();
            candidates.extend(
                members
                    .iter()
                    .filter_map(|&index| input[index].values[instant].map(|value| (index, value))),
            );
            let compare = |a: &(usize, f64), b: &(usize, f64)| {
                b.1.total_cmp(&a.1).then_with(|| {
                    for name in tag_order {
                        let order = match (input[a.0].labels.get(name), input[b.0].labels.get(name))
                        {
                            (None, None) => Ordering::Equal,
                            (None, Some(_)) => Ordering::Less,
                            (Some(_), None) => Ordering::Greater,
                            (Some(a), Some(b)) => b.cmp(a),
                        };
                        if order != Ordering::Equal {
                            return order;
                        }
                    }
                    Ordering::Equal
                })
            };
            if k < candidates.len() {
                candidates.select_nth_unstable_by(k, compare);
            }
            for &(index, value) in candidates.iter().take(k) {
                output.entry(index).or_insert_with(|| vec![None; points])[instant] = Some(value);
            }
        }
    }
    Ok(output
        .into_iter()
        .map(|(index, values)| SeriesVector {
            labels: input[index].labels.clone(),
            values,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn series(labels: &[(&str, &str)], values: &[Option<f64>]) -> SeriesVector {
        SeriesVector {
            labels: labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            values: values.to_vec(),
        }
    }

    #[test]
    fn broadcast_preserves_operand_order_labels_and_absence() {
        let unlabeled = [series(&[], &[Some(12.0), None, Some(8.0)])];
        let labeled = [
            series(&[("host", "a")], &[Some(3.0), Some(2.0), None]),
            series(&[("host", "b")], &[Some(6.0), Some(4.0), Some(2.0)]),
        ];
        let result = broadcast(&unlabeled, &labeled, false, Arithmetic::Divide, 3).unwrap();
        assert_eq!(result[0].labels, labeled[0].labels);
        assert_eq!(result[0].values, [Some(4.0), None, None]);
        assert_eq!(result[1].values, [Some(2.0), None, Some(4.0)]);
        let reversed = broadcast(&labeled, &unlabeled, true, Arithmetic::Divide, 3).unwrap();
        assert_eq!(reversed[0].labels, labeled[0].labels);
        assert_eq!(reversed[0].values, [Some(0.25), None, None]);
        assert_eq!(reversed[1].values, [Some(0.5), None, Some(0.25)]);
    }

    #[test]
    fn empty_and_absent_labels_remain_distinct() {
        let empty = series(&[("host", "")], &[Some(2.0)]);
        let absent = series(&[], &[Some(3.0)]);
        let groups = aggregate(
            vec![empty.clone(), absent.clone()],
            &["host".into()],
            false,
            Aggregate::Sum,
            1,
        )
        .unwrap();
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].labels, absent.labels);
        assert_eq!(groups[0].values, [Some(3.0)]);
        assert_eq!(groups[1].labels, empty.labels);
        assert_eq!(groups[1].values, [Some(2.0)]);
        assert!(
            binary(
                &[empty],
                &[absent],
                Some(&["host".into()]),
                None,
                Arithmetic::Multiply,
                1,
            )
            .unwrap()
            .is_empty()
        );
    }

    #[test]
    fn group_left_retains_empty_included_labels() {
        let result = binary(
            &[series(&[], &[Some(2.0)])],
            &[series(&[("host", "")], &[Some(3.0)])],
            Some(&[]),
            Some(&["host".into()]),
            Arithmetic::Multiply,
            1,
        )
        .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].labels.get("host").map(String::as_str), Some(""));
        assert_eq!(result[0].values, [Some(6.0)]);
    }

    #[test]
    fn aggregates_keep_presence_separate_from_nan() {
        let input = vec![
            series(&[("pod", "a")], &[Some(f64::NAN), None, Some(2.0)]),
            series(&[("pod", "b")], &[Some(3.0), None, Some(4.0)]),
        ];
        let max = aggregate(input.clone(), &[], false, Aggregate::Max, 3).unwrap();
        assert_eq!(max[0].values, [Some(3.0), None, Some(4.0)]);
        let mut reversed = input.clone();
        reversed.reverse();
        let max = aggregate(reversed, &[], false, Aggregate::Max, 3).unwrap();
        assert!(max[0].values[0].unwrap().is_nan());
        let sum = aggregate(input, &[], false, Aggregate::Sum, 3).unwrap();
        assert!(sum[0].values[0].unwrap().is_nan());
        assert_eq!(sum[0].values[1..], [None, Some(6.0)]);
    }

    #[test]
    fn count_includes_nan_but_not_missing_samples() {
        let input = vec![
            series(&[], &[Some(f64::NAN), None, Some(0.0)]),
            series(&[("pod", "b")], &[Some(2.0), None, None]),
        ];
        let result = aggregate(input, &[], false, Aggregate::Count, 3).unwrap();
        assert_eq!(result[0].values, [Some(2.0), None, Some(1.0)]);
    }

    #[test]
    fn temporal_rhs_identity_changes_are_not_duplicates() {
        let lhs = [series(&[("pod", "a")], &[Some(2.0), Some(3.0)])];
        let rhs = [
            series(&[("pod", "a"), ("owner", "old")], &[Some(1.0), None]),
            series(&[("pod", "a"), ("owner", "new")], &[None, Some(2.0)]),
        ];
        let result = binary(
            &lhs,
            &rhs,
            Some(&["pod".into()]),
            Some(&["owner".into()]),
            Arithmetic::Multiply,
            2,
        )
        .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(
            result
                .iter()
                .find(|s| s.labels["owner"] == "old")
                .unwrap()
                .values,
            [Some(2.0), None]
        );
        assert_eq!(
            result
                .iter()
                .find(|s| s.labels["owner"] == "new")
                .unwrap()
                .values,
            [None, Some(6.0)]
        );
        let mut overlapping = rhs.clone();
        overlapping[1].values[0] = Some(2.0);
        assert!(
            binary(
                &lhs,
                &overlapping,
                Some(&["pod".into()]),
                Some(&["owner".into()]),
                Arithmetic::Multiply,
                2
            )
            .is_err()
        );
    }

    #[test]
    fn matching_ignores_metric_names_and_preserves_ieee_division() {
        let lhs = [series(
            &[("__name__", "a"), ("empty", "")],
            &[Some(2.0), Some(0.0)],
        )];
        let rhs = [series(
            &[("__name__", "b"), ("empty", "")],
            &[Some(0.0), Some(0.0)],
        )];
        let result = binary(&lhs, &rhs, None, None, Arithmetic::Divide, 2).unwrap();
        assert_eq!(
            result[0].labels,
            Labels::from([("empty".into(), "".into())])
        );
        assert_eq!(result[0].values[0], Some(f64::INFINITY));
        assert!(result[0].values[1].unwrap().is_nan());
    }

    #[test]
    fn topk_changes_membership_and_uses_schema_tie_order() {
        let input = [
            series(&[("a", "z"), ("b", "a")], &[Some(2.0), Some(5.0), None]),
            series(
                &[("a", "a"), ("b", "z")],
                &[Some(2.0), Some(1.0), Some(3.0)],
            ),
        ];
        let result = topk(&input, &[], false, 1, 3, &["b".into(), "a".into()]).unwrap();
        assert_eq!(result[0].values, [None, Some(5.0), None]);
        assert_eq!(result[1].values, [Some(2.0), None, Some(3.0)]);
        assert!(topk(&input, &[], false, 0, 3, &[]).unwrap().is_empty());
    }

    #[test]
    fn topk_uses_arrow_float_order_and_null_first_labels() {
        let input = [
            series(&[], &[Some(1.0), Some(-0.0), Some(f64::NAN)]),
            series(&[("pod", "z")], &[Some(1.0), Some(0.0), Some(2.0)]),
        ];
        let result = topk(&input, &[], false, 1, 3, &["pod".into()]).unwrap();
        assert_eq!(result[0].values[0..2], [Some(1.0), None]);
        assert!(result[0].values[2].unwrap().is_nan());
        assert_eq!(result[1].values, [None, Some(0.0), None]);
    }
}
