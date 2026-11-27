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

use std::sync::Arc;

use super::{Label, LabelStorage, SeriesLabels};

fn label(name: &str, value: &str) -> Label {
    Label {
        name: Arc::from(name),
        value: Arc::from(value),
    }
}

/// The unconditionally rebuilding `keep_only` the early return replaces, as an oracle.
fn keep_only_oracle(metric: &mut SeriesLabels, labels: &[&str]) {
    if !labels.contains(&"__name__") {
        metric.name = None;
    }
    let kept: Vec<_> = metric
        .tags()
        .iter()
        .filter(|tag| labels.contains(&tag.name.as_ref()))
        .cloned()
        .collect();
    if kept.len() != metric.tags().len() {
        metric.tags = LabelStorage::Owned(kept.into());
    }
}

/// The unconditionally rebuilding `remove_all` the early return replaces, as an oracle.
fn remove_all_oracle(metric: &mut SeriesLabels, labels: &[&str]) {
    if labels.contains(&"__name__") {
        metric.name = None;
    }
    let kept: Vec<_> = metric
        .tags()
        .iter()
        .filter(|tag| !labels.contains(&tag.name.as_ref()))
        .cloned()
        .collect();
    if kept.len() != metric.tags().len() {
        metric.tags = LabelStorage::Owned(kept.into());
    }
}

#[test]
fn metric_name_sorts_labels_and_rejects_ambiguous_identity() {
    let metric = SeriesLabels::try_new(
        Some(Arc::from("cpu")),
        vec![label("zone", "west"), label("host", "node-1")],
    )
    .unwrap();
    assert_eq!(metric.name(), Some("cpu"));
    assert_eq!(metric.tags()[0].name.as_ref(), "host");
    assert_eq!(metric.tags()[1].name.as_ref(), "zone");

    assert!(SeriesLabels::try_new(None, vec![label("x", "1"), label("x", "2")]).is_err());
    assert!(SeriesLabels::try_new(None, vec![label("__name__", "cpu")]).is_err());
}

#[test]
fn metric_identity_uses_contents_and_distinguishes_absent_from_empty() {
    let one = SeriesLabels::try_new(
        Some(Arc::from(String::from("cpu"))),
        vec![label("empty", ""), label("host", "a")],
    )
    .unwrap();
    let same = SeriesLabels::try_new(
        Some(Arc::from(String::from("cpu"))),
        vec![label("host", "a"), label("empty", "")],
    )
    .unwrap();
    assert_eq!(one, same);
    assert_eq!(one.get("empty"), Some(""));
    assert_eq!(one.get("missing"), None);
    assert_eq!(one.get("__name__"), Some("cpu"));
}

#[test]
fn metric_name_edits_retain_shared_labels_and_labels_are_copy_on_write() {
    let mut metric = SeriesLabels::try_new(
        Some(Arc::from("cpu")),
        vec![label("host", "a"), label("zone", "west")],
    )
    .unwrap();
    let sibling = metric.clone();
    assert!(std::ptr::eq(metric.tags(), sibling.tags()));

    metric.set("__name__", Arc::from("memory")).unwrap();
    assert!(std::ptr::eq(metric.tags(), sibling.tags()));
    assert_eq!(sibling.name(), Some("cpu"));

    metric.set("host", Arc::from("b")).unwrap();
    metric.set("rack", Arc::from("r1")).unwrap();
    assert_eq!(sibling.get("host"), Some("a"));
    assert_eq!(sibling.get("rack"), None);
    assert_eq!(
        metric
            .tags()
            .iter()
            .map(|tag| tag.name.as_ref())
            .collect::<Vec<_>>(),
        ["host", "rack", "zone"]
    );
    metric.remove("zone");
    assert_eq!(metric.get("zone"), None);
    assert_eq!(sibling.get("zone"), Some("west"));
}

#[test]
fn keep_only_and_remove_all_handle_metric_name_and_ordinary_labels() {
    let mut metric = SeriesLabels::try_new(
        Some(Arc::from("cpu")),
        vec![label("host", "a"), label("zone", "west")],
    )
    .unwrap();
    metric.keep_only(&["host"]);
    assert_eq!(metric.name(), None);
    assert_eq!(metric.tags().len(), 1);
    assert_eq!(metric.get("host"), Some("a"));

    metric.set("__name__", Arc::from("cpu")).unwrap();
    metric.set("zone", Arc::from("west")).unwrap();
    metric.remove_all(&["host", "__name__"]);
    assert_eq!(metric.name(), None);
    assert_eq!(metric.get("host"), None);
    assert_eq!(metric.get("zone"), Some("west"));
    metric.remove_all(&["zone"]);
    assert!(metric.tags().is_empty());
}

/// Ordinary labels as `(name, value)` pairs.
type Tags<'a> = &'a [(&'a str, &'a str)];

/// The labels an operation names.
type Names<'a> = &'a [&'a str];

/// `(metric name, ordinary labels, labels the operation names)`.
type LabelCase = (Option<&'static str>, Tags<'static>, Names<'static>);

const LABEL_CASES: &[LabelCase] = &[
    // Nothing to drop: the existing label array already is the answer.
    (
        Some("cpu"),
        &[("host", "a"), ("zone", "west")],
        &["__name__", "host", "zone"],
    ),
    // No ordinary labels at all.
    (Some("cpu"), &[], &["__name__", "host"]),
    (None, &[], &[]),
    // Only the metric name changes; the label array stays shared.
    (
        Some("cpu"),
        &[("host", "a"), ("zone", "west")],
        &["host", "zone"],
    ),
    // Partial removal, with the metric name kept or dropped.
    (
        Some("cpu"),
        &[("host", "a"), ("zone", "west")],
        &["__name__", "host"],
    ),
    (Some("cpu"), &[("host", "a"), ("zone", "west")], &["host"]),
    // Every label removed or kept, and a selection the labels do not intersect.
    (Some("cpu"), &[("host", "a"), ("zone", "west")], &[]),
    (None, &[("host", "a")], &["host", "other"]),
];

/// Differentially checks one operation against its unconditionally rebuilding oracle.
fn assert_operation(
    apply: fn(&mut SeriesLabels, &[&str]),
    oracle: fn(&mut SeriesLabels, &[&str]),
    drops_tag: fn(Tags<'_>, Names<'_>) -> bool,
) {
    for &(name, tags, selection) in LABEL_CASES {
        let case = format!("name={name:?} tags={tags:?} selection={selection:?}");
        let input = SeriesLabels::try_new(
            name.map(Arc::from),
            tags.iter()
                .map(|&(name, value)| label(name, value))
                .collect(),
        )
        .unwrap();

        let mut actual = input.clone();
        apply(&mut actual, selection);
        let mut expected = input.clone();
        oracle(&mut expected, selection);

        assert_eq!(actual.name(), expected.name(), "{case}");
        assert_eq!(actual.tags().len(), expected.tags().len(), "{case}");
        for (got, want) in actual.tags().iter().zip(expected.tags()) {
            assert_eq!(
                (got.name.as_ref(), got.value.as_ref()),
                (want.name.as_ref(), want.value.as_ref()),
                "{case}"
            );
            // Kept labels keep sharing the strings they shared before.
            assert!(Arc::ptr_eq(&got.name, &want.name), "{case}");
            assert!(Arc::ptr_eq(&got.value, &want.value), "{case}");
        }
        assert_eq!(actual, expected, "{case}");

        if !drops_tag(tags, selection) {
            assert!(
                std::ptr::eq(actual.tags(), input.tags()),
                "unchanged labels must stay shared instead of being rebuilt: {case}"
            );
            // Nothing was dropped and the metric name did not change: the result is the input.
            if actual.name() == input.name() {
                assert_eq!(actual, input, "{case}");
            }
        }
    }
}

#[test]
fn keep_only_leaves_unchanged_labels_shared() {
    assert_operation(
        SeriesLabels::keep_only,
        keep_only_oracle,
        |tags, selection| tags.iter().any(|(name, _)| !selection.contains(name)),
    );
}

#[test]
fn remove_all_leaves_unchanged_labels_shared() {
    assert_operation(
        SeriesLabels::remove_all,
        remove_all_oracle,
        |tags, selection| tags.iter().any(|(name, _)| selection.contains(name)),
    );
}

#[test]
fn batch_labels_borrow_values_and_retain_owner() {
    use super::{LabelStorage, SeriesLabelBatch};
    use arrow::array::{ArrayRef, StringArray};
    use arrow::record_batch::RecordBatch;
    let owner = Arc::new(());
    let weak = Arc::downgrade(&owner);
    let batch = RecordBatch::try_from_iter(vec![(
        "host",
        Arc::new(StringArray::from(vec![Some(""), None, Some("node")])) as ArrayRef,
    )])
    .unwrap();
    let batch =
        Arc::new(SeriesLabelBatch::try_new(batch, vec![(Arc::from("host"), 0)], owner).unwrap());
    let empty = SeriesLabels::from_batch(Some(Arc::from("cpu")), batch.clone(), 0).unwrap();
    let absent = SeriesLabels::from_batch(None, batch.clone(), 1).unwrap();
    assert!(SeriesLabels::from_batch(None, batch.clone(), 3).is_err());
    assert_eq!(empty.get("host"), Some(""));
    assert_eq!(absent.get("host"), None);
    let LabelStorage::Batch(view) = &empty.tags else {
        panic!("expected row view")
    };
    assert!(view.materialized.get().is_none());
    let mut clone = empty.clone();
    clone.set("host", Arc::from("other")).unwrap();
    assert_eq!(empty.get("host"), Some(""));
    assert_eq!(clone.get("host"), Some("other"));
    assert_eq!(
        empty,
        SeriesLabels::try_new(Some(Arc::from("cpu")), vec![label("host", "")]).unwrap()
    );
    // Representation changes must preserve keyed collections and persisted labels.
    let owned = SeriesLabels::try_new(Some(Arc::from("cpu")), vec![label("host", "")]).unwrap();
    let hash = |labels: &SeriesLabels| {
        use std::hash::{Hash, Hasher};
        let mut state = std::collections::hash_map::DefaultHasher::new();
        labels.hash(&mut state);
        state.finish()
    };
    assert_eq!(hash(&owned), hash(&empty));
    let encoded = serde_json::to_string(&empty).unwrap();
    assert_eq!(encoded, serde_json::to_string(&owned).unwrap());
    assert_eq!(
        empty,
        serde_json::from_str::<SeriesLabels>(&encoded).unwrap()
    );
    drop(batch);
    drop(absent);
    assert!(weak.upgrade().is_some());
    drop(empty);
    assert!(weak.upgrade().is_none());
}

#[test]
fn detached_batch_labels_release_the_batch() {
    use super::{LabelStorage, SeriesLabelBatch};
    use arrow::array::{ArrayRef, StringArray};
    use arrow::record_batch::RecordBatch;
    let owner = Arc::new(());
    let weak = Arc::downgrade(&owner);
    let batch = RecordBatch::try_from_iter(vec![(
        "host",
        Arc::new(StringArray::from(vec![Some("node")])) as ArrayRef,
    )])
    .unwrap();
    let batch =
        Arc::new(SeriesLabelBatch::try_new(batch, vec![(Arc::from("host"), 0)], owner).unwrap());
    let view = SeriesLabels::from_batch(Some(Arc::from("cpu")), batch.clone(), 0).unwrap();
    assert!(matches!(view.tags, LabelStorage::Batch(_)));

    let detached = view.detached();
    let LabelStorage::Owned(tags) = &detached.tags else {
        panic!("expected owned labels")
    };
    assert_eq!(view.tags(), &tags[..]);

    // The view keeps the batch alive; the detached value holds the materialized array instead,
    // so dropping both the view and the batch handle releases the owner.
    drop(view);
    assert!(weak.upgrade().is_some(), "the batch handle still owns it");
    drop(batch);
    assert!(
        weak.upgrade().is_none(),
        "a detached value must not keep the scan batch alive"
    );

    // Owned labels keep sharing their array rather than copying it.
    let owned = SeriesLabels::try_new(Some(Arc::from("cpu")), vec![label("host", "node")]).unwrap();
    let again = owned.detached();
    let (LabelStorage::Owned(a), LabelStorage::Owned(b)) = (&owned.tags, &again.tags) else {
        panic!("expected owned labels")
    };
    assert!(Arc::ptr_eq(a, b));
}
