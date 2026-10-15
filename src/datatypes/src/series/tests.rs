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

use super::{Label, MetricName};

fn label(name: &str, value: &str) -> Label {
    Label {
        name: Arc::from(name),
        value: Arc::from(value),
    }
}

#[test]
fn metric_name_sorts_labels_and_rejects_ambiguous_identity() {
    let metric = MetricName::try_new(
        Some(Arc::from("cpu")),
        vec![label("zone", "west"), label("host", "node-1")],
    )
    .unwrap();
    assert_eq!(metric.name(), Some("cpu"));
    assert_eq!(metric.tags()[0].name.as_ref(), "host");
    assert_eq!(metric.tags()[1].name.as_ref(), "zone");

    assert!(MetricName::try_new(None, vec![label("x", "1"), label("x", "2")]).is_err());
    assert!(MetricName::try_new(None, vec![label("__name__", "cpu")]).is_err());
}

#[test]
fn metric_identity_uses_contents_and_distinguishes_absent_from_empty() {
    let one = MetricName::try_new(
        Some(Arc::from(String::from("cpu"))),
        vec![label("empty", ""), label("host", "a")],
    )
    .unwrap();
    let same = MetricName::try_new(
        Some(Arc::from(String::from("cpu"))),
        vec![label("host", "a"), label("empty", "")],
    )
    .unwrap();
    assert_eq!(one, same);
    assert_eq!(one.get("empty"), Some(""));
    assert_eq!(one.get("missing"), None);
    assert_eq!(one.get("__name__"), Some("cpu"));
    assert_eq!(one.get("__name_missing"), None);
}

#[test]
fn metric_name_edits_retain_shared_labels_and_labels_are_copy_on_write() {
    let mut metric = MetricName::try_new(
        Some(Arc::from("cpu")),
        vec![label("host", "a"), label("zone", "west")],
    )
    .unwrap();
    let sibling = metric.clone();
    assert!(Arc::ptr_eq(&metric.tags, &sibling.tags));

    metric.set("__name__", Arc::from("memory")).unwrap();
    assert!(Arc::ptr_eq(&metric.tags, &sibling.tags));
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
    let mut metric = MetricName::try_new(
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
