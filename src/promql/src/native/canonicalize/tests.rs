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

//! What the insertion pass has to get right: the tree it produces is one its own validator accepts,
//! and the facts it derives are the ones the nodes below it actually preserve.
//!
//! The trees here are built node by node, over the one-field schemas of the `rate(x[5m])` fixture:
//! a raw scan, the window that folds it onto a grid, and - for `info` - the instant selection that
//! reports each info sample's own time. The compiler lives in the `query` crate, which depends on
//! this one, so a test in this crate cannot call it.

use std::sync::Arc;

use datatypes::series::SeriesOrdering;
use datatypes::series::samples::{FieldSpec, SeriesSchema};
use datatypes::series::time::{EvalGrid, GridSpec};

use super::insert_native_merge_inside_current_fused;
use crate::info::InfoSelector;
use crate::native::plan::{
    BoundInput, BoundInstant, BoundWindow, CanonicalGrouping, CanonicalOwner, InstantValue,
    NativeExpr, NativeNode, NativeProperties, NativeTree, NativeType,
};
use crate::range_eval::{RangeFunction, RangeWindow};

fn grid(start: i64, step: i64, len: u64) -> Arc<EvalGrid> {
    Arc::new(EvalGrid::try_new(GridSpec::try_new(start, step, len).unwrap()).unwrap())
}

fn fields(count: usize) -> Arc<[FieldSpec]> {
    (0..count)
        .map(|index| FieldSpec {
            name: format!("v{index}").into(),
        })
        .collect()
}

fn evaluated(schema_fields: Arc<[FieldSpec]>, grid: Arc<EvalGrid>) -> NativeType {
    NativeType::Samples(Arc::new(SeriesSchema {
        fields: schema_fields,
        eval_grid: Some(grid),
    }))
}

/// A scan of raw samples, the leaf every tree here is built over.
fn raw_input(source_index: usize) -> NativeNode {
    let output = NativeType::Samples(Arc::new(SeriesSchema {
        fields: fields(1),
        eval_grid: None,
    }));
    NativeNode {
        expr: NativeExpr::Input(BoundInput {
            output: output.clone(),
            source_index,
            ordering: SeriesOrdering::KeyThenTime,
        }),
        output,
    }
}

/// `rate(x[5m])` over `inner`: a window that folds raw samples onto `out` and drops the metric name.
fn window_over(inner: NativeNode, out: Arc<EvalGrid>) -> NativeNode {
    NativeNode {
        expr: NativeExpr::Window {
            input: Box::new(inner),
            op: BoundWindow {
                function: RangeFunction::Rate,
                window: RangeWindow {
                    range_ms: 300_000,
                    offset_ms: 0,
                    anchor_ms: None,
                    modifier: None,
                },
                input_grid: None,
                output_grid: Arc::clone(&out),
            },
        },
        output: evaluated(fields(1), out),
    }
}

/// An instant selection over `inner` that reports what `value` says it reports, on `out`.
fn selection(inner: NativeNode, out: Arc<EvalGrid>, value: InstantValue) -> NativeNode {
    NativeNode {
        expr: NativeExpr::Instant {
            input: Box::new(inner),
            op: BoundInstant {
                lookback_ms: 300_000,
                offset_ms: 0,
                at_ms: None,
                value,
                output_grid: Arc::clone(&out),
            },
        },
        output: evaluated(fields(1), out),
    }
}

/// `info(base, {})`: the base series with the data labels of the info series that share theirs.
fn info_over(base: NativeNode, info: NativeNode) -> NativeNode {
    let output = base.output.clone();
    NativeNode {
        expr: NativeExpr::Info {
            base: Box::new(base),
            info: Box::new(info),
            selector: InfoSelector {
                name_matchers: Vec::new(),
                data_labels: Vec::new(),
                keep_unmatched: true,
            },
        },
        output,
    }
}

/// The node for the info side of [`info_over`]: the instant selection that reports sample times.
fn info_selection(out: &Arc<EvalGrid>) -> NativeNode {
    selection(raw_input(1), Arc::clone(out), InstantValue::InfoTimestamp)
}

/// A legal `info` tree survives the pass, and the tree the pass produces is the one its validator
/// accepts.
///
/// This is the contract the pass broke: it wraps every whole-series consumer in a merge, `info`'s
/// info side included, and the validator used to require that side be *directly* an instant
/// selection reporting sample times - so the pass built a tree `NativeTree::try_new` then rejected.
/// The merge only collects the pieces of one label set into one series, so the times it hands on are
/// the times it read, and the check looks through it.
#[test]
fn a_legal_info_tree_survives_canonicalize_and_validation() {
    let out = grid(0, 60_000, 4);
    let tree = NativeTree::try_new(info_over(
        window_over(raw_input(0), Arc::clone(&out)),
        info_selection(&out),
    ))
    .expect("the compiled tree is legal");
    let boundary = [NativeProperties::source_pieces()];
    let passed = insert_native_merge_inside_current_fused(&tree, &boundary)
        .expect("the pass keeps the tree legal");

    // The pass wrapped what it had to; what matters is that the info side still *proves* an instant
    // selection reporting sample times, however many merges sit above it.
    fn through_merges(node: &NativeNode) -> &NativeNode {
        let mut node = node;
        while let NativeExpr::Merge { input } = &node.expr {
            node = input;
        }
        node
    }
    let NativeExpr::Info { info, .. } = &through_merges(passed.root()).expr else {
        panic!("the passed tree is not an `info` node below its merges:\n{passed}");
    };
    let NativeExpr::Instant { op, .. } = &through_merges(info).expr else {
        panic!("the info side is not an instant selection below its merges:\n{passed}");
    };
    assert_eq!(
        InstantValue::InfoTimestamp,
        op.value,
        "the info side no longer proves the sample times `info` reads:\n{passed}"
    );

    // The pass returns a tree it validated, and the same tree validates again from scratch.
    NativeTree::try_new(passed.root().clone()).expect("the passed tree is legal on its own");

    // The pass is idempotent on its own output: every consumer it wrapped is handed a merge already,
    // so a second run finds nothing to add.
    let again = insert_native_merge_inside_current_fused(&passed, &boundary)
        .expect("the pass accepts its own output");
    assert_eq!(
        passed.to_string(),
        again.to_string(),
        "the pass changed its own output"
    );
}

/// An instant selection that reports an info series' own time keeps every fact its input had: the
/// runtime files that series under all of its labels, so which series share a label set is
/// unchanged. `timestamp(x)` is the one that does not - it drops the metric name on the way - and
/// the two are asserted together so the distinction cannot be lost.
#[test]
fn info_timestamp_keeps_its_input_facts() {
    let out = grid(0, 60_000, 4);
    // A boundary that proves something, so a downgrade is visible rather than a coincidence of two
    // `Unknown`s.
    let boundary = [NativeProperties {
        owner: CanonicalOwner::ByCanonicalKey,
        grouping: CanonicalGrouping::SourcePiecesOnly,
    }];
    let facts = |value| selection(raw_input(0), Arc::clone(&out), value).properties(&boundary);

    assert_eq!(
        boundary[0],
        facts(InstantValue::InfoTimestamp),
        "an info timestamp rewrote the label sets it read"
    );
    // The sample itself is answered under the label set it was read from, so it is the same answer.
    assert_eq!(boundary[0], facts(InstantValue::Sample));
    assert_eq!(
        NativeProperties {
            owner: CanonicalOwner::Unknown,
            grouping: CanonicalGrouping::SourcePiecesOnly,
        },
        facts(InstantValue::Timestamp),
        "a timestamp kept facts it rewrote"
    );
}
