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

use api::v1::Rows;
use store_api::region_engine::RegionEngine;
use store_api::region_request::RegionRequest;
use store_api::storage::{CursorShard, RegionId};

use super::scan_test::{
    adopt_foreign_file, build_foreign_source_file, create_foreign_target, creator_cursor_scan,
    manifest_files, partition_slice, partition_slice_cursor, partition_str_slice,
    partition_str_slice_cursor, scan_flat_row_sequences,
};
use crate::config::MitoConfig;
use crate::test_util;
use crate::test_util::{CreateRequestBuilder, TestEnv};

/// The cursor is keyed by `(region, expression slice)`: a file is filtered by the slice it
/// belongs to, not by the region projection of the creator's slices.
#[tokio::test]
async fn test_read_row_sequence_picks_the_intersecting_creator_slice() {
    let mut env = TestEnv::with_prefix("test_read_row_sequence_picks_intersecting_slice").await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let source = RegionId::new(1, 2);
    let target = RegionId::new(1, 1);

    // The creator's file states the numbers 1..12 and belongs to its `[50, 100)` slice; the
    // cut 3 says a substitution produced the first three of them.
    let (_, mut source_file) = build_foreign_source_file(&engine, source, 12, true).await;
    source_file.partition_expr = Some(partition_slice(50, 100).into());
    create_foreign_target(&engine, target).await;
    adopt_foreign_file(&engine, target, source_file, 4, true, Some(3));

    // `[0, 50)` is at 3 and the file's own `[50, 100)` at 10: the lower bound is 10, so only
    // the rows 11 and 12 are delivered. The region projection of the same cursor (`min` = 3)
    // would deliver the rows 4..12 again, which is exactly the duplication this keying
    // removes.
    let rows = scan_flat_row_sequences(
        &engine,
        target,
        creator_cursor_scan(
            source,
            vec![
                partition_slice_cursor(0, 50, 3),
                partition_slice_cursor(50, 100, 10),
            ],
            1,
            20,
        ),
    )
    .await;
    assert_eq!(vec![(10_000, 11), (11_000, 12)], rows);

    // The mirror image: `[0, 50)` at 10 and the file's own `[50, 100)` at 3. The lower bound
    // is the file's slice, so the rows 4..12 are delivered — the other slice's cursor must
    // not be applied to a file it does not cover.
    let rows = scan_flat_row_sequences(
        &engine,
        target,
        creator_cursor_scan(
            source,
            vec![
                partition_slice_cursor(0, 50, 10),
                partition_slice_cursor(50, 100, 3),
            ],
            1,
            20,
        ),
    )
    .await;
    let expected = (4_u64..=12)
        .map(|seq| (((seq - 1) * 1000) as i64, seq))
        .collect::<Vec<(i64, u64)>>();
    assert_eq!(expected, rows);
}

/// Two files of **one** creator region, each stating a different expression of its domain,
/// with different consumed progress on the two expressions: a merged read delivers exactly
/// the rows above the cursor of the expression each file states.
///
/// The numbers are allocated in write order, so the slices are cross-wired on purpose: the
/// right slice (`k0 >= 50`) holds the *older* numbers (1..6) while the left slice
/// (`k0 < 50`) holds the newer ones (7..12). A read that applied the left cursor to both
/// files would lose the right slice's rows 3..6 (the left cursor is higher than the right
/// one), and a read that applied the region projection (`min` = 2) to both would deliver
/// the left slice's 7..10 again. Only selecting the slice each file belongs to delivers
/// both correctly.
#[tokio::test]
async fn test_read_row_sequence_filters_each_creator_expression_by_its_own_cursor() {
    let mut env = TestEnv::with_prefix("test_read_row_sequence_filters_each_expression").await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let source = RegionId::new(1, 2);
    let target = RegionId::new(1, 1);

    let request = CreateRequestBuilder::new()
        .insert_option("append_mode", "true")
        .insert_option("preserve_row_sequence", "true")
        .build();
    let schema = test_util::rows_schema(&request);
    engine
        .handle_request(source, RegionRequest::Create(request))
        .await
        .unwrap();

    test_util::put_rows(
        &engine,
        source,
        Rows {
            schema: schema.clone(),
            rows: test_util::build_rows_for_key("0", 0, 6, 0),
        },
    )
    .await;
    test_util::flush_region(&engine, source, None).await;
    let mut right_file = manifest_files(&engine, source)
        .await
        .into_values()
        .next()
        .expect("first flushed file");
    assert_eq!(None, right_file.partition_expr);
    right_file.partition_expr = Some(partition_slice(50, 100).into());

    test_util::put_rows(
        &engine,
        source,
        Rows {
            schema,
            rows: test_util::build_rows_for_key("0", 6, 12, 0),
        },
    )
    .await;
    test_util::flush_region(&engine, source, None).await;
    let mut left_file = manifest_files(&engine, source)
        .await
        .into_values()
        .find(|file| file.file_id != right_file.file_id)
        .expect("second flushed file");
    assert_eq!(None, left_file.partition_expr);
    left_file.partition_expr = Some(partition_slice(0, 50).into());

    create_foreign_target(&engine, target).await;
    adopt_foreign_file(&engine, target, right_file, 13, true, None);
    adopt_foreign_file(&engine, target, left_file, 14, true, None);

    // The left expression was consumed up to 10, the right one only up to 2.
    let mut rows = scan_flat_row_sequences(
        &engine,
        target,
        creator_cursor_scan(
            source,
            vec![
                partition_slice_cursor(0, 50, 10),
                partition_slice_cursor(50, 100, 2),
            ],
            1,
            20,
        ),
    )
    .await;
    rows.sort_unstable();

    assert_eq!(
        vec![
            (2000, 3),
            (3000, 4),
            (4000, 5),
            (5000, 6),
            (10000, 11),
            (11000, 12)
        ],
        rows,
        "each file is filtered by the cursor of the expression it states: the right slice \
         keeps the rows above 2 (no loss) and the left slice starts above 10 (no duplicate)"
    );
}

/// The counterexample per-row selection exists for: **one** file of one creator holds rows
/// of two of its expressions, and the two cursors advanced differently.
///
/// The file was written under the coarse expression `k0 >= "0" AND k0 < "9"`, which the
/// creator's domain has since been split out of. Its rows belong to `k0 < "5"` (consumed up
/// to 5) and to `k0 >= "5"` (consumed up to 2). The least advanced cursor of the file is 2,
/// so a file-level bound would deliver the already consumed rows 3, 4 and 5 again; the most
/// advanced one (5) would drop the live row 6. Only splitting the rows by the expression
/// each of them satisfies delivers the row at 6 and nothing else.
#[tokio::test]
async fn test_read_row_sequence_splits_one_file_by_the_expression_of_each_row() {
    let mut env = TestEnv::with_prefix("test_read_row_sequence_splits_by_row_expression").await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let source = RegionId::new(1, 2);
    let target = RegionId::new(1, 1);

    // One file of the creator: five rows of the left expression (numbers 1..5) and one row
    // of the right one (number 6).
    let request = CreateRequestBuilder::new()
        .insert_option("append_mode", "true")
        .insert_option("preserve_row_sequence", "true")
        .build();
    let schema = test_util::rows_schema(&request);
    engine
        .handle_request(source, RegionRequest::Create(request))
        .await
        .unwrap();
    let mut rows = test_util::build_rows_for_key("1", 0, 5, 0);
    rows.extend(test_util::build_rows_for_key("6", 5, 6, 0));
    test_util::put_rows(&engine, source, Rows { schema, rows }).await;
    test_util::flush_region(&engine, source, None).await;

    let mut source_file = manifest_files(&engine, source)
        .await
        .into_values()
        .next()
        .expect("flushed file");
    // The coarse expression the file was written under: it holds rows of both slices.
    source_file.partition_expr = Some(partition_str_slice("0", "9").into());
    create_foreign_target(&engine, target).await;
    adopt_foreign_file(&engine, target, source_file, 7, true, None);

    let rows = scan_flat_row_sequences(
        &engine,
        target,
        creator_cursor_scan(
            source,
            vec![
                partition_str_slice_cursor("0", "5", 5),
                partition_str_slice_cursor("5", "9", 2),
            ],
            1,
            20,
        ),
    )
    .await;

    assert_eq!(
        vec![(5000, 6)],
        rows,
        "the row at 6 belongs to the slice consumed up to 2 and must be delivered; the rows \
         at 3, 4 and 5 belong to the slice consumed up to 5 and must not be delivered again"
    );

    // The same read with only `field_0` projected: the tag column the slice expressions
    // reference is added to the read columns as an internal column, so the per-row decision
    // still works and the output schema stays the projected one.
    let mut request = creator_cursor_scan(
        source,
        vec![
            partition_str_slice_cursor("0", "5", 5),
            partition_str_slice_cursor("5", "9", 2),
        ],
        1,
        20,
    );
    request.projection = Some(vec![1]);
    let rows = scan_flat_row_sequences(&engine, target, request).await;
    assert_eq!(
        vec![(5000, 6)],
        rows,
        "the partition column is read even though the scan does not project it"
    );
}

/// Several candidate slices may select one row, and then the **least advanced** of their
/// cursors decides it — exactly like the time-series and bulk memtable filters
/// (`Batch::filter_by_creator_slices`, `creator_slice_mask`), which take the minimum over the
/// candidates that select a row. Taking the first match instead drops the rows a less advanced
/// piece still holds.
///
/// The counterexample this pins: the retired records `A = [0,8): 1` and `B = [5,9): 0` — the
/// domain was not split cleanly, so both select a row at `k0 = 7` — and a merged holder file
/// holding one row at `k0 = 7, sequence = 1`. The row is required because
/// `1 > min(1, 0) = 0`; taking `A` first, as the reader did, drops it. It matches candidates,
/// so the fail-closed rule for a row no candidate selects does not apply.
#[tokio::test]
async fn test_read_row_sequence_takes_the_least_cursor_when_several_slices_select_a_row() {
    let mut env = TestEnv::with_prefix("test_read_row_sequence_least_cursor_of_several").await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let source = RegionId::new(1, 2);
    let target = RegionId::new(1, 1);

    // One file of the creator holding one row of the series `k0 = "7"`, numbered 1, under a
    // coarse statement (`k0 >= "0" AND k0 < "9"`) that covers both retired records: the file
    // merges the rows of both pieces. The bounds are single characters, so their lexicographic
    // order (the order of the string tag column) is the numeric order of the tag values.
    let request = CreateRequestBuilder::new()
        .insert_option("append_mode", "true")
        .insert_option("preserve_row_sequence", "true")
        .build();
    let schema = test_util::rows_schema(&request);
    engine
        .handle_request(source, RegionRequest::Create(request))
        .await
        .unwrap();
    test_util::put_rows(
        &engine,
        source,
        Rows {
            schema,
            rows: test_util::build_rows_for_key("7", 0, 1, 0),
        },
    )
    .await;
    test_util::flush_region(&engine, source, None).await;

    let mut source_file = manifest_files(&engine, source)
        .await
        .into_values()
        .next()
        .expect("flushed file");
    source_file.partition_expr = Some(partition_str_slice("0", "9").into());
    create_foreign_target(&engine, target).await;
    adopt_foreign_file(&engine, target, source_file, 2, true, None);

    // `A` is consumed up to 1, `B` only up to 0, and the row at 7 falls into both domains.
    let rows = scan_flat_row_sequences(
        &engine,
        target,
        creator_cursor_scan(
            source,
            vec![
                partition_str_slice_cursor("0", "8", 1),
                partition_str_slice_cursor("5", "9", 0),
            ],
            0,
            10,
        ),
    )
    .await;

    assert_eq!(
        vec![(0, 1)],
        rows,
        "the row at sequence 1 is above the least advanced of the slices that select it (0), so \
         it must be delivered even though the first matching slice is consumed up to 1"
    );
}

/// The same per-row rule for a **memtable** of this region: a memtable row is filtered by
/// the cursor of the piece its own tag values fall into, not by the single floor of the
/// region.
///
/// The region's own pieces are the only candidates, because a memtable holds the rows this
/// region allocated. The rows of the piece consumed up to 5 (the rows at 3, 4 and 5) are
/// not delivered again, and the row at 6 of the piece consumed up to 2 still is.
#[tokio::test]
async fn test_read_row_sequence_splits_memtable_rows_by_the_expression_of_each_row() {
    let mut env =
        TestEnv::with_prefix("test_read_row_sequence_splits_memtable_rows_by_expression").await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let region = RegionId::new(1, 1);

    // Five rows of the left expression (numbers 1..5) and one row of the right one
    // (number 6), all of them in the memtable: no flush, so no file takes part in the read.
    // The primary-key SST format keeps the time-series memtable (the flat format would put
    // the rows in the bulk memtable, which does not apply the per-piece rule), and
    // `preserve_row_sequence` keeps the exact `(C, H]` delta admissible.
    let request = CreateRequestBuilder::new()
        .insert_option("append_mode", "true")
        .insert_option("preserve_row_sequence", "true")
        .insert_option("sst_format", "primary_key")
        .build();
    let schema = test_util::rows_schema(&request);
    engine
        .handle_request(region, RegionRequest::Create(request))
        .await
        .unwrap();
    let mut rows = test_util::build_rows_for_key("1", 0, 5, 0);
    rows.extend(test_util::build_rows_for_key("6", 5, 6, 0));
    test_util::put_rows(&engine, region, Rows { schema, rows }).await;

    // One cursor value for every piece: the delivered rows are exactly the rows above the
    // region-level floor 2, which is what the read delivered before the per-row rule.
    let mut rows = scan_flat_row_sequences(
        &engine,
        region,
        creator_cursor_scan(
            region,
            vec![
                partition_str_slice_cursor("0", "5", 2),
                partition_str_slice_cursor("5", "9", 2),
            ],
            2,
            20,
        ),
    )
    .await;
    rows.sort_unstable();
    assert_eq!(
        vec![(2000, 3), (3000, 4), (4000, 5), (5000, 6)],
        rows,
        "one cursor value for every piece is the region-level floor of the whole read"
    );

    // The pieces advanced differently: the rows of the left one (3, 4 and 5) are consumed,
    // the row at 6 belongs to the right one and stays live.
    let mut rows = scan_flat_row_sequences(
        &engine,
        region,
        creator_cursor_scan(
            region,
            vec![
                partition_str_slice_cursor("0", "5", 5),
                partition_str_slice_cursor("5", "9", 2),
            ],
            2,
            20,
        ),
    )
    .await;
    rows.sort_unstable();
    assert_eq!(
        vec![(5000, 6)],
        rows,
        "each memtable row is filtered by the cursor of the piece it belongs to: the rows at \
         3, 4 and 5 were consumed by the piece advanced to 5, the row at 6 was not"
    );
}

/// The same per-row rule for a **bulk** memtable: a bulk part keeps the tag columns and the
/// physical sequence of every row as real columns, so the piece a row belongs to is resolved
/// per row there too, instead of by the single floor of the region.
///
/// As above, the rows of the piece consumed up to 5 (3, 4 and 5) are not delivered again
/// while the row at 6 of the piece consumed up to 2 still is.
#[tokio::test]
async fn test_read_row_sequence_splits_bulk_memtable_rows_by_the_expression_of_each_row() {
    let mut env = TestEnv::with_prefix("test_read_row_sequence_splits_bulk_memtable_rows").await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let region = RegionId::new(1, 1);

    // Five rows of the left expression (numbers 1..5) and one row of the right one
    // (number 6), all of them in the bulk memtable: no flush, so no file takes part in the
    // read, and `preserve_row_sequence` keeps the exact `(C, H]` delta admissible.
    let request = CreateRequestBuilder::new()
        .insert_option("append_mode", "true")
        .insert_option("preserve_row_sequence", "true")
        .insert_option("memtable.type", "bulk")
        .build();
    let schema = test_util::rows_schema(&request);
    engine
        .handle_request(region, RegionRequest::Create(request))
        .await
        .unwrap();
    let mut rows = test_util::build_rows_for_key("1", 0, 5, 0);
    rows.extend(test_util::build_rows_for_key("6", 5, 6, 0));
    test_util::put_rows(&engine, region, Rows { schema, rows }).await;

    // The rows really are in a bulk memtable, so the read below goes through a bulk part
    // and the per-row rule there, not through the time-series memtable.
    assert!(
        matches!(
            engine
                .get_region(region)
                .unwrap()
                .version_control
                .current()
                .version
                .options
                .memtable,
            Some(crate::region::options::MemtableOptions::Bulk(_))
        ),
        "the region must hold the bulk memtable"
    );

    // One cursor value for every piece: the delivered rows are exactly the rows above the
    // region-level floor 2, which is what the read delivered before the per-row rule.
    let mut rows = scan_flat_row_sequences(
        &engine,
        region,
        creator_cursor_scan(
            region,
            vec![
                partition_str_slice_cursor("0", "5", 2),
                partition_str_slice_cursor("5", "9", 2),
            ],
            2,
            20,
        ),
    )
    .await;
    rows.sort_unstable();
    assert_eq!(
        vec![(2000, 3), (3000, 4), (4000, 5), (5000, 6)],
        rows,
        "one cursor value for every piece is the region-level floor of the whole read"
    );

    // The pieces advanced differently: the rows of the left one (3, 4 and 5) are consumed,
    // the row at 6 belongs to the right one and stays live.
    let mut rows = scan_flat_row_sequences(
        &engine,
        region,
        creator_cursor_scan(
            region,
            vec![
                partition_str_slice_cursor("0", "5", 5),
                partition_str_slice_cursor("5", "9", 2),
            ],
            2,
            20,
        ),
    )
    .await;
    rows.sort_unstable();
    assert_eq!(
        vec![(5000, 6)],
        rows,
        "each bulk memtable row is filtered by the cursor of the piece it belongs to: the \
         rows at 3, 4 and 5 were consumed by the piece advanced to 5, the row at 6 was not"
    );
}

/// A `cut = None` adopted file is filtered by the creator's cursor exactly like one that
/// states a cut. Its numbers were allocated in the creator's domain whether or not a
/// substitution touched them, so the holder's own `(C, H]` neither bounds them nor may
/// replace the creator's slice.
#[tokio::test]
async fn test_read_row_sequence_filters_adopted_file_without_cut_by_creator_cursor() {
    let mut env = TestEnv::with_prefix("test_read_row_sequence_filters_adopted_without_cut").await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let source = RegionId::new(1, 2);
    let target = RegionId::new(1, 1);

    let (_, source_file) = build_foreign_source_file(&engine, source, 5, true).await;
    create_foreign_target(&engine, target).await;
    adopt_foreign_file(&engine, target, source_file, 6, true, None);

    // The creator consumed up to 3, so only its rows 4 and 5 are still owed. The holder's
    // own cursor (0 in this scan) would deliver all five again.
    let rows = scan_flat_row_sequences(
        &engine,
        target,
        creator_cursor_scan(source, vec![CursorShard::whole_domain(3)], 0, 10),
    )
    .await;
    assert_eq!(
        vec![(3000, 4), (4000, 5)],
        rows,
        "an adopted file without a cut is still bounded by the creator's cursor"
    );
}
