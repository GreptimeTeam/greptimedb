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
use futures::TryStreamExt;
use store_api::region_engine::RegionEngine;
use store_api::region_request::RegionRequest;
use store_api::storage::{CursorShard, RegionId};

use super::scan_test::{
    adopt_foreign_file, build_foreign_source_file, create_foreign_target, creator_cursor_scan,
    manifest_files, partition_str_slice_cursor, physical_sequence_scan, scan_flat_row_sequences,
};
use crate::config::MitoConfig;
use crate::error::Error;
use crate::test_util;
use crate::test_util::{CreateRequestBuilder, TestEnv};

/// The same rule at the memtable end of a scan: the per-piece rule of this region's own
/// memtable is built from the region's own cursor slices, so a slice set the read cannot
/// apply per row (a slice covering the whole value domain, or an identity this build cannot
/// read back) keeps the region-level floor only while every candidate carries it. With
/// several cursor values the floor delivers the rows a more advanced piece consumed, so the
/// read fails just like it does for a file of an adopted creator.
#[tokio::test]
async fn test_read_row_sequence_fails_closed_when_memtable_pieces_are_unresolved() {
    let mut env = TestEnv::with_prefix("test_read_row_sequence_fails_closed_memtable_pieces").await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let region = RegionId::new(1, 1);

    // Five rows in the time-series memtable (numbers 1..5): no flush, so no file takes part
    // in the read.
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
    test_util::put_rows(
        &engine,
        region,
        Rows {
            schema,
            rows: test_util::build_rows_for_key("1", 0, 5, 0),
        },
    )
    .await;

    // A whole-domain slice at 3 next to a piece slice at 1: no row of the memtable can be
    // attributed to a piece, and the region-level floor (1) would deliver the rows 2 and 3
    // the more advanced piece has already consumed.
    let err = match engine
        .scan_batch(
            region,
            creator_cursor_scan(
                region,
                vec![
                    CursorShard::whole_domain(3),
                    partition_str_slice_cursor("0", "5", 1),
                ],
                1,
                20,
            ),
            true,
        )
        .await
    {
        Err(err) => err,
        Ok(mut stream) => match stream.try_next().await {
            Err(err) => err,
            Ok(_) => panic!("an unresolvable memtable piece with disagreeing cursors must fail"),
        },
    };
    let message = format!("{err}");
    assert!(
        message.contains("exact sequence range unavailable"),
        "the shared exact-range contract prefix must reach the caller: {message}"
    );
    assert!(
        message.contains("the memtable of region")
            && message.contains("covers the whole value domain"),
        "the failure must name the memtable and say what could not be resolved: {message}"
    );

    // One cursor value for every candidate: the region-level floor is the per-row answer, so
    // the rows above 1 are delivered exactly as before.
    let mut rows = scan_flat_row_sequences(
        &engine,
        region,
        creator_cursor_scan(
            region,
            vec![
                CursorShard::whole_domain(1),
                partition_str_slice_cursor("0", "5", 1),
            ],
            1,
            20,
        ),
    )
    .await;
    rows.sort_unstable();
    assert_eq!(
        vec![(1000, 2), (2000, 3), (3000, 4), (4000, 5)],
        rows,
        "one cursor value for every piece is the region-level floor of the whole read"
    );
}

/// A creator this scan has no cursor slice for fails closed. Reading the file against this
/// region's own `(C, H]` compares two sequence domains, and reading it unfiltered delivers
/// every already consumed row again on every round; the scan reports the shared
/// "exact range unavailable" contract instead of choosing one of them.
#[tokio::test]
async fn test_read_row_sequence_fails_closed_without_creator_cursor() {
    let mut env = TestEnv::with_prefix("test_read_row_sequence_fails_closed").await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let source = RegionId::new(1, 2);
    let target = RegionId::new(1, 1);

    let (_, source_file) = build_foreign_source_file(&engine, source, 3, true).await;
    create_foreign_target(&engine, target).await;
    adopt_foreign_file(&engine, target, source_file, 4, true, None);

    // No creator slice, and no cut either: the file states nothing this scan can filter
    // with.
    let mut stream = engine
        .scan_batch(target, physical_sequence_scan(true), true)
        .await
        .unwrap();
    let err = match stream.try_next().await {
        Err(err) => err,
        Ok(_) => panic!("a foreign file without a creator cursor must fail closed"),
    };
    let message = format!("{err}");
    assert!(
        message.contains("exact sequence range unavailable"),
        "the shared exact-range contract prefix must reach the caller: {message}"
    );
    assert!(
        message.contains("no cursor slice of its creator region"),
        "the failure must say what is missing: {message}"
    );
}

/// The `Display` of `err` and of every cause below it, one per line.
///
/// A file-level decision is taken while the file is pruned, and the pruner reports a failed
/// file through a wrapper whose own `Display` does not repeat the cause, so the failure is
/// found in the chain.
fn error_chain(err: &Error) -> String {
    let mut text = format!("{err}");
    let mut source = std::error::Error::source(err);
    while let Some(cause) = source {
        text.push('\n');
        text.push_str(&format!("{cause}"));
        source = cause.source();
    }
    text
}

/// The counterexample to the file-level fallback of a physical-sequence read: when the
/// per-row rule of a file cannot be applied, the file is bounded by the **least** advanced
/// candidate. That floor is exact only while every candidate carries it — with several cursor
/// values it re-delivers the rows a more advanced piece has already consumed, so the read
/// fails instead of degrading an exact read into a duplicate.
///
/// Two shapes the per-row rule cannot apply, both on a file of an adopted creator: a slice
/// identity this build cannot read back, and a slice covering the whole value domain.
#[tokio::test]
async fn test_read_row_sequence_fails_closed_when_pieces_disagree_and_piece_is_unresolved() {
    let mut env =
        TestEnv::with_prefix("test_read_row_sequence_fails_closed_disagreeing_pieces").await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let source = RegionId::new(1, 2);
    let target = RegionId::new(1, 1);

    // One file stating no expression, so every slice of the creator is a candidate for it.
    let (_, source_file) = build_foreign_source_file(&engine, source, 5, true).await;
    assert_eq!(None, source_file.partition_expr);
    create_foreign_target(&engine, target).await;
    adopt_foreign_file(&engine, target, source_file, 6, true, None);

    // The JSON `"MaxValue"` is a partition *bound*, not an expression: this build cannot read
    // it back, so the file cannot be split per row. The two candidates carry different
    // cursors, so the file-level floor (1) would deliver the rows 2 and 3 a piece consumed up
    // to 3 again.
    let unreadable = |cursor| CursorShard::new(Some("\"MaxValue\"".to_string()), cursor);
    let mut stream = engine
        .scan_batch(
            target,
            creator_cursor_scan(
                source,
                vec![partition_str_slice_cursor("0", "5", 1), unreadable(3)],
                0,
                10,
            ),
            true,
        )
        .await
        .unwrap();
    let err = match stream.try_next().await {
        Err(err) => err,
        Ok(_) => {
            panic!("an unreadable slice identity with disagreeing cursors must fail the read")
        }
    };
    let message = error_chain(&err);
    assert!(
        message.contains("exact sequence range unavailable"),
        "the shared exact-range contract prefix must reach the caller: {message}"
    );
    assert!(
        message.contains("cannot resolve a per-piece cursor"),
        "the failure must say what could not be resolved: {message}"
    );

    // One cursor value for every candidate: the file-level floor *is* the per-row answer, so
    // the unreadable identity keeps today's fallback and the rows above it are delivered.
    let rows = scan_flat_row_sequences(
        &engine,
        target,
        creator_cursor_scan(
            source,
            vec![partition_str_slice_cursor("0", "5", 1), unreadable(1)],
            0,
            10,
        ),
    )
    .await;
    assert_eq!(
        vec![(1000, 2), (2000, 3), (3000, 4), (4000, 5)],
        rows,
        "with one cursor value the file-level floor is exact and nothing is dropped"
    );

    // A whole-domain slice cannot be evaluated per row either: it applies to every row, so no
    // slice expression can tell which piece a row belongs to. With disagreeing cursors the
    // same rule applies.
    let mut stream = engine
        .scan_batch(
            target,
            creator_cursor_scan(
                source,
                vec![
                    CursorShard::whole_domain(3),
                    partition_str_slice_cursor("0", "5", 1),
                ],
                0,
                10,
            ),
            true,
        )
        .await
        .unwrap();
    let err = match stream.try_next().await {
        Err(err) => err,
        Ok(_) => panic!("a whole-domain slice with disagreeing cursors must fail the read"),
    };
    let message = error_chain(&err);
    assert!(
        message.contains("cannot resolve a per-piece cursor")
            && message.contains("covers the whole value domain"),
        "the failure must say that the slice covers the whole domain: {message}"
    );

    // The same whole-domain read with one cursor value for every candidate keeps the
    // file-level floor, exactly as before.
    let rows = scan_flat_row_sequences(
        &engine,
        target,
        creator_cursor_scan(
            source,
            vec![
                CursorShard::whole_domain(1),
                partition_str_slice_cursor("0", "5", 1),
            ],
            0,
            10,
        ),
    )
    .await;
    assert_eq!(
        vec![(1000, 2), (2000, 3), (3000, 4), (4000, 5)],
        rows,
        "one cursor value for every piece is the file-level floor of the whole file"
    );
}

/// The third shape the per-row rule cannot answer: a row of the file that **no** candidate
/// expression selects, i.e. a row outside the value domain of every piece of the creator.
///
/// Such a row has no piece to attribute it to, so only the least advanced candidate is left.
/// That floor is exact while every candidate carries the same cursor value — it then *is* what
/// the row's own piece states — and says nothing once they differ: it delivers the rows a more
/// advanced piece already consumed, so the read fails instead of replaying them.
#[tokio::test]
async fn test_read_row_sequence_fails_closed_when_no_piece_selects_a_row() {
    let mut env = TestEnv::with_prefix("test_read_row_sequence_fails_closed_unselected_row").await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let source = RegionId::new(1, 2);
    let target = RegionId::new(1, 1);

    // One file of the creator: four rows of the series `k0 = "1"`, inside the domain of the
    // left piece (`k0 < "5"`), and one row of `k0 = "9"`, outside the domain of both candidates.
    let request = CreateRequestBuilder::new()
        .insert_option("append_mode", "true")
        .insert_option("preserve_row_sequence", "true")
        .build();
    let schema = test_util::rows_schema(&request);
    engine
        .handle_request(source, RegionRequest::Create(request))
        .await
        .unwrap();
    let mut rows = test_util::build_rows_for_key("1", 0, 4, 0);
    rows.extend(test_util::build_rows_for_key("9", 4, 5, 0));
    test_util::put_rows(&engine, source, Rows { schema, rows }).await;
    test_util::flush_region(&engine, source, None).await;
    let source_file = manifest_files(&engine, source)
        .await
        .into_values()
        .next()
        .expect("flushed file");
    create_foreign_target(&engine, target).await;
    adopt_foreign_file(&engine, target, source_file, 6, true, None);

    // The candidates carry 5 and 2. The row at 5 (`k0 = "9"`) belongs to no piece, and the
    // least advanced cursor (2) does not stand for the pieces: it would deliver the rows 3 and
    // 4 the left piece already consumed up to 5.
    let mut stream = engine
        .scan_batch(
            target,
            creator_cursor_scan(
                source,
                vec![
                    partition_str_slice_cursor("0", "5", 5),
                    partition_str_slice_cursor("5", "9", 2),
                ],
                0,
                10,
            ),
            true,
        )
        .await
        .unwrap();
    let err = match stream.try_next().await {
        Err(err) => err,
        Ok(_) => panic!("a row no candidate selects with disagreeing cursors must fail the read"),
    };
    let message = error_chain(&err);
    assert!(
        message.contains("exact sequence range unavailable"),
        "the shared exact-range contract prefix must reach the caller: {message}"
    );
    assert!(
        message.contains("cannot resolve a per-piece cursor"),
        "the failure must say what could not be resolved: {message}"
    );
    assert!(
        message.contains("match no candidate creator cursor slice") && message.contains("(2..=5)"),
        "the failure must name the unselected row and the cursors' own span: {message}"
    );

    // One cursor value for every candidate: the least advanced cursor is then every piece's
    // cursor, so the row outside both domains keeps today's treatment and nothing is dropped.
    let mut rows = scan_flat_row_sequences(
        &engine,
        target,
        creator_cursor_scan(
            source,
            vec![
                partition_str_slice_cursor("0", "5", 2),
                partition_str_slice_cursor("5", "9", 2),
            ],
            0,
            10,
        ),
    )
    .await;
    rows.sort_unstable();
    assert_eq!(
        vec![(2000, 3), (3000, 4), (4000, 5)],
        rows,
        "with one cursor value the row no candidate selects is bounded by it exactly, so the \
         rows above 2 are delivered and the rows at or below it are not"
    );
}
