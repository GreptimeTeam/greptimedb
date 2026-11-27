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

use std::time::Duration;

use api::v1::Rows;
use common_base::readable_size::ReadableSize;
use datafusion_common::ScalarValue;
use datafusion_expr::{col, lit};
use store_api::region_engine::RegionEngine;
use store_api::region_request::RegionRequest;
use store_api::storage::{RegionId, TimeSeriesRowSelector};

use super::scan_test::{
    adopt_foreign_file, create_foreign_target, creator_cursor_scan, manifest_files,
    partition_str_slice_cursor, scan_partition_rows_and_foreign_creators,
};
use crate::config::MitoConfig;
use crate::test_util;
use crate::test_util::{CreateRequestBuilder, TestEnv};

/// A range result is only reusable for a read whose **per-piece** selection is the same.
///
/// A physical-sequence read decides each row of an adopted file by the creator cursor slice
/// that selects it, and bounds the file by `(least advanced cursor, least until]`. Neither is
/// stated by the file, so two reads of the same file under the same region-level `(C, H]`
/// deliver different rows once a piece cursor or an `until` advances; a cache key that only
/// records `(C, H]` replays the first read's rows to all of them.
///
/// The counterexample this pins: `(cursor 0, until 6]` caches the rows 1..6, `(cursor 3,
/// until 8]` requires the rows 4..8, and `(cursor 0, until 2]` requires the rows 1..2 and
/// nothing above its `until`. Replaying the first result delivers the rows 1..6 to all three:
/// rows an advanced piece already consumed, rows the new range requires that are missing, and
/// rows above the new `until`.
///
/// The `LastRow` selector is what puts this read on the sequential scan, the only path that
/// fills the range result cache: without it an append-only region reads through the unordered
/// scan, which bypasses the cache entirely.
#[tokio::test]
async fn test_range_cache_never_replays_a_range_read_under_other_creator_cursors() {
    let mut env = TestEnv::with_prefix("test_range_cache_creator_cursor_slices").await;
    let engine = env
        .create_engine(MitoConfig {
            // The flat format is the one the range result cache covers.
            default_flat_format: true,
            range_result_cache_size: ReadableSize::mb(64),
            ..Default::default()
        })
        .await;
    let source = RegionId::new(1, 2);
    let target = RegionId::new(1, 1);

    // Eight rows numbered 1..8, one per series, all of them in one file of the creator. The
    // holder adopts that file as a foreign file.
    let create = CreateRequestBuilder::new()
        .insert_option("append_mode", "true")
        .insert_option("preserve_row_sequence", "true")
        .build();
    let schema = test_util::rows_schema(&create);
    engine
        .handle_request(source, RegionRequest::Create(create))
        .await
        .unwrap();
    let mut rows = Vec::new();
    for series in 0..8_usize {
        rows.extend(test_util::build_rows_for_key(
            &series.to_string(),
            series,
            series + 1,
            0,
        ));
    }
    test_util::put_rows(&engine, source, Rows { schema, rows }).await;
    test_util::flush_region(&engine, source, None).await;
    let source_file = manifest_files(&engine, source)
        .await
        .into_values()
        .next()
        .expect("flushed file");
    create_foreign_target(&engine, target).await;
    adopt_foreign_file(&engine, target, source_file, 20, true, None);

    // The `(cursor, until]` of the one piece the rows of the adopted file belong to. The
    // region-level `(0, 20]` is the same in every read, so only the slice separates them; the
    // tag filter is what makes the range eligible for the cache.
    let request = |cursor: u64, until: u64| {
        let mut request = creator_cursor_scan(
            source,
            vec![partition_str_slice_cursor("0", "9", cursor).with_until(until)],
            0,
            20,
        );
        request.filters = vec![col("tag_0").gt_eq(lit(ScalarValue::Utf8(Some("0".to_string()))))];
        request.series_row_selector = Some(TimeSeriesRowSelector::LastRow { after_merge: false });
        request
    };

    let first = scan_partition_rows_and_foreign_creators(&engine, target, request(0, 6))
        .await
        .0;
    assert_eq!(
        vec![0, 1000, 2000, 3000, 4000, 5000],
        first,
        "the first read delivers the rows above its cursor and up to its `until`"
    );

    // Wait for the completed range result: the same read then replays it, which it reports by
    // saying that the files its rows came from are not readable from that call.
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let (rows, creators) =
                scan_partition_rows_and_foreign_creators(&engine, target, request(0, 6)).await;
            assert_eq!(first, rows, "a replay delivers the rows it replayed");
            if creators.is_none() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the range result cache did not replay the range within the timeout");

    // An advanced piece cursor with a higher `until`: the rows 4..8 are what this range
    // requires, and the rows 1..3 are already consumed.
    let advanced = scan_partition_rows_and_foreign_creators(&engine, target, request(3, 8))
        .await
        .0;
    assert_eq!(
        vec![3000, 4000, 5000, 6000, 7000],
        advanced,
        "a read under an advanced piece cursor must not replay the rows of the earlier read: \
         only its own `(cursor, until]` decides"
    );

    // A narrower `until`: no row above it may be delivered, whatever an earlier read of the
    // same files cached.
    let bounded = scan_partition_rows_and_foreign_creators(&engine, target, request(0, 2))
        .await
        .0;
    assert_eq!(
        vec![0, 1000],
        bounded,
        "a range result must never deliver a row above the `until` of the read it serves"
    );
}
