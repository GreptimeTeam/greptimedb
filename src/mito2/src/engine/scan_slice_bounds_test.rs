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

use store_api::storage::{CursorShard, RegionId};

use super::scan_test::{
    adopt_foreign_file, build_foreign_source_file, create_foreign_target, creator_cursor_scan,
    partition_str_slice_cursor, scan_flat_row_sequences,
};
use crate::config::MitoConfig;
use crate::test_util::TestEnv;

/// A creator slice that states the last sequence its delta covers bounds an adopted file from
/// above as well: a row above that bound was numbered after the delta this read stands for,
/// so delivering it here reports identity the caller has not consumed yet while its marker
/// stays below the row's number.
#[tokio::test]
async fn test_read_row_sequence_drops_adopted_rows_above_creator_upper_bound() {
    let mut env = TestEnv::with_prefix("test_read_row_sequence_creator_upper_bound").await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let source = RegionId::new(1, 2);
    let target = RegionId::new(1, 1);

    // The creator's file states the numbers 1..5 and no cut, so the cursor decides alone.
    let (_, source_file) = build_foreign_source_file(&engine, source, 5, true).await;
    create_foreign_target(&engine, target).await;
    adopt_foreign_file(&engine, target, source_file, 6, true, None);

    // The delta is `(1, 3]`: rows 4 and 5 carry numbers the creator's cursor did not consume
    // and its bound does not cover, so this round must not read them.
    let rows = scan_flat_row_sequences(
        &engine,
        target,
        creator_cursor_scan(
            source,
            vec![CursorShard::whole_domain(1).with_until(3)],
            0,
            10,
        ),
    )
    .await;
    assert_eq!(
        vec![(1000, 2), (2000, 3)],
        rows,
        "the rows above the creator's upper bound belong to a later delta, not to this read"
    );

    // The bound is a filter on the creator's domain, not a change of the cursor: stating a
    // bound that covers every row delivers every row above the cursor, as before.
    let rows = scan_flat_row_sequences(
        &engine,
        target,
        creator_cursor_scan(
            source,
            vec![CursorShard::whole_domain(1).with_until(5)],
            0,
            10,
        ),
    )
    .await;
    assert_eq!(vec![(1000, 2), (2000, 3), (3000, 4), (4000, 5)], rows);
}

/// A slice that states no upper bound keeps the unbounded `(cursor, +inf)` filter it always
/// had: the bound is optional evidence, and a read that lacks it must not lose a row that no
/// bound excludes.
#[tokio::test]
async fn test_read_row_sequence_reads_above_cursor_without_creator_upper_bound() {
    let mut env = TestEnv::with_prefix("test_read_row_sequence_without_upper_bound").await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let source = RegionId::new(1, 2);
    let target = RegionId::new(1, 1);

    let (_, source_file) = build_foreign_source_file(&engine, source, 5, true).await;
    create_foreign_target(&engine, target).await;
    adopt_foreign_file(&engine, target, source_file, 6, true, None);

    let rows = scan_flat_row_sequences(
        &engine,
        target,
        creator_cursor_scan(source, vec![CursorShard::whole_domain(1)], 0, 10),
    )
    .await;
    assert_eq!(
        vec![(1000, 2), (2000, 3), (3000, 4), (4000, 5)],
        rows,
        "without a bound every row above the creator's cursor is still owed"
    );
}

/// Several candidate slices: the file-level upper bound is the **least** bound any of them
/// states, because the file-level filter cannot tell which slice a row belongs to. The
/// counterexample this pins: the slice holding the file's rows proves the delta ends at 3
/// while another candidate's bound reaches 9, and reading up to 9 delivers rows 4 and 5,
/// which no slice yields this round.
#[tokio::test]
async fn test_read_row_sequence_takes_the_least_creator_upper_bound() {
    let mut env = TestEnv::with_prefix("test_read_row_sequence_least_upper_bound").await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let source = RegionId::new(1, 2);
    let target = RegionId::new(1, 1);

    // One file stating no expression, so every slice of the creator is a candidate for it.
    // Its five rows carry the tag `0`, i.e. they all fall in the `[0, 5)` slice.
    let (_, source_file) = build_foreign_source_file(&engine, source, 5, true).await;
    assert_eq!(None, source_file.partition_expr);
    create_foreign_target(&engine, target).await;
    adopt_foreign_file(&engine, target, source_file, 6, true, None);

    let rows = scan_flat_row_sequences(
        &engine,
        target,
        creator_cursor_scan(
            source,
            vec![
                // The slice the file's rows belong to: consumed up to 1, delta ends at 3.
                partition_str_slice_cursor("0", "5", 1).with_until(3),
                // A candidate that overlaps no row of this file, with a looser bound.
                partition_str_slice_cursor("5", "9", 3).with_until(9),
            ],
            0,
            10,
        ),
    )
    .await;
    assert_eq!(
        vec![(1000, 2), (2000, 3)],
        rows,
        "the file-level bound is the least bound of the candidates, so row 4 is left out"
    );
}
