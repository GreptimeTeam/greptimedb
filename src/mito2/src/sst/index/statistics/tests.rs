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

use super::*;

#[test]
fn test_unmeasured_updates_do_not_publish_zero_durations() {
    let name = "test_sketch_fused_counter_only";
    let update = INDEX_CREATE_ELAPSED.with_label_values(&["update", name]);
    let total = INDEX_CREATE_ELAPSED.with_label_values(&["total", name]);
    let update_before = update.get_sample_count();
    let total_before = total.get_sample_count();
    let mut stats = Statistics::new_without_update_timing(name);
    stats.inc_row_count(7);
    assert_eq!(stats.row_count(), 7);
    drop(stats);
    assert_eq!(update.get_sample_count(), update_before);
    assert_eq!(total.get_sample_count(), total_before);
    let mut measured = Statistics::new_without_update_timing(name);
    {
        let _timer = measured.record_update();
    }
    drop(measured);
    assert_eq!(update.get_sample_count(), update_before + 1);
    assert_eq!(total.get_sample_count(), total_before + 1);
}

#[test]
fn test_merge_does_not_count_input_rows_as_output_rows() {
    let mut stats = Statistics::new("test_merge");
    {
        let _guard = stats.record_merge();
    }
    assert_eq!(stats.row_count(), 0);
    {
        let mut guard = stats.record_finish();
        guard.inc_row_count(7);
        guard.inc_byte_count(32);
    }
    assert_eq!(stats.row_count(), 7);
    assert_eq!(stats.byte_count(), 32);
}

#[test]
fn test_statistics_basic() {
    let mut stats = Statistics::new("test");
    {
        let mut guard = stats.record_update();
        guard.inc_byte_count(100);
        guard.inc_row_count(10);

        let now = Instant::now();
        while now.elapsed().is_zero() {
            // busy loop
        }
    }
    {
        let _guard = stats.record_finish();
        let now = Instant::now();
        while now.elapsed().is_zero() {
            // busy loop
        }
    }
    {
        let _guard = stats.record_cleanup();
        let now = Instant::now();
        while now.elapsed().is_zero() {
            // busy loop
        }
    }
    assert_eq!(stats.row_count(), 10);
    assert_eq!(stats.byte_count(), 100);
    assert!(stats.update_elapsed > Duration::default());
    assert!(stats.finish_elapsed > Duration::default());
    assert!(stats.cleanup_eplased > Duration::default());
}
