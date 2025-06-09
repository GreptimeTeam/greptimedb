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
use common_recordbatch::RecordBatches;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use futures::TryStreamExt;
use store_api::region_engine::{PrepareRequest, RegionEngine, RegionScanner};
use store_api::region_request::RegionRequest;
use store_api::storage::{RegionId, ScanRequest, TimeSeriesDistribution};

use crate::config::MitoConfig;
use crate::read::scan_region::Scanner;
use crate::test_util;
use crate::test_util::{CreateRequestBuilder, TestEnv};

#[tokio::test]
async fn test_scan_with_min_sst_sequence() {
    let mut env = TestEnv::with_prefix("test_scan_with_min_sst_sequence");
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let column_schemas = test_util::rows_schema(&request);

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let put_rows = async |start, end| {
        let rows = Rows {
            schema: column_schemas.clone(),
            rows: test_util::build_rows(start, end),
        };
        test_util::put_rows(&engine, region_id, rows).await;
        test_util::flush_region(&engine, region_id, None).await;
    };
    // generates 3 SST files
    put_rows(0, 3).await;
    put_rows(3, 6).await;
    put_rows(6, 9).await;

    let scan_engine = async |file_min_sequence, expected_files, expected_data| {
        let request = ScanRequest {
            sst_min_sequence: file_min_sequence,
            ..Default::default()
        };
        let scanner = engine.scanner(region_id, request).unwrap();
        assert_eq!(scanner.num_files(), expected_files);

        let stream = scanner.scan().await.unwrap();
        let batches = RecordBatches::try_collect(stream).await.unwrap();
        assert_eq!(batches.pretty_print().unwrap(), expected_data);
    };

    // scans with no sst minimal sequence limit
    scan_engine(
        None,
        3,
        "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
| 3     | 3.0     | 1970-01-01T00:00:03 |
| 4     | 4.0     | 1970-01-01T00:00:04 |
| 5     | 5.0     | 1970-01-01T00:00:05 |
| 6     | 6.0     | 1970-01-01T00:00:06 |
| 7     | 7.0     | 1970-01-01T00:00:07 |
| 8     | 8.0     | 1970-01-01T00:00:08 |
+-------+---------+---------------------+",
    )
    .await;

    // scans with sst minimal sequence > 2
    scan_engine(
        Some(2),
        3,
        "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
| 3     | 3.0     | 1970-01-01T00:00:03 |
| 4     | 4.0     | 1970-01-01T00:00:04 |
| 5     | 5.0     | 1970-01-01T00:00:05 |
| 6     | 6.0     | 1970-01-01T00:00:06 |
| 7     | 7.0     | 1970-01-01T00:00:07 |
| 8     | 8.0     | 1970-01-01T00:00:08 |
+-------+---------+---------------------+",
    )
    .await;

    // scans with sst minimal sequence > 3
    scan_engine(
        Some(3),
        2,
        "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 3     | 3.0     | 1970-01-01T00:00:03 |
| 4     | 4.0     | 1970-01-01T00:00:04 |
| 5     | 5.0     | 1970-01-01T00:00:05 |
| 6     | 6.0     | 1970-01-01T00:00:06 |
| 7     | 7.0     | 1970-01-01T00:00:07 |
| 8     | 8.0     | 1970-01-01T00:00:08 |
+-------+---------+---------------------+",
    )
    .await;

    // scans with sst minimal sequence > 7
    scan_engine(
        Some(7),
        1,
        "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 6     | 6.0     | 1970-01-01T00:00:06 |
| 7     | 7.0     | 1970-01-01T00:00:07 |
| 8     | 8.0     | 1970-01-01T00:00:08 |
+-------+---------+---------------------+",
    )
    .await;

    // scans with sst minimal sequence > 9 (no sst files will be selected to scan)
    scan_engine(
        Some(9),
        0,
        "\
++
++",
    )
    .await;
}

#[tokio::test]
async fn test_series_scan() {
    let mut env = TestEnv::with_prefix("test_series_scan");
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
        .insert_option("compaction.twcs.time_window", "1h")
        .build();
    let column_schemas = test_util::rows_schema(&request);

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let put_flush_rows = async |start, end| {
        let rows = Rows {
            schema: column_schemas.clone(),
            rows: test_util::build_rows(start, end),
        };
        test_util::put_rows(&engine, region_id, rows).await;
        test_util::flush_region(&engine, region_id, None).await;
    };
    // generates 3 SST files
    put_flush_rows(0, 3).await;
    put_flush_rows(2, 6).await;
    put_flush_rows(3600, 3603).await;
    // Put to memtable.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: test_util::build_rows(7200, 7203),
    };
    test_util::put_rows(&engine, region_id, rows).await;

    let request = ScanRequest {
        distribution: Some(TimeSeriesDistribution::PerSeries),
        ..Default::default()
    };
    let scanner = engine.scanner(region_id, request).unwrap();
    let Scanner::Series(mut scanner) = scanner else {
        panic!("Scanner should be series scan");
    };
    // 3 partition ranges for 3 time window.
    assert_eq!(
        3,
        scanner.properties().partitions[0].len(),
        "unexpected ranges: {:?}",
        scanner.properties().partitions
    );
    let raw_ranges: Vec<_> = scanner
        .properties()
        .partitions
        .iter()
        .flatten()
        .cloned()
        .collect();
    let mut new_ranges = Vec::with_capacity(3);
    for range in raw_ranges {
        new_ranges.push(vec![range]);
    }
    scanner
        .prepare(PrepareRequest {
            ranges: Some(new_ranges),
            ..Default::default()
        })
        .unwrap();

    let metrics_set = ExecutionPlanMetricsSet::default();

    let mut partition_batches = vec![vec![]; 3];
    let mut streams: Vec<_> = (0..3)
        .map(|partition| {
            let stream = scanner.scan_partition(&metrics_set, partition).unwrap();
            Some(stream)
        })
        .collect();
    let mut num_done = 0;
    let mut schema = None;
    // Pull streams in round-robin fashion to get the consistent output from the sender.
    while num_done < 3 {
        if schema.is_none() {
            schema = Some(streams[0].as_ref().unwrap().schema().clone());
        }
        for i in 0..3 {
            let Some(mut stream) = streams[i].take() else {
                continue;
            };
            let Some(rb) = stream.try_next().await.unwrap() else {
                num_done += 1;
                continue;
            };
            partition_batches[i].push(rb);
            streams[i] = Some(stream);
        }
    }

    let mut check_result = |expected| {
        let batches =
            RecordBatches::try_new(schema.clone().unwrap(), partition_batches.remove(0)).unwrap();
        assert_eq!(expected, batches.pretty_print().unwrap());
    };

    // Output series order is 0, 1, 2, 3, 3600, 3601, 3602, 4, 5, 7200, 7201, 7202
    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 3     | 3.0     | 1970-01-01T00:00:03 |
| 3602  | 3602.0  | 1970-01-01T01:00:02 |
| 7200  | 7200.0  | 1970-01-01T02:00:00 |
+-------+---------+---------------------+";
    check_result(expected);

    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 3600  | 3600.0  | 1970-01-01T01:00:00 |
| 4     | 4.0     | 1970-01-01T00:00:04 |
| 7201  | 7201.0  | 1970-01-01T02:00:01 |
+-------+---------+---------------------+";
    check_result(expected);

    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 2     | 2.0     | 1970-01-01T00:00:02 |
| 3601  | 3601.0  | 1970-01-01T01:00:01 |
| 5     | 5.0     | 1970-01-01T00:00:05 |
| 7202  | 7202.0  | 1970-01-01T02:00:02 |
+-------+---------+---------------------+";
    check_result(expected);
}
