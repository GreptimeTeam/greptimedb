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
use store_api::region_engine::RegionEngine;
use store_api::region_request::RegionRequest;
use store_api::storage::{RegionId, ScanRequest};

use crate::config::MitoConfig;
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
