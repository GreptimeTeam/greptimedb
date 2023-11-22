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

use api::v1::value::ValueData;
use api::v1::{Row, Rows};
use common_recordbatch::RecordBatches;
use store_api::region_engine::RegionEngine;
use store_api::region_request::RegionRequest;
use store_api::storage::{RegionId, ScanRequest};

use crate::config::MitoConfig;
use crate::test_util::{put_rows, rows_schema, CreateRequestBuilder, TestEnv};

/// Build rows for multiple tags and fields.
fn build_rows_multi_tags_fields(
    tags: &[&str],
    field_starts: &[usize],
    ts_range: (usize, usize),
) -> Vec<Row> {
    (ts_range.0..ts_range.1)
        .enumerate()
        .map(|(idx, ts)| {
            let mut values = Vec::with_capacity(tags.len() + field_starts.len() + 1);
            for tag in tags {
                values.push(api::v1::Value {
                    value_data: Some(ValueData::StringValue(tag.to_string())),
                });
            }
            for field_start in field_starts {
                values.push(api::v1::Value {
                    value_data: Some(ValueData::F64Value((field_start + idx) as f64)),
                });
            }
            values.push(api::v1::Value {
                value_data: Some(ValueData::TimestampMillisecondValue(ts as i64 * 1000)),
            });

            api::v1::Row { values }
        })
        .collect()
}

#[tokio::test]
async fn test_scan_projection() {
    let mut env = TestEnv::new();
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    // [tag_0, tag_1, field_0, field_1, ts]
    let request = CreateRequestBuilder::new().tag_num(2).field_num(2).build();

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas,
        rows: build_rows_multi_tags_fields(&["a", "b"], &[0, 10], (0, 3)),
    };
    put_rows(&engine, region_id, rows).await;

    // Scans tag_1, field_1, ts
    let request = ScanRequest {
        projection: Some(vec![1, 3, 4]),
        filters: Vec::new(),
        output_ordering: None,
        limit: None,
    };
    let stream = engine.handle_query(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+---------+---------------------+
| tag_1 | field_1 | ts                  |
+-------+---------+---------------------+
| b     | 10.0    | 1970-01-01T00:00:00 |
| b     | 11.0    | 1970-01-01T00:00:01 |
| b     | 12.0    | 1970-01-01T00:00:02 |
+-------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());
}
