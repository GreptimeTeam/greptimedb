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

use api::v1::helper::row;
use api::v1::value::ValueData;
use api::v1::Rows;
use datatypes::value::Value;
use mito_codec::row_converter::{DensePrimaryKeyCodec, PrimaryKeyCodec};
use parquet::file::statistics::Statistics;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{PathType, RegionRequest};
use store_api::storage::consts::PRIMARY_KEY_COLUMN_NAME;
use store_api::storage::RegionId;

use crate::config::MitoConfig;
use crate::sst::parquet::reader::ParquetReaderBuilder;
use crate::test_util;
use crate::test_util::{CreateRequestBuilder, TestEnv};

#[tokio::test]
async fn test_scan_corrupt() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("test_write_stats_with_long_string_value").await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let table_dir = request.table_dir.clone();
    let column_schemas = test_util::rows_schema(&request);

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let build_rows = |start: i32, end: i32| {
        (start..end)
            .map(|i| {
                row(vec![
                    ValueData::StringValue(i.to_string().repeat(128)),
                    ValueData::F64Value(i as f64),
                    ValueData::TimestampMillisecondValue(i as i64 * 1000),
                ])
            })
            .collect()
    };
    let put_rows = async |start, end| {
        let rows = Rows {
            schema: column_schemas.clone(),
            rows: build_rows(start, end),
        };
        test_util::put_rows(&engine, region_id, rows).await;
        test_util::flush_region(&engine, region_id, None).await;
    };
    put_rows(0, 3).await;

    let region = engine.get_region(region_id).unwrap();

    let version = region.version();
    let file = version
        .ssts
        .levels()
        .iter()
        .flat_map(|l| l.files.values())
        .next()
        .unwrap();

    let object_store = env.get_object_store().unwrap();
    let reader = ParquetReaderBuilder::new(
        table_dir.clone(),
        PathType::Bare,
        file.clone(),
        object_store.clone(),
    )
    .build()
    .await
    .unwrap();

    let codec = DensePrimaryKeyCodec::new(&version.metadata);
    for r in reader.parquet_metadata().row_groups() {
        for c in r.columns() {
            if c.column_descr().name() == PRIMARY_KEY_COLUMN_NAME {
                let stats = c.statistics().unwrap();
                let Statistics::ByteArray(b) = stats else {
                    unreachable!()
                };
                let min = codec
                    .decode_leftmost(b.min_bytes_opt().unwrap())
                    .unwrap()
                    .unwrap();
                assert_eq!(Value::String("0".repeat(128).into()), min);

                let max = codec
                    .decode_leftmost(b.max_bytes_opt().unwrap())
                    .unwrap()
                    .unwrap();
                assert_eq!(Value::String("2".repeat(128).into()), max);
            }
        }
    }
}
