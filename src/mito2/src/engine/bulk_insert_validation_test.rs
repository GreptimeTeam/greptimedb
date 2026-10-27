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

//! Bulk inserts checked against the region schema.

use std::sync::Arc;

use api::v1::{ArrowIpc, Rows};
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_recordbatch::{DfRecordBatch, RecordBatches};
use common_test_util::flight::encode_to_flight_data;
use datatypes::arrow::array::{
    ArrayRef, BinaryArray, Float64Array, Int64Array, LargeStringArray, ListArray, StringArray,
    StringViewArray, TimestampMillisecondArray, TimestampSecondArray,
};
use datatypes::arrow::datatypes::{Field, Int64Type, Schema};
use datatypes::prelude::ConcreteDataType;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{
    AlterKind, ModifyColumnType, RegionAlterRequest, RegionBulkInsertsRequest, RegionPutRequest,
    RegionRequest,
};
use store_api::storage::{RegionId, ScanRequest};

use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::request::WorkerRequest;
use crate::test_util::{CreateRequestBuilder, TestEnv, build_rows, reopen_region, rows_schema};

const REGION_LAYOUTS: [(&str, &str); 4] = [
    ("flat", "dense"),
    ("primary_key", "dense"),
    ("flat", "sparse"),
    ("primary_key", "sparse"),
];

fn column(name: &str, array: ArrayRef) -> (Field, ArrayRef) {
    (Field::new(name, array.data_type().clone(), true), array)
}

fn tag() -> (Field, ArrayRef) {
    column("tag_0", Arc::new(StringArray::from(vec!["a", "b"])))
}

fn field() -> (Field, ArrayRef) {
    column("field_0", Arc::new(Float64Array::from(vec![1.0, 2.0])))
}

fn ts() -> (Field, ArrayRef) {
    column(
        "ts",
        Arc::new(TimestampMillisecondArray::from(vec![0, 1000])),
    )
}

fn bulk_request(region_id: RegionId, columns: Vec<(Field, ArrayRef)>) -> RegionRequest {
    RegionRequest::BulkInserts(bulk_insert(region_id, columns))
}

fn bulk_insert(region_id: RegionId, columns: Vec<(Field, ArrayRef)>) -> RegionBulkInsertsRequest {
    let (fields, arrays): (Vec<_>, Vec<_>) = columns.into_iter().unzip();
    let payload = DfRecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
    let (schema, record_batch) = encode_to_flight_data(payload.clone());
    RegionBulkInsertsRequest {
        skip_wal: false,
        region_id,
        payload,
        raw_data: ArrowIpc {
            schema: schema.data_header,
            data_header: record_batch.data_header,
            payload: record_batch.data_body,
        },
        partition_expr_version: None,
        aligned_schema_version: None,
    }
}

async fn scan_num_rows(engine: &MitoEngine, region_id: RegionId) -> usize {
    let stream = engine
        .scan_to_stream(region_id, ScanRequest::default())
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    batches.iter().map(|batch| batch.num_rows()).sum()
}

#[tokio::test]
async fn test_encoded_sparse_keys_reject_whole_batch_and_replay() {
    use api::v1::value::ValueData;
    use api::v1::{ColumnDataType, WriteHint};
    use mito_codec::row_converter::SparsePrimaryKeyCodec;

    for bulk in [false, true] {
        let mut env = TestEnv::new().await;
        let engine = env.create_engine(MitoConfig::default()).await;
        let region_id = RegionId::new(1, 1);
        let create = CreateRequestBuilder::new()
            .insert_option("primary_key_encoding", "sparse")
            .build();
        let table_dir = create.table_dir.clone();
        let options = create.options.clone();
        let mut schema = rows_schema(&create);
        schema[0].column_name = "__primary_key".to_string();
        schema[0].datatype = ColumnDataType::Binary as i32;
        schema.swap(1, 2);
        engine
            .handle_request(region_id, RegionRequest::Create(create))
            .await
            .unwrap();

        let codec = SparsePrimaryKeyCodec::schemaless();
        let mut key = Vec::new();
        codec.encode_internal(0, 0, &mut key).unwrap();
        codec
            .encode_raw_tag_value([(0, "1234567🦀".as_bytes())].into_iter(), &mut key)
            .unwrap();
        let mut malformed = key.clone();
        malformed.push(0); // A truncated label ID after a valid Unicode label.
        let request = |second: &[u8]| {
            if bulk {
                bulk_request(
                    region_id,
                    vec![
                        column(
                            "__primary_key",
                            Arc::new(BinaryArray::from_vec(vec![key.as_slice(), second])),
                        ),
                        field(),
                        ts(),
                    ],
                )
            } else {
                let mut rows = build_rows(0, 2);
                for (row, key) in rows.iter_mut().zip([key.as_slice(), second]) {
                    row.values[0].value_data = Some(ValueData::BinaryValue(key.to_vec()));
                    row.values.swap(1, 2);
                }
                RegionRequest::Put(RegionPutRequest {
                    rows: Rows {
                        schema: schema.clone(),
                        rows,
                    },
                    hint: Some(WriteHint {
                        primary_key_encoding: api::v1::PrimaryKeyEncoding::Sparse as i32,
                    }),
                    skip_wal: false,
                    partition_expr_version: None,
                })
            }
        };
        let error = engine
            .handle_request(region_id, request(&malformed))
            .await
            .unwrap_err();
        assert_eq!(StatusCode::InvalidArguments, error.status_code());
        assert_eq!(0, scan_num_rows(&engine, region_id).await);
        // Byte validation follows WAL append; replay must also discard the malformed batch.
        reopen_region(&engine, region_id, table_dir.clone(), true, options.clone()).await;
        assert_eq!(0, scan_num_rows(&engine, region_id).await);
        engine
            .handle_request(region_id, request(&key))
            .await
            .unwrap();
        assert_eq!(2, scan_num_rows(&engine, region_id).await);

        reopen_region(&engine, region_id, table_dir, true, options).await;
        assert_eq!(2, scan_num_rows(&engine, region_id).await);
        engine.stop().await.unwrap();
    }
}

/// Batches that do not match the region schema are rejected before they are
/// appended to the WAL, and the region keeps accepting writes.
#[tokio::test]
async fn test_bulk_insert_rejects_batches_not_matching_schema() {
    common_telemetry::init_default_ut_logging();
    let list = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
        Some(vec![Some(1)]),
        Some(vec![Some(2)]),
    ]);
    let rejected = vec![
        vec![
            tag(),
            field(),
            column("ts", Arc::new(Int64Array::from(vec![0, 1000]))),
        ],
        vec![
            tag(),
            field(),
            column(
                "ts",
                Arc::new(TimestampMillisecondArray::from(vec![Some(0), None])),
            ),
        ],
        vec![
            tag(),
            field(),
            column(
                "ts",
                Arc::new(TimestampMillisecondArray::from(vec![None, None])),
            ),
        ],
        vec![
            tag(),
            field(),
            column("ts", Arc::new(TimestampSecondArray::from(vec![0, 1]))),
        ],
        vec![tag(), field()],
        vec![field(), ts()],
        vec![
            column("tag_0", Arc::new(Int64Array::from(vec![1, 2]))),
            field(),
            ts(),
        ],
        vec![
            column(
                "__primary_key",
                Arc::new(Float64Array::from(vec![1.0, 2.0])),
            ),
            field(),
            ts(),
        ],
        vec![
            column(
                "__primary_key",
                Arc::new(BinaryArray::from(vec![None::<&[u8]>; 2])),
            ),
            field(),
            ts(),
        ],
        vec![
            tag(),
            column("field_0", Arc::new(StringArray::from(vec!["1", "2"]))),
            ts(),
        ],
        vec![tag(), column("field_0", Arc::new(list)), ts()],
        vec![
            tag(),
            column(
                "field_0",
                Arc::new(Float64Array::from(vec![Some(1.0), None])),
            ),
            ts(),
        ],
        vec![tag(), field(), field(), ts()],
        vec![
            tag(),
            field(),
            ts(),
            column("unknown", Arc::new(Int64Array::from(vec![1, 2]))),
        ],
    ];
    for (i, (sst_format, encoding)) in REGION_LAYOUTS.into_iter().enumerate() {
        let mut env = TestEnv::new().await;
        let engine = env.create_engine(MitoConfig::default()).await;
        let region_id = RegionId::new(1, i as u32 + 1);
        let request = CreateRequestBuilder::new()
            .all_not_null(true)
            .insert_option("sst_format", sst_format)
            .insert_option("primary_key_encoding", encoding)
            .build();
        let column_schemas = rows_schema(&request);
        let table_dir = request.table_dir.clone();
        let options = request.options.clone();
        engine
            .handle_request(region_id, RegionRequest::Create(request))
            .await
            .unwrap();

        for columns in &rejected {
            let err = engine
                .handle_request(region_id, bulk_request(region_id, columns.clone()))
                .await
                .unwrap_err();
            assert_eq!(
                StatusCode::InvalidArguments,
                err.status_code(),
                "{sst_format}: {err:?}"
            );
        }

        let put = RegionRequest::Put(RegionPutRequest {
            rows: Rows {
                schema: column_schemas,
                rows: build_rows(0, 3),
            },
            hint: None,
            skip_wal: false,
            partition_expr_version: None,
        });
        engine.handle_request(region_id, put).await.unwrap();
        // Reopening replays the WAL, which holds none of the rejected batches.
        reopen_region(&engine, region_id, table_dir, true, options).await;
        assert_eq!(3, scan_num_rows(&engine, region_id).await, "{sst_format}");
    }
}

/// Tags may use any string encoding, which the memtables convert.
#[tokio::test]
async fn test_bulk_insert_accepts_tag_encodings() {
    common_telemetry::init_default_ut_logging();
    let tags: [ArrayRef; 3] = [
        Arc::new(StringArray::from(vec!["a", "b"])),
        Arc::new(LargeStringArray::from(vec!["a", "b"])),
        Arc::new(StringViewArray::from(vec!["a", "b"])),
    ];
    for (i, (sst_format, encoding)) in REGION_LAYOUTS.into_iter().enumerate() {
        let mut env = TestEnv::new().await;
        let engine = env.create_engine(MitoConfig::default()).await;
        for (j, tag) in tags.iter().enumerate() {
            let region_id = RegionId::new(i as u32 + 1, j as u32 + 1);
            let request = CreateRequestBuilder::new()
                .insert_option("sst_format", sst_format)
                .insert_option("primary_key_encoding", encoding)
                .build();
            let table_dir = request.table_dir.clone();
            let options = request.options.clone();
            engine
                .handle_request(region_id, RegionRequest::Create(request))
                .await
                .unwrap();

            let columns = vec![column("tag_0", tag.clone()), field(), ts()];
            let response = engine
                .handle_request(region_id, bulk_request(region_id, columns))
                .await
                .unwrap();
            assert_eq!(2, response.affected_rows, "{sst_format}: {tag:?}");
            reopen_region(&engine, region_id, table_dir, true, options).await;
            assert_eq!(
                2,
                scan_num_rows(&engine, region_id).await,
                "{sst_format}: {tag:?}"
            );
        }
    }
}

/// The worker checks a batch again if the region schema changed after the
/// request was checked.
#[tokio::test]
async fn test_bulk_insert_rechecks_batch_after_schema_change() {
    common_telemetry::init_default_ut_logging();
    for (i, (sst_format, encoding)) in REGION_LAYOUTS.into_iter().enumerate() {
        let mut env = TestEnv::new().await;
        let engine = env.create_engine(MitoConfig::default()).await;
        let region_id = RegionId::new(1, i as u32 + 1);
        let request = CreateRequestBuilder::new()
            .insert_option("sst_format", sst_format)
            .insert_option("primary_key_encoding", encoding)
            .build();
        let table_dir = request.table_dir.clone();
        let options = request.options.clone();
        engine
            .handle_request(region_id, RegionRequest::Create(request))
            .await
            .unwrap();
        let metadata = engine.get_region(region_id).unwrap().metadata();

        // The batch matches the schema before `field_0` becomes a string.
        let alter = RegionAlterRequest {
            kind: AlterKind::ModifyColumnTypes {
                columns: vec![ModifyColumnType {
                    column_name: "field_0".to_string(),
                    target_type: ConcreteDataType::string_datatype(),
                }],
            },
        };
        engine
            .handle_request(region_id, RegionRequest::Alter(alter))
            .await
            .unwrap();
        let (request, receiver) = WorkerRequest::try_from_region_request(
            region_id,
            bulk_request(region_id, vec![tag(), field(), ts()]),
            Some(metadata),
        )
        .unwrap();
        engine
            .inner
            .workers
            .submit_to_worker(region_id, request)
            .await
            .unwrap();
        let err = receiver.await.unwrap().unwrap_err();
        assert_eq!(
            StatusCode::InvalidArguments,
            err.status_code(),
            "{sst_format}: {err:?}"
        );

        reopen_region(&engine, region_id, table_dir, true, options).await;
        assert_eq!(0, scan_num_rows(&engine, region_id).await, "{sst_format}");
    }
}

/// A batch that claims to match the current schema but misses a column still
/// gets the column filled.
#[tokio::test]
async fn test_bulk_insert_fills_columns_missing_from_aligned_batch() {
    common_telemetry::init_default_ut_logging();
    for (i, (sst_format, encoding)) in REGION_LAYOUTS.into_iter().enumerate() {
        let mut env = TestEnv::new().await;
        let engine = env.create_engine(MitoConfig::default()).await;
        let region_id = RegionId::new(1, i as u32 + 1);
        let request = CreateRequestBuilder::new()
            .tag_num(2)
            .insert_option("sst_format", sst_format)
            .insert_option("primary_key_encoding", encoding)
            .build();
        let table_dir = request.table_dir.clone();
        let options = request.options.clone();
        engine
            .handle_request(region_id, RegionRequest::Create(request))
            .await
            .unwrap();

        let mut request = bulk_insert(region_id, vec![tag(), ts()]);
        request.aligned_schema_version = Some(
            engine
                .get_region(region_id)
                .unwrap()
                .metadata()
                .schema_version,
        );
        let response = engine
            .handle_request(region_id, RegionRequest::BulkInserts(request))
            .await
            .unwrap();
        assert_eq!(2, response.affected_rows, "{sst_format}");
        reopen_region(&engine, region_id, table_dir, true, options).await;
        assert_eq!(2, scan_num_rows(&engine, region_id).await, "{sst_format}");
        let stream = engine
            .scan_to_stream(region_id, ScanRequest::default())
            .await
            .unwrap();
        let batches = RecordBatches::try_collect(stream).await.unwrap();
        for batch in batches.iter() {
            let tag = batch.column_by_name("tag_1").unwrap();
            assert_eq!(tag.null_count(), batch.num_rows());
        }
    }
}

#[tokio::test]
async fn test_bulk_insert_json2_physical_layout() {
    use datatypes::arrow::array::{NullArray, StructArray};
    use datatypes::arrow::datatypes::DataType;
    use datatypes::extension::json::{Json2ExtensionType, JsonMetadata};
    use datatypes::json::JsonSettings;
    use datatypes::types::json_type::JsonNativeType;

    for encoding in ["dense", "sparse"] {
        for layout in ["v2", "legacy", "missing"] {
            let mut env = TestEnv::new().await;
            let engine = env.create_engine(MitoConfig::default()).await;
            let region_id = RegionId::new(1, 1);
            let settings = JsonSettings::default();
            let mut request = CreateRequestBuilder::new()
                .field_datatype(ConcreteDataType::json2(JsonNativeType::Null))
                .insert_option("sst_format", "flat")
                .insert_option("primary_key_encoding", encoding)
                .build();
            let json_column = &mut request.column_metadatas[1].column_schema;
            json_column.with_extension_type(&Json2ExtensionType::new(Arc::new(JsonMetadata::new(
                settings.clone(),
            ))));
            let mut columns = vec![tag(), ts()];
            if layout != "missing" {
                let (array, metadata) = if layout == "legacy" {
                    let array = Arc::new(StructArray::from(vec![(
                        Arc::new(Field::new("trace_id", DataType::Utf8, true)),
                        Arc::new(StringArray::from(vec!["abc", "def"])) as ArrayRef,
                    )])) as ArrayRef;
                    (array, JsonMetadata::new_v1(settings.clone()))
                } else {
                    let mut builder = json_column.create_mutable_vector(2);
                    for id in ["abc", "def"] {
                        let value = settings
                            .encode(serde_json::json!({"trace_id": id}))
                            .unwrap();
                        builder.try_push_value_ref(&value.as_value_ref()).unwrap();
                    }
                    (
                        builder.to_vector().to_arrow_array(),
                        JsonMetadata::new(settings.clone()),
                    )
                };
                let field = Field::new("field_0", array.data_type().clone(), true)
                    .with_extension_type(Json2ExtensionType::new(Arc::new(metadata)));
                columns.push((field, array));
            }
            let table_dir = request.table_dir.clone();
            let options = request.options.clone();
            engine
                .handle_request(region_id, RegionRequest::Create(request))
                .await
                .unwrap();
            if layout == "v2" {
                let malformed = Arc::new(StructArray::from(vec![(
                    Arc::new(Field::new(
                        datatypes::json::JSON2_REMAINDER_FIELD_NAME,
                        DataType::Int64,
                        true,
                    )),
                    Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
                )])) as ArrayRef;
                for malformed in [
                    malformed,
                    Arc::new(StringArray::from(vec!["abc", "def"])) as ArrayRef,
                ] {
                    let malformed_field =
                        Field::new("field_0", malformed.data_type().clone(), true)
                            .with_extension_type(Json2ExtensionType::new(Arc::new(
                                JsonMetadata::new(settings.clone()),
                            )));
                    let error = engine
                        .handle_request(
                            region_id,
                            bulk_request(
                                region_id,
                                vec![tag(), ts(), (malformed_field, malformed)],
                            ),
                        )
                        .await
                        .unwrap_err();
                    assert_eq!(error.status_code(), StatusCode::InvalidArguments);
                }
                let mut invalid_layout =
                    serde_json::to_value(JsonMetadata::new(settings.clone())).unwrap();
                invalid_layout["layout_version"] = serde_json::json!(3);
                let mut null_field = Field::new("field_0", DataType::Null, true)
                    .with_extension_type(Json2ExtensionType::new(Arc::new(JsonMetadata::new(
                        settings.clone(),
                    ))));
                null_field.metadata_mut().insert(
                    arrow_schema::extension::EXTENSION_TYPE_METADATA_KEY.to_string(),
                    invalid_layout.to_string(),
                );
                let error = engine
                    .handle_request(
                        region_id,
                        bulk_request(
                            region_id,
                            vec![tag(), ts(), (null_field, Arc::new(NullArray::new(2)))],
                        ),
                    )
                    .await
                    .unwrap_err();
                assert_eq!(error.status_code(), StatusCode::InvalidArguments);
            }
            let response = engine
                .handle_request(region_id, bulk_request(region_id, columns))
                .await
                .unwrap();
            assert_eq!(response.affected_rows, 2);
            for phase in 0..3 {
                if phase == 1 {
                    reopen_region(&engine, region_id, table_dir.clone(), true, options.clone())
                        .await;
                } else if phase == 2 {
                    crate::test_util::flush_region(&engine, region_id, None).await;
                }
                let stream = engine
                    .scan_to_stream(region_id, ScanRequest::default())
                    .await
                    .unwrap();
                let batches = RecordBatches::try_collect(stream).await.unwrap();
                assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
                let printed = batches.pretty_print().unwrap();
                if layout != "missing" {
                    assert!(
                        printed.contains("abc") && printed.contains("def"),
                        "{encoding}/{layout}/{phase}: {printed}"
                    );
                } else {
                    for batch in batches.iter() {
                        assert_eq!(
                            batch.column_by_name("field_0").unwrap().null_count(),
                            batch.num_rows()
                        );
                    }
                }
            }
        }
    }
}
