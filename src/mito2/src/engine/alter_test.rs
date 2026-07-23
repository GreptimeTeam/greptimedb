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

use std::assert_matches;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use api::v1::helper::{row, tag_column_schema};
use api::v1::value::ValueData;
use api::v1::{ArrowIpc, ColumnDataType, Row, Rows, SemanticType, Value};
use async_trait::async_trait;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_meta::ddl::utils::{parse_column_metadatas, parse_manifest_infos_from_extensions};
use common_recordbatch::{DfRecordBatch, RecordBatches};
use common_test_util::flight::encode_to_flight_data;
use datafusion_expr::col;
use datatypes::arrow::array::{ArrayRef, Float64Array, StringArray, TimestampMillisecondArray};
use datatypes::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, FulltextAnalyzer, FulltextBackend, FulltextOptions};
use object_store::Buffer;
use object_store::layers::mock::{
    Error as MockError, ErrorKind, Metadata, MockLayerBuilder, Result as MockResult, Write, Writer,
};
use parquet::basic::Encoding;
use parquet::file::metadata::PageIndexPolicy;
use store_api::metadata::ColumnMetadata;
use store_api::metric_engine_consts::TABLE_COLUMN_METADATA_EXTENSION_KEY;
use store_api::region_engine::{RegionEngine, RegionManifestInfo, RegionRole};
use store_api::region_request::{
    AddColumn, AddColumnLocation, AlterKind, PathType, RegionAlterRequest,
    RegionBulkInsertsRequest, RegionCreateRequest, RegionOpenRequest, RegionRequest,
    SetIndexOption, SetRegionOption,
};
use store_api::storage::{ColumnId, FileId, RegionId, ScanRequest};
use tokio::sync::{Notify, Semaphore};

use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::engine::listener::{
    AlterFlushListener, EventListener, NotifyRegionChangeResultListener,
};
use crate::error;
use crate::memtable::RangesOptions;
use crate::region::options::{FloatFieldEncodingPolicy, MemtableOptions};
use crate::sst::FormatType;
use crate::sst::parquet::reader::MetadataCacheMetrics;
use crate::test_util::batch_util::sort_batches_and_print;
use crate::test_util::{
    CreateRequestBuilder, TestEnv, build_rows, build_rows_for_key,
    column_metadata_to_column_schema, flush_region, put_rows, rows_schema,
};

async fn scan_check_after_alter(engine: &MitoEngine, region_id: RegionId, expected: &str) {
    let request = ScanRequest::default();
    let scanner = engine.scanner(region_id, request).await.unwrap();
    assert_eq!(0, scanner.num_memtables());
    assert_eq!(1, scanner.num_files());
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected, batches.pretty_print().unwrap());
}

fn alter_float_field_encoding(encoding: &str) -> RegionAlterRequest {
    RegionAlterRequest {
        kind: AlterKind::SetRegionOptions {
            options: vec![SetRegionOption::FloatFieldEncoding(encoding.to_string())],
        },
    }
}

async fn assert_sst_float_field_encoding(
    engine: &MitoEngine,
    region_id: RegionId,
    expected: &[bool],
) {
    let mut actual = sst_float_field_encodings(engine, region_id).await;
    let mut expected = expected.to_vec();
    actual.sort_unstable();
    expected.sort_unstable();
    assert_eq!(expected, actual);
}

async fn sst_float_field_encodings(engine: &MitoEngine, region_id: RegionId) -> Vec<bool> {
    sst_float_field_encodings_by_file_id(engine, region_id)
        .await
        .into_values()
        .collect()
}

async fn sst_float_field_encodings_by_file_id(
    engine: &MitoEngine,
    region_id: RegionId,
) -> HashMap<FileId, bool> {
    let region = engine.get_region(region_id).unwrap();
    let version = region.version();
    let files = &version.ssts.levels()[0].files;
    let mut encodings = HashMap::with_capacity(files.len());

    for (file_id, file) in files {
        let file_path = file.file_path(
            region.access_layer.table_dir(),
            region.access_layer.path_type(),
        );
        let (metadata, _) = region
            .access_layer
            .read_sst(file.clone())
            .read_parquet_metadata(
                &file_path,
                file.meta_ref().file_size,
                &mut MetadataCacheMetrics::default(),
                PageIndexPolicy::default(),
            )
            .await
            .unwrap();
        let parquet_metadata = metadata.parquet_metadata();
        let float_column = parquet_metadata
            .row_group(0)
            .columns()
            .iter()
            .find(|column| column.column_path().string() == "field_0")
            .unwrap();
        encodings.insert(
            *file_id,
            float_column
                .encodings()
                .any(|encoding| encoding == Encoding::BYTE_STREAM_SPLIT),
        );
    }
    encodings
}

struct FailOnceCloseState {
    path_matcher: Arc<dyn Fn(&str) -> bool + Send + Sync>,
    skipped_matches: usize,
    error_message: String,
    armed: AtomicBool,
    matches: AtomicUsize,
    failures: AtomicUsize,
    matched_paths: Mutex<Vec<String>>,
    failed_path: Mutex<Option<String>>,
    failing_close_started: Notify,
    release_failing_close: Semaphore,
}

impl FailOnceCloseState {
    fn for_path_suffix(path_suffix: impl Into<String>) -> Self {
        let path_suffix = path_suffix.into();
        Self::new(
            Arc::new(move |path| path.ends_with(&path_suffix)),
            0,
            "injected parquet close failure",
        )
    }

    fn new(
        path_matcher: Arc<dyn Fn(&str) -> bool + Send + Sync>,
        skipped_matches: usize,
        error_message: impl Into<String>,
    ) -> Self {
        Self {
            path_matcher,
            skipped_matches,
            error_message: error_message.into(),
            armed: AtomicBool::new(false),
            matches: AtomicUsize::new(0),
            failures: AtomicUsize::new(0),
            matched_paths: Mutex::new(Vec::new()),
            failed_path: Mutex::new(None),
            failing_close_started: Notify::new(),
            release_failing_close: Semaphore::new(0),
        }
    }

    fn matches(&self, path: &str) -> bool {
        (self.path_matcher)(path)
    }

    fn arm_once(&self) {
        assert!(
            self.armed
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok(),
            "fail-on-close state was armed more than once"
        );
    }

    fn take_failure(&self, path: &str) -> bool {
        self.matches(path)
            && self.armed.load(Ordering::SeqCst)
            && {
                let match_index = self.matches.fetch_add(1, Ordering::SeqCst);
                self.matched_paths.lock().unwrap().push(path.to_string());
                match_index >= self.skipped_matches
            }
            && self
                .failures
                .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
    }

    async fn wait_failing_close_started(&self) {
        self.failing_close_started.notified().await;
    }

    fn release_failure(&self) {
        self.release_failing_close.add_permits(1);
    }

    fn failures(&self) -> usize {
        self.failures.load(Ordering::SeqCst)
    }

    fn matched_paths(&self) -> Vec<String> {
        self.matched_paths.lock().unwrap().clone()
    }

    fn failed_path(&self) -> Option<String> {
        self.failed_path.lock().unwrap().clone()
    }
}

struct FailOnceCloseWriter {
    path: String,
    state: Arc<FailOnceCloseState>,
    inner: Writer,
}

impl Write for FailOnceCloseWriter {
    async fn write(&mut self, buffer: Buffer) -> MockResult<()> {
        self.inner.write(buffer).await
    }

    async fn close(&mut self) -> MockResult<Metadata> {
        if self.state.take_failure(&self.path) {
            *self.state.failed_path.lock().unwrap() = Some(self.path.clone());
            self.state.failing_close_started.notify_one();
            self.state
                .release_failing_close
                .acquire()
                .await
                .unwrap()
                .forget();
            return Err(MockError::new(
                ErrorKind::Unexpected,
                self.state.error_message.clone(),
            ));
        }
        self.inner.close().await
    }

    async fn abort(&mut self) -> MockResult<()> {
        self.inner.abort().await
    }
}

fn bulk_create_request(initial_encoding: Option<&str>) -> RegionCreateRequest {
    let builder = CreateRequestBuilder::new()
        .insert_option("memtable.type", "bulk")
        .insert_option("memtable.bulk.merge_threshold", "2")
        .insert_option("memtable.bulk.encode_row_threshold", "0")
        .insert_option("memtable.bulk.encode_bytes_threshold", "0");
    match initial_encoding {
        Some(encoding) => builder
            .insert_option("experimental_sst_float_field_encoding", encoding)
            .build(),
        None => builder.build(),
    }
}

async fn assert_encoded_bulk_part(engine: &MitoEngine, region_id: RegionId, expected_rows: usize) {
    let version = engine.get_region(region_id).unwrap().version();
    match version.options.memtable.as_ref().unwrap() {
        MemtableOptions::Bulk(config) => {
            assert_eq!(2, config.merge_threshold);
            assert_eq!(0, config.encode_row_threshold);
            assert_eq!(0, config.encode_bytes_threshold);
        }
        options => panic!("expected bulk memtable options, got {options:?}"),
    }
    let memtables = version.memtables.list_memtables();
    assert_eq!(1, memtables.len());
    let memtable = &memtables[0];
    // Force the two-part merge, then wait on the observable encoded-range state
    // if a concurrently dispatched background merge owns the parts first.
    for _ in 0..100 {
        memtable.compact(false).unwrap();
        let ranges = memtable.ranges(None, RangesOptions::default()).unwrap();
        if ranges.ranges.len() == 1
            && ranges.num_rows() == expected_rows
            && ranges
                .ranges
                .values()
                .all(|range| range.encoded().is_some())
        {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!(
        "bulk memtable did not install one {expected_rows}-row encoded range: memtable={memtable:?}"
    );
}

fn build_bulk_insert_request(
    region_id: RegionId,
    start: usize,
    end: usize,
) -> RegionBulkInsertsRequest {
    let schema = Arc::new(Schema::new(vec![
        Field::new("tag_0", DataType::Utf8, true),
        Field::new("field_0", DataType::Float64, true),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]));
    let tag = Arc::new(StringArray::from_iter_values(
        (start..end).map(|value| value.to_string()),
    )) as ArrayRef;
    let field = Arc::new(Float64Array::from_iter_values(
        (start..end).map(|value| value as f64),
    )) as ArrayRef;
    let ts = Arc::new(TimestampMillisecondArray::from_iter_values(
        (start..end).map(|value| value as i64 * 1000),
    )) as ArrayRef;
    let payload = DfRecordBatch::try_new(schema, vec![tag, field, ts]).unwrap();
    let (schema, record_batch) = encode_to_flight_data(payload.clone());

    RegionBulkInsertsRequest {
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

struct AlterBarrierListener {
    region_change_started: Notify,
    release_region_change: Semaphore,
    request_count: AtomicUsize,
    request_received: Notify,
}

impl Default for AlterBarrierListener {
    fn default() -> Self {
        Self {
            region_change_started: Notify::new(),
            release_region_change: Semaphore::new(0),
            request_count: AtomicUsize::new(0),
            request_received: Notify::new(),
        }
    }
}

impl AlterBarrierListener {
    async fn wait_region_change_started(&self) {
        self.region_change_started.notified().await;
    }

    fn release_region_change(&self) {
        self.release_region_change.add_permits(1);
    }

    fn request_count(&self) -> usize {
        self.request_count.load(Ordering::Relaxed)
    }

    async fn wait_request_count(&self, expected: usize) {
        while self.request_count() < expected {
            self.request_received.notified().await;
        }
    }
}

#[async_trait]
impl EventListener for AlterBarrierListener {
    fn on_recv_requests(&self, request_num: usize) {
        self.request_count.fetch_add(request_num, Ordering::Relaxed);
        self.request_received.notify_waiters();
    }

    async fn on_notify_region_change_result_begin(&self, _region_id: RegionId) {
        self.region_change_started.notify_one();
        self.release_region_change.acquire().await.unwrap().forget();
    }
}

fn add_tag1() -> RegionAlterRequest {
    RegionAlterRequest {
        kind: AlterKind::AddColumns {
            columns: vec![AddColumn {
                column_metadata: ColumnMetadata {
                    column_schema: ColumnSchema::new(
                        "tag_1",
                        ConcreteDataType::string_datatype(),
                        true,
                    ),
                    semantic_type: SemanticType::Tag,
                    column_id: 3,
                },
                location: Some(AddColumnLocation::First),
            }],
        },
    }
}

fn alter_column_inverted_index() -> RegionAlterRequest {
    RegionAlterRequest {
        kind: AlterKind::SetIndexes {
            options: vec![SetIndexOption::Inverted {
                column_name: "tag_0".to_string(),
            }],
        },
    }
}

fn alter_column_fulltext_options() -> RegionAlterRequest {
    RegionAlterRequest {
        kind: AlterKind::SetIndexes {
            options: vec![SetIndexOption::Fulltext {
                column_name: "tag_0".to_string(),
                options: FulltextOptions::new_unchecked(
                    true,
                    FulltextAnalyzer::English,
                    false,
                    FulltextBackend::Bloom,
                    1000,
                    0.01,
                ),
            }],
        },
    }
}

fn add_nullable_field1() -> RegionAlterRequest {
    RegionAlterRequest {
        kind: AlterKind::AddColumns {
            columns: vec![AddColumn {
                column_metadata: ColumnMetadata {
                    column_schema: ColumnSchema::new(
                        "field_1",
                        ConcreteDataType::float64_datatype(),
                        true,
                    ),
                    semantic_type: SemanticType::Field,
                    column_id: 3,
                },
                location: None,
            }],
        },
    }
}

fn build_row_with_added_field(
    metadata: &[ColumnMetadata],
    tag_0: &str,
    field_0: f64,
    field_1: Option<f64>,
    ts_millis: i64,
) -> Row {
    let values = metadata
        .iter()
        .map(|column| match column.column_schema.name.as_str() {
            "tag_0" => Value {
                value_data: Some(ValueData::StringValue(tag_0.to_string())),
            },
            "field_0" => Value {
                value_data: Some(ValueData::F64Value(field_0)),
            },
            "field_1" => Value {
                value_data: field_1.map(ValueData::F64Value),
            },
            "ts" => Value {
                value_data: Some(ValueData::TimestampMillisecondValue(ts_millis)),
            },
            name => panic!("unexpected column {name}"),
        })
        .collect();

    Row { values }
}

fn check_region_version(
    engine: &MitoEngine,
    region_id: RegionId,
    last_entry_id: u64,
    committed_sequence: u64,
    flushed_entry_id: u64,
    flushed_sequence: u64,
) {
    let region = engine.get_region(region_id).unwrap();
    let version_data = region.version_control.current();
    assert_eq!(last_entry_id, version_data.last_entry_id);
    assert_eq!(committed_sequence, version_data.committed_sequence);
    assert_eq!(flushed_entry_id, version_data.version.flushed_entry_id);
    assert_eq!(flushed_sequence, version_data.version.flushed_sequence);
}

fn assert_column_metadatas(column_name: &[(&str, ColumnId)], column_metadatas: &[ColumnMetadata]) {
    assert_eq!(column_name.len(), column_metadatas.len());
    for (name, id) in column_name {
        let column_metadata = column_metadatas
            .iter()
            .find(|c| c.column_id == *id)
            .unwrap();
        assert_eq!(column_metadata.column_schema.name, *name);
    }
}

#[tokio::test]
async fn test_alter_region() {
    test_alter_region_with_format(false).await;
    test_alter_region_with_format(true).await;
}

async fn test_alter_region_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    let column_schemas = rows_schema(&request);
    let table_dir = request.table_dir.clone();
    let response = engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    let column_metadatas =
        parse_column_metadatas(&response.extensions, TABLE_COLUMN_METADATA_EXTENSION_KEY).unwrap();
    assert_column_metadatas(
        &[("tag_0", 0), ("field_0", 1), ("ts", 2)],
        &column_metadatas,
    );

    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    let request = add_tag1();
    let response = engine
        .handle_request(region_id, RegionRequest::Alter(request))
        .await
        .unwrap();

    let expected = "\
+-------+-------+---------+---------------------+
| tag_1 | tag_0 | field_0 | ts                  |
+-------+-------+---------+---------------------+
|       | 0     | 0.0     | 1970-01-01T00:00:00 |
|       | 1     | 1.0     | 1970-01-01T00:00:01 |
|       | 2     | 2.0     | 1970-01-01T00:00:02 |
+-------+-------+---------+---------------------+";
    scan_check_after_alter(&engine, region_id, expected).await;
    check_region_version(&engine, region_id, 1, 3, 1, 3);

    let mut manifests = parse_manifest_infos_from_extensions(&response.extensions).unwrap();
    assert_eq!(manifests.len(), 1);
    let (return_region_id, manifest) = manifests.remove(0);
    assert_eq!(return_region_id, region_id);
    assert_eq!(manifest, RegionManifestInfo::mito(2, 1, 0));
    let column_metadatas =
        parse_column_metadatas(&response.extensions, TABLE_COLUMN_METADATA_EXTENSION_KEY).unwrap();
    assert_column_metadatas(
        &[("tag_0", 0), ("field_0", 1), ("ts", 2), ("tag_1", 3)],
        &column_metadatas,
    );

    // Reopen region.
    let engine = env
        .reopen_engine(
            engine,
            MitoConfig {
                default_flat_format: flat_format,
                ..Default::default()
            },
        )
        .await;
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                table_dir,
                path_type: PathType::Bare,
                options: HashMap::default(),
                skip_wal_replay: false,
                checkpoint: None,
                requirements: Default::default(),
            }),
        )
        .await
        .unwrap();
    scan_check_after_alter(&engine, region_id, expected).await;
    check_region_version(&engine, region_id, 1, 3, 1, 3);
}

#[tokio::test]
async fn test_filter_is_null_after_alter_add_field() {
    test_filter_is_null_after_alter_add_field_with_format(false).await;
    test_filter_is_null_after_alter_add_field_with_format(true).await;
}

async fn test_filter_is_null_after_alter_add_field_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas,
            rows: vec![build_rows_for_key("a", 0, 1, 1).into_iter().next().unwrap()],
        },
    )
    .await;
    flush_region(&engine, region_id, None).await;

    engine
        .handle_request(region_id, RegionRequest::Alter(add_nullable_field1()))
        .await
        .unwrap();

    let region = engine.get_region(region_id).unwrap();
    let metadata = region.metadata().column_metadatas.clone();
    let schema = metadata
        .iter()
        .map(column_metadata_to_column_schema)
        .collect();

    put_rows(
        &engine,
        region_id,
        Rows {
            schema,
            rows: vec![build_row_with_added_field(
                &metadata,
                "a",
                1.0,
                Some(10.0),
                0,
            )],
        },
    )
    .await;
    flush_region(&engine, region_id, None).await;

    // We skip field filters under merge mode because the flushed field values may be stale before
    // the row is merged with newer field data.
    let stream = engine
        .scan_to_stream(
            region_id,
            ScanRequest {
                filters: vec![col("field_1").is_null()],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+---------+---------------------+---------+
| tag_0 | field_0 | ts                  | field_1 |
+-------+---------+---------------------+---------+
| a     | 1.0     | 1970-01-01T00:00:00 | 10.0    |
+-------+---------+---------------------+---------+";
    assert_eq!(expected, batches.pretty_print().unwrap());
}

/// Build rows with schema (string, f64, ts_millis, string).
fn build_rows_for_tags(
    tag0: &str,
    tag1: &str,
    start: usize,
    end: usize,
    value_start: usize,
) -> Vec<Row> {
    (start..end)
        .enumerate()
        .map(|(idx, ts)| {
            row(vec![
                ValueData::StringValue(tag0.to_string()),
                ValueData::F64Value((value_start + idx) as f64),
                ValueData::TimestampMillisecondValue(ts as i64 * 1000),
                ValueData::StringValue(tag1.to_string()),
            ])
        })
        .collect()
}

#[tokio::test]
async fn test_put_after_alter() {
    test_put_after_alter_with_format(false).await;
    test_put_after_alter_with_format(true).await;
}

async fn test_put_after_alter_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;
    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    let mut column_schemas = rows_schema(&request);
    let table_dir = request.table_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("b", 0, 2, 0),
    };
    put_rows(&engine, region_id, rows).await;

    let request = add_tag1();
    engine
        .handle_request(region_id, RegionRequest::Alter(request))
        .await
        .unwrap();

    let expected = "\
+-------+-------+---------+---------------------+
| tag_1 | tag_0 | field_0 | ts                  |
+-------+-------+---------+---------------------+
|       | b     | 0.0     | 1970-01-01T00:00:00 |
|       | b     | 1.0     | 1970-01-01T00:00:01 |
+-------+-------+---------+---------------------+";
    scan_check_after_alter(&engine, region_id, expected).await;

    // Reopen region.
    let engine = env
        .reopen_engine(
            engine,
            MitoConfig {
                default_flat_format: flat_format,
                ..Default::default()
            },
        )
        .await;
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                table_dir,
                path_type: PathType::Bare,
                options: HashMap::default(),
                skip_wal_replay: false,
                checkpoint: None,
                requirements: Default::default(),
            }),
        )
        .await
        .unwrap();
    // Convert region to leader.
    engine
        .set_region_role(region_id, RegionRole::Leader)
        .unwrap();

    // Put with old schema.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("b", 2, 3, 2),
    };
    put_rows(&engine, region_id, rows).await;

    // Push tag_1 to schema.
    column_schemas.push(tag_column_schema("tag_1", ColumnDataType::String));
    // Put with new schema.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_tags("a", "a", 0, 2, 0),
    };
    put_rows(&engine, region_id, rows).await;

    // Scan again.
    let expected = "\
+-------+-------+---------+---------------------+
| tag_1 | tag_0 | field_0 | ts                  |
+-------+-------+---------+---------------------+
| a     | a     | 0.0     | 1970-01-01T00:00:00 |
| a     | a     | 1.0     | 1970-01-01T00:00:01 |
|       | b     | 0.0     | 1970-01-01T00:00:00 |
|       | b     | 1.0     | 1970-01-01T00:00:01 |
|       | b     | 2.0     | 1970-01-01T00:00:02 |
+-------+-------+---------+---------------------+";
    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_alter_region_retry() {
    test_alter_region_retry_with_format(false).await;
    test_alter_region_retry_with_format(true).await;
}

async fn test_alter_region_retry_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas,
        rows: build_rows_for_key("a", 0, 2, 0),
    };
    put_rows(&engine, region_id, rows).await;

    let request = add_tag1();
    engine
        .handle_request(region_id, RegionRequest::Alter(request))
        .await
        .unwrap();
    // Retries request.
    let request = add_tag1();
    let err = engine
        .handle_request(region_id, RegionRequest::Alter(request))
        .await
        .unwrap_err();
    let err = err.as_any().downcast_ref::<error::Error>().unwrap();
    assert_matches!(err, &error::Error::InvalidRegionRequest { .. });

    let expected = "\
+-------+-------+---------+---------------------+
| tag_1 | tag_0 | field_0 | ts                  |
+-------+-------+---------+---------------------+
|       | a     | 0.0     | 1970-01-01T00:00:00 |
|       | a     | 1.0     | 1970-01-01T00:00:01 |
+-------+-------+---------+---------------------+";
    scan_check_after_alter(&engine, region_id, expected).await;
    check_region_version(&engine, region_id, 1, 2, 1, 2);
}

#[tokio::test]
async fn test_alter_on_flushing() {
    test_alter_on_flushing_with_format(false).await;
    test_alter_on_flushing_with_format(true).await;
}

async fn test_alter_on_flushing_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new().await;
    let listener = Arc::new(AlterFlushListener::default());
    let engine = env
        .create_engine_with(
            MitoConfig {
                default_flat_format: flat_format,
                ..Default::default()
            },
            None,
            Some(listener.clone()),
            None,
        )
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Prepares rows for flush.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("a", 0, 2, 0),
    };
    put_rows(&engine, region_id, rows).await;

    // Spawns a task to flush the engine.
    let engine_cloned = engine.clone();
    let flush_job = tokio::spawn(async move {
        flush_region(&engine_cloned, region_id, None).await;
    });
    // Waits for flush begin.
    listener.wait_flush_begin().await;

    // Consumes the notify permit in the listener.
    listener.wait_request_begin().await;

    // Submits an alter request to the region. The region should add the request
    // to the pending ddl request list.
    let request = add_tag1();
    let engine_cloned = engine.clone();
    let alter_job = tokio::spawn(async move {
        engine_cloned
            .handle_request(region_id, RegionRequest::Alter(request))
            .await
            .unwrap();
    });
    // Waits until the worker handles the alter request.
    listener.wait_request_begin().await;

    // Spawns two task to flush the engine. The flush scheduler should put them to the
    // pending task list.
    let engine_cloned = engine.clone();
    let pending_flush_job = tokio::spawn(async move {
        flush_region(&engine_cloned, region_id, None).await;
    });
    // Waits until the worker handles the flush request.
    listener.wait_request_begin().await;

    // Wake up flush.
    listener.wake_flush();
    // Wait for the flush job.
    tokio::time::timeout(Duration::from_secs(5), flush_job)
        .await
        .unwrap()
        .unwrap();
    // Wait for pending flush job.
    tokio::time::timeout(Duration::from_secs(5), pending_flush_job)
        .await
        .unwrap()
        .unwrap();
    // Wait for the write job.
    tokio::time::timeout(Duration::from_secs(5), alter_job)
        .await
        .unwrap()
        .unwrap();

    let request = ScanRequest::default();
    let scanner = engine.scanner(region_id, request).await.unwrap();
    assert_eq!(0, scanner.num_memtables());
    assert_eq!(1, scanner.num_files());
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+-------+---------+---------------------+
| tag_1 | tag_0 | field_0 | ts                  |
+-------+-------+---------+---------------------+
|       | a     | 0.0     | 1970-01-01T00:00:00 |
|       | a     | 1.0     | 1970-01-01T00:00:01 |
+-------+-------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_alter_column_fulltext_options() {
    test_alter_column_fulltext_options_with_format(false).await;
    test_alter_column_fulltext_options_with_format(true).await;
}

async fn test_alter_column_fulltext_options_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new().await;
    let listener = Arc::new(AlterFlushListener::default());
    let engine = env
        .create_engine_with(
            MitoConfig {
                default_flat_format: flat_format,
                ..Default::default()
            },
            None,
            Some(listener.clone()),
            None,
        )
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    let column_schemas = rows_schema(&request);
    let table_dir = request.table_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    // Spawns a task to flush the engine.
    let engine_cloned = engine.clone();
    let flush_job = tokio::spawn(async move {
        flush_region(&engine_cloned, region_id, None).await;
    });
    // Waits for flush begin.
    listener.wait_flush_begin().await;

    // Consumes the notify permit in the listener.
    listener.wait_request_begin().await;

    // Submits an alter request to the region. The region should add the request
    // to the pending ddl request list.
    let request = alter_column_fulltext_options();
    let engine_cloned = engine.clone();
    let alter_job = tokio::spawn(async move {
        engine_cloned
            .handle_request(region_id, RegionRequest::Alter(request))
            .await
            .unwrap();
    });
    // Waits until the worker handles the alter request.
    listener.wait_request_begin().await;

    // Spawns two task to flush the engine. The flush scheduler should put them to the
    // pending task list.
    let engine_cloned = engine.clone();
    let pending_flush_job = tokio::spawn(async move {
        flush_region(&engine_cloned, region_id, None).await;
    });
    // Waits until the worker handles the flush request.
    listener.wait_request_begin().await;

    // Wake up flush.
    listener.wake_flush();
    // Wait for the flush job.
    flush_job.await.unwrap();
    // Wait for pending flush job.
    pending_flush_job.await.unwrap();
    // Wait for the write job.
    alter_job.await.unwrap();

    let expect_fulltext_options = FulltextOptions::new_unchecked(
        true,
        FulltextAnalyzer::English,
        false,
        FulltextBackend::Bloom,
        1000,
        0.01,
    );
    let check_fulltext_options = |engine: &MitoEngine, expected: &FulltextOptions| {
        let current_fulltext_options = engine
            .get_region(region_id)
            .unwrap()
            .metadata()
            .column_by_name("tag_0")
            .unwrap()
            .column_schema
            .fulltext_options()
            .unwrap()
            .unwrap();
        assert_eq!(*expected, current_fulltext_options);
    };
    check_fulltext_options(&engine, &expect_fulltext_options);
    check_region_version(&engine, region_id, 1, 3, 1, 3);

    // Reopen region.
    let engine = env
        .reopen_engine(
            engine,
            MitoConfig {
                default_flat_format: flat_format,
                ..Default::default()
            },
        )
        .await;
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                table_dir,
                path_type: PathType::Bare,
                options: HashMap::default(),
                skip_wal_replay: false,
                checkpoint: None,
                requirements: Default::default(),
            }),
        )
        .await
        .unwrap();
    check_fulltext_options(&engine, &expect_fulltext_options);
    check_region_version(&engine, region_id, 1, 3, 1, 3);
}

#[tokio::test]
async fn test_alter_column_set_inverted_index() {
    test_alter_column_set_inverted_index_with_format(false).await;
    test_alter_column_set_inverted_index_with_format(true).await;
}

async fn test_alter_column_set_inverted_index_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new().await;
    let listener = Arc::new(AlterFlushListener::default());
    let engine = env
        .create_engine_with(
            MitoConfig {
                default_flat_format: flat_format,
                ..Default::default()
            },
            None,
            Some(listener.clone()),
            None,
        )
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    let column_schemas = rows_schema(&request);
    let table_dir = request.table_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    // Spawns a task to flush the engine.
    let engine_cloned = engine.clone();
    let flush_job = tokio::spawn(async move {
        flush_region(&engine_cloned, region_id, None).await;
    });
    // Waits for flush begin.
    listener.wait_flush_begin().await;

    // Consumes the notify permit in the listener.
    listener.wait_request_begin().await;

    // Submits an alter request to the region. The region should add the request
    // to the pending ddl request list.
    let request = alter_column_inverted_index();
    let engine_cloned = engine.clone();
    let alter_job = tokio::spawn(async move {
        engine_cloned
            .handle_request(region_id, RegionRequest::Alter(request))
            .await
            .unwrap();
    });
    // Waits until the worker handles the alter request.
    listener.wait_request_begin().await;

    // Spawns two task to flush the engine. The flush scheduler should put them to the
    // pending task list.
    let engine_cloned = engine.clone();
    let pending_flush_job = tokio::spawn(async move {
        flush_region(&engine_cloned, region_id, None).await;
    });
    // Waits until the worker handles the flush request.
    listener.wait_request_begin().await;

    // Wake up flush.
    listener.wake_flush();
    // Wait for the flush job.
    flush_job.await.unwrap();
    // Wait for pending flush job.
    pending_flush_job.await.unwrap();
    // Wait for the write job.
    alter_job.await.unwrap();

    let check_inverted_index_set = |engine: &MitoEngine| {
        assert!(
            engine
                .get_region(region_id)
                .unwrap()
                .metadata()
                .column_by_name("tag_0")
                .unwrap()
                .column_schema
                .is_inverted_indexed()
        )
    };
    check_inverted_index_set(&engine);
    check_region_version(&engine, region_id, 1, 3, 1, 3);

    // Reopen region.
    let engine = env
        .reopen_engine(
            engine,
            MitoConfig {
                default_flat_format: flat_format,
                ..Default::default()
            },
        )
        .await;
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                table_dir,
                path_type: PathType::Bare,
                options: HashMap::default(),
                skip_wal_replay: false,
                checkpoint: None,
                requirements: Default::default(),
            }),
        )
        .await
        .unwrap();
    check_inverted_index_set(&engine);
    check_region_version(&engine, region_id, 1, 3, 1, 3);
}

#[tokio::test]
async fn test_alter_region_ttl_options() {
    test_alter_region_ttl_options_with_format(false).await;
    test_alter_region_ttl_options_with_format(true).await;
}

async fn test_alter_region_ttl_options_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new().await;
    let listener = Arc::new(AlterFlushListener::default());
    let engine = env
        .create_engine_with(
            MitoConfig {
                default_flat_format: flat_format,
                ..Default::default()
            },
            None,
            Some(listener.clone()),
            None,
        )
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    let engine_cloned = engine.clone();
    let alter_ttl_request = RegionAlterRequest {
        kind: AlterKind::SetRegionOptions {
            options: vec![SetRegionOption::Ttl(Some(Duration::from_secs(500).into()))],
        },
    };
    let alter_job = tokio::spawn(async move {
        engine_cloned
            .handle_request(region_id, RegionRequest::Alter(alter_ttl_request))
            .await
            .unwrap();
    });

    alter_job.await.unwrap();

    let check_ttl = |engine: &MitoEngine, expected: &Duration| {
        let current_ttl = engine.get_region(region_id).unwrap().version().options.ttl;
        assert_eq!(current_ttl, Some((*expected).into()));
    };
    // Verify the ttl.
    check_ttl(&engine, &Duration::from_secs(500));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_write_stall_on_altering() {
    common_telemetry::init_default_ut_logging();

    test_write_stall_on_altering_with_format(false).await;
    test_write_stall_on_altering_with_format(true).await;
}

async fn test_write_stall_on_altering_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let listener = Arc::new(NotifyRegionChangeResultListener::default());
    let engine = env
        .create_engine_with(
            MitoConfig {
                default_flat_format: flat_format,
                ..Default::default()
            },
            None,
            Some(listener.clone()),
            None,
        )
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let engine_cloned = engine.clone();
    let alter_job = tokio::spawn(async move {
        let request = add_tag1();
        engine_cloned
            .handle_request(region_id, RegionRequest::Alter(request))
            .await
            .unwrap();
    });
    // Make sure the loop is handling the alter request.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let column_schemas_cloned = column_schemas.clone();
    let engine_cloned = engine.clone();
    let put_job = tokio::spawn(async move {
        let rows = Rows {
            schema: column_schemas_cloned,
            rows: build_rows(0, 3),
        };
        put_rows(&engine_cloned, region_id, rows).await;
    });
    // Make sure the loop is handling the put request.
    tokio::time::sleep(Duration::from_millis(100)).await;

    listener.wake_notify();
    alter_job.await.unwrap();
    put_job.await.unwrap();

    let expected = "\
+-------+-------+---------+---------------------+
| tag_1 | tag_0 | field_0 | ts                  |
+-------+-------+---------+---------------------+
|       | 0     | 0.0     | 1970-01-01T00:00:00 |
|       | 1     | 1.0     | 1970-01-01T00:00:01 |
|       | 2     | 2.0     | 1970-01-01T00:00:02 |
+-------+-------+---------+---------------------+";
    let request = ScanRequest::default();
    let scanner = engine.scanner(region_id, request).await.unwrap();
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_alter_region_sst_format_with_flush() {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: false,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    let column_schemas = rows_schema(&request);
    let table_dir = request.table_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Inserts some data before alter
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    // Flushes to create SST files with primary_key format
    flush_region(&engine, region_id, None).await;

    let expected_data = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
+-------+---------+---------------------+";
    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected_data, batches.pretty_print().unwrap());

    // Alters sst_format from primary_key to flat
    let alter_format_request = RegionAlterRequest {
        kind: AlterKind::SetRegionOptions {
            options: vec![SetRegionOption::Format("flat".to_string())],
        },
    };
    engine
        .handle_request(region_id, RegionRequest::Alter(alter_format_request))
        .await
        .unwrap();

    // Inserts more data after alter
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(3, 6),
    };
    put_rows(&engine, region_id, rows).await;

    // Flushes to create SST files with flat format
    flush_region(&engine, region_id, None).await;

    let expected_all_data = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
| 3     | 3.0     | 1970-01-01T00:00:03 |
| 4     | 4.0     | 1970-01-01T00:00:04 |
| 5     | 5.0     | 1970-01-01T00:00:05 |
+-------+---------+---------------------+";
    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected_all_data, batches.pretty_print().unwrap());

    // Reopens region to verify format persists
    let engine = env
        .reopen_engine(
            engine,
            MitoConfig {
                default_flat_format: false,
                ..Default::default()
            },
        )
        .await;
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                table_dir,
                path_type: PathType::Bare,
                options: HashMap::default(),
                skip_wal_replay: false,
                checkpoint: None,
                requirements: Default::default(),
            }),
        )
        .await
        .unwrap();

    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected_all_data, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_alter_region_sst_format_without_flush() {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: false,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    let column_schemas = rows_schema(&request);
    let table_dir = request.table_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let check_format = |engine: &MitoEngine, expected: Option<FormatType>| {
        let current_format = engine
            .get_region(region_id)
            .unwrap()
            .version()
            .options
            .sst_format;
        assert_eq!(current_format, expected);
    };
    check_format(&engine, Some(FormatType::PrimaryKey));

    // Inserts some data before alter
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    // Alters sst_format from primary_key to flat
    let alter_format_request = RegionAlterRequest {
        kind: AlterKind::SetRegionOptions {
            options: vec![SetRegionOption::Format("flat".to_string())],
        },
    };
    engine
        .handle_request(region_id, RegionRequest::Alter(alter_format_request))
        .await
        .unwrap();

    check_format(&engine, Some(FormatType::Flat));

    // Inserts more data after alter
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(3, 6),
    };
    put_rows(&engine, region_id, rows).await;

    let expected_all_data = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
| 3     | 3.0     | 1970-01-01T00:00:03 |
| 4     | 4.0     | 1970-01-01T00:00:04 |
| 5     | 5.0     | 1970-01-01T00:00:05 |
+-------+---------+---------------------+";
    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected_all_data, batches.pretty_print().unwrap());

    // Reopens region to verify format persists
    let engine = env
        .reopen_engine(
            engine,
            MitoConfig {
                default_flat_format: false,
                ..Default::default()
            },
        )
        .await;
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                table_dir,
                path_type: PathType::Bare,
                options: HashMap::default(),
                skip_wal_replay: false,
                checkpoint: None,
                requirements: Default::default(),
            }),
        )
        .await
        .unwrap();

    check_format(&engine, Some(FormatType::Flat));

    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected_all_data, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_alter_region_sst_format_flat_to_pk_with_flush() {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: true,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    let column_schemas = rows_schema(&request);
    let table_dir = request.table_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Inserts some data with flat format
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    // Flushes to create SST files with flat format
    flush_region(&engine, region_id, None).await;

    let expected_data = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
+-------+---------+---------------------+";
    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected_data, batches.pretty_print().unwrap());

    // Alters sst_format from flat to primary_key
    let alter_format_request = RegionAlterRequest {
        kind: AlterKind::SetRegionOptions {
            options: vec![SetRegionOption::Format("primary_key".to_string())],
        },
    };
    engine
        .handle_request(region_id, RegionRequest::Alter(alter_format_request))
        .await
        .unwrap();

    // Inserts more data after alter
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(3, 6),
    };
    put_rows(&engine, region_id, rows).await;

    // Flushes to create SST files with primary_key format
    flush_region(&engine, region_id, None).await;

    let expected_all_data = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
| 3     | 3.0     | 1970-01-01T00:00:03 |
| 4     | 4.0     | 1970-01-01T00:00:04 |
| 5     | 5.0     | 1970-01-01T00:00:05 |
+-------+---------+---------------------+";
    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected_all_data, batches.pretty_print().unwrap());

    // Reopens region to verify format persists
    let engine = env
        .reopen_engine(
            engine,
            MitoConfig {
                default_flat_format: false,
                ..Default::default()
            },
        )
        .await;
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                table_dir,
                path_type: PathType::Bare,
                options: HashMap::default(),
                skip_wal_replay: false,
                checkpoint: None,
                requirements: Default::default(),
            }),
        )
        .await
        .unwrap();

    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected_all_data, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_alter_region_sst_format_flat_to_pk_without_flush() {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: true,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    let column_schemas = rows_schema(&request);
    let table_dir = request.table_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let check_format = |engine: &MitoEngine, expected: Option<FormatType>| {
        let current_format = engine
            .get_region(region_id)
            .unwrap()
            .version()
            .options
            .sst_format;
        assert_eq!(current_format, expected);
    };
    check_format(&engine, Some(FormatType::Flat));

    // Inserts some data with flat format
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    // Alters sst_format from flat to primary_key
    let alter_format_request = RegionAlterRequest {
        kind: AlterKind::SetRegionOptions {
            options: vec![SetRegionOption::Format("primary_key".to_string())],
        },
    };
    engine
        .handle_request(region_id, RegionRequest::Alter(alter_format_request))
        .await
        .unwrap();

    check_format(&engine, Some(FormatType::PrimaryKey));

    // Inserts more data after alter
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(3, 6),
    };
    put_rows(&engine, region_id, rows).await;

    let expected_all_data = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
| 3     | 3.0     | 1970-01-01T00:00:03 |
| 4     | 4.0     | 1970-01-01T00:00:04 |
| 5     | 5.0     | 1970-01-01T00:00:05 |
+-------+---------+---------------------+";
    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected_all_data, batches.pretty_print().unwrap());

    // Reopens region to verify format persists
    let engine = env
        .reopen_engine(
            engine,
            MitoConfig {
                default_flat_format: false,
                ..Default::default()
            },
        )
        .await;
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                table_dir,
                path_type: PathType::Bare,
                options: HashMap::default(),
                skip_wal_replay: false,
                checkpoint: None,
                requirements: Default::default(),
            }),
        )
        .await
        .unwrap();

    check_format(&engine, Some(FormatType::PrimaryKey));

    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected_all_data, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_alter_region_append_mode_with_flush() {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    // Create a region with append_mode=false (default)
    let request = CreateRequestBuilder::new().build();

    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    let column_schemas = rows_schema(&request);
    let table_dir = request.table_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let check_append_mode = |engine: &MitoEngine, expected: bool| {
        let append_mode = engine
            .get_region(region_id)
            .unwrap()
            .version()
            .options
            .append_mode;
        assert_eq!(append_mode, expected);
    };
    check_append_mode(&engine, false);

    // Inserts some data before alter (memtable not empty, alter will trigger flush)
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    // Alters append_mode from false to true (this triggers internal flush)
    let alter_request = RegionAlterRequest {
        kind: AlterKind::SetRegionOptions {
            options: vec![SetRegionOption::AppendMode(true)],
        },
    };
    engine
        .handle_request(region_id, RegionRequest::Alter(alter_request))
        .await
        .unwrap();

    check_append_mode(&engine, true);

    // Inserts duplicate data after alter (same as rows 0, 1, 2)
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    // Flushes again
    flush_region(&engine, region_id, None).await;

    // After append_mode=true, duplicates should be preserved
    let expected_all_data = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
+-------+---------+---------------------+";
    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(
        expected_all_data,
        sort_batches_and_print(&batches, &["tag_0", "ts"])
    );

    // Reopens region to verify append_mode persists
    let engine = env.reopen_engine(engine, MitoConfig::default()).await;
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                table_dir,
                path_type: PathType::Bare,
                options: HashMap::default(),
                skip_wal_replay: false,
                checkpoint: None,
                requirements: Default::default(),
            }),
        )
        .await
        .unwrap();

    check_append_mode(&engine, true);

    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(
        expected_all_data,
        sort_batches_and_print(&batches, &["tag_0", "ts"])
    );
}

#[tokio::test]
async fn test_alter_region_append_mode_without_flush() {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    // Create a region with append_mode=false (default)
    let request = CreateRequestBuilder::new().build();

    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    let column_schemas = rows_schema(&request);
    let table_dir = request.table_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let check_append_mode = |engine: &MitoEngine, expected: bool| {
        let append_mode = engine
            .get_region(region_id)
            .unwrap()
            .version()
            .options
            .append_mode;
        assert_eq!(append_mode, expected);
    };
    check_append_mode(&engine, false);

    // Alters append_mode from false to true immediately (no data, no flush needed)
    let alter_request = RegionAlterRequest {
        kind: AlterKind::SetRegionOptions {
            options: vec![SetRegionOption::AppendMode(true)],
        },
    };
    engine
        .handle_request(region_id, RegionRequest::Alter(alter_request))
        .await
        .unwrap();

    check_append_mode(&engine, true);

    // Inserts duplicate data
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    // Insert same data again
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    // Flushes
    flush_region(&engine, region_id, None).await;

    // Duplicates should be preserved
    let expected_all_data = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
+-------+---------+---------------------+";
    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(
        expected_all_data,
        sort_batches_and_print(&batches, &["tag_0", "ts"])
    );

    // Reopens region to verify append_mode persists
    let engine = env.reopen_engine(engine, MitoConfig::default()).await;
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                table_dir,
                path_type: PathType::Bare,
                options: HashMap::default(),
                skip_wal_replay: false,
                checkpoint: None,
                requirements: Default::default(),
            }),
        )
        .await
        .unwrap();

    check_append_mode(&engine, true);

    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(
        expected_all_data,
        sort_batches_and_print(&batches, &["tag_0", "ts"])
    );
}

#[tokio::test]
async fn test_alter_region_append_mode_invalid() {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    // Create a region with append_mode=true
    let request = CreateRequestBuilder::new()
        .insert_option("append_mode", "true")
        .build();

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let check_append_mode = |engine: &MitoEngine, expected: bool| {
        let append_mode = engine
            .get_region(region_id)
            .unwrap()
            .version()
            .options
            .append_mode;
        assert_eq!(append_mode, expected);
    };
    check_append_mode(&engine, true);

    // Try to alter append_mode from true to false (should fail)
    let alter_request = RegionAlterRequest {
        kind: AlterKind::SetRegionOptions {
            options: vec![SetRegionOption::AppendMode(false)],
        },
    };
    engine
        .handle_request(region_id, RegionRequest::Alter(alter_request))
        .await
        .unwrap_err();

    // append_mode should still be true
    check_append_mode(&engine, true);
}

#[tokio::test]
async fn test_alter_float_field_encoding_with_flush() {
    test_alter_float_field_encoding_with_flush_direction(None, "byte_stream_split", false, true)
        .await;
    test_alter_float_field_encoding_with_flush_direction(
        Some("byte_stream_split"),
        "default",
        true,
        false,
    )
    .await;
}

async fn test_alter_float_field_encoding_with_flush_direction(
    initial_encoding: Option<&str>,
    new_encoding: &str,
    initial_bss: bool,
    new_bss: bool,
) {
    let mut env = TestEnv::new().await;
    let listener = Arc::new(AlterBarrierListener::default());
    let engine = env
        .create_engine_with(
            MitoConfig {
                min_compaction_interval: Duration::from_secs(60 * 60),
                ..Default::default()
            },
            None,
            Some(listener.clone()),
            None,
        )
        .await;
    let region_id = RegionId::new(1, 1);
    let builder = match initial_encoding {
        Some(encoding) => CreateRequestBuilder::new()
            .insert_option("experimental_sst_float_field_encoding", encoding),
        None => CreateRequestBuilder::new(),
    };
    let request = builder.build();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas.clone(),
            rows: build_rows(0, 3),
        },
    )
    .await;
    let new_encoding = new_encoding.to_string();
    let alter_engine = engine.clone();
    let alter_job = tokio::spawn(async move {
        alter_engine
            .handle_request(
                region_id,
                RegionRequest::Alter(alter_float_field_encoding(&new_encoding)),
            )
            .await
            .unwrap();
    });
    listener.wait_region_change_started().await;
    assert_sst_float_field_encoding(&engine, region_id, &[initial_bss]).await;
    listener.release_region_change();
    alter_job.await.unwrap();
    let expected_policy = if new_bss {
        FloatFieldEncodingPolicy::ByteStreamSplit
    } else {
        FloatFieldEncodingPolicy::Default
    };
    assert_eq!(
        expected_policy,
        engine
            .get_region(region_id)
            .unwrap()
            .version()
            .options
            .float_field_encoding
    );

    put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas,
            rows: build_rows(3, 6),
        },
    )
    .await;
    flush_region(&engine, region_id, None).await;
    assert_sst_float_field_encoding(&engine, region_id, &[initial_bss, new_bss]).await;

    let stream = engine
        .scan_to_stream(region_id, ScanRequest::default())
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
| 3     | 3.0     | 1970-01-01T00:00:03 |
| 4     | 4.0     | 1970-01-01T00:00:04 |
| 5     | 5.0     | 1970-01-01T00:00:05 |
+-------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_alter_float_field_encoding_with_empty_memtable() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1, 1);
    engine
        .handle_request(
            region_id,
            RegionRequest::Create(CreateRequestBuilder::new().build()),
        )
        .await
        .unwrap();

    for (encoding, expected) in [
        (
            "byte_stream_split",
            FloatFieldEncodingPolicy::ByteStreamSplit,
        ),
        ("default", FloatFieldEncodingPolicy::Default),
    ] {
        engine
            .handle_request(
                region_id,
                RegionRequest::Alter(alter_float_field_encoding(encoding)),
            )
            .await
            .unwrap();
        let version = engine.get_region(region_id).unwrap().version();
        assert!(version.memtables.is_empty());
        assert!(version.ssts.levels()[0].files.is_empty());
        assert_eq!(expected, version.options.float_field_encoding);
    }
}

#[tokio::test]
async fn test_alter_float_field_encoding_same_value_and_invalid_requests() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas,
            rows: build_rows(0, 3),
        },
    )
    .await;

    let version_before = engine.get_region(region_id).unwrap().version();
    engine
        .handle_request(
            region_id,
            RegionRequest::Alter(alter_float_field_encoding("default")),
        )
        .await
        .unwrap();
    let version_after = engine.get_region(region_id).unwrap().version();
    assert!(Arc::ptr_eq(&version_before, &version_after));
    assert!(!version_after.memtables.is_empty());

    for encoding in ["Byte_Stream_Split", "invalid"] {
        let err = engine
            .handle_request(
                region_id,
                RegionRequest::Alter(alter_float_field_encoding(encoding)),
            )
            .await;
        assert!(err.is_err());
        assert!(Arc::ptr_eq(
            &version_before,
            &engine.get_region(region_id).unwrap().version()
        ));
    }

    let err = engine
        .handle_request(
            region_id,
            RegionRequest::Alter(RegionAlterRequest {
                kind: AlterKind::SetRegionOptions {
                    options: vec![
                        SetRegionOption::Ttl(Some(Duration::from_secs(10).into())),
                        SetRegionOption::FloatFieldEncoding("invalid".to_string()),
                    ],
                },
            }),
        )
        .await;
    assert!(err.is_err());
    let version_after_invalid = engine.get_region(region_id).unwrap().version();
    assert!(Arc::ptr_eq(&version_before, &version_after_invalid));
    assert_eq!(None, version_after_invalid.options.ttl);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_write_is_queued_until_float_field_encoding_change_completes() {
    let mut env = TestEnv::new().await;
    let listener = Arc::new(NotifyRegionChangeResultListener::default());
    let engine = env
        .create_engine_with(MitoConfig::default(), None, Some(listener.clone()), None)
        .await;
    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas.clone(),
            rows: build_rows(0, 3),
        },
    )
    .await;

    let alter_engine = engine.clone();
    let alter_job = tokio::spawn(async move {
        alter_engine
            .handle_request(
                region_id,
                RegionRequest::Alter(alter_float_field_encoding("byte_stream_split")),
            )
            .await
            .unwrap();
    });
    // The listener gates the manifest result after the old memtable has been
    // flushed. The write below must wait for the new builder to be installed.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let write_engine = engine.clone();
    let write_job = tokio::spawn(async move {
        put_rows(
            &write_engine,
            region_id,
            Rows {
                schema: column_schemas,
                rows: build_rows(3, 6),
            },
        )
        .await;
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        engine
            .get_region(region_id)
            .unwrap()
            .version()
            .memtables
            .is_empty()
    );

    listener.wake_notify();
    alter_job.await.unwrap();
    write_job.await.unwrap();
    flush_region(&engine, region_id, None).await;
    assert_sst_float_field_encoding(&engine, region_id, &[false, true]).await;
}

#[tokio::test]
async fn test_reopen_uses_caller_float_field_encoding_option() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let table_dir = request.table_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    engine
        .handle_request(
            region_id,
            RegionRequest::Alter(alter_float_field_encoding("byte_stream_split")),
        )
        .await
        .unwrap();

    let engine = env.reopen_engine(engine, MitoConfig::default()).await;
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                table_dir: table_dir.clone(),
                path_type: PathType::Bare,
                options: HashMap::default(),
                skip_wal_replay: false,
                checkpoint: None,
                requirements: Default::default(),
            }),
        )
        .await
        .unwrap();
    assert_eq!(
        FloatFieldEncodingPolicy::Default,
        engine
            .get_region(region_id)
            .unwrap()
            .version()
            .options
            .float_field_encoding
    );

    let engine = env.reopen_engine(engine, MitoConfig::default()).await;
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                table_dir,
                path_type: PathType::Bare,
                options: HashMap::from([(
                    "experimental_sst_float_field_encoding".to_string(),
                    "byte_stream_split".to_string(),
                )]),
                skip_wal_replay: false,
                checkpoint: None,
                requirements: Default::default(),
            }),
        )
        .await
        .unwrap();
    assert_eq!(
        FloatFieldEncodingPolicy::ByteStreamSplit,
        engine
            .get_region(region_id)
            .unwrap()
            .version()
            .options
            .float_field_encoding
    );
}

#[tokio::test]
async fn test_alter_float_field_encoding_with_preencoded_bulk_memtable() {
    test_alter_float_field_encoding_with_preencoded_bulk_memtable_direction(
        None,
        "byte_stream_split",
        false,
        true,
    )
    .await;
    test_alter_float_field_encoding_with_preencoded_bulk_memtable_direction(
        Some("byte_stream_split"),
        "default",
        true,
        false,
    )
    .await;
}

async fn test_alter_float_field_encoding_with_preencoded_bulk_memtable_direction(
    initial_encoding: Option<&str>,
    new_encoding: &str,
    initial_bss: bool,
    new_bss: bool,
) {
    let mut env = TestEnv::new().await;
    let listener = Arc::new(AlterBarrierListener::default());
    let engine = env
        .create_engine_with(
            MitoConfig {
                min_compaction_interval: Duration::from_secs(60 * 60),
                ..Default::default()
            },
            None,
            Some(listener.clone()),
            None,
        )
        .await;
    let region_id = RegionId::new(1, 1);
    let request = bulk_create_request(initial_encoding);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    for (start, end) in [(0, 1024), (1024, 2048)] {
        engine
            .handle_request(
                region_id,
                RegionRequest::BulkInserts(build_bulk_insert_request(region_id, start, end)),
            )
            .await
            .unwrap();
    }
    assert_encoded_bulk_part(&engine, region_id, 2048).await;

    let new_encoding = new_encoding.to_string();
    let alter_engine = engine.clone();
    let alter_job = tokio::spawn(async move {
        alter_engine
            .handle_request(
                region_id,
                RegionRequest::Alter(alter_float_field_encoding(&new_encoding)),
            )
            .await
            .unwrap();
    });
    listener.wait_region_change_started().await;
    assert_sst_float_field_encoding(&engine, region_id, &[initial_bss]).await;
    listener.release_region_change();
    alter_job.await.unwrap();

    engine
        .handle_request(
            region_id,
            RegionRequest::BulkInserts(build_bulk_insert_request(region_id, 2048, 2049)),
        )
        .await
        .unwrap();
    flush_region(&engine, region_id, None).await;
    assert_sst_float_field_encoding(&engine, region_id, &[initial_bss, new_bss]).await;

    let stream = engine
        .scan_to_stream(region_id, ScanRequest::default())
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(
        2049,
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>()
    );
    let printed = batches.pretty_print().unwrap();
    assert!(printed.contains("| 0     | 0.0"));
    assert!(printed.contains("| 2048  | 2048.0"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_bulk_insert_is_queued_until_float_field_encoding_alter_completes() {
    let mut env = TestEnv::new().await;
    let listener = Arc::new(AlterBarrierListener::default());
    let engine = env
        .create_engine_with(
            MitoConfig {
                min_compaction_interval: Duration::from_secs(60 * 60),
                ..Default::default()
            },
            None,
            Some(listener.clone()),
            None,
        )
        .await;
    let region_id = RegionId::new(1, 1);
    let request = bulk_create_request(None);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    for (start, end) in [(0, 1024), (1024, 2048)] {
        engine
            .handle_request(
                region_id,
                RegionRequest::BulkInserts(build_bulk_insert_request(region_id, start, end)),
            )
            .await
            .unwrap();
    }
    assert_encoded_bulk_part(&engine, region_id, 2048).await;

    let alter_engine = engine.clone();
    let alter_job = tokio::spawn(async move {
        alter_engine
            .handle_request(
                region_id,
                RegionRequest::Alter(alter_float_field_encoding("byte_stream_split")),
            )
            .await
            .unwrap();
    });
    listener.wait_region_change_started().await;
    assert_sst_float_field_encoding(&engine, region_id, &[false]).await;

    let request_count = listener.request_count();
    let bulk_engine = engine.clone();
    let bulk_job = tokio::spawn(async move {
        bulk_engine
            .handle_request(
                region_id,
                RegionRequest::BulkInserts(build_bulk_insert_request(region_id, 2048, 2051)),
            )
            .await
            .unwrap()
    });
    listener.wait_request_count(request_count + 1).await;
    assert!(!bulk_job.is_finished());
    assert!(
        engine
            .get_region(region_id)
            .unwrap()
            .version()
            .memtables
            .is_empty()
    );

    listener.release_region_change();
    alter_job.await.unwrap();
    assert_eq!(3, bulk_job.await.unwrap().affected_rows);
    assert_eq!(
        FloatFieldEncodingPolicy::ByteStreamSplit,
        engine
            .get_region(region_id)
            .unwrap()
            .version()
            .options
            .float_field_encoding
    );
    flush_region(&engine, region_id, None).await;
    assert_sst_float_field_encoding(&engine, region_id, &[false, true]).await;

    let stream = engine
        .scan_to_stream(region_id, ScanRequest::default())
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(
        2051,
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_alter_float_field_encoding_flush_failure_is_atomic() {
    let fail_state = Arc::new(FailOnceCloseState::for_path_suffix(".parquet"));
    let writer_state = fail_state.clone();
    let mock_layer = MockLayerBuilder::default()
        .writer_factory(Arc::new(move |path, _args, writer| {
            if writer_state.matches(path) {
                Box::new(FailOnceCloseWriter {
                    path: path.to_string(),
                    state: writer_state.clone(),
                    inner: writer,
                })
            } else {
                writer
            }
        }))
        .build()
        .unwrap();
    let mut env = TestEnv::new().await.with_mock_layer(mock_layer);
    let listener = Arc::new(AlterBarrierListener::default());
    let engine = env
        .create_engine_with(
            MitoConfig {
                min_compaction_interval: Duration::from_secs(60 * 60),
                ..Default::default()
            },
            None,
            Some(listener.clone()),
            None,
        )
        .await;
    let region_id = RegionId::new(1, 1);
    let request = bulk_create_request(None);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    for (start, end) in [(0, 1024), (1024, 2048)] {
        engine
            .handle_request(
                region_id,
                RegionRequest::BulkInserts(build_bulk_insert_request(region_id, start, end)),
            )
            .await
            .unwrap();
    }
    assert_encoded_bulk_part(&engine, region_id, 2048).await;

    fail_state.arm_once();
    let alter_engine = engine.clone();
    let alter_job = tokio::spawn(async move {
        alter_engine
            .handle_request(
                region_id,
                RegionRequest::Alter(alter_float_field_encoding("byte_stream_split")),
            )
            .await
    });
    tokio::time::timeout(
        Duration::from_secs(5),
        fail_state.wait_failing_close_started(),
    )
    .await
    .unwrap();

    let request_count = listener.request_count();
    let bulk_engine = engine.clone();
    let bulk_job = tokio::spawn(async move {
        bulk_engine
            .handle_request(
                region_id,
                RegionRequest::BulkInserts(build_bulk_insert_request(region_id, 2048, 2049)),
            )
            .await
    });
    tokio::time::timeout(
        Duration::from_secs(5),
        listener.wait_request_count(request_count + 1),
    )
    .await
    .unwrap();
    assert!(!bulk_job.is_finished());

    fail_state.release_failure();
    let alter_error = tokio::time::timeout(Duration::from_secs(5), alter_job)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(StatusCode::StorageUnavailable, alter_error.status_code());
    let queued_bulk_error = tokio::time::timeout(Duration::from_secs(5), bulk_job)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(
        StatusCode::StorageUnavailable,
        queued_bulk_error.status_code()
    );

    assert_eq!(1, fail_state.failures());
    assert_eq!(
        FloatFieldEncodingPolicy::Default,
        engine
            .get_region(region_id)
            .unwrap()
            .version()
            .options
            .float_field_encoding
    );

    let old_rows = engine
        .scan_to_stream(region_id, ScanRequest::default())
        .await
        .unwrap();
    let old_rows = RecordBatches::try_collect(old_rows).await.unwrap();
    assert_eq!(
        2048,
        old_rows.iter().map(|batch| batch.num_rows()).sum::<usize>()
    );
    let old_rows = old_rows.pretty_print().unwrap();
    assert!(old_rows.contains("| 0     | 0.0"));
    assert!(old_rows.contains("| 2047  | 2047.0"));

    engine
        .handle_request(
            region_id,
            RegionRequest::BulkInserts(build_bulk_insert_request(region_id, 2048, 2049)),
        )
        .await
        .unwrap();
    flush_region(&engine, region_id, None).await;
    let default_files = sst_float_field_encodings_by_file_id(&engine, region_id).await;
    assert!(!default_files.is_empty());
    assert!(default_files.values().all(|encoding| !encoding));

    let retry_engine = engine.clone();
    let retry_alter_job = tokio::spawn(async move {
        retry_engine
            .handle_request(
                region_id,
                RegionRequest::Alter(alter_float_field_encoding("byte_stream_split")),
            )
            .await
    });
    tokio::time::timeout(
        Duration::from_secs(5),
        listener.wait_region_change_started(),
    )
    .await
    .unwrap();
    listener.release_region_change();
    tokio::time::timeout(Duration::from_secs(5), retry_alter_job)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        FloatFieldEncodingPolicy::ByteStreamSplit,
        engine
            .get_region(region_id)
            .unwrap()
            .version()
            .options
            .float_field_encoding
    );
    engine
        .handle_request(
            region_id,
            RegionRequest::BulkInserts(build_bulk_insert_request(region_id, 2049, 2050)),
        )
        .await
        .unwrap();
    flush_region(&engine, region_id, None).await;
    let bss_files = sst_float_field_encodings_by_file_id(&engine, region_id).await;
    let new_file_ids = bss_files
        .keys()
        .filter(|file_id| !default_files.contains_key(file_id))
        .copied()
        .collect::<HashSet<_>>();
    assert!(!new_file_ids.is_empty());
    assert!(new_file_ids.iter().all(|file_id| bss_files[file_id]));

    let stream = engine
        .scan_to_stream(region_id, ScanRequest::default())
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(
        2050,
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>()
    );
    let printed = batches.pretty_print().unwrap();
    assert!(printed.contains("| 0     | 0.0"));
    assert!(printed.contains("| 2048  | 2048.0"));
    assert!(printed.contains("| 2049  | 2049.0"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_alter_float_field_encoding_manifest_failure_is_atomic() {
    let fail_state = Arc::new(FailOnceCloseState::new(
        Arc::new(|path| path.contains("/manifest/") && path.ends_with(".json")),
        1,
        "injected manifest close failure",
    ));
    let writer_state = fail_state.clone();
    let mock_layer = MockLayerBuilder::default()
        .writer_factory(Arc::new(move |path, _args, writer| {
            if writer_state.matches(path) {
                Box::new(FailOnceCloseWriter {
                    path: path.to_string(),
                    state: writer_state.clone(),
                    inner: writer,
                })
            } else {
                writer
            }
        }))
        .build()
        .unwrap();
    let mut env = TestEnv::new().await.with_mock_layer(mock_layer);
    let listener = Arc::new(AlterBarrierListener::default());
    let engine = env
        .create_engine_with(
            MitoConfig {
                min_compaction_interval: Duration::from_secs(60 * 60),
                ..Default::default()
            },
            None,
            Some(listener.clone()),
            None,
        )
        .await;
    let region_id = RegionId::new(1, 1);
    engine
        .handle_request(region_id, RegionRequest::Create(bulk_create_request(None)))
        .await
        .unwrap();
    for (start, end) in [(0, 1024), (1024, 2048)] {
        engine
            .handle_request(
                region_id,
                RegionRequest::BulkInserts(build_bulk_insert_request(region_id, start, end)),
            )
            .await
            .unwrap();
    }
    assert_encoded_bulk_part(&engine, region_id, 2048).await;

    let manifest_version = engine
        .get_region(region_id)
        .unwrap()
        .manifest_ctx
        .manifest_version()
        .await;
    let flush_manifest = format!("manifest/{:020}.json", manifest_version + 1);
    let change_manifest = format!("manifest/{:020}.json", manifest_version + 2);
    fail_state.arm_once();
    let alter_engine = engine.clone();
    let alter_job = tokio::spawn(async move {
        alter_engine
            .handle_request(
                region_id,
                RegionRequest::Alter(alter_float_field_encoding("byte_stream_split")),
            )
            .await
    });
    tokio::time::timeout(
        Duration::from_secs(5),
        fail_state.wait_failing_close_started(),
    )
    .await
    .unwrap();
    let matched_paths = fail_state.matched_paths();
    assert_eq!(2, matched_paths.len());
    assert!(matched_paths[0].ends_with(&flush_manifest));
    assert!(matched_paths[1].ends_with(&change_manifest));
    assert_eq!(Some(matched_paths[1].clone()), fail_state.failed_path());

    fail_state.release_failure();
    tokio::time::timeout(
        Duration::from_secs(5),
        listener.wait_region_change_started(),
    )
    .await
    .unwrap();
    assert!(!alter_job.is_finished());
    assert_sst_float_field_encoding(&engine, region_id, &[false]).await;

    let request_count = listener.request_count();
    let bulk_engine = engine.clone();
    let bulk_job = tokio::spawn(async move {
        bulk_engine
            .handle_request(
                region_id,
                RegionRequest::BulkInserts(build_bulk_insert_request(region_id, 2048, 2049)),
            )
            .await
    });
    tokio::time::timeout(
        Duration::from_secs(5),
        listener.wait_request_count(request_count + 1),
    )
    .await
    .unwrap();
    assert!(!bulk_job.is_finished());

    listener.release_region_change();
    let alter_error = tokio::time::timeout(Duration::from_secs(5), alter_job)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(StatusCode::StorageUnavailable, alter_error.status_code());
    assert_eq!(
        1,
        tokio::time::timeout(Duration::from_secs(5), bulk_job)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .affected_rows
    );

    assert_eq!(1, fail_state.failures());
    assert_eq!(
        FloatFieldEncodingPolicy::Default,
        engine
            .get_region(region_id)
            .unwrap()
            .version()
            .options
            .float_field_encoding
    );
    let stream = engine
        .scan_to_stream(region_id, ScanRequest::default())
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(
        2049,
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>()
    );
    let printed = batches.pretty_print().unwrap();
    assert!(printed.contains("| 0     | 0.0"));
    assert!(printed.contains("| 2048  | 2048.0"));

    flush_region(&engine, region_id, None).await;
    let default_files = sst_float_field_encodings_by_file_id(&engine, region_id).await;
    assert!(!default_files.is_empty());
    assert!(default_files.values().all(|encoding| !encoding));

    let retry_engine = engine.clone();
    let retry_alter_job = tokio::spawn(async move {
        retry_engine
            .handle_request(
                region_id,
                RegionRequest::Alter(alter_float_field_encoding("byte_stream_split")),
            )
            .await
    });
    tokio::time::timeout(
        Duration::from_secs(5),
        listener.wait_region_change_started(),
    )
    .await
    .unwrap();
    listener.release_region_change();
    tokio::time::timeout(Duration::from_secs(5), retry_alter_job)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        FloatFieldEncodingPolicy::ByteStreamSplit,
        engine
            .get_region(region_id)
            .unwrap()
            .version()
            .options
            .float_field_encoding
    );

    engine
        .handle_request(
            region_id,
            RegionRequest::BulkInserts(build_bulk_insert_request(region_id, 2049, 2050)),
        )
        .await
        .unwrap();
    flush_region(&engine, region_id, None).await;
    let bss_files = sst_float_field_encodings_by_file_id(&engine, region_id).await;
    let new_file_ids = bss_files
        .keys()
        .filter(|file_id| !default_files.contains_key(file_id))
        .copied()
        .collect::<HashSet<_>>();
    assert!(!new_file_ids.is_empty());
    assert!(new_file_ids.iter().all(|file_id| bss_files[file_id]));

    let stream = engine
        .scan_to_stream(region_id, ScanRequest::default())
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(
        2050,
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>()
    );
    let printed = batches.pretty_print().unwrap();
    assert!(printed.contains("| 0     | 0.0"));
    assert!(printed.contains("| 2048  | 2048.0"));
    assert!(printed.contains("| 2049  | 2049.0"));
}
