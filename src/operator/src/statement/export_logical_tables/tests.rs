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

use arrow::array::{
    ArrayRef, DictionaryArray, Float64Array, Int32Array, ListArray, StringArray, StructArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{Field, Schema, UInt32Type};
use bytes::Bytes;
use common_recordbatch::{RecordBatch as GreptimeRecordBatch, RecordBatches};
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use table::test_util::EmptyTable;
use table::test_util::table_info::test_table_info;

use super::*;

fn table(id: u32, name: &str, fields: Vec<Field>, physical: bool) -> TableRef {
    let schema = Arc::new(datatypes::schema::Schema::try_from(Schema::new(fields)).unwrap());
    let mut info = test_table_info(id, name, "public", "greptime", schema);
    info.meta.engine = METRIC_ENGINE_NAME.into();
    let (key, value) = if physical {
        (PHYSICAL_TABLE_METADATA_KEY, "")
    } else {
        (LOGICAL_TABLE_METADATA_KEY, "phy")
    };
    info.meta
        .options
        .extra_options
        .insert(key.into(), value.into());
    EmptyTable::from_table_info(&info)
}

fn export_limits() -> LogicalTableExportLimits {
    let mut limits = LogicalTableExportLimits::default();
    limits.writer.row_group_rows = 1;
    limits
}

fn unit() -> LogicalTableExport {
    let ts = Field::new(
        "ts",
        DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
        false,
    );
    let host = Field::new("host", DataType::Utf8, true);
    let a = Field::new("a", DataType::Float64, true);
    let b = Field::new("b", DataType::Float64, true);
    let physical = table(
        1024,
        "phy",
        vec![
            Field::new(TABLE_ID, DataType::UInt32, false),
            ts.clone(),
            host.clone(),
            a.clone(),
            b.clone(),
        ],
        true,
    );
    let tables = vec![
        table(
            1025,
            "cpu.v1",
            vec![host.clone(), a.clone(), ts.clone()],
            false,
        ),
        table(1026, "empty", vec![ts.clone(), a], false),
        table(1027, "requests", vec![ts, b, host], false),
    ];
    LogicalTableExport::try_new(physical, &tables).unwrap()
}

fn batch(ids: Vec<Option<u32>>, hosts: Vec<Option<&str>>) -> RecordBatch {
    use arrow::array::TimestampMillisecondArray;
    let rows = ids.len();
    let host: DictionaryArray<UInt32Type> = hosts.into_iter().collect();
    RecordBatch::try_from_iter_with_nullable([
        (TABLE_ID, Arc::new(UInt32Array::from(ids)) as ArrayRef, true),
        (
            "ts",
            Arc::new(TimestampMillisecondArray::from(vec![100; rows])),
            false,
        ),
        ("host", Arc::new(host), true),
        (
            "a",
            Arc::new(Float64Array::from(vec![Some(1.5); rows])),
            true,
        ),
        ("b", Arc::new(Float64Array::from(vec![None; rows])), true),
    ])
    .unwrap()
}

fn stream(batches: Vec<RecordBatch>) -> SendableRecordBatchStream {
    let schema = Arc::new(datatypes::schema::Schema::try_from(batches[0].schema()).unwrap());
    let batches = batches
        .into_iter()
        .map(|batch| GreptimeRecordBatch::from_df_record_batch(schema.clone(), batch))
        .collect();
    RecordBatches::try_new(schema, batches).unwrap().as_stream()
}

async fn read(store: &ObjectStore, path: &str) -> (SchemaRef, Vec<RecordBatch>) {
    let bytes = store.read(path).await.unwrap().to_bytes();
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
    let schema = builder.schema().clone();
    (
        schema,
        builder
            .build()
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap(),
    )
}

#[tokio::test]
async fn routes_across_batches_and_writes_empty_files() {
    let unit = unit();
    let store = ObjectStore::new(object_store::services::Memory::default()).unwrap();
    let batches = vec![
        batch(vec![Some(1025), Some(1025)], vec![Some(""), None]),
        batch(
            vec![Some(1025), Some(1027), Some(1028)],
            vec![Some("a"), Some("b"), Some("unselected")],
        ),
    ];
    let budget = ExportWriteBudget::new(4);
    let result = export_stream_managed(
        &unit,
        stream(batches),
        &store,
        export_limits(),
        &CancellationToken::new(),
        budget.clone(),
    )
    .await
    .unwrap();
    assert_eq!(
        result,
        LogicalTableExportSummary {
            rows: 4,
            skipped_rows: 1,
            files: 3
        }
    );
    let (schema, cpu) = read(&store, "cpu.v1.parquet").await;
    assert_eq!(schema.fields(), unit.logical_tables[&1025].schema.fields());
    let values: Vec<_> = cpu
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_string::<i32>()
                .iter()
                .map(|s| s.map(str::to_owned))
        })
        .collect();
    assert_eq!(values, vec![Some("".into()), None, Some("a".into())]);
    let (schema, empty) = read(&store, "empty.parquet").await;
    assert_eq!(schema.fields(), unit.logical_tables[&1026].schema.fields());
    assert!(empty.is_empty());
    let (schema, requests) = read(&store, "requests.parquet").await;
    assert_eq!(schema.fields(), unit.logical_tables[&1027].schema.fields());
    assert_eq!(requests[0].column(1).null_count(), 1);
    assert_eq!(budget.available(), (4, 64 * 1024 * 1024));
}

#[tokio::test]
async fn rejects_invalid_order_ids_and_resource_exhaustion() {
    let unit = unit();
    for (batches, limits, message) in [
        (
            vec![
                batch(vec![Some(1027)], vec![Some("a")]),
                batch(vec![Some(1025)], vec![Some("b")]),
            ],
            LogicalTableExportLimits::default(),
            "not ordered",
        ),
        (
            vec![batch(vec![None], vec![None])],
            LogicalTableExportLimits::default(),
            "null __table_id",
        ),
        (
            vec![batch(vec![Some(1025)], vec![Some("a")])],
            LogicalTableExportLimits {
                input_batch_bytes: 1,
                ..Default::default()
            },
            "input byte budget",
        ),
        (
            vec![batch(vec![Some(1025)], vec![Some("too big")])],
            LogicalTableExportLimits {
                conversion_bytes: 1,
                ..Default::default()
            },
            "one expanded logical row",
        ),
    ] {
        let store = ObjectStore::new(object_store::services::Memory::default()).unwrap();
        let err = export_stream(
            &unit,
            stream(batches),
            &store,
            limits,
            &CancellationToken::new(),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains(message), "{err}");
    }
}

#[tokio::test]
async fn existing_outputs_are_not_overwritten() {
    let store = ObjectStore::new(object_store::services::Memory::default()).unwrap();
    store.write("cpu.v1.parquet", "keep").await.unwrap();
    let err = export_stream(
        &unit(),
        stream(vec![batch(vec![Some(1025)], vec![None])]),
        &store,
        LogicalTableExportLimits::default(),
        &CancellationToken::new(),
    )
    .await
    .unwrap_err();
    if store.info().capability().write_with_if_not_exists {
        assert!(matches!(err, error::Error::WriteStreamToFile {
            source: common_datasource::error::Error::WriteObject { error, .. }, ..
        } if error.kind() == object_store::ErrorKind::ConditionNotMatch));
    } else {
        assert!(matches!(
            err,
            error::Error::InvalidLogicalTableExport { .. }
        ));
    }
    assert_eq!(
        store.read("cpu.v1.parquet").await.unwrap().to_bytes(),
        Bytes::from_static(b"keep")
    );
}

#[test]
fn dictionary_and_nested_histogram_values_are_bounded_before_expansion() {
    let dictionary = DictionaryArray::<UInt32Type>::try_new(
        UInt32Array::from(vec![0, 0, 0]),
        Arc::new(StringArray::from(vec!["x".repeat(4096)])),
    )
    .unwrap();
    let nested = ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        OffsetBuffer::new(vec![0i32, 2, 3, 5].into()),
        Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
        None,
    );
    let histogram = StructArray::from(vec![(
        Arc::new(Field::new("buckets", nested.data_type().clone(), true)),
        Arc::new(nested) as ArrayRef,
    )]);
    let batch = RecordBatch::try_from_iter([
        ("tag", Arc::new(dictionary) as ArrayRef),
        ("histogram", Arc::new(histogram)),
    ])
    .unwrap();
    assert_eq!(rows_within_budget(&batch, 0, 3, 4300).unwrap().0, 1);
    // The dictionary and container overhead fit; the nested list elements do not.
    assert!(rows_within_budget(&batch, 0, 3, 4180).is_err());
    assert_eq!(rows_within_budget(&batch, 0, 3, 15000).unwrap().0, 3);
}

#[test]
fn validates_selected_schemas_and_projects_only_selected_columns() {
    let unit = unit();
    let selected = table(
        1025,
        "only",
        vec![Field::new("a", DataType::Float64, true)],
        false,
    );
    let one =
        LogicalTableExport::try_new(unit.physical_table.clone(), std::slice::from_ref(&selected))
            .unwrap();
    assert_eq!(one.scan_projection, vec![0, 3]);
    assert_eq!(one.logical_tables[&1025].projection, vec![1]);
    assert!(
        LogicalTableExport::try_new(unit.physical_table.clone(), &[selected.clone(), selected])
            .is_err()
    );
    let wrong_type = table(
        1030,
        "wrong",
        vec![Field::new("a", DataType::Int32, true)],
        false,
    );
    assert!(LogicalTableExport::try_new(unit.physical_table, &[wrong_type]).is_err());
}

#[tokio::test]
async fn cancellation_drops_input_and_aborts_active_upload() {
    let store = ObjectStore::new(object_store::services::Memory::default()).unwrap();
    let unit = unit();
    let batch = batch(vec![Some(1025)], vec![Some("a")]);
    let schema = batch.schema();
    let token = CancellationToken::new();
    let trigger = token.clone();
    let batches = async_stream::stream! {
        yield Ok(batch);
        // Resuming the source proves the previous batch reached the writer.
        trigger.cancel();
        std::future::pending::<()>().await;
    };
    let df_stream =
        datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(schema, batches);
    let stream =
        common_recordbatch::adapter::RecordBatchStreamAdapter::try_new(Box::pin(df_stream))
            .unwrap();
    let result = export_stream(&unit, Box::pin(stream), &store, export_limits(), &token).await;
    assert!(matches!(
        result,
        Err(error::Error::LogicalTableExportCancelled { .. })
    ));
    assert!(!store.exists("cpu.v1.parquet").await.unwrap());
    assert!(!store.exists("empty.parquet").await.unwrap());
}

#[tokio::test]
async fn native_histogram_parquet_roundtrip() {
    fn sample(data_type: &DataType) -> ArrayRef {
        match data_type {
            DataType::Struct(fields) => Arc::new(StructArray::new(
                fields.clone(),
                fields.iter().map(|f| sample(f.data_type())).collect(),
                None,
            )),
            DataType::List(field) => Arc::new(ListArray::new(
                field.clone(),
                OffsetBuffer::new(vec![0i32, 1, 2].into()),
                sample(field.data_type()),
                None,
            )),
            DataType::Int32 => Arc::new(Int32Array::from(vec![Some(1), None])),
            DataType::Int64 => Arc::new(arrow::array::Int64Array::from(vec![Some(2), None])),
            DataType::Float64 => Arc::new(Float64Array::from(vec![Some(3.5), None])),
            DataType::Timestamp(_, _) => {
                Arc::new(arrow::array::TimestampMillisecondArray::from(vec![
                    Some(4),
                    None,
                ]))
            }
            other => panic!("unexpected histogram field {other}"),
        }
    }
    let data_type = common_query::native_histogram::native_histogram_arrow_type();
    let field = Field::new("histogram", data_type.clone(), true);
    let unit = LogicalTableExport::try_new(
        table(
            1024,
            "phy",
            vec![Field::new(TABLE_ID, DataType::UInt32, false), field.clone()],
            true,
        ),
        &[table(1025, "histogram", vec![field], false)],
    )
    .unwrap();
    let batch = RecordBatch::try_from_iter_with_nullable([
        (
            TABLE_ID,
            Arc::new(UInt32Array::from(vec![1025; 2])) as ArrayRef,
            false,
        ),
        ("histogram", sample(&data_type), true),
    ])
    .unwrap();
    let store = ObjectStore::new(object_store::services::Memory::default()).unwrap();
    let summary = export_stream(
        &unit,
        stream(vec![batch.clone()]),
        &store,
        export_limits(),
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(summary.rows, 2);
    let (schema, batches) = read(&store, "histogram.parquet").await;
    assert_eq!(schema.fields(), unit.logical_tables[&1025].schema.fields());
    assert_eq!(
        arrow::compute::concat_batches(&schema, &batches).unwrap(),
        batch.project(&[1]).unwrap()
    );
}

struct PausedFileWriter {
    inner: Option<object_store::layers::mock::oio::Writer>,
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

impl object_store::layers::mock::oio::Write for PausedFileWriter {
    async fn write(&mut self, bytes: object_store::Buffer) -> object_store::Result<()> {
        let mut inner = self.inner.take().unwrap();
        let started = self.started.clone();
        let release = self.release.clone();
        // Like SecureFs's blocking open, this task survives a dropped I/O future.
        let (inner, result) = tokio::spawn(async move {
            started.notify_one();
            release.notified().await;
            let result = inner.write(bytes).await;
            (inner, result)
        })
        .await
        .unwrap();
        self.inner = Some(inner);
        result
    }

    async fn close(&mut self) -> object_store::Result<object_store::layers::mock::Metadata> {
        self.inner.as_mut().unwrap().close().await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        match self.inner.as_mut() {
            Some(inner) => inner.abort().await,
            None => Err(object_store::Error::new(
                object_store::ErrorKind::Unsupported,
                "open is pending",
            )),
        }
    }
}

#[tokio::test]
async fn cancellation_drains_storage_and_preserves_committed_files() {
    for large in [false, true] {
        use object_store::layers::mock::{MockLayerBuilder, MockWriterFactory};
        let directory = common_test_util::temp_dir::create_temp_dir("metric_export_pending_open");
        let access =
            common_datasource::object_store::LocalFileAccess::sandboxed(directory.path()).unwrap();
        let store = build_backend_for_write(
            &format!("{}/", directory.path().display()),
            &HashMap::new(),
            &access,
        )
        .await
        .unwrap();
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let factory: MockWriterFactory = Arc::new({
            let started = started.clone();
            let release = release.clone();
            move |_, _, inner| {
                Box::new(PausedFileWriter {
                    inner: Some(inner),
                    started: started.clone(),
                    release: release.clone(),
                })
            }
        });
        let store = store.layer(
            MockLayerBuilder::default()
                .writer_factory(factory)
                .build()
                .unwrap(),
        );
        let cancellation = CancellationToken::new();
        let (unit, input) = if large {
            let field = Field::new("value", DataType::Utf8, true);
            let unit = LogicalTableExport::try_new(
                table(
                    1024,
                    "phy",
                    vec![Field::new(TABLE_ID, DataType::UInt32, false), field.clone()],
                    true,
                ),
                &[table(1025, "cpu.v1", vec![field], false)],
            )
            .unwrap();
            let mut state = 17u64;
            let strings = (0..512)
                .map(|_| {
                    (0..32768)
                        .map(|_| {
                            state ^= state << 13;
                            state ^= state >> 7;
                            state ^= state << 17;
                            char::from(b' ' + (state % 95) as u8)
                        })
                        .collect::<String>()
                })
                .collect::<Vec<_>>();
            let input = RecordBatch::try_from_iter([
                (
                    TABLE_ID,
                    Arc::new(UInt32Array::from(vec![1025; 512])) as ArrayRef,
                ),
                ("value", Arc::new(StringArray::from(strings))),
            ])
            .unwrap();
            (unit, input)
        } else {
            (unit(), batch(vec![Some(1025)], vec![Some("a")]))
        };
        let budget = ExportWriteBudget::new(1);
        let mut limits = LogicalTableExportLimits::default();
        limits.writer.flush_threshold_bytes = 1024 * 1024;
        let export = export_stream_managed(
            &unit,
            stream(vec![input]),
            &store,
            limits,
            &cancellation,
            budget.clone(),
        );
        tokio::pin!(export);
        tokio::select! {
            result = &mut export => panic!("export completed before the file open: {result:?}"),
            _ = started.notified() => {},
        }
        if large {
            assert!(budget.available().1 < 64 * 1024 * 1024);
        }
        let held = budget.available();
        cancellation.cancel();
        assert!(futures::poll!(&mut export).is_pending());
        assert_eq!(budget.available().0, held.0);
        if large {
            // Cancelled admission releases its pending reservation; the worker
            // still owns payload capacity while the storage operation is paused.
            assert!(budget.available().1 < 64 * 1024 * 1024);
        }
        release.notify_one();
        let result = export.await;
        assert!(matches!(
            result,
            Err(error::Error::LogicalTableExportCancelled { .. })
        ));
        assert_eq!(store.exists("cpu.v1.parquet").await.unwrap(), !large);
        if !large {
            let (_, batches) = read(&store, "cpu.v1.parquet").await;
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        }
        assert_eq!(budget.available(), (1, 64 * 1024 * 1024));
    }
}

struct FailedAbortWriter(object_store::layers::mock::oio::Writer);

#[tokio::test]
async fn metric_fallback_and_ordinary_preserve_ambiguous_commits() {
    use object_store::layers::CapabilityOverrideLayer;
    use object_store::layers::mock::{Metadata, MockLayerBuilder, MockWriterFactory, oio};

    use crate::statement::copy_table_to::stream_to_managed_parquet;

    struct AmbiguousCommit(oio::Writer);
    impl oio::Write for AmbiguousCommit {
        async fn write(&mut self, bytes: object_store::Buffer) -> object_store::Result<()> {
            self.0.write(bytes).await
        }
        async fn close(&mut self) -> object_store::Result<Metadata> {
            self.0.close().await?;
            Err(object_store::Error::new(
                object_store::ErrorKind::Unexpected,
                "lost close reply",
            ))
        }
        async fn abort(&mut self) -> object_store::Result<()> {
            Err(object_store::Error::new(
                object_store::ErrorKind::Unsupported,
                "cannot abort",
            ))
        }
    }
    let factory: MockWriterFactory = Arc::new(|_, args, inner| {
        assert!(!args.if_not_exists());
        Box::new(AmbiguousCommit(inner))
    });
    let store = ObjectStore::new(object_store::services::Memory::default())
        .unwrap()
        .layer(CapabilityOverrideLayer::new(|mut capability| {
            capability.write_with_if_not_exists = false;
            capability
        }))
        .layer(
            MockLayerBuilder::default()
                .writer_factory(factory)
                .build()
                .unwrap(),
        );
    assert!(!store.info().capability().write_with_if_not_exists);
    let rows = || stream(vec![batch(vec![Some(1025)], vec![Some("a")])]);
    assert!(
        export_stream(
            &unit(),
            rows(),
            &store,
            export_limits(),
            &CancellationToken::new()
        )
        .await
        .is_err()
    );
    let budget = ExportWriteBudget::new(1);
    assert!(
        stream_to_managed_parquet(
            rows(),
            store.clone(),
            "ordinary.parquet",
            &budget,
            &CancellationToken::new()
        )
        .await
        .is_err()
    );
    for path in ["cpu.v1.parquet", "ordinary.parquet"] {
        let (_, batches) = read(&store, path).await;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    }
    assert_eq!(budget.available(), (1, 64 * 1024 * 1024));
}

impl object_store::layers::mock::oio::Write for FailedAbortWriter {
    async fn write(&mut self, bytes: object_store::Buffer) -> object_store::Result<()> {
        self.0.write(bytes).await
    }
    async fn close(&mut self) -> object_store::Result<object_store::layers::mock::Metadata> {
        self.0.close().await
    }
    async fn abort(&mut self) -> object_store::Result<()> {
        Err(object_store::Error::new(
            object_store::ErrorKind::PermissionDenied,
            "injected abort failure",
        ))
    }
}

#[tokio::test]
async fn cleanup_failure_preserves_resource_error() {
    use object_store::layers::mock::{MockLayerBuilder, MockWriterFactory};
    let factory: MockWriterFactory = Arc::new(|_, _, inner| Box::new(FailedAbortWriter(inner)));
    let store = ObjectStore::new(object_store::services::Memory::default())
        .unwrap()
        .layer(
            MockLayerBuilder::default()
                .writer_factory(factory)
                .build()
                .unwrap(),
        );
    let mut limits = export_limits();
    limits.writer.max_row_groups = 1;
    let result = export_stream(
        &unit(),
        stream(vec![batch(vec![Some(1025); 2], vec![Some("a"); 2])]),
        &store,
        limits,
        &CancellationToken::new(),
    )
    .await;
    assert!(matches!(
        result,
        Err(error::Error::LogicalTableExportResource { .. })
    ));
}

#[tokio::test]
async fn validates_membership_by_table_route() {
    use common_meta::kv_backend::TxnService;
    use common_meta::kv_backend::memory::MemoryKvBackend;

    let unit = unit();
    for later_physical_id in [None, Some(2048), Some(1024)] {
        let kv = Arc::new(MemoryKvBackend::default());
        let manager = TableRouteManager::new(kv.clone());
        for (&table_id, physical_id) in
            unit.logical_tables
                .keys()
                .zip([Some(1024), later_physical_id, Some(1024)])
        {
            if let Some(physical_id) = physical_id {
                let (txn, _) = manager
                    .table_route_storage()
                    .build_create_txn(table_id, &TableRouteValue::logical(physical_id))
                    .unwrap();
                assert!(kv.txn(txn).await.unwrap().succeeded);
            }
        }
        let result = unit.validate_table_routes(&manager).await;
        if later_physical_id == Some(1024) {
            result.unwrap();
        } else {
            assert!(matches!(
                result,
                Err(error::Error::InvalidLogicalTableExport { .. })
            ));
        }
    }
}

#[tokio::test]
async fn retained_backing_is_reserved_before_conversion_and_until_payload_drop() {
    let input = batch(vec![Some(1025); 8192], vec![Some("shared"); 8192]);
    let tiny = input.slice(0, 1);
    let schema = Arc::new(Schema::new(vec![Field::new("host", DataType::Utf8, true)]));
    let projected = tiny.project(&[2]).unwrap();
    let full = input.project(&[2]).unwrap().get_array_memory_size();
    assert_eq!(projected.get_array_memory_size(), full);
    let budget = ExportWriteBudget::new(1);
    let token = CancellationToken::new();
    let blocker = budget.reserve(64 * 1024 * 1024, &token).await.unwrap();
    let convert = expand_bounded_slice(projected, schema, 0, 1, 1024, &budget, &token);
    tokio::pin!(convert);
    assert!(futures::poll!(&mut convert).is_pending());
    drop(blocker);
    let (payload, rows) = convert.await.unwrap();
    assert_eq!(rows, 1);
    assert!(
        64 * 1024 * 1024 - budget.available().1 >= full + payload.batch.get_array_memory_size()
    );
    drop(payload);
    assert_eq!(budget.available(), (1, 64 * 1024 * 1024));
    assert!(budget.reserve(64 * 1024 * 1024 + 1, &token).await.is_err());
}

#[tokio::test]
async fn groups_and_ordinary_files_share_writer_admission_and_drain() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use object_store::layers::mock::{Metadata, MockLayerBuilder, MockWriterFactory, oio};
    use tokio::sync::Semaphore;

    use crate::statement::copy_table_to::stream_to_managed_parquet;

    struct CountedWriter {
        inner: oio::Writer,
        active: Arc<AtomicUsize>,
        started: Arc<Semaphore>,
        release: Arc<Semaphore>,
    }
    impl Drop for CountedWriter {
        fn drop(&mut self) {
            self.active.fetch_sub(1, Ordering::SeqCst);
        }
    }
    impl oio::Write for CountedWriter {
        async fn write(&mut self, bytes: object_store::Buffer) -> object_store::Result<()> {
            self.inner.write(bytes).await
        }
        async fn close(&mut self) -> object_store::Result<Metadata> {
            self.started.add_permits(1);
            self.release.acquire().await.unwrap().forget();
            self.inner.close().await
        }
        async fn abort(&mut self) -> object_store::Result<()> {
            self.inner.abort().await
        }
    }
    for parallelism in [1, 4] {
        for cancel in [false, true] {
            let active = Arc::new(AtomicUsize::new(0));
            let peak = Arc::new(AtomicUsize::new(0));
            let started = Arc::new(Semaphore::new(0));
            let release = Arc::new(Semaphore::new(0));
            let factory: MockWriterFactory = Arc::new({
                let (active, peak, started, release) = (
                    active.clone(),
                    peak.clone(),
                    started.clone(),
                    release.clone(),
                );
                move |_, args, inner| {
                    assert_eq!(args.concurrent(), 1);
                    let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(now, Ordering::SeqCst);
                    Box::new(CountedWriter {
                        inner,
                        active: active.clone(),
                        started: started.clone(),
                        release: release.clone(),
                    })
                }
            });
            let store = ObjectStore::new(object_store::services::Memory::default())
                .unwrap()
                .layer(
                    MockLayerBuilder::default()
                        .writer_factory(factory)
                        .build()
                        .unwrap(),
                );
            let budget = ExportWriteBudget::new(parallelism);
            let token = CancellationToken::new();
            let mut a = unit();
            let mut b = unit();
            for (group, unit) in [("a", &mut a), ("b", &mut b)] {
                for table in unit.logical_tables.values_mut() {
                    table.output.path = format!("{group}/{}", table.output.path);
                }
            }
            let rows = || {
                stream(vec![batch(
                    vec![Some(1025), Some(1027)],
                    vec![Some("a"), Some("b")],
                )])
            };
            let ordinary = async {
                let _permit = budget.writer(&token).await?;
                stream_to_managed_parquet(
                    rows(),
                    store.clone(),
                    "ordinary.parquet",
                    &budget,
                    &token,
                )
                .await
            };
            let work = async {
                tokio::join!(
                    export_stream_managed(
                        &a,
                        rows(),
                        &store,
                        export_limits(),
                        &token,
                        budget.clone()
                    ),
                    export_stream_managed(
                        &b,
                        rows(),
                        &store,
                        export_limits(),
                        &token,
                        budget.clone()
                    ),
                    ordinary,
                )
            };
            tokio::pin!(work);
            tokio::select! {
                _ = started.acquire_many(parallelism as u32) => {},
                result = &mut work => panic!("completed while close paused: {result:?}"),
            }
            assert_eq!(budget.available().0, 0);
            assert_eq!(peak.load(Ordering::SeqCst), parallelism);
            if cancel {
                token.cancel();
            }
            assert!(futures::poll!(&mut work).is_pending());
            release.add_permits(10);
            let (a, b, ordinary) = work.await;
            assert_eq!(a.is_err(), cancel);
            assert_eq!(b.is_err(), cancel);
            assert_eq!(ordinary.is_err(), cancel);
            assert_eq!(active.load(Ordering::SeqCst), 0);
            assert!(peak.load(Ordering::SeqCst) <= parallelism);
            assert_eq!(budget.available(), (parallelism, 64 * 1024 * 1024));
        }
    }
}

#[tokio::test]
async fn nested_dictionary_conversion_limits_child_ranges_and_charges_null_parents() {
    let values = Arc::new(DictionaryArray::<UInt32Type>::new(
        UInt32Array::from(vec![0; 1024]),
        Arc::new(StringArray::from(vec!["x".repeat(4096)])),
    )) as ArrayRef;
    let list = ListArray::new(
        Arc::new(Field::new("item", values.data_type().clone(), true)),
        OffsetBuffer::new(vec![0i32, 1023, 1024].into()),
        values,
        None,
    );
    let input = RecordBatch::try_from_iter([("list", Arc::new(list) as ArrayRef)]).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "list",
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        true,
    )]));
    let budget = ExportWriteBudget::new(1);
    let (payload, rows) = expand_bounded_slice(
        input,
        schema,
        1,
        2,
        8192,
        &budget,
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(rows, 1);
    assert!(payload.batch.get_array_memory_size() < 16384);
    let list = payload.batch.column(0).as_list::<i32>();
    assert_eq!(list.value_offsets(), &[0, 1]);
    assert_eq!(list.values().as_string::<i32>().value(0), "x".repeat(4096));
    drop(payload);
    assert_eq!(budget.available(), (1, 64 * 1024 * 1024));

    let values = Arc::new(DictionaryArray::<UInt32Type>::new(
        UInt32Array::from(vec![0]),
        Arc::new(StringArray::from(vec!["x".repeat(4096)])),
    )) as ArrayRef;
    let structure = StructArray::new(
        vec![Arc::new(Field::new(
            "child",
            values.data_type().clone(),
            true,
        ))]
        .into(),
        vec![values],
        Some(arrow::buffer::NullBuffer::from(vec![false])),
    );
    let input = RecordBatch::try_from_iter([("struct", Arc::new(structure) as ArrayRef)]).unwrap();
    assert!(rows_within_budget(&input, 0, 1, 128).is_err());
}

#[tokio::test]
async fn completed_table_workers_are_reaped_during_admission() {
    for parallelism in [1, 4] {
        let mut unit = unit();
        let file = unit.logical_tables.get_mut(&1025).unwrap();
        let store = ObjectStore::new(object_store::services::Memory::default()).unwrap();
        let budget = ExportWriteBudget::new(parallelism);
        let token = CancellationToken::new();
        let mut writers = TableWriters::new(budget.clone());
        for id in 0..64 {
            file.output.path = format!("empty-{id}.parquet");
            writers
                .open(id, file, &store, export_limits(), &token)
                .await
                .unwrap();
            assert!(writers.pending_tasks() <= parallelism + 1);
        }
        writers.drain(Ok(()), &token).await.unwrap();
        assert_eq!(writers.pending_tasks(), 0);
        assert_eq!(budget.available(), (parallelism, 64 * 1024 * 1024));
    }
}
