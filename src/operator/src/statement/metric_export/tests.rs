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

fn unit() -> MetricExportUnit {
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
    MetricExportUnit::try_new(physical, &tables).unwrap()
}

fn batch(ids: Vec<Option<u32>>, hosts: Vec<Option<&str>>) -> RecordBatch {
    use arrow::array::TimestampMillisecondArray;
    let rows = ids.len();
    let host: DictionaryArray<UInt32Type> = hosts.into_iter().collect();
    let fields = vec![
        Field::new(TABLE_ID, DataType::UInt32, true),
        Field::new(
            "ts",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("host", host.data_type().clone(), true),
        Field::new("a", DataType::Float64, true),
        Field::new("b", DataType::Float64, true),
    ];
    RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        vec![
            Arc::new(UInt32Array::from(ids)),
            Arc::new(TimestampMillisecondArray::from(vec![100; rows])),
            Arc::new(host),
            Arc::new(Float64Array::from(vec![Some(1.5); rows])),
            Arc::new(Float64Array::from(vec![None; rows])),
        ],
    )
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
    let result = export_stream(
        &unit,
        stream(batches),
        &store,
        MetricExportLimits {
            row_group_rows: 1,
            ..Default::default()
        },
        &mut None,
    )
    .await
    .unwrap();
    assert_eq!(
        result,
        MetricExportSummary {
            rows: 4,
            skipped_rows: 1,
            files: 3
        }
    );
    let (schema, cpu) = read(&store, "cpu.v1.parquet").await;
    assert_eq!(schema.fields(), unit.logical[&1025].schema.fields());
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
    assert_eq!(schema.fields(), unit.logical[&1026].schema.fields());
    assert!(empty.is_empty());
    let (schema, requests) = read(&store, "requests.parquet").await;
    assert_eq!(schema.fields(), unit.logical[&1027].schema.fields());
    assert_eq!(requests[0].column(1).null_count(), 1);
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
            MetricExportLimits::default(),
            "not ordered",
        ),
        (
            vec![batch(vec![None], vec![None])],
            MetricExportLimits::default(),
            "null __table_id",
        ),
        (
            vec![batch(vec![Some(1025)], vec![Some("a")])],
            MetricExportLimits {
                input_batch_bytes: 1,
                ..Default::default()
            },
            "input byte budget",
        ),
        (
            vec![batch(vec![Some(1025)], vec![Some("too big")])],
            MetricExportLimits {
                conversion_bytes: 1,
                ..Default::default()
            },
            "one expanded logical row",
        ),
        (
            vec![batch(vec![Some(1025); 2], vec![Some("a"); 2])],
            MetricExportLimits {
                row_group_rows: 1,
                max_row_groups: 1,
                ..Default::default()
            },
            "metadata budget",
        ),
    ] {
        let store = ObjectStore::new(object_store::services::Memory::default()).unwrap();
        let mut active = None;
        let err = export_stream(&unit, stream(batches), &store, limits, &mut active)
            .await
            .unwrap_err();
        assert!(err.to_string().contains(message), "{err}");
        if let Some(mut writer) = active {
            writer.sink.abort().await.unwrap();
        }
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
        MetricExportLimits::default(),
        &mut None,
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("already exists"));
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
    let fields = vec![Arc::new(Field::new(
        "buckets",
        nested.data_type().clone(),
        true,
    ))];
    let histogram = StructArray::new(fields.into(), vec![Arc::new(nested)], None);
    let arrays: Vec<ArrayRef> = vec![Arc::new(dictionary), Arc::new(histogram)];
    let schema = Arc::new(Schema::new(
        arrays
            .iter()
            .enumerate()
            .map(|(i, a)| Field::new(i.to_string(), a.data_type().clone(), true))
            .collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(schema, arrays).unwrap();
    assert_eq!(bounded_slice_len(&batch, 0, 3, 4300).unwrap(), 1);
    assert!(bounded_slice_len(&batch, 0, 3, 4096).is_err());
    assert_eq!(bounded_slice_len(&batch, 0, 3, 15000).unwrap(), 3);
}

#[test]
fn validates_membership_and_projects_only_selected_columns() {
    let unit = unit();
    let selected = table(
        1025,
        "only",
        vec![Field::new("a", DataType::Float64, true)],
        false,
    );
    let one = MetricExportUnit::try_new(unit.physical.clone(), &[selected.clone()]).unwrap();
    assert_eq!(one.projection, vec![0, 3]);
    assert_eq!(one.logical[&1025].projection, vec![1]);
    assert!(
        MetricExportUnit::try_new(unit.physical.clone(), &[selected.clone(), selected]).is_err()
    );
    let unsafe_name = table(1030, "a/b", vec![], false);
    assert!(MetricExportUnit::try_new(unit.physical.clone(), &[unsafe_name]).is_err());
    let wrong_type = table(
        1030,
        "wrong",
        vec![Field::new("a", DataType::Int32, true)],
        false,
    );
    assert!(MetricExportUnit::try_new(unit.physical, &[wrong_type]).is_err());
}

#[tokio::test]
async fn cancellation_drops_input_and_aborts_active_upload() {
    let directory = common_test_util::temp_dir::create_temp_dir("metric_export_cancel");
    let access =
        common_datasource::object_store::LocalFileAccess::sandboxed(directory.path()).unwrap();
    let file_store = build_backend_for_write(
        &format!("{}/", directory.path().display()),
        &HashMap::new(),
        &access,
    )
    .await
    .unwrap();
    let memory_store = ObjectStore::new(object_store::services::Memory::default()).unwrap();
    for store in [memory_store, file_store] {
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
        let result = export_cancellable_stream(
            &unit,
            Box::pin(stream),
            &store,
            MetricExportLimits {
                row_group_rows: 1,
                ..Default::default()
            },
            &token,
        )
        .await;
        assert!(matches!(
            result,
            Err(error::Error::MetricExportCancelled { .. })
        ));
        assert!(!store.exists("cpu.v1.parquet").await.unwrap());
        assert!(!store.exists("empty.parquet").await.unwrap());
    }
}

#[tokio::test]
async fn native_histogram_parquet_roundtrip() {
    use datatypes::data_type::DataType as _;
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
    let data_type = common_query::native_histogram::native_histogram_value_type().as_arrow_type();
    let histogram = sample(&data_type);
    let schema = Arc::new(Schema::new(vec![Field::new("histogram", data_type, true)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![histogram]).unwrap();
    let store = ObjectStore::new(object_store::services::Memory::default()).unwrap();
    let file = LogicalFile {
        name: "histogram".into(),
        schema: schema.clone(),
        projection: vec![0],
    };
    let limits = MetricExportLimits {
        row_group_rows: 1,
        ..Default::default()
    };
    let mut active = Some(LogicalWriter::open(1, &file, &store, limits).await.unwrap());
    for start in 0..2 {
        let writer = active.as_mut().unwrap();
        let (encoder, rows, bytes) = encode_slice(
            writer.encoder.take().unwrap(),
            batch.clone(),
            schema.clone(),
            start,
            2,
            limits,
        )
        .await
        .unwrap();
        assert_eq!(rows, 1);
        writer.encoder = Some(encoder);
        writer.write(bytes).await.unwrap();
    }
    finish_active(&mut active).await.unwrap();
    let (actual_schema, batches) = read(&store, "histogram.parquet").await;
    assert_eq!(actual_schema.fields(), schema.fields());
    let actual = arrow::compute::concat_batches(&actual_schema, &batches).unwrap();
    assert_eq!(actual, batch);
}
