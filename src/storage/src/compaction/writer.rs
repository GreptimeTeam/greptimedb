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

use common_query::logical_plan::{DfExpr, Expr};
use datafusion_common::ScalarValue;
use datafusion_expr::{BinaryExpr, Operator};

use crate::chunk::{ChunkReaderBuilder, ChunkReaderImpl};
use crate::error;
use crate::schema::RegionSchemaRef;
use crate::sst::{AccessLayerRef, FileHandle};

/// Builds an SST reader that only reads rows within given time range.
pub(crate) async fn build_sst_reader(
    schema: RegionSchemaRef,
    sst_layer: AccessLayerRef,
    files: &[FileHandle],
    lower_sec_inclusive: i64,
    upper_sec_exclusive: i64,
) -> error::Result<ChunkReaderImpl> {
    // TODO(hl): Schemas in different SSTs may differ, thus we should infer
    // timestamp column name from Parquet metadata.
    let ts_col_name = schema
        .user_schema()
        .timestamp_column()
        .unwrap()
        .name
        .clone();

    ChunkReaderBuilder::new(schema, sst_layer)
        .pick_ssts(files)
        .filters(vec![build_time_range_filter(
            lower_sec_inclusive,
            upper_sec_exclusive,
            &ts_col_name,
        )])
        .build()
        .await
}

fn build_time_range_filter(low_sec: i64, high_sec: i64, ts_col_name: &str) -> Expr {
    let ts_col = Box::new(DfExpr::Column(datafusion_common::Column::from_name(
        ts_col_name,
    )));
    let lower_bound_expr = Box::new(DfExpr::Literal(ScalarValue::TimestampSecond(
        Some(low_sec),
        None,
    )));

    let upper_bound_expr = Box::new(DfExpr::Literal(ScalarValue::TimestampSecond(
        Some(high_sec),
        None,
    )));

    let expr = DfExpr::BinaryExpr(BinaryExpr {
        left: Box::new(DfExpr::BinaryExpr(BinaryExpr {
            left: ts_col.clone(),
            op: Operator::GtEq,
            right: lower_bound_expr,
        })),
        op: Operator::And,
        right: Box::new(DfExpr::BinaryExpr(BinaryExpr {
            left: ts_col,
            op: Operator::Lt,
            right: upper_bound_expr,
        })),
    });

    Expr::from(expr)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    use common_base::readable_size::ReadableSize;
    use common_test_util::temp_dir::create_temp_dir;
    use common_time::Timestamp;
    use datatypes::prelude::{LogicalTypeId, ScalarVector, ScalarVectorBuilder};
    use datatypes::timestamp::TimestampMillisecond;
    use datatypes::vectors::{
        TimestampMillisecondVector, TimestampMillisecondVectorBuilder, UInt64VectorBuilder,
    };
    use object_store::services::Fs;
    use object_store::ObjectStore;
    use store_api::storage::{ChunkReader, OpType, SequenceNumber};

    use super::*;
    use crate::file_purger::noop::new_noop_file_purger;
    use crate::memtable::{
        DefaultMemtableBuilder, IterContext, KeyValues, Memtable, MemtableBuilder,
    };
    use crate::metadata::RegionMetadata;
    use crate::sst::parquet::ParquetWriter;
    use crate::sst::{self, FileId, FileMeta, FsAccessLayer, Source, SstInfo, WriteOptions};
    use crate::test_util::descriptor_util::RegionDescBuilder;

    fn schema_for_test() -> RegionSchemaRef {
        // Just build a region desc and use its columns metadata.
        let desc = RegionDescBuilder::new("test")
            .push_field_column(("v", LogicalTypeId::UInt64, true))
            .build();
        let metadata: RegionMetadata = desc.try_into().unwrap();
        metadata.schema().clone()
    }

    pub fn write_kvs(
        memtable: &dyn Memtable,
        sequence: SequenceNumber,
        op_type: OpType,
        ts: &[i64], // timestamp
        values: &[Option<u64>],
    ) {
        let keys: Vec<TimestampMillisecond> = ts.iter().map(|ts| (*ts).into()).collect();
        let kvs = kvs_for_test(sequence, op_type, &keys, values);
        memtable.write(&kvs).unwrap();
    }

    fn kvs_for_test(
        sequence: SequenceNumber,
        op_type: OpType,
        ts: &[TimestampMillisecond],
        values: &[Option<u64>],
    ) -> KeyValues {
        let start_index_in_batch = 0;
        assert_eq!(ts.len(), values.len());
        let mut key_builders = TimestampMillisecondVectorBuilder::with_capacity(ts.len());
        for key in ts {
            key_builders.push(Some(*key));
        }
        let ts_col = Arc::new(key_builders.finish()) as _;
        let mut value_builders = UInt64VectorBuilder::with_capacity(values.len());

        for value in values {
            value_builders.push(*value);
        }
        let row_values = vec![Arc::new(value_builders.finish()) as _];

        let kvs = KeyValues {
            sequence,
            op_type,
            start_index_in_batch,
            keys: vec![],
            values: row_values,
            timestamp: Some(ts_col),
        };

        assert_eq!(ts.len(), kvs.len());
        assert_eq!(ts.is_empty(), kvs.is_empty());

        kvs
    }

    async fn write_sst(
        sst_file_id: FileId,
        schema: RegionSchemaRef,
        seq: &AtomicU64,
        object_store: ObjectStore,
        ts: &[i64],
        ops: &[OpType],
    ) -> FileHandle {
        let memtable = DefaultMemtableBuilder::default().build(schema.clone());
        let mut breaks = ops
            .iter()
            .zip(ops.iter().skip(1))
            .enumerate()
            .filter_map(
                |(idx, (prev, next))| {
                    if prev != next {
                        Some(idx + 1)
                    } else {
                        None
                    }
                },
            )
            .collect::<Vec<_>>();

        breaks.insert(0, 0);
        breaks.push(ts.len());

        for i in 0..breaks.len() - 1 {
            let op = ops[i];
            let seg_len = breaks[i + 1] - breaks[i];
            let ts_seg = ts
                .iter()
                .skip(breaks[i])
                .take(seg_len)
                .copied()
                .collect::<Vec<_>>();
            let value_seg = ts
                .iter()
                .skip(breaks[i])
                .take(seg_len)
                .map(|i| (*i) as u64)
                .map(Some)
                .collect::<Vec<_>>();

            write_kvs(
                &*memtable,
                seq.load(Ordering::Relaxed), // sequence
                op,
                &ts_seg,    // keys
                &value_seg, // values
            );
            seq.fetch_add(1, Ordering::Relaxed);
        }

        let iter = memtable.iter(IterContext::default()).unwrap();
        let file_path = sst_file_id.as_parquet();
        let writer = ParquetWriter::new(&file_path, Source::Iter(iter), object_store.clone());

        let SstInfo {
            time_range,
            file_size,
            ..
        } = writer
            .write_sst(&sst::WriteOptions::default())
            .await
            .unwrap()
            .unwrap();
        let handle = FileHandle::new(
            FileMeta {
                region_id: 0,
                file_id: sst_file_id,
                time_range,
                level: 0,
                file_size,
            },
            Arc::new(crate::test_util::access_layer_util::MockAccessLayer {}),
            new_noop_file_purger(),
        );
        seq.fetch_add(1, Ordering::Relaxed);
        handle
    }

    async fn check_reads(
        schema: RegionSchemaRef,
        sst_layer: AccessLayerRef,
        files: &[FileHandle],
        lower_sec_inclusive: i64,
        upper_sec_exclusive: i64,
        expect: &[i64],
    ) {
        let mut reader = build_sst_reader(
            schema,
            sst_layer,
            files,
            lower_sec_inclusive,
            upper_sec_exclusive,
        )
        .await
        .unwrap();

        let mut res = vec![];
        while let Some(f) = reader.next_chunk().await.unwrap() {
            let ts_col = f.columns[0]
                .as_any()
                .downcast_ref::<TimestampMillisecondVector>()
                .unwrap();
            res.extend(ts_col.iter_data().map(|t| t.unwrap().0.value()));
        }
        assert_eq!(expect, &res);
    }

    #[tokio::test]
    async fn test_sst_reader() {
        let dir = create_temp_dir("write_parquet");
        let path = dir.path().to_str().unwrap();
        let mut builder = Fs::default();
        builder.root(path);

        let object_store = ObjectStore::new(builder).unwrap().finish();

        let seq = AtomicU64::new(0);
        let schema = schema_for_test();
        let file1 = write_sst(
            FileId::random(),
            schema.clone(),
            &seq,
            object_store.clone(),
            &[1000, 2000, 3000, 4001, 5001],
            &[
                OpType::Put,
                OpType::Put,
                OpType::Put,
                OpType::Put,
                OpType::Put,
            ],
        )
        .await;
        let file2 = write_sst(
            FileId::random(),
            schema.clone(),
            &seq,
            object_store.clone(),
            &[4002, 5002, 6000, 7000, 8000],
            &[
                OpType::Put,
                OpType::Put,
                OpType::Put,
                OpType::Put,
                OpType::Put,
            ],
        )
        .await;
        let sst_layer = Arc::new(FsAccessLayer::new("./", object_store));

        let files = vec![file1, file2];
        // read from two sst files with time range filter,
        check_reads(
            schema.clone(),
            sst_layer.clone(),
            &files,
            3,
            6,
            &[3000, 4001, 4002, 5001, 5002],
        )
        .await;

        check_reads(schema, sst_layer, &files, 1, 2, &[1000]).await;
    }

    async fn read_file(
        files: &[FileHandle],
        schema: RegionSchemaRef,
        sst_layer: AccessLayerRef,
    ) -> Vec<i64> {
        let mut timestamps = vec![];
        let mut reader = build_sst_reader(schema, sst_layer, files, i64::MIN, i64::MAX)
            .await
            .unwrap();
        while let Some(chunk) = reader.next_chunk().await.unwrap() {
            let ts = chunk.columns[0]
                .as_any()
                .downcast_ref::<TimestampMillisecondVector>()
                .unwrap();
            timestamps.extend(ts.iter_data().map(|t| t.unwrap().0.value()));
        }
        timestamps
    }

    /// Writes rows into file i1/i2 and splits these rows into sst file o1/o2/o3,
    /// and check the output contains the same data as input files.
    #[tokio::test]
    async fn test_sst_split() {
        let dir = create_temp_dir("write_parquet");
        let path = dir.path().to_str().unwrap();
        let mut builder = Fs::default();
        builder.root(path);
        let object_store = ObjectStore::new(builder).unwrap().finish();

        let schema = schema_for_test();
        let seq = AtomicU64::new(0);

        let input_file_ids = [FileId::random(), FileId::random()];
        let output_file_ids = [FileId::random(), FileId::random(), FileId::random()];

        let file1 = write_sst(
            input_file_ids[0],
            schema.clone(),
            &seq,
            object_store.clone(),
            &[1000, 2000, 3000, 4001, 5001],
            &[
                OpType::Put,
                OpType::Put,
                OpType::Put,
                OpType::Put,
                OpType::Put,
            ],
        )
        .await;

        // in file2 we delete the row with timestamp 1000.
        let file2 = write_sst(
            input_file_ids[1],
            schema.clone(),
            &seq,
            object_store.clone(),
            &[1000, 5002, 6000, 7000, 8000],
            &[
                OpType::Delete, // a deletion
                OpType::Put,
                OpType::Put,
                OpType::Put,
                OpType::Put,
            ],
        )
        .await;
        let sst_layer = Arc::new(FsAccessLayer::new("./", object_store.clone()));
        let input_files = vec![file2, file1];

        let reader1 = build_sst_reader(schema.clone(), sst_layer.clone(), &input_files, 0, 3)
            .await
            .unwrap();
        let reader2 = build_sst_reader(schema.clone(), sst_layer.clone(), &input_files, 3, 6)
            .await
            .unwrap();
        let reader3 = build_sst_reader(schema.clone(), sst_layer.clone(), &input_files, 6, 10)
            .await
            .unwrap();

        let opts = WriteOptions {
            sst_write_buffer_size: ReadableSize::mb(8),
        };
        let s1 = ParquetWriter::new(
            &output_file_ids[0].as_parquet(),
            Source::Reader(reader1),
            object_store.clone(),
        )
        .write_sst(&opts)
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            Some((
                Timestamp::new_millisecond(2000),
                Timestamp::new_millisecond(2000)
            )),
            s1.time_range,
        );

        let s2 = ParquetWriter::new(
            &output_file_ids[1].as_parquet(),
            Source::Reader(reader2),
            object_store.clone(),
        )
        .write_sst(&opts)
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            Some((
                Timestamp::new_millisecond(3000),
                Timestamp::new_millisecond(5002)
            )),
            s2.time_range,
        );

        let s3 = ParquetWriter::new(
            &output_file_ids[2].as_parquet(),
            Source::Reader(reader3),
            object_store.clone(),
        )
        .write_sst(&opts)
        .await
        .unwrap()
        .unwrap();

        assert_eq!(
            Some((
                Timestamp::new_millisecond(6000),
                Timestamp::new_millisecond(8000)
            )),
            s3.time_range
        );

        let output_files = output_file_ids
            .into_iter()
            .map(|f| {
                FileHandle::new(
                    FileMeta {
                        region_id: 0,
                        file_id: f,
                        level: 1,
                        time_range: None,
                        file_size: 0,
                    },
                    Arc::new(crate::test_util::access_layer_util::MockAccessLayer {}),
                    new_noop_file_purger(),
                )
            })
            .collect::<Vec<_>>();

        let timestamps_in_inputs = read_file(&input_files, schema.clone(), sst_layer.clone()).await;
        let timestamps_in_outputs =
            read_file(&output_files, schema.clone(), sst_layer.clone()).await;

        assert_eq!(timestamps_in_outputs, timestamps_in_inputs);
    }
}
