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
use crate::schema::RegionSchemaRef;
use crate::sst::{AccessLayerRef, FileHandle};

pub(crate) async fn build_sst_reader(
    schema: RegionSchemaRef,
    sst_layer: AccessLayerRef,
    files: &[FileHandle],
    lower_sec_inclusive: i64,
    upper_sec_exclusive: i64,
) -> ChunkReaderImpl {
    let ts_col_name = schema
        .user_schema()
        .timestamp_column()
        .unwrap()
        .name
        .clone();
    let reader = ChunkReaderBuilder::new(schema, sst_layer)
        .pick_ssts(files)
        .filters(vec![build_time_range_filter(
            lower_sec_inclusive,
            upper_sec_exclusive,
            &ts_col_name,
        )])
        .build()
        .await
        .unwrap();

    reader
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
    use std::sync::Arc;

    use datatypes::prelude::{LogicalTypeId, ScalarVector, ScalarVectorBuilder};
    use datatypes::timestamp::TimestampMillisecond;
    use datatypes::vectors::{
        TimestampMillisecondVector, TimestampMillisecondVectorBuilder, UInt64VectorBuilder,
    };
    use object_store::backend::fs::Builder;
    use object_store::ObjectStore;
    use store_api::storage::{ChunkReader, OpType, SequenceNumber};

    use super::*;
    use crate::memtable::{
        DefaultMemtableBuilder, IterContext, KeyValues, Memtable, MemtableBuilder,
    };
    use crate::metadata::RegionMetadata;
    use crate::sst;
    use crate::sst::parquet::ParquetWriter;
    use crate::sst::{FileMeta, FsAccessLayer, SstInfo};
    use crate::test_util::descriptor_util::RegionDescBuilder;

    fn schema_for_test() -> RegionSchemaRef {
        // Just build a region desc and use its columns metadata.
        let desc = RegionDescBuilder::new("test")
            .enable_version_column(false)
            .push_value_column(("v", LogicalTypeId::UInt64, true))
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
            key_builders.push(Some(key.clone()));
        }
        let row_keys = vec![Arc::new(key_builders.finish()) as _];

        let mut value_builders = UInt64VectorBuilder::with_capacity(values.len());

        for value in values {
            value_builders.push(*value);
        }
        let row_values = vec![Arc::new(value_builders.finish()) as _];

        let kvs = KeyValues {
            sequence,
            op_type,
            start_index_in_batch,
            keys: row_keys,
            values: row_values,
        };

        assert_eq!(ts.len(), kvs.len());
        assert_eq!(ts.is_empty(), kvs.is_empty());

        kvs
    }

    async fn write_sst(
        sst_file_name: &str,
        schema: RegionSchemaRef,
        object_store: ObjectStore,
        ts: &[i64],
    ) -> FileHandle {
        let memtable = DefaultMemtableBuilder::default().build(schema.clone());

        let values = ts.iter().map(|i| (*i) as u64).map(Some).collect::<Vec<_>>();

        write_kvs(
            &*memtable,
            10, // sequence
            OpType::Put,
            ts,      // keys
            &values, // values
        );

        let iter = memtable.iter(&IterContext::default()).unwrap();
        let writer = ParquetWriter::new(sst_file_name, iter, object_store.clone());

        let SstInfo {
            start_timestamp,
            end_timestamp,
        } = writer
            .write_sst(&sst::WriteOptions::default())
            .await
            .unwrap();
        FileHandle::new(FileMeta {
            file_name: sst_file_name.to_string(),
            level: 0,
            start_timestamp,
            end_timestamp,
        })
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
        .await;

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
        // let dir = TempDir::new("write_parquet").unwrap();
        // let path = dir.path().to_str().unwrap();
        let path = "/Users/lei/parquet";
        let backend = Builder::default().root(path).build().unwrap();
        let object_store = ObjectStore::new(backend);

        let schema = schema_for_test();
        let file1 = write_sst(
            "a.parquet",
            schema.clone(),
            object_store.clone(),
            &[1000, 2000, 3000, 4001, 5001],
        )
        .await;
        let file2 = write_sst(
            "b.parquet",
            schema.clone(),
            object_store.clone(),
            &[4002, 5002, 6000, 7000, 8000],
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
}
