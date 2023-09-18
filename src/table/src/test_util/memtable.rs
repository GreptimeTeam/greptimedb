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

use std::pin::Pin;
use std::sync::Arc;

use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_error::ext::BoxedError;
use common_recordbatch::error::Result as RecordBatchResult;
use common_recordbatch::{RecordBatch, RecordBatchStream, SendableRecordBatchStream};
use datatypes::prelude::*;
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use datatypes::vectors::UInt32Vector;
use futures::task::{Context, Poll};
use futures::Stream;
use snafu::prelude::*;
use store_api::data_source::DataSource;
use store_api::storage::{RegionNumber, ScanRequest};

use crate::error::{SchemaConversionSnafu, TableProjectionSnafu, TablesRecordBatchSnafu};
use crate::metadata::{
    FilterPushDownType, TableId, TableInfoBuilder, TableMetaBuilder, TableType, TableVersion,
};
use crate::thin_table::{ThinTable, ThinTableAdapter};
use crate::TableRef;

pub struct MemTable;

impl MemTable {
    pub fn table(table_name: impl Into<String>, recordbatch: RecordBatch) -> TableRef {
        Self::new_with_region(table_name, recordbatch, vec![0])
    }

    pub fn new_with_region(
        table_name: impl Into<String>,
        recordbatch: RecordBatch,
        regions: Vec<RegionNumber>,
    ) -> TableRef {
        Self::new_with_catalog(
            table_name,
            recordbatch,
            1,
            DEFAULT_CATALOG_NAME.to_string(),
            DEFAULT_SCHEMA_NAME.to_string(),
            regions,
        )
    }

    pub fn new_with_catalog(
        table_name: impl Into<String>,
        recordbatch: RecordBatch,
        table_id: TableId,
        catalog_name: String,
        schema_name: String,
        regions: Vec<RegionNumber>,
    ) -> TableRef {
        let schema = recordbatch.schema.clone();

        let meta = TableMetaBuilder::default()
            .schema(schema)
            .primary_key_indices(vec![])
            .value_indices(vec![])
            .engine("mito".to_string())
            .next_column_id(0)
            .options(Default::default())
            .created_on(Default::default())
            .region_numbers(regions)
            .build()
            .unwrap();

        let info = Arc::new(
            TableInfoBuilder::default()
                .table_id(table_id)
                .table_version(0 as TableVersion)
                .name(table_name.into())
                .schema_name(schema_name)
                .catalog_name(catalog_name)
                .desc(None)
                .table_type(TableType::Base)
                .meta(meta)
                .build()
                .unwrap(),
        );

        let thin_table = ThinTable::new(info, FilterPushDownType::Unsupported);
        let data_source = Arc::new(MemtableDataSource { recordbatch });
        Arc::new(ThinTableAdapter::new(thin_table, data_source))
    }

    /// Creates a 1 column 100 rows table, with table name "numbers", column name "uint32s" and
    /// column type "uint32". Column data increased from 0 to 100.
    pub fn default_numbers_table() -> TableRef {
        let column_schemas = vec![ColumnSchema::new(
            "uint32s",
            ConcreteDataType::uint32_datatype(),
            true,
        )];
        let schema = Arc::new(Schema::new(column_schemas));
        let columns: Vec<VectorRef> = vec![Arc::new(UInt32Vector::from_slice(
            (0..100).collect::<Vec<_>>(),
        ))];
        let recordbatch = RecordBatch::new(schema, columns).unwrap();
        MemTable::table("numbers", recordbatch)
    }
}

struct MemtableDataSource {
    recordbatch: RecordBatch,
}

impl DataSource for MemtableDataSource {
    fn get_stream(
        &self,
        request: ScanRequest,
    ) -> std::result::Result<SendableRecordBatchStream, BoxedError> {
        let df_recordbatch = if let Some(indices) = request.projection {
            self.recordbatch
                .df_record_batch()
                .project(&indices)
                .context(TableProjectionSnafu)
                .map_err(BoxedError::new)?
        } else {
            self.recordbatch.df_record_batch().clone()
        };

        let rows = df_recordbatch.num_rows();
        let limit = if let Some(limit) = request.limit {
            limit.min(rows)
        } else {
            rows
        };
        let df_recordbatch = df_recordbatch.slice(0, limit);

        let recordbatch = RecordBatch::try_from_df_record_batch(
            Arc::new(
                Schema::try_from(df_recordbatch.schema())
                    .context(SchemaConversionSnafu)
                    .map_err(BoxedError::new)?,
            ),
            df_recordbatch,
        )
        .map_err(BoxedError::new)
        .context(TablesRecordBatchSnafu)
        .map_err(BoxedError::new)?;

        Ok(Box::pin(MemtableStream {
            schema: recordbatch.schema.clone(),
            recordbatch: Some(recordbatch),
        }))
    }
}

impl RecordBatchStream for MemtableStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

struct MemtableStream {
    schema: SchemaRef,
    recordbatch: Option<RecordBatch>,
}

impl Stream for MemtableStream {
    type Item = RecordBatchResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.recordbatch.take() {
            Some(records) => Poll::Ready(Some(Ok(records))),
            None => Poll::Ready(None),
        }
    }
}

#[cfg(test)]
mod test {
    use common_recordbatch::util;
    use datatypes::prelude::*;
    use datatypes::schema::ColumnSchema;
    use datatypes::vectors::{Helper, Int32Vector, StringVector};

    use super::*;

    #[tokio::test]
    async fn test_scan_with_projection() {
        let table = build_testing_table();

        let scan_req = ScanRequest {
            projection: Some(vec![1]),
            ..Default::default()
        };
        let stream = table.scan_to_stream(scan_req).await.unwrap();
        let recordbatch = util::collect(stream).await.unwrap();
        assert_eq!(1, recordbatch.len());
        let columns = recordbatch[0].df_record_batch().columns();
        assert_eq!(1, columns.len());

        let string_column = Helper::try_into_vector(&columns[0]).unwrap();
        let string_column = string_column
            .as_any()
            .downcast_ref::<StringVector>()
            .unwrap();
        let string_column = string_column.iter_data().flatten().collect::<Vec<&str>>();
        assert_eq!(vec!["hello", "greptime"], string_column);
    }

    #[tokio::test]
    async fn test_scan_with_limit() {
        let table = build_testing_table();

        let scan_req = ScanRequest {
            limit: Some(2),
            ..Default::default()
        };
        let stream = table.scan_to_stream(scan_req).await.unwrap();
        let recordbatch = util::collect(stream).await.unwrap();
        assert_eq!(1, recordbatch.len());
        let columns = recordbatch[0].df_record_batch().columns();
        assert_eq!(2, columns.len());

        let i32_column = Helper::try_into_vector(&columns[0]).unwrap();
        let i32_column = i32_column.as_any().downcast_ref::<Int32Vector>().unwrap();
        let i32_column = i32_column.iter_data().flatten().collect::<Vec<i32>>();
        assert_eq!(vec![-100], i32_column);

        let string_column = Helper::try_into_vector(&columns[1]).unwrap();
        let string_column = string_column
            .as_any()
            .downcast_ref::<StringVector>()
            .unwrap();
        let string_column = string_column.iter_data().flatten().collect::<Vec<&str>>();
        assert_eq!(vec!["hello"], string_column);
    }

    fn build_testing_table() -> TableRef {
        let i32_column_schema =
            ColumnSchema::new("i32_numbers", ConcreteDataType::int32_datatype(), true);
        let string_column_schema =
            ColumnSchema::new("strings", ConcreteDataType::string_datatype(), true);
        let column_schemas = vec![i32_column_schema, string_column_schema];

        let schema = Arc::new(Schema::new(column_schemas));
        let columns: Vec<VectorRef> = vec![
            Arc::new(Int32Vector::from(vec![
                Some(-100),
                None,
                Some(1),
                Some(100),
            ])),
            Arc::new(StringVector::from(vec![
                Some("hello"),
                None,
                Some("greptime"),
                None,
            ])),
        ];
        let recordbatch = RecordBatch::new(schema, columns).unwrap();
        MemTable::table("", recordbatch)
    }
}
