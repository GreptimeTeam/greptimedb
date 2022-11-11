use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use common_query::physical_plan::PhysicalPlanRef;
use common_query::prelude::Expr;
use common_recordbatch::error::Result as RecordBatchResult;
use common_recordbatch::{RecordBatch, RecordBatchStream};
use datatypes::prelude::*;
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use datatypes::vectors::UInt32Vector;
use futures::task::{Context, Poll};
use futures::Stream;
use snafu::prelude::*;
use store_api::storage::RegionNumber;

use crate::error::{Result, SchemaConversionSnafu, TableProjectionSnafu};
use crate::metadata::{
    TableId, TableInfoBuilder, TableInfoRef, TableMetaBuilder, TableType, TableVersion,
};
use crate::table::scan::SimpleTableScan;
use crate::Table;

#[derive(Debug, Clone)]
pub struct MemTable {
    info: TableInfoRef,
    recordbatch: RecordBatch,
}

impl MemTable {
    pub fn new(table_name: impl Into<String>, recordbatch: RecordBatch) -> Self {
        Self::new_with_region(table_name, recordbatch, vec![0])
    }

    pub fn new_with_region(
        table_name: impl Into<String>,
        recordbatch: RecordBatch,
        regions: Vec<RegionNumber>,
    ) -> Self {
        Self::new_with_catalog(
            table_name,
            recordbatch,
            0,
            "greptime".to_string(),
            "public".to_string(),
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
    ) -> Self {
        let schema = recordbatch.schema.clone();

        let meta = TableMetaBuilder::default()
            .schema(schema)
            .primary_key_indices(vec![])
            .value_indices(vec![])
            .engine("mock".to_string())
            .next_column_id(0)
            .engine_options(Default::default())
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

        Self { info, recordbatch }
    }

    pub fn table_name(&self) -> &str {
        &self.info.name
    }

    /// Creates a 1 column 100 rows table, with table name "numbers", column name "uint32s" and
    /// column type "uint32". Column data increased from 0 to 100.
    pub fn default_numbers_table() -> Self {
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
        MemTable::new("numbers", recordbatch)
    }
}

#[async_trait]
impl Table for MemTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.recordbatch.schema.clone()
    }

    fn table_info(&self) -> TableInfoRef {
        self.info.clone()
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<PhysicalPlanRef> {
        let df_recordbatch = if let Some(indices) = projection {
            self.recordbatch
                .df_recordbatch
                .project(indices)
                .context(TableProjectionSnafu)?
        } else {
            self.recordbatch.df_recordbatch.clone()
        };

        let rows = df_recordbatch.num_rows();
        let limit = if let Some(limit) = limit {
            limit.min(rows)
        } else {
            rows
        };
        let df_recordbatch = df_recordbatch.slice(0, limit);

        let recordbatch = RecordBatch {
            schema: Arc::new(
                Schema::try_from(df_recordbatch.schema().clone()).context(SchemaConversionSnafu)?,
            ),
            df_recordbatch,
        };
        Ok(Arc::new(SimpleTableScan::new(Box::pin(MemtableStream {
            schema: recordbatch.schema.clone(),
            recordbatch: Some(recordbatch),
        }))))
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
    use common_query::physical_plan::RuntimeEnv;
    use common_recordbatch::util;
    use datatypes::prelude::*;
    use datatypes::schema::ColumnSchema;
    use datatypes::vectors::{Int32Vector, StringVector};

    use super::*;

    #[tokio::test]
    async fn test_scan_with_projection() {
        let table = build_testing_table();

        let scan_stream = table.scan(&Some(vec![1]), &[], None).await.unwrap();
        let scan_stream = scan_stream
            .execute(0, Arc::new(RuntimeEnv::default()))
            .await
            .unwrap();
        let recordbatch = util::collect(scan_stream).await.unwrap();
        assert_eq!(1, recordbatch.len());
        let columns = recordbatch[0].df_recordbatch.columns();
        assert_eq!(1, columns.len());

        let string_column = VectorHelper::try_into_vector(&columns[0]).unwrap();
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

        let scan_stream = table.scan(&None, &[], Some(2)).await.unwrap();
        let scan_stream = scan_stream
            .execute(0, Arc::new(RuntimeEnv::default()))
            .await
            .unwrap();
        let recordbatch = util::collect(scan_stream).await.unwrap();
        assert_eq!(1, recordbatch.len());
        let columns = recordbatch[0].df_recordbatch.columns();
        assert_eq!(2, columns.len());

        let i32_column = VectorHelper::try_into_vector(&columns[0]).unwrap();
        let i32_column = i32_column.as_any().downcast_ref::<Int32Vector>().unwrap();
        let i32_column = i32_column.iter_data().flatten().collect::<Vec<i32>>();
        assert_eq!(vec![-100], i32_column);

        let string_column = VectorHelper::try_into_vector(&columns[1]).unwrap();
        let string_column = string_column
            .as_any()
            .downcast_ref::<StringVector>()
            .unwrap();
        let string_column = string_column.iter_data().flatten().collect::<Vec<&str>>();
        assert_eq!(vec!["hello"], string_column);
    }

    fn build_testing_table() -> MemTable {
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
        MemTable::new("", recordbatch)
    }
}
