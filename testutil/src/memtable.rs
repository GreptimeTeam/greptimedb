use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use common_query::prelude::Expr;
use common_recordbatch::{RecordBatch, SendableRecordBatchStream};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::execution::runtime_env::RuntimeEnv;
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use snafu::prelude::*;
use table::error::{DatafusionSnafu, Result};
use table::table::adapter::RecordBatchStreamAdapter;
use table::Table;

/// [DfMemTable] is backed by Datafusion's [MemTable] (hence the "`Df`" in its name).
pub struct DfMemTable {
    schema: SchemaRef,
    table: MemTable,
    runtime_env: Arc<RuntimeEnv>,
}

impl DfMemTable {
    /// Creates a new [DfMemTable], column schemas **MUST** match recordbatch schema.
    pub fn try_new(
        column_schemas: Vec<ColumnSchema>,
        recordbatch: Vec<RecordBatch>,
    ) -> Result<Self> {
        let schema = Schema::new(column_schemas);

        let arrow_schema = schema.arrow_schema().clone();
        let df_recordbatch = recordbatch.into_iter().map(|r| r.df_recordbatch).collect();
        let table =
            MemTable::try_new(arrow_schema, vec![df_recordbatch]).context(DatafusionSnafu)?;

        Ok(Self {
            schema: Arc::new(schema),
            table,
            runtime_env: Arc::new(RuntimeEnv::default()),
        })
    }
}

#[async_trait]
impl Table for DfMemTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<SendableRecordBatchStream> {
        let scan_plan = self
            .table
            .scan(projection, &[], None)
            .await
            .context(DatafusionSnafu)?;
        // DfMemTable has only one partition (see DfMemTable::try_new), so we always pass 0 here.
        let scan_stream = scan_plan
            .execute(0, self.runtime_env.clone())
            .await
            .context(DatafusionSnafu)?;
        Ok(Box::pin(RecordBatchStreamAdapter::try_new(scan_stream)?))
    }
}

#[cfg(test)]
mod test {
    use common_recordbatch::util;
    use datatypes::prelude::*;
    use datatypes::vectors::{Int32Vector, StringVector};

    use super::*;

    #[tokio::test]
    async fn test_scan() {
        let i32_column_schema =
            ColumnSchema::new("i32_numbers", ConcreteDataType::int32_datatype(), true);
        let string_column_schema =
            ColumnSchema::new("strings", ConcreteDataType::string_datatype(), true);
        let column_schemas = vec![i32_column_schema, string_column_schema];

        let schema = Arc::new(Schema::new(column_schemas.clone()));
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
        let table = DfMemTable::try_new(column_schemas, vec![recordbatch]).unwrap();

        let scan_stream = table.scan(&Some(vec![0, 1]), &[], None).await.unwrap();
        let recordbatch = util::collect(scan_stream).await.unwrap();
        assert_eq!(1, recordbatch.len());
        let columns = recordbatch[0].df_recordbatch.columns();
        assert_eq!(2, columns.len());

        let i32_column = VectorHelper::try_into_vector(&columns[0]).unwrap();
        let i32_column = i32_column.as_any().downcast_ref::<Int32Vector>().unwrap();
        let i32_column = i32_column.iter_data().flatten().collect::<Vec<i32>>();
        assert_eq!(vec![-100, 1, 100], i32_column);

        let string_column = VectorHelper::try_into_vector(&columns[1]).unwrap();
        let string_column = string_column
            .as_any()
            .downcast_ref::<StringVector>()
            .unwrap();
        let string_column = string_column.iter_data().flatten().collect::<Vec<&str>>();
        assert_eq!(vec!["hello", "greptime"], string_column);
    }
}
