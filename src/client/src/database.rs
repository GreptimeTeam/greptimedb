use std::sync::Arc;

use api::v1::codec::SelectResult as GrpcSelectResult;
use api::v1::{
    object_expr, object_result, select_expr, DatabaseRequest, ExprHeader, InsertExpr,
    MutateResult as GrpcMutateResult, ObjectExpr, ObjectResult as GrpcObjectResult, PhysicalPlan,
    SelectExpr,
};
use common_error::status_code::StatusCode;
use common_grpc::AsExcutionPlan;
use common_grpc::DefaultAsPlanImpl;
use common_insert::column_to_vector;
use common_query::Output;
use common_recordbatch::{RecordBatch, RecordBatches};
use datafusion::physical_plan::ExecutionPlan;
use datatypes::prelude::*;
use datatypes::schema::{ColumnSchema, Schema};
use snafu::{ensure, OptionExt, ResultExt};

use crate::error;
use crate::error::ColumnToVectorSnafu;
use crate::{
    error::{ConvertSchemaSnafu, DatanodeSnafu, DecodeSelectSnafu, EncodePhysicalSnafu},
    Client, Result,
};

pub const PROTOCOL_VERSION: u32 = 1;

#[derive(Clone, Debug)]
pub struct Database {
    name: String,
    client: Client,
}

impl Database {
    pub fn new(name: impl Into<String>, client: Client) -> Self {
        Self {
            name: name.into(),
            client,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub async fn insert(&self, insert: InsertExpr) -> Result<ObjectResult> {
        let header = ExprHeader {
            version: PROTOCOL_VERSION,
        };
        let expr = ObjectExpr {
            header: Some(header),
            expr: Some(object_expr::Expr::Insert(insert)),
        };
        self.object(expr).await?.try_into()
    }

    pub async fn batch_insert(&self, insert_exprs: Vec<InsertExpr>) -> Result<Vec<ObjectResult>> {
        let header = ExprHeader {
            version: PROTOCOL_VERSION,
        };
        let obj_exprs = insert_exprs
            .into_iter()
            .map(|expr| ObjectExpr {
                header: Some(header.clone()),
                expr: Some(object_expr::Expr::Insert(expr)),
            })
            .collect();
        self.objects(obj_exprs)
            .await?
            .into_iter()
            .map(|result| result.try_into())
            .collect()
    }

    pub async fn select(&self, expr: Select) -> Result<ObjectResult> {
        let select_expr = match expr {
            Select::Sql(sql) => SelectExpr {
                expr: Some(select_expr::Expr::Sql(sql)),
            },
        };
        self.do_select(select_expr).await
    }

    pub async fn physical_plan(
        &self,
        physical: Arc<dyn ExecutionPlan>,
        original_ql: Option<String>,
    ) -> Result<ObjectResult> {
        let plan = DefaultAsPlanImpl::try_from_physical_plan(physical.clone())
            .context(EncodePhysicalSnafu { physical })?
            .bytes;
        let original_ql = original_ql.unwrap_or_default();
        let select_expr = SelectExpr {
            expr: Some(select_expr::Expr::PhysicalPlan(PhysicalPlan {
                original_ql: original_ql.into_bytes(),
                plan,
            })),
        };
        self.do_select(select_expr).await
    }

    pub async fn logical_plan(&self, logical_plan: Vec<u8>) -> Result<ObjectResult> {
        let select_expr = SelectExpr {
            expr: Some(select_expr::Expr::LogicalPlan(logical_plan)),
        };
        self.do_select(select_expr).await
    }

    async fn do_select(&self, select_expr: SelectExpr) -> Result<ObjectResult> {
        let header = ExprHeader {
            version: PROTOCOL_VERSION,
        };

        let expr = ObjectExpr {
            header: Some(header),
            expr: Some(object_expr::Expr::Select(select_expr)),
        };

        let obj_result = self.object(expr).await?;
        obj_result.try_into()
    }

    pub async fn object(&self, expr: ObjectExpr) -> Result<GrpcObjectResult> {
        let res = self.objects(vec![expr]).await?.pop().unwrap();
        Ok(res)
    }

    async fn objects(&self, exprs: Vec<ObjectExpr>) -> Result<Vec<GrpcObjectResult>> {
        let expr_count = exprs.len();
        let req = DatabaseRequest {
            name: self.name.clone(),
            exprs,
        };

        let res = self.client.database(req).await?;
        let res = res.results;

        ensure!(
            res.len() == expr_count,
            error::MissingResultSnafu {
                name: "object_results",
                expected: expr_count,
                actual: res.len(),
            }
        );

        Ok(res)
    }
}

#[derive(Debug)]
pub enum ObjectResult {
    Select(GrpcSelectResult),
    Mutate(GrpcMutateResult),
}

impl TryFrom<api::v1::ObjectResult> for ObjectResult {
    type Error = error::Error;

    fn try_from(object_result: api::v1::ObjectResult) -> std::result::Result<Self, Self::Error> {
        let header = object_result.header.context(error::MissingHeaderSnafu)?;
        if !StatusCode::is_success(header.code) {
            return DatanodeSnafu {
                code: header.code,
                msg: header.err_msg,
            }
            .fail();
        }

        let obj_result = object_result.result.context(error::MissingResultSnafu {
            name: "result".to_string(),
            expected: 1_usize,
            actual: 0_usize,
        })?;
        Ok(match obj_result {
            object_result::Result::Select(select) => {
                let result = (*select.raw_data).try_into().context(DecodeSelectSnafu)?;
                ObjectResult::Select(result)
            }
            object_result::Result::Mutate(mutate) => ObjectResult::Mutate(mutate),
        })
    }
}

pub enum Select {
    Sql(String),
}

impl TryFrom<ObjectResult> for Output {
    type Error = error::Error;

    fn try_from(value: ObjectResult) -> Result<Self> {
        let output = match value {
            ObjectResult::Select(select) => {
                let vectors = select
                    .columns
                    .iter()
                    .map(|column| {
                        column_to_vector(column, select.row_count).context(ColumnToVectorSnafu)
                    })
                    .collect::<Result<Vec<VectorRef>>>()?;

                let column_schemas = select
                    .columns
                    .iter()
                    .zip(vectors.iter())
                    .map(|(column, vector)| {
                        let datatype = vector.data_type();
                        // nullable or not, does not affect the output
                        ColumnSchema::new(&column.column_name, datatype, true)
                    })
                    .collect::<Vec<ColumnSchema>>();

                let schema = Arc::new(Schema::try_new(column_schemas).context(ConvertSchemaSnafu)?);
                let recordbatches = if vectors.is_empty() {
                    RecordBatches::try_new(schema, vec![])
                } else {
                    RecordBatch::new(schema, vectors)
                        .and_then(|batch| RecordBatches::try_new(batch.schema.clone(), vec![batch]))
                }
                .context(error::CreateRecordBatchesSnafu)?;
                Output::RecordBatches(recordbatches)
            }
            ObjectResult::Mutate(mutate) => {
                if mutate.failure != 0 {
                    return error::MutateFailureSnafu {
                        failure: mutate.failure,
                    }
                    .fail();
                }
                Output::AffectedRows(mutate.success as usize)
            }
        };
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use api::helper::ColumnDataTypeWrapper;
    use api::v1::Column;
    use datanode::server::grpc::select::{null_mask, values};
    use datatypes::vectors::{
        BinaryVector, BooleanVector, DateTimeVector, DateVector, Float32Vector, Float64Vector,
        Int16Vector, Int32Vector, Int64Vector, Int8Vector, StringVector, UInt16Vector,
        UInt32Vector, UInt64Vector, UInt8Vector,
    };

    use super::*;

    #[test]
    fn test_column_to_vector() {
        let mut column = create_test_column(Arc::new(BooleanVector::from(vec![true])));
        column.datatype = -100;
        let result = column_to_vector(&column, 1);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Column datatype error, source: Unknown proto column datatype: -100"
        );

        macro_rules! test_with_vector {
            ($vector: expr) => {
                let vector = Arc::new($vector);
                let column = create_test_column(vector.clone());
                let result = column_to_vector(&column, vector.len() as u32).unwrap();
                assert_eq!(result, vector as VectorRef);
            };
        }

        test_with_vector!(BooleanVector::from(vec![Some(true), None, Some(false)]));
        test_with_vector!(Int8Vector::from(vec![Some(i8::MIN), None, Some(i8::MAX)]));
        test_with_vector!(Int16Vector::from(vec![
            Some(i16::MIN),
            None,
            Some(i16::MAX)
        ]));
        test_with_vector!(Int32Vector::from(vec![
            Some(i32::MIN),
            None,
            Some(i32::MAX)
        ]));
        test_with_vector!(Int64Vector::from(vec![
            Some(i64::MIN),
            None,
            Some(i64::MAX)
        ]));
        test_with_vector!(UInt8Vector::from(vec![Some(u8::MIN), None, Some(u8::MAX)]));
        test_with_vector!(UInt16Vector::from(vec![
            Some(u16::MIN),
            None,
            Some(u16::MAX)
        ]));
        test_with_vector!(UInt32Vector::from(vec![
            Some(u32::MIN),
            None,
            Some(u32::MAX)
        ]));
        test_with_vector!(UInt64Vector::from(vec![
            Some(u64::MIN),
            None,
            Some(u64::MAX)
        ]));
        test_with_vector!(Float32Vector::from(vec![
            Some(f32::MIN),
            None,
            Some(f32::MAX)
        ]));
        test_with_vector!(Float64Vector::from(vec![
            Some(f64::MIN),
            None,
            Some(f64::MAX)
        ]));
        test_with_vector!(BinaryVector::from(vec![
            Some(b"".to_vec()),
            None,
            Some(b"hello".to_vec())
        ]));
        test_with_vector!(StringVector::from(vec![Some(""), None, Some("foo"),]));
        test_with_vector!(DateVector::from(vec![Some(1), None, Some(3)]));
        test_with_vector!(DateTimeVector::from(vec![Some(4), None, Some(6)]));
    }

    fn create_test_column(vector: VectorRef) -> Column {
        let wrapper: ColumnDataTypeWrapper = vector.data_type().try_into().unwrap();
        let array = vector.to_arrow_array();
        Column {
            column_name: "test".to_string(),
            semantic_type: 1,
            values: Some(values(&[array.clone()]).unwrap()),
            null_mask: null_mask(&vec![array], vector.len()),
            datatype: wrapper.datatype() as i32,
        }
    }
}
