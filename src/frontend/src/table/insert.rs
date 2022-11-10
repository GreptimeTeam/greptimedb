use std::collections::HashMap;

use api::helper::ColumnDataTypeWrapper;
use api::v1::codec;
use api::v1::insert_expr;
use api::v1::insert_expr::Expr;
use api::v1::Column;
use api::v1::InsertExpr;
use api::v1::MutateResult;
use client::ObjectResult;
use snafu::ensure;
use snafu::OptionExt;
use snafu::ResultExt;
use store_api::storage::RegionNumber;
use table::requests::InsertRequest;

use super::DistTable;
use crate::error;
use crate::error::Result;

impl DistTable {
    pub async fn dist_insert(
        &self,
        inserts: HashMap<RegionNumber, InsertRequest>,
    ) -> Result<ObjectResult> {
        let mut joins = Vec::with_capacity(inserts.len());

        for (region_id, insert) in inserts {
            let db = self
                .region_dist_map
                .get(&region_id)
                .context(error::FindDatanodeSnafu { region: region_id })?;

            let instance = self
                .datanode_instances
                .get(db)
                .context(error::DatanodeInstanceSnafu { datanode: *db })?;

            let instance = instance.clone();

            // TODO(fys): a separate runtime should be used here.
            let join = tokio::spawn(async move {
                instance
                    .grpc_insert(to_insert_expr(region_id, insert)?)
                    .await
                    .context(error::RequestDatanodeSnafu)
            });

            joins.push(join);
        }

        let mut success = 0;
        let mut failure = 0;

        for join in joins {
            let object_result = join.await.context(error::JoinTaskSnafu)??;
            let result = match object_result {
                client::ObjectResult::Select(_) => unreachable!(),
                client::ObjectResult::Mutate(result) => result,
            };
            success += result.success;
            failure += result.failure;
        }

        Ok(ObjectResult::Mutate(MutateResult { success, failure }))
    }
}

fn to_insert_expr(region_id: RegionNumber, insert: InsertRequest) -> Result<InsertExpr> {
    let mut row_count = None;

    let columns = insert
        .columns_values
        .into_iter()
        .map(|(column_name, vector)| {
            match row_count {
                Some(rows) => ensure!(
                    rows == vector.len(),
                    error::InvalidInsertRequestSnafu {
                        reason: "The row count of columns is not the same."
                    }
                ),

                None => row_count = Some(vector.len()),
            }

            let datatype: ColumnDataTypeWrapper = vector
                .data_type()
                .try_into()
                .context(error::ColumnDataTypeSnafu)?;

            let mut column = Column {
                column_name,
                datatype: datatype.datatype() as i32,
                ..Default::default()
            };

            column.push_vals(0, vector);
            Ok(column)
        })
        .collect::<Result<Vec<_>>>()?;

    let insert_batch = codec::InsertBatch {
        columns,
        row_count: row_count.map(|rows| rows as u32).unwrap_or(0),
    };

    let mut options = HashMap::with_capacity(1);
    options.insert(
        // TODO(fys): Temporarily hard code here
        "region_id".to_string(),
        codec::RegionNumber { id: region_id }.into(),
    );

    Ok(InsertExpr {
        table_name: insert.table_name,
        options,
        expr: Some(Expr::Values(insert_expr::Values {
            values: vec![insert_batch.into()],
        })),
    })
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, ops::Deref};

    use api::v1::{
        codec::{self, InsertBatch},
        insert_expr::Expr,
        ColumnDataType, InsertExpr,
    };
    use datatypes::{prelude::ConcreteDataType, types::StringType, vectors::VectorBuilder};
    use table::requests::InsertRequest;

    use super::to_insert_expr;

    #[test]
    fn test_to_insert_expr() {
        let insert_request = mock_insert_request();

        let insert_expr = to_insert_expr(12, insert_request).unwrap();

        verify_insert_expr(insert_expr);
    }

    fn mock_insert_request() -> InsertRequest {
        let mut columns_values = HashMap::with_capacity(4);

        let mut builder = VectorBuilder::new(ConcreteDataType::String(StringType));
        builder.push(&"host1".into());
        builder.push_null();
        builder.push(&"host3".into());
        columns_values.insert("host".to_string(), builder.finish());

        let mut builder = VectorBuilder::new(ConcreteDataType::int16_datatype());
        builder.push(&1_i16.into());
        builder.push(&2_i16.into());
        builder.push(&3_i16.into());
        columns_values.insert("id".to_string(), builder.finish());

        InsertRequest {
            table_name: "demo".to_string(),
            columns_values,
        }
    }

    fn verify_insert_expr(insert_expr: InsertExpr) {
        let table_name = insert_expr.table_name;
        assert_eq!("demo", table_name);

        let expr = insert_expr.expr.as_ref().unwrap();
        let vals = match expr {
            Expr::Values(vals) => vals,
            Expr::Sql(_) => unreachable!(),
        };

        let batch: &[u8] = vals.values[0].as_ref();
        let vals: InsertBatch = batch.try_into().unwrap();

        for column in vals.columns {
            let name = column.column_name;
            if name == "id" {
                assert_eq!(0, column.null_mask[0]);
                assert_eq!(ColumnDataType::Int16 as i32, column.datatype);
                assert_eq!(vec![1, 2, 3], column.values.as_ref().unwrap().i16_values);
            }
            if name == "host" {
                assert_eq!(2, column.null_mask[0]);
                assert_eq!(ColumnDataType::String as i32, column.datatype);
                assert_eq!(
                    vec!["host1", "host3"],
                    column.values.as_ref().unwrap().string_values
                );
            }
        }

        let bytes = insert_expr.options.get("region_id").unwrap();
        let region_id: codec::RegionNumber = bytes.deref().try_into().unwrap();
        assert_eq!(12, region_id.id);
    }
}
