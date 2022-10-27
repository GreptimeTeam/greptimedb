use std::collections::HashMap;

use api::v1::codec::InsertBatch;
use api::v1::insert_expr;
use api::v1::insert_expr::Expr;
use api::v1::Column;
use api::v1::ColumnDataType;
use api::v1::InsertExpr;
use api::v1::MutateResult;
use client::ObjectResult;
use snafu::ResultExt;
use table::requests::InsertRequest;
use tokio::sync::oneshot;

use super::DistTable;
use crate::error;
use crate::error::Result;
use crate::mock::Region;
use crate::spliter::RegionId;

impl DistTable {
    pub async fn dist_insert(
        &self,
        inserts: HashMap<RegionId, InsertRequest>,
    ) -> Result<ObjectResult> {
        let mut rxs = Vec::with_capacity(inserts.len());
        for (region_id, insert) in inserts {
            let (tx, rx) = oneshot::channel();
            rxs.push(rx);
            // TODO(fys): remove unwrap()
            let db = self.region_dist_map.get(&Region::new(region_id)).unwrap();
            let guard = self.datanode_instances.lock().await;
            let instance = guard.get(db).unwrap();
            let instance = instance.clone();

            tokio::spawn(async move {
                let insert_expr = to_insert_expr(insert);
                let result = instance
                    .grpc_insert(insert_expr)
                    .await
                    .with_context(|_| error::RequestDatanodeSnafu {});
                tx.send(result).unwrap();
            });
        }
        let mut success = 0;
        let mut failure = 0;
        for rx in rxs {
            let object_result = rx.await.unwrap()?;
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

fn to_insert_expr(insert: InsertRequest) -> InsertExpr {
    let mut row_count = 0;
    let columns: Vec<_> = insert
        .columns_values
        .into_iter()
        .map(|(column_name, vector)| {
            row_count = vector.len();
            let datatype: ColumnDataType = vector.data_type().into();
            let mut column: Column = Column {
                column_name,
                datatype: datatype as i32,
                ..Default::default()
            };
            let vals = (0..row_count).into_iter().map(|i| vector.get(i)).collect();
            column.push_vals(0, vals);
            column
        })
        .collect();

    let insert_batch = InsertBatch {
        columns,
        row_count: row_count as u32,
    };

    let options = HashMap::new();

    InsertExpr {
        table_name: insert.table_name,
        options,
        expr: Some(Expr::Values(insert_expr::Values {
            values: vec![insert_batch.into()],
        })),
    }
}

// TODO(fys): add unit test

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use api::v1::InsertExpr;
    use datatypes::{
        prelude::ConcreteDataType,
        types::{BooleanType, StringType},
        vectors::VectorBuilder,
    };
    use table::requests::InsertRequest;

    use super::to_insert_expr;

    #[test]
    fn test_to_insert_expr() {
        let insert_request = mock_insert_request();

        let insert_expr = to_insert_expr(insert_request);

        verify_insert_expr(insert_expr);
    }

    fn mock_insert_request() -> InsertRequest {
        let mut columns_values = HashMap::with_capacity(4);
        let mut builder = VectorBuilder::new(ConcreteDataType::Boolean(BooleanType));
        builder.push(&true.into());
        builder.push(&false.into());
        builder.push(&true.into());
        columns_values.insert("enable_reboot".to_string(), builder.finish());

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

        // TODO(fys): add other verify
    }
}
