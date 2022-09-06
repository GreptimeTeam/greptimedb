use std::sync::Arc;
mod function;
use common_recordbatch::error::Result as RecordResult;
use common_recordbatch::{util, RecordBatch};
use datafusion::field_util::FieldExt;
use datafusion::field_util::SchemaExt;
use datatypes::for_all_primitive_types;
use datatypes::prelude::*;
use datatypes::types::PrimitiveElement;
use function::{create_query_engine, get_numbers_from_table};
use query::error::Result;
use query::query_engine::Output;
use query::QueryEngine;

#[tokio::test]
async fn test_argmax_aggregator() -> Result<()> {
    common_telemetry::init_default_ut_logging();
    let engine = create_query_engine();

    macro_rules! test_argmax {
        ([], $( { $T:ty } ),*) => {
            $(
                let column_name = format!("{}_number", std::any::type_name::<$T>());
                test_argmax_success::<$T>(&column_name, "numbers", engine.clone()).await?;
            )*
        }
    }
    for_all_primitive_types! { test_argmax }
    Ok(())
}

async fn test_argmax_success<T>(
    column_name: &str,
    table_name: &str,
    engine: Arc<dyn QueryEngine>,
) -> Result<()>
where
    T: PrimitiveElement + PartialOrd,
    for<'a> T: Scalar<RefType<'a> = T>,
{
    let result = execute_argmax(column_name, table_name, engine.clone())
        .await
        .unwrap();
    assert_eq!(1, result.len());
    assert_eq!(result[0].df_recordbatch.num_columns(), 1);
    assert_eq!(1, result[0].schema.arrow_schema().fields().len());
    assert_eq!("argmax", result[0].schema.arrow_schema().field(0).name());

    let columns = result[0].df_recordbatch.columns();
    assert_eq!(1, columns.len());
    assert_eq!(columns[0].len(), 1);
    let v = VectorHelper::try_into_vector(&columns[0]).unwrap();
    assert_eq!(1, v.len());
    let value = v.get(0);

    let numbers = get_numbers_from_table::<T>(column_name, table_name, engine.clone()).await;
    let expected_value = match numbers.len() {
        0 => 0_u64,
        _ => {
            let mut index = 0;
            let mut max = numbers[0].into();
            for (i, &number) in numbers.iter().enumerate() {
                if max < number.into() {
                    max = number.into();
                    index = i;
                }
            }
            index as u64
        }
    };
    let expected_value = Value::from(expected_value);
    assert_eq!(value, expected_value);
    Ok(())
}

async fn execute_argmax<'a>(
    column_name: &'a str,
    table_name: &'a str,
    engine: Arc<dyn QueryEngine>,
) -> RecordResult<Vec<RecordBatch>> {
    let sql = format!(
        "select ARGMAX({}) as argmax from {}",
        column_name, table_name
    );
    let plan = engine.sql_to_plan(&sql).unwrap();

    let output = engine.execute(&plan).await.unwrap();
    let recordbatch_stream = match output {
        Output::RecordBatch(batch) => batch,
        _ => unreachable!(),
    };
    util::collect(recordbatch_stream).await
}
