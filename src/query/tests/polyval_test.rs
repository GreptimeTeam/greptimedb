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
use num_traits::AsPrimitive;
use query::error::Result;
use query::query_engine::Output;
use query::QueryEngine;

#[tokio::test]
async fn test_polyval_aggregator() -> Result<()> {
    common_telemetry::init_default_ut_logging();
    let engine = create_query_engine();

    macro_rules! test_polyval {
        ([], $( { $T:ty } ),*) => {
            $(
                let column_name = format!("{}_number", std::any::type_name::<$T>());
                test_polyval_success::<$T,<$T as Primitive>::LargestType>(&column_name, "numbers", engine.clone()).await?;
            )*
        }
    }
    for_all_primitive_types! { test_polyval }
    Ok(())
}

async fn test_polyval_success<T, PolyT>(
    column_name: &str,
    table_name: &str,
    engine: Arc<dyn QueryEngine>,
) -> Result<()>
where
    T: Primitive + AsPrimitive<PolyT> + PrimitiveElement,
    PolyT: Primitive + std::ops::Mul<Output = PolyT> + std::iter::Sum,
    for<'a> T: Scalar<RefType<'a> = T>,
    for<'a> PolyT: Scalar<RefType<'a> = PolyT>,
    i64: AsPrimitive<PolyT>,
{
    let result = execute_polyval(column_name, table_name, engine.clone())
        .await
        .unwrap();
    assert_eq!(1, result.len());
    assert_eq!(result[0].df_recordbatch.num_columns(), 1);
    assert_eq!(1, result[0].schema.arrow_schema().fields().len());
    assert_eq!("polyval", result[0].schema.arrow_schema().field(0).name());

    let columns = result[0].df_recordbatch.columns();
    assert_eq!(1, columns.len());
    assert_eq!(columns[0].len(), 1);
    let v = VectorHelper::try_into_vector(&columns[0]).unwrap();
    assert_eq!(1, v.len());
    let value = v.get(0);

    let numbers = get_numbers_from_table::<T>(column_name, table_name, engine.clone()).await;
    let expected_value = numbers.iter().copied();
    let x = 0i64;
    let len = expected_value.len();
    let expected_value: PolyT = expected_value
        .enumerate()
        .map(|(i, value)| value.as_() * (x.pow((len - 1 - i) as u32)).as_())
        .sum();
    assert_eq!(value, expected_value.into());
    Ok(())
}

async fn execute_polyval<'a>(
    column_name: &'a str,
    table_name: &'a str,
    engine: Arc<dyn QueryEngine>,
) -> RecordResult<Vec<RecordBatch>> {
    let sql = format!(
        "select POLYVAL({}, 0) as polyval from {}",
        column_name, table_name
    );
    let plan = engine.sql_to_plan(&sql).unwrap();

    let output = engine.execute(&plan).await.unwrap();
    let recordbatch_stream = match output {
        Output::Stream(batch) => batch,
        _ => unreachable!(),
    };
    util::collect(recordbatch_stream).await
}
