use std::sync::Arc;

mod testing_table;

use common_query::error::Result as QueryResult;
use common_query::logical_plan::create_udaf;
use common_query::logical_plan::Accumulator;
use common_query::prelude::Volatility;
use common_recordbatch::util;
use datafusion::arrow_print;
use datafusion_common::record_batch::RecordBatch as DfRecordBatch;
use datafusion_common::ScalarValue;
use datatypes::data_type::ConcreteDataType;
use datatypes::prelude::VectorRef;
use datatypes::types::DataTypeBuilder;
use datatypes::types::Primitive;
use datatypes::value::Value;
use datatypes::vectors::PrimitiveVector;
use query::error::Result;
use query::query_engine::Output;

use crate::testing_table::TestingTable;

#[derive(Debug)]
struct MySumAccumulator {
    // TODO(LFC) make `sum` a `Value`?
    sum: u32,
}

impl MySumAccumulator {
    fn new() -> Self {
        Self { sum: 0 }
    }
}

impl Accumulator for MySumAccumulator {
    fn state(&self) -> QueryResult<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::from(self.sum)])
    }

    fn update_batch(&mut self, values: &[VectorRef]) -> QueryResult<()> {
        if values.is_empty() {
            return Ok(());
        };
        let column = &values[0];
        (0..column.len()).try_for_each(|index| {
            let value = column.try_get(index).unwrap();
            match value {
                Value::UInt32(v) => self.sum += v,
                _ => unreachable!(),
            }
            Ok(())
        })
    }

    fn merge_batch(&mut self, states: &[VectorRef]) -> QueryResult<()> {
        if states.is_empty() {
            return Ok(());
        };
        let states = &states[0];
        (0..states.len()).try_for_each(|index| {
            let state = states.try_get(index).unwrap();
            match state {
                Value::UInt32(v) => self.sum += v,
                _ => unreachable!(),
            }
            Ok(())
        })
    }

    fn evaluate(&self) -> QueryResult<ScalarValue> {
        Ok(ScalarValue::from(self.sum))
    }
}

#[tokio::test]
async fn test_my_sum() -> Result<()> {
    common_telemetry::init_default_ut_logging();
    test_my_sum_with((1..10).collect::<Vec<u32>>()).await?;
    // TODO(LFC) test other data types
    Ok(())
}

async fn test_my_sum_with<T>(numbers: Vec<T>) -> Result<()>
where
    T: Primitive + DataTypeBuilder,
{
    let table_name = format!("{}_numbers", std::any::type_name::<T>());
    let column_name = format!("{}_number", std::any::type_name::<T>());

    let testing_table = Arc::new(TestingTable::new(
        &column_name,
        Arc::new(PrimitiveVector::<T>::from_vec(numbers.clone())),
    ));

    let factory = testing_table::new_query_engine_factory(table_name.clone(), testing_table);
    let engine = factory.query_engine();

    let my_sum_udaf = create_udaf(
        "my_sum",
        ConcreteDataType::uint32_datatype(),
        Arc::new(ConcreteDataType::uint32_datatype()),
        Volatility::Immutable,
        Arc::new(|| Ok(Box::new(MySumAccumulator::new()))),
        Arc::new(vec![ConcreteDataType::uint32_datatype()]),
    );
    engine.register_udaf(my_sum_udaf);

    let sql = format!(
        "select MY_SUM({}) as my_sum from {}",
        column_name, table_name
    );
    let plan = engine.sql_to_plan(&sql)?;

    let output = engine.execute(&plan).await?;
    let recordbatch_stream = match output {
        Output::RecordBatch(batch) => batch,
        _ => unreachable!(),
    };
    let recordbatch = util::collect(recordbatch_stream).await.unwrap();
    let df_recordbatch = recordbatch
        .into_iter()
        .map(|r| r.df_recordbatch)
        .collect::<Vec<DfRecordBatch>>();

    let pretty_print = arrow_print::write(&df_recordbatch);
    // TODO(LFC) assertion
    println!("{}", pretty_print);
    Ok(())
}
