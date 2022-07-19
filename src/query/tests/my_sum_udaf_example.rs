use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

mod testing_table;

use common_query::error::ExecuteFunctionSnafu;
use common_query::error::Result as QueryResult;
use common_query::logical_plan::Accumulator;
use common_query::logical_plan::{create_udaf, AccumulatorCreator};
use common_query::prelude::*;
use common_recordbatch::util;
use datafusion::arrow_print;
use datafusion_common::record_batch::RecordBatch as DfRecordBatch;
use datafusion_common::DataFusionError;
use datatypes::prelude::*;
use datatypes::types::DataTypeBuilder;
use datatypes::types::PrimitiveType;
use datatypes::vectors::PrimitiveVector;
use datatypes::with_match_primitive_type_id;
use num_traits::AsPrimitive;
use query::error::Result;
use query::query_engine::Output;
use snafu::ResultExt;

use crate::testing_table::TestingTable;

#[derive(Debug, Default)]
struct MySumAccumulator<T, SumT>
where
    T: Primitive + AsPrimitive<SumT>,
    SumT: Primitive + std::ops::AddAssign,
{
    sum: SumT,
    _phantom: PhantomData<T>,
}

impl<T, SumT> MySumAccumulator<T, SumT>
where
    T: Primitive + AsPrimitive<SumT>,
    SumT: Primitive + std::ops::AddAssign,
{
    #[inline(always)]
    fn add(&mut self, v: T) {
        self.sum += v.as_();
    }

    #[inline(always)]
    fn merge(&mut self, s: SumT) {
        self.sum += s;
    }
}

#[derive(Debug, Default)]
struct MySumAccumulatorCreator {
    input_type: Arc<Mutex<Option<Vec<ConcreteDataType>>>>,
}

impl AccumulatorCreator for MySumAccumulatorCreator {
    fn creator(&self) -> AccumulatorCreatorFunction {
        let creator: AccumulatorCreatorFunction = Arc::new(move |types: &[ConcreteDataType]| {
            let input_type = &types[0];
            with_match_primitive_type_id!(
                input_type.logical_type_id(),
                |$S| {
                    Ok(Box::new(MySumAccumulator::<$S, <$S as Primitive>::LargestType>::default()))
                },
                {
                    let err_msg = format!(
                        "\"MY_SUM\" aggregate function not support date type {:?}",
                        input_type.logical_type_id(),
                    );
                    let err: std::result::Result<(), DataFusionError> = Err(DataFusionError::Execution(err_msg));
                    Err(err.context(ExecuteFunctionSnafu).err().unwrap().into())
                }
            )
        });
        creator
    }

    fn input_types(&self) -> Vec<ConcreteDataType> {
        self.input_type
            .lock()
            .unwrap()
            .as_ref()
            .expect("input_type is not present, check if DataFusion has changed its UDAF execution logic")
            .clone()
    }

    fn set_input_types(&self, input_types: Vec<ConcreteDataType>) {
        let mut holder = self.input_type.lock().unwrap();
        if let Some(old) = holder.as_ref() {
            assert_eq!(old.len(), input_types.len());
            old.iter().zip(input_types.iter()).for_each(|(x, y)|
                assert_eq!(x, y, "input type {:?} != {:?}, check if DataFusion has changed its UDAF execution logic", x, y)
            );
        }
        let _ = holder.insert(input_types);
    }

    fn output_type(&self) -> ConcreteDataType {
        let input_type = &self.input_types()[0];
        with_match_primitive_type_id!(
            input_type.logical_type_id(),
            |$S| {
                PrimitiveType::<<$S as Primitive>::LargestType>::default().logical_type_id().data_type()
            },
            {
                unreachable!()
            }
        )
    }

    fn state_types(&self) -> Vec<ConcreteDataType> {
        vec![self.output_type()]
    }
}

impl<T, SumT> Accumulator for MySumAccumulator<T, SumT>
where
    T: Primitive + AsPrimitive<SumT>,
    for<'a> T: Scalar<RefType<'a> = T>,
    SumT: Primitive + std::ops::AddAssign,
    for<'a> SumT: Scalar<RefType<'a> = SumT>,
{
    fn state(&self) -> QueryResult<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::from(self.sum.into())])
    }

    fn update_batch(&mut self, values: &[VectorRef]) -> QueryResult<()> {
        if values.is_empty() {
            return Ok(());
        };
        let column = &values[0];
        let column: &<T as Scalar>::VectorType = unsafe { VectorHelper::static_cast(column) };
        for v in column.iter_data().flatten() {
            self.add(v)
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[VectorRef]) -> QueryResult<()> {
        if states.is_empty() {
            return Ok(());
        };
        let states = &states[0];
        let states: &<SumT as Scalar>::VectorType = unsafe { VectorHelper::static_cast(states) };
        for s in states.iter_data().flatten() {
            self.merge(s)
        }
        Ok(())
    }

    fn evaluate(&self) -> QueryResult<ScalarValue> {
        Ok(ScalarValue::from(self.sum.into()))
    }
}

#[tokio::test]
async fn test_my_sum() -> Result<()> {
    common_telemetry::init_default_ut_logging();

    test_my_sum_with(
        (1..=10).collect::<Vec<u32>>(),
        vec![
            "+--------+",
            "| my_sum |",
            "+--------+",
            "| 55     |",
            "+--------+",
        ],
    )
    .await?;
    test_my_sum_with(
        (-10..=11).collect::<Vec<i32>>(),
        vec![
            "+--------+",
            "| my_sum |",
            "+--------+",
            "| 11     |",
            "+--------+",
        ],
    )
    .await?;
    test_my_sum_with(
        vec![-1.0f32, 1.0, 2.0, 3.0, 4.0],
        vec![
            "+--------+",
            "| my_sum |",
            "+--------+",
            "| 9      |",
            "+--------+",
        ],
    )
    .await?;
    test_my_sum_with(
        vec![u32::MAX, u32::MAX],
        vec![
            "+------------+",
            "| my_sum     |",
            "+------------+",
            "| 8589934590 |",
            "+------------+",
        ],
    )
    .await?;
    Ok(())
}

async fn test_my_sum_with<T>(numbers: Vec<T>, expected: Vec<&str>) -> Result<()>
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

    let my_sum_udaf = create_udaf("my_sum", Arc::new(MySumAccumulatorCreator::default()));
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
    let pretty_print = pretty_print.lines().collect::<Vec<&str>>();
    assert_eq!(expected, pretty_print);
    Ok(())
}
