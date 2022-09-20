use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use catalog::memory::{MemoryCatalogList, MemoryCatalogProvider, MemorySchemaProvider};
use catalog::{
    CatalogList, CatalogProvider, SchemaProvider, DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME,
};
use common_function::scalars::aggregate::AggregateFunctionMeta;
use common_function_macro::{as_aggr_func_creator, AggrFuncTypeStore};
use common_query::error::CreateAccumulatorSnafu;
use common_query::error::Result as QueryResult;
use common_query::logical_plan::Accumulator;
use common_query::logical_plan::AggregateFunctionCreator;
use common_query::prelude::*;
use common_query::Output;
use common_recordbatch::{util, RecordBatch};
use datafusion::arrow_print;
use datafusion_common::record_batch::RecordBatch as DfRecordBatch;
use datatypes::prelude::*;
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::types::PrimitiveElement;
use datatypes::types::PrimitiveType;
use datatypes::vectors::PrimitiveVector;
use datatypes::with_match_primitive_type_id;
use num_traits::AsPrimitive;
use query::error::Result;
use query::QueryEngineFactory;
use test_util::MemTable;

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

#[as_aggr_func_creator]
#[derive(Debug, Default, AggrFuncTypeStore)]
struct MySumAccumulatorCreator {}

impl AggregateFunctionCreator for MySumAccumulatorCreator {
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
                        "\"MY_SUM\" aggregate function not support data type {:?}",
                        input_type.logical_type_id(),
                    );
                    CreateAccumulatorSnafu { err_msg }.fail()?
                }
            )
        });
        creator
    }

    fn output_type(&self) -> QueryResult<ConcreteDataType> {
        let input_type = &self.input_types()?[0];
        with_match_primitive_type_id!(
            input_type.logical_type_id(),
            |$S| {
                Ok(PrimitiveType::<<$S as Primitive>::LargestType>::default().logical_type_id().data_type())
            },
            {
                unreachable!()
            }
        )
    }

    fn state_types(&self) -> QueryResult<Vec<ConcreteDataType>> {
        Ok(vec![self.output_type()?])
    }
}

impl<T, SumT> Accumulator for MySumAccumulator<T, SumT>
where
    T: Primitive + AsPrimitive<SumT>,
    for<'a> T: Scalar<RefType<'a> = T>,
    SumT: Primitive + std::ops::AddAssign,
    for<'a> SumT: Scalar<RefType<'a> = SumT>,
{
    fn state(&self) -> QueryResult<Vec<Value>> {
        Ok(vec![self.sum.into()])
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

    fn evaluate(&self) -> QueryResult<Value> {
        Ok(self.sum.into())
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
    T: PrimitiveElement,
{
    let table_name = format!("{}_numbers", std::any::type_name::<T>());
    let column_name = format!("{}_number", std::any::type_name::<T>());

    let column_schemas = vec![ColumnSchema::new(
        column_name.clone(),
        T::build_data_type(),
        true,
    )];
    let schema = Arc::new(Schema::try_new(column_schemas.clone()).unwrap());
    let column: VectorRef = Arc::new(PrimitiveVector::<T>::from_vec(numbers));
    let recordbatch = RecordBatch::new(schema, vec![column]).unwrap();
    let testing_table = MemTable::new(&table_name, recordbatch);

    let factory = new_query_engine_factory(testing_table);
    let engine = factory.query_engine();

    engine.register_aggregate_function(Arc::new(AggregateFunctionMeta::new(
        "my_sum",
        1,
        Arc::new(|| Arc::new(MySumAccumulatorCreator::default())),
    )));

    let sql = format!(
        "select MY_SUM({}) as my_sum from {}",
        column_name, table_name
    );
    let plan = engine.sql_to_plan(&sql)?;

    let output = engine.execute(&plan).await?;
    let recordbatch_stream = match output {
        Output::Stream(batch) => batch,
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

fn new_query_engine_factory(table: MemTable) -> QueryEngineFactory {
    let table_name = table.table_name().to_string();
    let table = Arc::new(table);

    let schema_provider = Arc::new(MemorySchemaProvider::new());
    let catalog_provider = Arc::new(MemoryCatalogProvider::new());
    let catalog_list = Arc::new(MemoryCatalogList::default());

    schema_provider.register_table(table_name, table).unwrap();
    catalog_provider.register_schema(DEFAULT_SCHEMA_NAME.to_string(), schema_provider);
    catalog_list.register_catalog(DEFAULT_CATALOG_NAME.to_string(), catalog_provider);

    QueryEngineFactory::new(catalog_list)
}
