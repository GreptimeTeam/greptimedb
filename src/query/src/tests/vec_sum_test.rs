use std::borrow::Cow;
use std::ops::AddAssign;

use common_function::scalars::vector::impl_conv::{
    as_veclit, as_veclit_if_const, veclit_to_binlit,
};
use datatypes::prelude::Value;
use nalgebra::{Const, DVectorView, Dyn, OVector};

use crate::tests::{exec_selection, function};

#[tokio::test]
async fn test_vec_sum_aggregator() -> Result<(), common_query::error::Error> {
    common_telemetry::init_default_ut_logging();
    let engine = function::create_query_engine_for_vector10x3();
    let sql = "select VEC_SUM(vector) as vec_sum from vectors";
    let result = exec_selection(engine.clone(), &sql).await;
    let value = function::get_value_from_batches("vec_sum", result);

    let mut expected_value = None;

    let sql = "SELECT vector FROM vectors";
    let vectors = exec_selection(engine, &sql).await;

    let column = vectors[0].column(0);
    let vector_const = as_veclit_if_const(column)?;

    for i in 0..column.len() {
        let vector = match vector_const.as_ref() {
            Some(vector) => Some(Cow::Borrowed(vector.as_ref())),
            None => as_veclit(column.get_ref(i))?,
        };
        let Some(vector) = vector else {
            expected_value = None;
            break;
        };
        expected_value
            .get_or_insert_with(|| OVector::zeros_generic(Dyn(3), Const::<1>))
            .add_assign(&DVectorView::from_slice(&vector, vector.len()));
    }
    let expected_value = match expected_value.map(|v| veclit_to_binlit(v.as_slice())) {
        None => Value::Null,
        Some(bytes) => Value::from(bytes),
    };
    assert_eq!(value, expected_value);

    Ok(())
}
