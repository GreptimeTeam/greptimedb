// P.S.: not extract to file because in file proc macro
use std::sync::Arc;

use arrow::array::NullArray;
use datafusion_expr::ColumnarValue as DFColValue;
use datafusion_physical_expr::expressions;
use datafusion_physical_expr::math_expressions;
use rustpython_vm::{AsObject, PyObjectRef, PyRef, PyResult, VirtualMachine};

use crate::scalars::math::PowFunction;
use crate::scalars::py_udf_module::builtins::{
    all_to_f64, eval_aggr_fn, from_df_err, try_into_columnar_value, try_into_py_obj,
    type_cast_error,
};
use crate::scalars::{function::FunctionContext, python::PyVector, Function};
type PyVectorRef = PyRef<PyVector>;

// the main binding code, due to proc macro things, can't directly use a simpler macro
// because pyfunction is not a attr?

// The math function return a general PyObjectRef
// so it can return both PyVector or a scalar PyInt/Float/Bool

/// simple math function, the backing implement is datafusion's `sqrt` math function
#[pyfunction]
fn sqrt(val: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
    bind_call_unary_math_function!(sqrt, vm, val);
}
/// simple math function, the backing implement is datafusion's `sin` math function
#[pyfunction]
fn sin(val: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
    bind_call_unary_math_function!(sin, vm, val);
}
/// simple math function, the backing implement is datafusion's `cos` math function
#[pyfunction]
fn cos(val: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
    bind_call_unary_math_function!(cos, vm, val);
}
/// simple math function, the backing implement is datafusion's `tan` math function
#[pyfunction]
fn tan(val: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
    bind_call_unary_math_function!(tan, vm, val);
}
/// simple math function, the backing implement is datafusion's `asin` math function
#[pyfunction]
fn asin(val: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
    bind_call_unary_math_function!(asin, vm, val);
}
/// simple math function, the backing implement is datafusion's `acos` math function
#[pyfunction]
fn acos(val: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
    bind_call_unary_math_function!(acos, vm, val);
}
/// simple math function, the backing implement is datafusion's `atan` math function
#[pyfunction]
fn atan(val: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
    bind_call_unary_math_function!(atan, vm, val);
}
/// simple math function, the backing implement is datafusion's `floor` math function
#[pyfunction]
fn floor(val: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
    bind_call_unary_math_function!(floor, vm, val);
}
/// simple math function, the backing implement is datafusion's `ceil` math function
#[pyfunction]
fn ceil(val: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
    bind_call_unary_math_function!(ceil, vm, val);
}
/// simple math function, the backing implement is datafusion's `round` math function
#[pyfunction]
fn round(val: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
    bind_call_unary_math_function!(round, vm, val);
}
/// simple math function, the backing implement is datafusion's `trunc` math function
#[pyfunction]
fn trunc(val: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
    bind_call_unary_math_function!(trunc, vm, val);
}
/// simple math function, the backing implement is datafusion's `abs` math function
#[pyfunction]
fn abs(val: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
    bind_call_unary_math_function!(abs, vm, val);
}
/// simple math function, the backing implement is datafusion's `signum` math function
#[pyfunction]
fn signum(val: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
    bind_call_unary_math_function!(signum, vm, val);
}
/// simple math function, the backing implement is datafusion's `exp` math function
#[pyfunction]
fn exp(val: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
    bind_call_unary_math_function!(exp, vm, val);
}
/// simple math function, the backing implement is datafusion's `ln` math function
#[pyfunction]
fn ln(val: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
    bind_call_unary_math_function!(ln, vm, val);
}
/// simple math function, the backing implement is datafusion's `log2` math function
#[pyfunction]
fn log2(val: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
    bind_call_unary_math_function!(log2, vm, val);
}
/// simple math function, the backing implement is datafusion's `log10` math function
#[pyfunction]
fn log10(val: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
    bind_call_unary_math_function!(log10, vm, val);
}

/// return a random vector range from 0 to 1 and length of len
#[pyfunction]
fn random(len: usize, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
    // This is in a proc macro so using full path to avoid strange things
    // more info at: https://doc.rust-lang.org/reference/procedural-macros.html#procedural-macro-hygiene
    let arg = NullArray::new(arrow::datatypes::DataType::Null, len);
    let args = &[DFColValue::Array(std::sync::Arc::new(arg) as _)];
    let res = math_expressions::random(args).map_err(|err| from_df_err(err, vm))?;
    let ret = try_into_py_obj(res, vm)?;
    Ok(ret)
}
// UDAF(User Defined Aggregate Function) in datafusion

#[pyfunction]
fn approx_distinct(values: PyVectorRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
    bind_aggr_fn!(
        ApproxDistinct,
        vm,
        &[values.to_arrow_array()],
        values.to_arrow_array().data_type(),
        expr0
    );
}

#[pyfunction]
fn approx_median(values: PyVectorRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
    bind_aggr_fn!(
        ApproxMedian,
        vm,
        &[values.to_arrow_array()],
        values.to_arrow_array().data_type(),
        expr0
    );
}

#[pyfunction]
fn approx_percentile_cont(
    values: PyVectorRef,
    percent: f64,
    vm: &VirtualMachine,
) -> PyResult<PyObjectRef> {
    let percent =
        expressions::Literal::new(datafusion_common::ScalarValue::Float64(Some(percent)));
    return eval_aggr_fn(
        expressions::ApproxPercentileCont::new(
            vec![
                Arc::new(expressions::Column::new("expr0", 0)) as _,
                Arc::new(percent) as _,
            ],
            "ApproxPercentileCont",
            (values.to_arrow_array().data_type()).to_owned(),
        )
        .map_err(|err| from_df_err(err, vm))?,
        &[values.to_arrow_array()],
        vm,
    );
}

#[pyfunction]
fn array_agg(values: PyVectorRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
    bind_aggr_fn!(
        ArrayAgg,
        vm,
        &[values.to_arrow_array()],
        values.to_arrow_array().data_type(),
        expr0
    );
}

/// directly port from datafusion's `avg` function
#[pyfunction]
fn avg(values: PyVectorRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
    bind_aggr_fn!(
        Avg,
        vm,
        &[values.to_arrow_array()],
        values.to_arrow_array().data_type(),
        expr0
    );
}

#[pyfunction]
fn correlation(
    arg0: PyVectorRef,
    arg1: PyVectorRef,
    vm: &VirtualMachine,
) -> PyResult<PyObjectRef> {
    bind_aggr_fn!(
        Correlation,
        vm,
        &[arg0.to_arrow_array(), arg1.to_arrow_array()],
        arg0.to_arrow_array().data_type(),
        expr0,
        expr1
    );
}

#[pyfunction]
fn count(values: PyVectorRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
    bind_aggr_fn!(
        Count,
        vm,
        &[values.to_arrow_array()],
        values.to_arrow_array().data_type(),
        expr0
    );
}

#[pyfunction]
fn covariance(
    arg0: PyVectorRef,
    arg1: PyVectorRef,
    vm: &VirtualMachine,
) -> PyResult<PyObjectRef> {
    bind_aggr_fn!(
        Covariance,
        vm,
        &[arg0.to_arrow_array(), arg1.to_arrow_array()],
        arg0.to_arrow_array().data_type(),
        expr0,
        expr1
    );
}

#[pyfunction]
fn covariance_pop(
    arg0: PyVectorRef,
    arg1: PyVectorRef,
    vm: &VirtualMachine,
) -> PyResult<PyObjectRef> {
    bind_aggr_fn!(
        CovariancePop,
        vm,
        &[arg0.to_arrow_array(), arg1.to_arrow_array()],
        arg0.to_arrow_array().data_type(),
        expr0,
        expr1
    );
}

#[pyfunction]
fn max(values: PyVectorRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
    bind_aggr_fn!(
        Max,
        vm,
        &[values.to_arrow_array()],
        values.to_arrow_array().data_type(),
        expr0
    );
}

#[pyfunction]
fn min(values: PyVectorRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
    bind_aggr_fn!(
        Min,
        vm,
        &[values.to_arrow_array()],
        values.to_arrow_array().data_type(),
        expr0
    );
}

#[pyfunction]
fn stddev(values: PyVectorRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
    bind_aggr_fn!(
        Stddev,
        vm,
        &[values.to_arrow_array()],
        values.to_arrow_array().data_type(),
        expr0
    );
}

#[pyfunction]
fn stddev_pop(values: PyVectorRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
    bind_aggr_fn!(
        StddevPop,
        vm,
        &[values.to_arrow_array()],
        values.to_arrow_array().data_type(),
        expr0
    );
}

#[pyfunction]
fn sum(values: PyVectorRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
    bind_aggr_fn!(
        Sum,
        vm,
        &[values.to_arrow_array()],
        values.to_arrow_array().data_type(),
        expr0
    );
}

#[pyfunction]
fn variance(values: PyVectorRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
    bind_aggr_fn!(
        Variance,
        vm,
        &[values.to_arrow_array()],
        values.to_arrow_array().data_type(),
        expr0
    );
}

#[pyfunction]
fn variance_pop(values: PyVectorRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
    bind_aggr_fn!(
        VariancePop,
        vm,
        &[values.to_arrow_array()],
        values.to_arrow_array().data_type(),
        expr0
    );
}

/// Pow function,
/// TODO: use PyObjectRef to adopt more type
#[pyfunction]
fn pow(base: PyObjectRef, pow: PyVectorRef, vm: &VirtualMachine) -> PyResult<PyVector> {
    let res = try_into_columnar_value(base.clone(), vm);
    if let Ok(res) = res {
        match res {
            DFColValue::Array(arr) => {
                dbg!(&arr);
            }
            DFColValue::Scalar(val) => {
                dbg!(&val);
            }
        };
    }
    let base = base
        .payload::<PyVector>()
        .ok_or_else(|| type_cast_error(&base.class().name(), "vector", vm))?;
    // pyfunction can return PyResult<...>, args can be like PyObjectRef or anything
    // impl IntoPyNativeFunc, see rustpython-vm function for more details
    let args = vec![base.as_vector_ref(), pow.as_vector_ref()];
    let res = PowFunction::default()
        .eval(FunctionContext::default(), &args)
        .unwrap();
    Ok(res.into())
}
