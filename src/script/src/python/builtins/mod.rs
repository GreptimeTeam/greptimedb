//!  Builtin module contains GreptimeDB builtin udf/udaf
#[cfg(test)]
#[allow(clippy::print_stdout)]
mod test;

use arrow::array::ArrayRef;
use arrow::compute::cast::CastOptions;
use arrow::datatypes::DataType;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::ColumnarValue as DFColValue;
use datafusion_physical_expr::AggregateExpr;
use datatypes::vectors::Helper as HelperVec;
use rustpython_vm::builtins::PyList;
use rustpython_vm::pymodule;
use rustpython_vm::{
    builtins::{PyBaseExceptionRef, PyBool, PyFloat, PyInt},
    AsObject, PyObjectRef, PyPayload, PyResult, VirtualMachine,
};

use crate::python::utils::is_instance;
use crate::python::PyVector;

/// "Can't cast operand of type `{name}` into `{ty}`."
fn type_cast_error(name: &str, ty: &str, vm: &VirtualMachine) -> PyBaseExceptionRef {
    vm.new_type_error(format!("Can't cast operand of type `{name}` into `{ty}`."))
}

/// try to turn a Python Object into a PyVector or a scalar that can be use for calculate
///
/// supported scalar are(leftside is python data type, right side is rust type):
///
/// | Python | Rust |
/// | ------ | ---- |
/// | integer| i64  |
/// | float  | f64  |
/// | bool   | bool |
/// | vector | array|
/// | list   | `ScalarValue::List` |
fn try_into_columnar_value(obj: PyObjectRef, vm: &VirtualMachine) -> PyResult<DFColValue> {
    if is_instance::<PyVector>(&obj, vm) {
        let ret = obj
            .payload::<PyVector>()
            .ok_or_else(|| type_cast_error(&obj.class().name(), "vector", vm))?;
        Ok(DFColValue::Array(ret.to_arrow_array()))
    } else if is_instance::<PyBool>(&obj, vm) {
        // Note that a `PyBool` is also a `PyInt`, so check if it is a bool first to get a more precise type
        let ret = obj.try_into_value::<bool>(vm)?;
        Ok(DFColValue::Scalar(ScalarValue::Boolean(Some(ret))))
    } else if is_instance::<PyInt>(&obj, vm) {
        let ret = obj.try_into_value::<i64>(vm)?;
        Ok(DFColValue::Scalar(ScalarValue::Int64(Some(ret))))
    } else if is_instance::<PyFloat>(&obj, vm) {
        let ret = obj.try_into_value::<f64>(vm)?;
        Ok(DFColValue::Scalar(ScalarValue::Float64(Some(ret))))
    } else if is_instance::<PyList>(&obj, vm) {
        let ret = obj
            .payload::<PyList>()
            .ok_or_else(|| type_cast_error(&obj.class().name(), "vector", vm))?;
        let ret: Vec<ScalarValue> = ret
            .borrow_vec()
            .iter()
            .map(|obj| -> PyResult<ScalarValue> {
                let col = try_into_columnar_value(obj.to_owned(), vm)?;
                match col {
                    DFColValue::Array(arr) => Err(vm.new_type_error(format!(
                        "Expect only scalar value in a list, found a vector of type {:?} nested in list", arr.data_type()
                    ))),
                    DFColValue::Scalar(val) => Ok(val),
                }
            })
            .collect::<Result<_, _>>()?;

        if ret.is_empty() {
            //TODO(dennis): empty list, we set type as f64.
            return Ok(DFColValue::Scalar(ScalarValue::List(
                None,
                Box::new(DataType::Float64),
            )));
        }

        let ty = ret[0].get_datatype();
        if ret.iter().any(|i| i.get_datatype() != ty) {
            let diff = ret
                .iter()
                .enumerate()
                .filter_map(|(idx, val)| {
                    if val.get_datatype() != ty {
                        Some((idx, val.get_datatype()))
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            return Err(vm.new_type_error(format!(
                "All elements in a list should be same type to cast to Datafusion list!\nExpect {ty:?}, found {}",
                diff.iter().map(|(idx, ty)|{
                    format!(" {:?} at {}th location\n", ty, idx+1)
                }).reduce(|mut acc, item|{
                    acc.push_str(&item);
                    acc
                }).unwrap_or_else(||"Nothing".to_string())
            )));
        }
        Ok(DFColValue::Scalar(ScalarValue::List(
            Some(Box::new(ret)),
            Box::new(ty),
        )))
    } else {
        Err(vm.new_type_error(format!(
            "Can't cast object of type {} into vector or scalar",
            obj.class().name()
        )))
    }
}

/// cast a columnar value into python object
///
/// | Rust   | Python          |
/// | ------ | --------------- |
/// | Array  | PyVector        |
/// | Scalar | int/float/bool  |
fn try_into_py_obj(col: DFColValue, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
    match col {
        DFColValue::Array(arr) => {
            let ret = PyVector::from(
                HelperVec::try_into_vector(arr)
                    .map_err(|err| vm.new_type_error(format!("Unsupported type: {:#?}", err)))?,
            )
            .into_pyobject(vm);
            Ok(ret)
        }
        DFColValue::Scalar(val) => scalar_val_try_into_py_obj(val, vm),
    }
}

/// turn a ScalarValue into a Python Object, currently support
///
/// ScalarValue -> Python Type
/// - Float64 -> PyFloat
/// - Int64 -> PyInt
/// - UInt64 -> PyInt
/// - List -> PyList(of inner ScalarValue)
fn scalar_val_try_into_py_obj(val: ScalarValue, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
    match val {
        ScalarValue::Float32(Some(v)) => Ok(vm.ctx.new_float(v.into()).into()),
        ScalarValue::Float64(Some(v)) => Ok(PyFloat::from(v).into_pyobject(vm)),
        ScalarValue::Int64(Some(v)) => Ok(PyInt::from(v).into_pyobject(vm)),
        ScalarValue::UInt64(Some(v)) => Ok(PyInt::from(v).into_pyobject(vm)),
        ScalarValue::List(Some(col), _) => {
            let list: Vec<PyObjectRef> = col
                .into_iter()
                .map(|v| scalar_val_try_into_py_obj(v, vm))
                .collect::<Result<_, _>>()?;
            let list = vm.ctx.new_list(list);
            Ok(list.into())
        }
        _ => Err(vm.new_type_error(format!(
            "Can't cast a Scalar Value `{val:#?}` of type {:#?} to a Python Object",
            val.get_datatype()
        ))),
    }
}

/// Becuase most of the datafusion's UDF only support f32/64, so cast all to f64 to use datafusion's UDF
fn all_to_f64(col: DFColValue, vm: &VirtualMachine) -> PyResult<DFColValue> {
    match col {
        DFColValue::Array(arr) => {
            let res = arrow::compute::cast::cast(
                arr.as_ref(),
                &DataType::Float64,
                CastOptions {
                    wrapped: true,
                    partial: true,
                },
            )
            .map_err(|err| {
                vm.new_type_error(format!(
                    "Arrow Type Cast Fail(from {:#?} to {:#?}): {err:#?}",
                    arr.data_type(),
                    DataType::Float64
                ))
            })?;
            Ok(DFColValue::Array(res.into()))
        }
        DFColValue::Scalar(val) => {
            let val_in_f64 = match val {
                ScalarValue::Float64(Some(v)) => v,
                ScalarValue::Int64(Some(v)) => v as f64,
                ScalarValue::Boolean(Some(v)) => v as i64 as f64,
                _ => {
                    return Err(vm.new_type_error(format!(
                        "Can't cast type {:#?} to {:#?}",
                        val.get_datatype(),
                        DataType::Float64
                    )))
                }
            };
            Ok(DFColValue::Scalar(ScalarValue::Float64(Some(val_in_f64))))
        }
    }
}

/// use to bind to Data Fusion's UDF function
/// P.S: seems due to proc macro issues, can't just use #[pyfunction] in here
macro_rules! bind_call_unary_math_function {
    ($DF_FUNC: ident, $vm: ident $(,$ARG: ident)*) => {
        fn inner_fn($($ARG: PyObjectRef,)* vm: &VirtualMachine) -> PyResult<PyObjectRef> {
            let args = &[$(all_to_f64(try_into_columnar_value($ARG, vm)?, vm)?,)*];
            let res = math_expressions::$DF_FUNC(args).map_err(|err| from_df_err(err, vm))?;
            let ret = try_into_py_obj(res, vm)?;
            Ok(ret)
        }
        return inner_fn($($ARG,)* $vm);
    };
}

/// The macro for binding function in `datafusion_physical_expr::expressions`(most of them are aggregate function)
///
/// - first arguements is the name of datafusion expression function like `Avg`
/// - second is the python virtual machine ident `vm`
/// - following is the actual args passing in(as a slice).i.e.`&[values.to_arrow_array()]`
/// - the data type of passing in args, i.e: `Datatype::Float64`
/// - lastly ARE names given to expr of those function, i.e. `expr0, expr1,`....
macro_rules! bind_aggr_fn {
    ($AGGR_FUNC: ident, $VM: ident, $ARGS:expr, $DATA_TYPE: expr $(, $EXPR_ARGS: ident)*) => {
        // just a place holder, we just want the inner `XXXAccumulator`'s function
        // so its expr is irrelevant
        return eval_aggr_fn(
            expressions::$AGGR_FUNC::new(
                $(
                    Arc::new(expressions::Column::new(stringify!($EXPR_ARGS), 0)) as _,
                )*
                stringify!($AGGR_FUNC), $DATA_TYPE.to_owned()),
            $ARGS, $VM)
    };
}

#[inline]
fn from_df_err(err: DataFusionError, vm: &VirtualMachine) -> PyBaseExceptionRef {
    vm.new_runtime_error(format!("Data Fusion Error: {err:#?}"))
}

/// evalute Aggregate Expr using its backing accumulator
fn eval_aggr_fn<T: AggregateExpr>(
    aggr: T,
    values: &[ArrayRef],
    vm: &VirtualMachine,
) -> PyResult<PyObjectRef> {
    // acquire the accumulator, where the actual implement of aggregate expr layers
    let mut acc = aggr
        .create_accumulator()
        .map_err(|err| from_df_err(err, vm))?;
    acc.update_batch(values)
        .map_err(|err| from_df_err(err, vm))?;
    let res = acc.evaluate().map_err(|err| from_df_err(err, vm))?;
    scalar_val_try_into_py_obj(res, vm)
}

/// GrepTime User Define Function module
///
/// allow Python Coprocessor Function to use already implmented udf functions from datafusion and GrepTime DB itself
///
#[pymodule]
pub(crate) mod greptime_builtin {
    // P.S.: not extract to file because not-inlined proc macro attribute is *unstable*
    use std::sync::Arc;

    use arrow::array::{ArrayRef, NullArray, PrimitiveArray};
    use arrow::compute;
    use common_function::scalars::math::PowFunction;
    use common_function::scalars::{function::FunctionContext, Function};
    use datafusion::physical_plan::expressions;
    use datafusion_expr::ColumnarValue as DFColValue;
    use datafusion_physical_expr::math_expressions;
    use datatypes::vectors::Helper;
    use rustpython_vm::builtins::{PyFloat, PyStr, PyInt};
    use rustpython_vm::function::OptionalArg;
    use rustpython_vm::{AsObject, PyObjectRef, PyRef, PyResult, VirtualMachine};

    use crate::python::builtins::{
        all_to_f64, eval_aggr_fn, from_df_err, try_into_columnar_value, try_into_py_obj,
        type_cast_error,
    };
    use crate::python::utils::is_instance;
    use crate::python::PyVector;
    type PyVectorRef = PyRef<PyVector>;

    #[pyfunction]
    fn vector(args: OptionalArg<PyObjectRef>, vm: &VirtualMachine) -> PyResult<PyVector> {
        PyVector::new(args, vm)
    }

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

    /// Not implement in datafusion
    /// TODO(discord9): use greptime's own impl instead
    /*
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
    */

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

    /// effectively equals to `list(vector)`
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

    /// Pow function, bind from gp's [`PowFunction`]
    #[pyfunction]
    fn pow(base: PyObjectRef, pow: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        let base = base
            .payload::<PyVector>()
            .ok_or_else(|| type_cast_error(&base.class().name(), "vector", vm))?;
        let arg_pow = if is_instance::<PyVector>(&pow, vm) {
            let pow = pow
                .payload::<PyVector>()
                .ok_or_else(|| type_cast_error(&pow.class().name(), "vector", vm))?;
            pow.as_vector_ref()
        } else if is_instance::<PyFloat>(&pow, vm) {
            let pow = pow.try_into_value::<f64>(vm)?;
            let arr = PrimitiveArray::from_vec(vec![pow; base.as_vector_ref().len()]);
            let arr: ArrayRef = Arc::new(arr) as _;
            Helper::try_into_vector(&arr).map_err(|e| {
                vm.new_type_error(format!(
                    "Can't cast result into vector, result: {:?}, error message: {:?}",
                    &arr, e
                ))
            })?
        } 
        else if is_instance::<PyInt>(&pow, vm) {
            let pow = pow.try_into_value::<i64>(vm)?;
            let arr = PrimitiveArray::from_vec(vec![pow; base.as_vector_ref().len()]);
            let arr: ArrayRef = Arc::new(arr) as _;
            Helper::try_into_vector(&arr).map_err(|e| {
                vm.new_type_error(format!(
                    "Can't cast result into vector, result: {:?}, error message: {:?}",
                    &arr, e
                ))
            })?
        } else {
            return Err(vm.new_type_error(format!("Unsupported type({:#?}) for pow()", pow)));
        };
        // pyfunction can return PyResult<...>, args can be like PyObjectRef or anything
        // impl IntoPyNativeFunc, see rustpython-vm function for more details
        let args = vec![base.as_vector_ref(), arg_pow];
        let res = PowFunction::default()
            .eval(FunctionContext::default(), &args)
            .unwrap();
        Ok(res.into())
    }

    // TODO: prev, sum, pow, sqrt, datetime, slice, and filter(through boolean array)

    /// TODO: for now prev(arr)[0] == arr[0], need better fill method
    #[pyfunction]
    fn prev(cur: PyVectorRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        let cur: ArrayRef = cur.to_arrow_array();
        let cur = cur.slice(0, cur.len() - 1); // except the last one that is
        let fill = cur.slice(0, 1);
        let ret = compute::concatenate::concatenate(&[&*fill, &*cur]).map_err(|err| {
            vm.new_runtime_error(format!("Can't concat array[0] with array[0:-1]!{err:#?}"))
        })?;
        let ret = Helper::try_into_vector(&*ret).map_err(|e| {
            vm.new_type_error(format!(
                "Can't cast result into vector, result: {:?}, err: {:?}",
                ret, e
            ))
        })?;
        Ok(ret.into())
    }

    #[pyfunction]
    fn datetime(input: &PyStr, vm: &VirtualMachine) -> PyResult<isize> {
        let mut parsed = Vec::new();
        let mut prev = 0;
        #[derive(Debug)]
        enum State {
            Num(isize),
            Separator(String),
        }
        let mut state = State::Num(Default::default());
        let input = input.as_str();
        for (idx, ch) in input.chars().enumerate() {
            match (ch.is_ascii_digit(), &state) {
                (true, State::Separator(_)) => {
                    let res = &input[prev..idx];
                    let res = State::Separator(res.to_owned());
                    parsed.push(res);
                    prev = idx;
                    state = State::Num(Default::default());
                }
                (false, State::Num(_)) => {
                    let res = str::parse(&input[prev..idx]).map_err(|err| {
                        vm.new_runtime_error(format!("Fail to parse num: {err:#?}"))
                    })?;
                    let res = State::Num(res);
                    parsed.push(res);
                    prev = idx;
                    state = State::Separator(Default::default());
                }
                _ => {}
            };
        }
        let last = match state {
            State::Num(_) => {
                let res = str::parse(&input[prev..])
                    .map_err(|err| vm.new_runtime_error(format!("Fail to parse num: {err:#?}")))?;
                State::Num(res)
            }
            State::Separator(_) => {
                let res = &input[prev..];
                State::Separator(res.to_owned())
            }
        };
        parsed.push(last);
        let mut cur_idx = 0;
        let mut tot_time = 0;
        fn factor(unit: &str, vm: &VirtualMachine) -> PyResult<isize> {
            let ret = match unit {
                "d" => 24 * 60 * 60,
                "h" => 60 * 60,
                "m" => 60,
                "s" => 1,
                _ => return Err(vm.new_type_error(format!("Unknown time unit: {unit}"))),
            };
            Ok(ret)
        }
        while cur_idx < parsed.len() {
            match &parsed[cur_idx] {
                State::Num(v) => {
                    if cur_idx + 1 > parsed.len() {
                        return Err(vm.new_runtime_error(format!(
                            "Expect a spearator after number, found nothing!"
                        )));
                    }
                    let nxt = &parsed[cur_idx + 1];
                    if let State::Separator(sep) = nxt {
                        tot_time += v * factor(sep, vm)?;
                    } else {
                        return Err(vm.new_runtime_error(format!(
                            "Expect a spearator after number, found `{nxt:#?}`"
                        )));
                    }
                    cur_idx += 2;
                }
                State::Separator(sep) => {
                    return Err(vm.new_runtime_error(format!("Expect a number, found `{sep}`")))
                }
            }
        }
        Ok(tot_time)
    }
}
