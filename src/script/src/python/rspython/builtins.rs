// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//!  Builtin module contains GreptimeDB builtin udf/udaf

#[cfg(test)]
mod test;

use std::sync::Arc;

use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::ColumnarValue as DFColValue;
use datafusion_physical_expr::AggregateExpr;
use datatypes::arrow::array::ArrayRef;
use datatypes::arrow::compute;
use datatypes::arrow::datatypes::{DataType as ArrowDataType, Field};
use datatypes::vectors::Helper as HelperVec;
use rustpython_vm::builtins::{PyBaseExceptionRef, PyBool, PyFloat, PyInt, PyList, PyStr};
use rustpython_vm::{pymodule, AsObject, PyObjectRef, PyPayload, PyResult, VirtualMachine};

use crate::python::ffi_types::PyVector;
use crate::python::rspython::utils::is_instance;

pub fn init_greptime_builtins(module_name: &str, vm: &mut VirtualMachine) {
    vm.add_native_module(
        module_name.to_string(),
        Box::new(greptime_builtin::make_module),
    );
}

/// "Can't cast operand of type `{name}` into `{ty}`."
fn type_cast_error(name: &str, ty: &str, vm: &VirtualMachine) -> PyBaseExceptionRef {
    vm.new_type_error(format!("Can't cast operand of type `{name}` into `{ty}`."))
}

fn collect_diff_types_string(values: &[ScalarValue], ty: &ArrowDataType) -> String {
    values
        .iter()
        .enumerate()
        .filter_map(|(idx, val)| {
            if val.data_type() != *ty {
                Some((idx, val.data_type()))
            } else {
                None
            }
        })
        .map(|(idx, ty)| format!(" {:?} at {}th location\n", ty, idx + 1))
        .reduce(|mut acc, item| {
            acc.push_str(&item);
            acc
        })
        .unwrap_or_else(|| "Nothing".to_string())
}

fn new_item_field(data_type: ArrowDataType) -> Field {
    Field::new("item", data_type, false)
}

/// try to turn a Python Object into a PyVector or a scalar that can be use for calculate
///
/// supported scalar are(leftside is python data type, right side is rust type):
///
/// | Python |  Rust  |
/// | ------ | ------ |
/// | integer| i64    |
/// | float  | f64    |
/// | str    | String |
/// | bool   | bool   |
/// | vector | array  |
/// | list   | `ScalarValue::List` |
pub fn try_into_columnar_value(obj: PyObjectRef, vm: &VirtualMachine) -> PyResult<DFColValue> {
    if is_instance::<PyVector>(&obj, vm) {
        let ret = obj
            .payload::<PyVector>()
            .ok_or_else(|| type_cast_error(&obj.class().name(), "vector", vm))?;
        Ok(DFColValue::Array(ret.to_arrow_array()))
    } else if is_instance::<PyBool>(&obj, vm) {
        // Note that a `PyBool` is also a `PyInt`, so check if it is a bool first to get a more precise type
        let ret = obj.try_into_value::<bool>(vm)?;
        Ok(DFColValue::Scalar(ScalarValue::Boolean(Some(ret))))
    } else if is_instance::<PyStr>(&obj, vm) {
        let ret = obj.try_into_value::<String>(vm)?;
        Ok(DFColValue::Scalar(ScalarValue::Utf8(Some(ret))))
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
                let col = try_into_columnar_value(obj.clone(), vm)?;
                match col {
                    DFColValue::Array(arr) => Err(vm.new_type_error(format!(
                        "Expect only scalar value in a list, found a vector of type {:?} nested in list", arr.data_type()
                    ))),
                    DFColValue::Scalar(val) => Ok(val),
                }
            })
            .collect::<Result<_, _>>()?;

        if ret.is_empty() {
            // TODO(dennis): empty list, we set type as null.
            return Ok(DFColValue::Scalar(ScalarValue::List(
                None,
                Arc::new(new_item_field(ArrowDataType::Null)),
            )));
        }

        let ty = ret[0].data_type();
        if ret.iter().any(|i| i.data_type() != ty) {
            return Err(vm.new_type_error(format!(
                "All elements in a list should be same type to cast to Datafusion list!\nExpect {ty:?}, found {}",
                collect_diff_types_string(&ret, &ty)
            )));
        }
        Ok(DFColValue::Scalar(ScalarValue::List(
            Some(ret),
            Arc::new(new_item_field(ty)),
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
                    .map_err(|err| vm.new_type_error(format!("Unsupported type: {err:#?}")))?,
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
            let list = col
                .into_iter()
                .map(|v| scalar_val_try_into_py_obj(v, vm))
                .collect::<Result<_, _>>()?;
            let list = vm.ctx.new_list(list);
            Ok(list.into())
        }
        _ => Err(vm.new_type_error(format!(
            "Can't cast a Scalar Value `{val:#?}` of type {:#?} to a Python Object",
            val.data_type()
        ))),
    }
}

/// Because most of the datafusion's UDF only support f32/64, so cast all to f64 to use datafusion's UDF
fn all_to_f64(col: DFColValue, vm: &VirtualMachine) -> PyResult<DFColValue> {
    match col {
        DFColValue::Array(arr) => {
            let res = compute::cast(&arr, &ArrowDataType::Float64).map_err(|err| {
                vm.new_type_error(format!(
                    "Arrow Type Cast Fail(from {:#?} to {:#?}): {err:#?}",
                    arr.data_type(),
                    ArrowDataType::Float64
                ))
            })?;
            Ok(DFColValue::Array(res))
        }
        DFColValue::Scalar(val) => {
            let val_in_f64 = match val {
                ScalarValue::Float64(Some(v)) => v,
                ScalarValue::Int64(Some(v)) => v as f64,
                ScalarValue::Boolean(Some(v)) => v as i64 as f64,
                _ => {
                    return Err(vm.new_type_error(format!(
                        "Can't cast type {:#?} to {:#?}",
                        val.data_type(),
                        ArrowDataType::Float64
                    )))
                }
            };
            Ok(DFColValue::Scalar(ScalarValue::Float64(Some(val_in_f64))))
        }
    }
}

/// use to bind to Data Fusion's UDF function
/// P.S: seems due to proc macro issues, can't just use `#[pyfunction]` in here
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
/// - first arguments is the name of datafusion expression function like `Avg`
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
                    stringify!($AGGR_FUNC), $DATA_TYPE.clone()),
            $ARGS, $VM)
    };
}

#[inline]
fn from_df_err(err: DataFusionError, vm: &VirtualMachine) -> PyBaseExceptionRef {
    vm.new_runtime_error(format!("Data Fusion Error: {err:#?}"))
}

/// evaluate Aggregate Expr using its backing accumulator
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
/// allow Python Coprocessor Function to use already implemented udf functions from datafusion and GrepTime DB itself
///
#[pymodule]
pub(crate) mod greptime_builtin {
    // P.S.: not extract to file because not-inlined proc macro attribute is *unstable*
    use std::sync::Arc;

    use arrow::compute::kernels::{aggregate, boolean};
    use common_function::function::{Function, FunctionContext, FunctionRef};
    use common_function::function_registry::FUNCTION_REGISTRY;
    use common_function::scalars::math::PowFunction;
    use datafusion::arrow::datatypes::DataType as ArrowDataType;
    use datafusion::dataframe::DataFrame as DfDataFrame;
    use datafusion::physical_plan::expressions;
    use datafusion_expr::{ColumnarValue as DFColValue, Expr as DfExpr};
    use datafusion_physical_expr::math_expressions;
    use datatypes::arrow::array::{ArrayRef, Int64Array, NullArray};
    use datatypes::arrow::error::ArrowError;
    use datatypes::arrow::{self, compute};
    use datatypes::vectors::{ConstantVector, Float64Vector, Helper, Int64Vector, VectorRef};
    use paste::paste;
    use rustpython_vm::builtins::{PyFloat, PyFunction, PyInt, PyStr};
    use rustpython_vm::function::{FuncArgs, KwArgs, OptionalArg};
    use rustpython_vm::{
        pyclass, AsObject, PyObjectRef, PyPayload, PyRef, PyResult, VirtualMachine,
    };

    use crate::python::ffi_types::copr::PyQueryEngine;
    use crate::python::ffi_types::vector::val_to_pyobj;
    use crate::python::ffi_types::{PyVector, PyVectorRef};
    use crate::python::rspython::builtins::{
        all_to_f64, eval_aggr_fn, from_df_err, try_into_columnar_value, try_into_py_obj,
        type_cast_error,
    };
    use crate::python::rspython::dataframe_impl::data_frame::{PyExpr, PyExprRef};
    use crate::python::rspython::utils::{is_instance, py_obj_to_value, py_obj_to_vec};

    #[pyattr]
    #[pyclass(module = "greptime_builtin", name = "PyDataFrame")]
    #[derive(PyPayload, Debug, Clone)]
    pub struct PyDataFrame {
        pub inner: DfDataFrame,
    }

    /// get `__dataframe__` from globals and return it
    /// TODO(discord9): this is a terrible hack, we should find a better way to get `__dataframe__`
    #[pyfunction]
    fn dataframe(vm: &VirtualMachine) -> PyResult<PyDataFrame> {
        let df = vm.current_globals().get_item("__dataframe__", vm)?;
        let df = df
            .payload::<PyDataFrame>()
            .ok_or_else(|| vm.new_type_error(format!("object {:?} is not a DataFrame", df)))?;
        let df = df.clone();
        Ok(df)
    }

    /// get `__query__` from globals and return it
    /// TODO(discord9): this is a terrible hack, we should find a better way to get `__query__`
    #[pyfunction]
    pub(crate) fn query(vm: &VirtualMachine) -> PyResult<PyQueryEngine> {
        let query_engine = vm.current_globals().get_item("__query__", vm)?;
        let query_engine = query_engine.payload::<PyQueryEngine>().ok_or_else(|| {
            vm.new_type_error(format!("object {:?} is not a QueryEngine", query_engine))
        })?;
        let query_engine = query_engine.clone();
        Ok(query_engine)
    }

    #[pyfunction]
    fn vector(args: OptionalArg<PyObjectRef>, vm: &VirtualMachine) -> PyResult<PyVector> {
        PyVector::new(args, vm)
    }

    #[pyfunction]
    fn col(name: String, vm: &VirtualMachine) -> PyExprRef {
        let expr: PyExpr = DfExpr::Column(datafusion_common::Column::from_name(name)).into();
        expr.into_ref(&vm.ctx)
    }

    #[pyfunction]
    pub(crate) fn lit(obj: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyExprRef> {
        let val = py_obj_to_value(&obj, vm)?;
        let scalar_val = val
            .try_to_scalar_value(&val.data_type())
            .map_err(|e| vm.new_runtime_error(format!("{e}")))?;
        let expr: PyExpr = DfExpr::Literal(scalar_val).into();
        Ok(expr.into_ref(&vm.ctx))
    }

    // the main binding code, due to proc macro things, can't directly use a simpler macro
    // because pyfunction is not a attr?
    // ------
    // GrepTime DB's own UDF&UDAF
    // ------

    fn eval_func(name: &str, v: &[PyVectorRef], vm: &VirtualMachine) -> PyResult<PyVector> {
        let v: Vec<VectorRef> = v.iter().map(|v| v.as_vector_ref()).collect();
        let func: Option<FunctionRef> = FUNCTION_REGISTRY.get_function(name);
        let res = match func {
            Some(f) => f.eval(Default::default(), &v),
            None => return Err(vm.new_type_error(format!("Can't find function {name}"))),
        };
        match res {
            Ok(v) => Ok(v.into()),
            Err(err) => Err(vm.new_runtime_error(format!("Fail to evaluate the function,: {err}"))),
        }
    }

    fn eval_aggr_func(
        name: &str,
        args: &[PyVectorRef],
        vm: &VirtualMachine,
    ) -> PyResult<PyObjectRef> {
        let v: Vec<VectorRef> = args.iter().map(|v| v.as_vector_ref()).collect();
        let func = FUNCTION_REGISTRY.get_aggr_function(name);
        let f = match func {
            Some(f) => f.create().creator(),
            None => return Err(vm.new_type_error(format!("Can't find function {name}"))),
        };
        let types: Vec<_> = v.iter().map(|v| v.data_type()).collect();
        let acc = f(&types);
        let mut acc = match acc {
            Ok(acc) => acc,
            Err(err) => {
                return Err(vm.new_runtime_error(format!("Failed to create accumulator: {err}")))
            }
        };
        match acc.update_batch(&v) {
            Ok(_) => (),
            Err(err) => return Err(vm.new_runtime_error(format!("Failed to update batch: {err}"))),
        };
        let res = match acc.evaluate() {
            Ok(r) => r,
            Err(err) => {
                return Err(vm.new_runtime_error(format!("Failed to evaluate accumulator: {err}")))
            }
        };
        let res = val_to_pyobj(res, vm)?;
        Ok(res)
    }

    /// GrepTime's own impl of pow function
    #[pyfunction]
    fn pow_gp(v0: PyVectorRef, v1: PyVectorRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        eval_func("pow", &[v0, v1], vm)
    }

    #[pyfunction]
    fn clip(
        v0: PyVectorRef,
        v1: PyVectorRef,
        v2: PyVectorRef,
        vm: &VirtualMachine,
    ) -> PyResult<PyVector> {
        eval_func("clip", &[v0, v1, v2], vm)
    }

    #[pyfunction]
    fn diff(v: PyVectorRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
        eval_aggr_func("diff", &[v], vm)
    }

    #[pyfunction]
    fn mean(v: PyVectorRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
        eval_aggr_func("mean", &[v], vm)
    }

    #[pyfunction]
    fn polyval(v0: PyVectorRef, v1: PyVectorRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
        eval_aggr_func("polyval", &[v0, v1], vm)
    }

    #[pyfunction]
    fn argmax(v0: PyVectorRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
        eval_aggr_func("argmax", &[v0], vm)
    }

    #[pyfunction]
    fn argmin(v0: PyVectorRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
        eval_aggr_func("argmin", &[v0], vm)
    }

    #[pyfunction]
    fn percentile(v0: PyVectorRef, v1: PyVectorRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
        eval_aggr_func("percentile", &[v0, v1], vm)
    }

    #[pyfunction]
    fn scipy_stats_norm_cdf(
        v0: PyVectorRef,
        v1: PyVectorRef,
        vm: &VirtualMachine,
    ) -> PyResult<PyObjectRef> {
        eval_aggr_func("scipystatsnormcdf", &[v0, v1], vm)
    }

    #[pyfunction]
    fn scipy_stats_norm_pdf(
        v0: PyVectorRef,
        v1: PyVectorRef,
        vm: &VirtualMachine,
    ) -> PyResult<PyObjectRef> {
        eval_aggr_func("scipystatsnormpdf", &[v0, v1], vm)
    }

    // The math function return a general PyObjectRef
    // so it can return both PyVector or a scalar PyInt/Float/Bool

    // ------
    // DataFusion's UDF&UDAF
    // ------
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
        let value = try_into_columnar_value(val, vm)?;
        let array = value.into_array(1);
        let result = math_expressions::round(&[array]).map_err(|e| from_df_err(e, vm))?;
        try_into_py_obj(DFColValue::Array(result), vm)
    }

    //
    // /// simple math function, the backing implement is datafusion's `trunc` math function
    // #[pyfunction]
    // fn trunc(val: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
    //     bind_call_unary_math_function!(trunc, vm, val);
    // }

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
    #[pyfunction(name = "log")]
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
        let arg = NullArray::new(len);
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
            values.arrow_data_type(),
            expr0
        );
    }

    #[pyfunction]
    fn median(values: PyVectorRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
        bind_aggr_fn!(
            Median,
            vm,
            &[values.to_arrow_array()],
            values.arrow_data_type(),
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
        values.arrow_data_type(),
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
        eval_aggr_fn(
            expressions::ApproxPercentileCont::new(
                vec![
                    Arc::new(expressions::Column::new("expr0", 0)) as _,
                    Arc::new(percent) as _,
                ],
                "ApproxPercentileCont",
                values.arrow_data_type(),
            )
            .map_err(|err| from_df_err(err, vm))?,
            &[values.to_arrow_array()],
            vm,
        )
    }

    /// effectively equals to `list(vector)`
    #[pyfunction]
    fn array_agg(values: PyVectorRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
        bind_aggr_fn!(
            ArrayAgg,
            vm,
            &[values.to_arrow_array()],
            values.arrow_data_type(),
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
            values.arrow_data_type(),
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
            arg0.arrow_data_type(),
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
            values.arrow_data_type(),
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
            arg0.arrow_data_type(),
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
            arg0.arrow_data_type(),
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
            values.arrow_data_type(),
            expr0
        );
    }

    #[pyfunction]
    fn min(values: PyVectorRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
        bind_aggr_fn!(
            Min,
            vm,
            &[values.to_arrow_array()],
            values.arrow_data_type(),
            expr0
        );
    }

    #[pyfunction]
    fn stddev(values: PyVectorRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
        bind_aggr_fn!(
            Stddev,
            vm,
            &[values.to_arrow_array()],
            values.arrow_data_type(),
            expr0
        );
    }

    #[pyfunction]
    fn stddev_pop(values: PyVectorRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
        bind_aggr_fn!(
            StddevPop,
            vm,
            &[values.to_arrow_array()],
            values.arrow_data_type(),
            expr0
        );
    }

    #[pyfunction]
    fn sum(values: PyVectorRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
        bind_aggr_fn!(
            Sum,
            vm,
            &[values.to_arrow_array()],
            values.arrow_data_type(),
            expr0
        );
    }

    #[pyfunction]
    fn variance(values: PyVectorRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
        bind_aggr_fn!(
            Variance,
            vm,
            &[values.to_arrow_array()],
            values.arrow_data_type(),
            expr0
        );
    }

    #[pyfunction]
    fn variance_pop(values: PyVectorRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
        bind_aggr_fn!(
            VariancePop,
            vm,
            &[values.to_arrow_array()],
            values.arrow_data_type(),
            expr0
        );
    }

    /// Pow function, bind from gp's [`PowFunction`]
    #[pyfunction]
    fn pow(base: PyObjectRef, pow: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        let base = base
            .payload::<PyVector>()
            .ok_or_else(|| type_cast_error(&base.class().name(), "vector", vm))?;
        let len_base = base.as_vector_ref().len();
        let arg_pow = if is_instance::<PyVector>(&pow, vm) {
            let pow = pow
                .payload::<PyVector>()
                .ok_or_else(|| type_cast_error(&pow.class().name(), "vector", vm))?;
            pow.as_vector_ref()
        } else if is_instance::<PyFloat>(&pow, vm) {
            let pow = pow.try_into_value::<f64>(vm)?;
            let ret =
                ConstantVector::new(Arc::new(Float64Vector::from_vec(vec![pow])) as _, len_base);
            Arc::new(ret) as _
        } else if is_instance::<PyInt>(&pow, vm) {
            let pow = pow.try_into_value::<i64>(vm)?;
            let ret =
                ConstantVector::new(Arc::new(Int64Vector::from_vec(vec![pow])) as _, len_base);
            Arc::new(ret) as _
        } else {
            return Err(vm.new_type_error(format!("Unsupported type({pow:#?}) for pow()")));
        };
        // pyfunction can return PyResult<...>, args can be like PyObjectRef or anything
        // impl IntoPyNativeFunc, see rustpython-vm function for more details
        let args = vec![base.as_vector_ref(), arg_pow];
        let res = PowFunction
            .eval(FunctionContext::default(), &args)
            .map_err(|err| {
                vm.new_runtime_error(format!(
                    "Fail to eval pow() withi given args: {args:?}, Error: {err}"
                ))
            })?;
        Ok(res.into())
    }

    fn gen_none_array(
        data_type: ArrowDataType,
        len: usize,
        vm: &VirtualMachine,
    ) -> PyResult<ArrayRef> {
        macro_rules! match_none_array {
            ($VAR:ident, $LEN: ident, [$($TY:ident),*]) => {
                paste!{
                    match $VAR{
                        $(ArrowDataType::$TY => Arc::new(arrow::array::[<$TY Array>]::from(vec![None;$LEN])), )*
                        _ => return Err(vm.new_type_error(format!("gen_none_array() does not support {:?}", data_type)))
                    }
                }
            };
        }
        let ret: ArrayRef = match_none_array!(
            data_type,
            len,
            [Boolean, Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Float32, Float64] // We don't support float16 right now, it's not common in usage.
        );
        Ok(ret)
    }

    #[pyfunction]
    fn prev(cur: PyVectorRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        let cur = cur.to_arrow_array();
        if cur.len() == 0 {
            let ret = cur.slice(0, 0);
            let ret = Helper::try_into_vector(ret.clone()).map_err(|e| {
                vm.new_type_error(format!(
                    "Can't cast result into vector, result: {ret:?}, err: {e:?}",
                ))
            })?;
            return Ok(ret.into());
        }
        let cur = cur.slice(0, cur.len() - 1); // except the last one that is
        let fill = gen_none_array(cur.data_type().clone(), 1, vm)?;
        let ret = compute::concat(&[&*fill, &*cur]).map_err(|err| {
            vm.new_runtime_error(format!("Can't concat array[0] with array[0:-1]!{err:#?}"))
        })?;
        let ret = Helper::try_into_vector(ret.clone()).map_err(|e| {
            vm.new_type_error(format!(
                "Can't cast result into vector, result: {ret:?}, err: {e:?}",
            ))
        })?;
        Ok(ret.into())
    }

    #[pyfunction]
    fn next(cur: PyVectorRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        let cur = cur.to_arrow_array();
        if cur.len() == 0 {
            let ret = cur.slice(0, 0);
            let ret = Helper::try_into_vector(ret.clone()).map_err(|e| {
                vm.new_type_error(format!(
                    "Can't cast result into vector, result: {ret:?}, err: {e:?}",
                ))
            })?;
            return Ok(ret.into());
        }
        let cur = cur.slice(1, cur.len() - 1); // except the last one that is
        let fill = gen_none_array(cur.data_type().clone(), 1, vm)?;
        let ret = compute::concat(&[&*cur, &*fill]).map_err(|err| {
            vm.new_runtime_error(format!("Can't concat array[0] with array[0:-1]!{err:#?}"))
        })?;
        let ret = Helper::try_into_vector(ret.clone()).map_err(|e| {
            vm.new_type_error(format!(
                "Can't cast result into vector, result: {ret:?}, err: {e:?}",
            ))
        })?;
        Ok(ret.into())
    }

    /// generate interval time point
    fn gen_inteveral(
        oldest: i64,
        newest: i64,
        duration: i64,
        vm: &VirtualMachine,
    ) -> PyResult<Vec<i64>> {
        if oldest > newest {
            return Err(vm.new_value_error(format!("{oldest} is greater than {newest}")));
        }
        if duration > 0 {
            let ret = (oldest..=newest)
                .step_by(duration as usize)
                .collect::<Vec<_>>();
            Ok(ret)
        } else {
            Err(vm.new_value_error(format!("duration: {duration} is not positive number.")))
        }
    }

    /// `func`: exec on sliding window slice of given `arr`, expect it to always return a PyVector of one element
    /// `ts`: a vector of time stamp, expect to be Monotonous increase
    /// `arr`: actual data vector
    /// `duration`: the size of sliding window, also is the default step of sliding window's per step
    #[pyfunction]
    fn interval(
        ts: PyVectorRef,
        arr: PyVectorRef,
        duration: i64,
        func: PyRef<PyFunction>,
        vm: &VirtualMachine,
    ) -> PyResult<PyVector> {
        // TODO(discord9): change to use PyDict to mimic a table?
        // then: table: PyDict, , lambda t:
        // ts: PyStr, duration: i64
        // TODO: try to return a PyVector if possible, using concat array in arrow's compute module
        // 1. slice them according to duration
        let arrow_error = |err: ArrowError| vm.new_runtime_error(format!("Arrow Error: {err:#?}"));
        let datatype_error =
            |err: datatypes::Error| vm.new_runtime_error(format!("DataType Errors!: {err:#?}"));
        let ts_array_ref: ArrayRef = ts.to_arrow_array();
        let ts = ts_array_ref
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                vm.new_type_error(format!("ts must be int64, found: {ts_array_ref:?}"))
            })?;
        let slices = {
            let oldest = aggregate::min(ts)
                .ok_or_else(|| vm.new_runtime_error("ts must has min value".to_string()))?;
            let newest = aggregate::max(ts)
                .ok_or_else(|| vm.new_runtime_error("ts must has max value".to_string()))?;
            gen_inteveral(oldest, newest, duration, vm)?
        };

        let windows = {
            slices
                .iter()
                .zip(slices.iter().skip(1))
                .map(|(first, second)| {
                    let first = Int64Array::new_scalar(*first);
                    let second = Int64Array::new_scalar(*second);
                    let left =
                        arrow::compute::kernels::cmp::gt_eq(ts, &first).map_err(arrow_error)?;
                    let right =
                        arrow::compute::kernels::cmp::lt_eq(ts, &second).map_err(arrow_error)?;
                    boolean::and(&left, &right).map_err(arrow_error)
                })
                .map(|mask| match mask {
                    Ok(mask) => {
                        let arrow_arr = arr.to_arrow_array();
                        compute::filter(&arrow_arr, &mask).map_err(arrow_error)
                    }
                    Err(e) => Err(e),
                })
                .collect::<Result<Vec<_>, _>>()?
        };

        let apply_interval_function = |v: PyResult<PyVector>| match v {
            Ok(v) => {
                let args = FuncArgs::new(vec![v.into_pyobject(vm)], KwArgs::default());
                let ret = func.invoke(args, vm);
                match ret{
                        Ok(obj) => match py_obj_to_vec(&obj, vm, 1){
                            Ok(v) => if v.len()==1{
                                Ok(v)
                            }else{
                                Err(vm.new_runtime_error(format!("Expect return's length to be at most one, found to be length of {}.", v.len())))
                            },
                            Err(err) => Err(vm
                                .new_runtime_error(
                                    format!("expect `interval()`'s `func` return a PyVector(`vector`) or int/float/bool, found return to be {obj:?}, error msg: {err}")
                                )
                            )
                        }
                        Err(e) => Err(e),
                    }
            }
            Err(e) => Err(e),
        };

        // 2. apply function on each slice
        let fn_results = windows
            .into_iter()
            .map(|window| {
                Helper::try_into_vector(window)
                    .map(PyVector::from)
                    .map_err(datatype_error)
            })
            .map(apply_interval_function)
            .collect::<Result<Vec<_>, _>>()?;

        // 3. get returned vector and concat them
        let result_arrays: Vec<_> = fn_results
            .iter()
            .map(|vector| vector.to_arrow_array())
            .collect();
        let result_dyn_arrays: Vec<_> = result_arrays.iter().map(|v| v.as_ref()).collect();
        let concat_array = compute::concat(&result_dyn_arrays).map_err(arrow_error)?;
        let vector = Helper::try_into_vector(concat_array).map_err(datatype_error)?;

        // 4. return result vector
        Ok(PyVector::from(vector))
    }

    /// return first element in a `PyVector` in sliced new `PyVector`, if vector's length is zero, return a zero sized slice instead
    #[pyfunction]
    fn first(arr: PyVectorRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        let arr: ArrayRef = arr.to_arrow_array();
        let ret = match arr.len() {
            0 => arr.slice(0, 0),
            _ => arr.slice(0, 1),
        };
        let ret = Helper::try_into_vector(ret.clone()).map_err(|e| {
            vm.new_type_error(format!(
                "Can't cast result into vector, result: {ret:?}, err: {e:?}",
            ))
        })?;
        Ok(ret.into())
    }

    /// return last element in a `PyVector` in sliced new `PyVector`, if vector's length is zero, return a zero sized slice instead
    #[pyfunction]
    fn last(arr: PyVectorRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        let arr: ArrayRef = arr.to_arrow_array();
        let ret = match arr.len() {
            0 => arr.slice(0, 0),
            _ => arr.slice(arr.len() - 1, 1),
        };
        let ret = Helper::try_into_vector(ret.clone()).map_err(|e| {
            vm.new_type_error(format!(
                "Can't cast result into vector, result: {ret:?}, err: {e:?}",
            ))
        })?;
        Ok(ret.into())
    }

    #[pyfunction]
    fn datetime(input: &PyStr, vm: &VirtualMachine) -> PyResult<i64> {
        let mut parsed = Vec::new();
        let mut prev = 0;
        #[derive(Debug)]
        enum State {
            Num(i64),
            Separator(String),
        }
        let mut state = State::Num(Default::default());
        let input = input.as_str();
        for (idx, ch) in input.chars().enumerate() {
            match (ch.is_ascii_digit(), &state) {
                (true, State::Separator(_)) => {
                    let res = &input[prev..idx];
                    let res = State::Separator(res.to_string());
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
                _ => continue,
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
                State::Separator(res.to_string())
            }
        };
        parsed.push(last);
        let mut cur_idx = 0;
        let mut tot_time = 0;
        fn factor(unit: &str, vm: &VirtualMachine) -> PyResult<i64> {
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
                        return Err(vm.new_runtime_error(
                            "Expect a separator after number, found nothing!".to_string(),
                        ));
                    }
                    let nxt = &parsed[cur_idx + 1];
                    if let State::Separator(sep) = nxt {
                        tot_time += v * factor(sep, vm)?;
                    } else {
                        return Err(vm.new_runtime_error(format!(
                            "Expect a separator after number, found `{nxt:#?}`"
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
