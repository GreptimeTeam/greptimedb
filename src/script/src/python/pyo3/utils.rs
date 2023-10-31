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

use std::sync::{Arc, Mutex};

use arrow::pyarrow::PyArrowException;
use common_telemetry::info;
use datafusion_common::ScalarValue;
use datafusion_expr::ColumnarValue;
use datatypes::arrow::datatypes::DataType as ArrowDataType;
use datatypes::prelude::ConcreteDataType;
use datatypes::value::{OrderedFloat, Value};
use datatypes::vectors::Helper;
use once_cell::sync::Lazy;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyFloat, PyInt, PyList, PyTuple};

use crate::python::ffi_types::utils::{collect_diff_types_string, new_item_field};
use crate::python::ffi_types::PyVector;
use crate::python::metric;
use crate::python::pyo3::builtins::greptime_builtins;

/// prevent race condition of init cpython
static START_PYO3: Lazy<Mutex<bool>> = Lazy::new(|| Mutex::new(false));
pub(crate) fn to_py_err(err: impl ToString) -> PyErr {
    PyArrowException::new_err(err.to_string())
}

/// init cpython interpreter with `greptime` builtins, if already inited, do nothing
pub(crate) fn init_cpython_interpreter() -> PyResult<()> {
    let _t = metric::METRIC_PYO3_INIT_ELAPSED.start_timer();
    let mut start = START_PYO3.lock().unwrap();
    if !*start {
        pyo3::append_to_inittab!(greptime_builtins);
        pyo3::prepare_freethreaded_python();
        *start = true;
        info!("Started CPython Interpreter");
    }
    Ok(())
}

pub fn val_to_py_any(py: Python<'_>, val: Value) -> PyResult<PyObject> {
    Ok(match val {
        Value::Null => py.None(),
        Value::Boolean(val) => val.to_object(py),
        Value::UInt8(val) => val.to_object(py),
        Value::UInt16(val) => val.to_object(py),
        Value::UInt32(val) => val.to_object(py),
        Value::UInt64(val) => val.to_object(py),
        Value::Int8(val) => val.to_object(py),
        Value::Int16(val) => val.to_object(py),
        Value::Int32(val) => val.to_object(py),
        Value::Int64(val) => val.to_object(py),
        Value::Float32(val) => val.0.to_object(py),
        Value::Float64(val) => val.0.to_object(py),
        Value::String(val) => val.as_utf8().to_object(py),
        Value::Binary(val) => val.to_object(py),
        Value::Date(val) => val.val().to_object(py),
        Value::DateTime(val) => val.val().to_object(py),
        Value::Timestamp(val) => val.value().to_object(py),
        Value::List(val) => {
            let list = val.items().clone().unwrap_or(Default::default());
            let list = list
                .into_iter()
                .map(|v| val_to_py_any(py, v))
                .collect::<PyResult<Vec<_>>>()?;
            list.to_object(py)
        }
        #[allow(unreachable_patterns)]
        _ => {
            return Err(PyValueError::new_err(format!(
                "Convert from {val:?} is not supported yet"
            )))
        }
    })
}

macro_rules! to_con_type {
    ($dtype:ident,$obj:ident, $($cty:ident => $rty:ty),*$(,)?) => {
        match $dtype {
            $(
                ConcreteDataType::$cty(_) => $obj.extract::<$rty>().map(Value::$cty),
            )*
            _ => unreachable!(),
        }
    };
    ($dtype:ident,$obj:ident, $($cty:ident =ord=> $rty:ty),*$(,)?) => {
        match $dtype {
            $(
                ConcreteDataType::$cty(_) => $obj.extract::<$rty>()
                .map(OrderedFloat)
                .map(Value::$cty),
            )*
            _ => unreachable!(),
        }
    };
}

/// Convert PyAny to [`ScalarValue`]
pub(crate) fn pyo3_obj_try_to_typed_scalar_value(
    obj: &PyAny,
    dtype: Option<ConcreteDataType>,
) -> PyResult<ScalarValue> {
    let val = pyo3_obj_try_to_typed_val(obj, dtype)?;
    val.try_to_scalar_value(&val.data_type())
        .map_err(|e| PyValueError::new_err(e.to_string()))
}
/// to int/float/boolean, if dtype is None, then convert to highest prec type
pub(crate) fn pyo3_obj_try_to_typed_val(
    obj: &PyAny,
    dtype: Option<ConcreteDataType>,
) -> PyResult<Value> {
    if let Ok(b) = obj.downcast::<PyBool>() {
        if let Some(ConcreteDataType::Boolean(_)) = dtype {
            let dtype = ConcreteDataType::boolean_datatype();
            let ret = to_con_type!(dtype, b,
                Boolean => bool
            )?;
            Ok(ret)
        } else {
            Err(PyValueError::new_err(format!(
                "Can't cast num to {dtype:?}"
            )))
        }
    } else if let Ok(num) = obj.downcast::<PyInt>() {
        if let Some(dtype) = dtype {
            if dtype.is_signed() || dtype.is_unsigned() {
                let ret = to_con_type!(dtype, num,
                    Int8 => i8,
                    Int16 => i16,
                    Int32 => i32,
                    Int64 => i64,
                    UInt8 => u8,
                    UInt16 => u16,
                    UInt32 => u32,
                    UInt64 => u64,
                )?;
                Ok(ret)
            } else {
                Err(PyValueError::new_err(format!(
                    "Can't cast num to {dtype:?}"
                )))
            }
        } else {
            num.extract::<i64>().map(Value::Int64)
        }
    } else if let Ok(num) = obj.downcast::<PyFloat>() {
        if let Some(dtype) = dtype {
            if dtype.is_float() {
                let ret = to_con_type!(dtype, num,
                    Float32 =ord=> f32,
                    Float64 =ord=> f64,
                )?;
                Ok(ret)
            } else {
                Err(PyValueError::new_err(format!(
                    "Can't cast num to {dtype:?}"
                )))
            }
        } else {
            num.extract::<f64>()
                .map(|v| Value::Float64(OrderedFloat(v)))
        }
    } else if let Ok(s) = obj.extract::<String>() {
        Ok(Value::String(s.into()))
    } else {
        Err(PyValueError::new_err(format!(
            "Can't cast {obj} to {dtype:?}"
        )))
    }
}

/// cast a columnar value into python object
///
/// | Rust   | Python          |
/// | ------ | --------------- |
/// | Array  | PyVector        |
/// | Scalar | int/float/bool/str  |
pub fn columnar_value_to_py_any(py: Python<'_>, val: ColumnarValue) -> PyResult<PyObject> {
    match val {
        ColumnarValue::Array(arr) => {
            let v = PyVector::from(
                Helper::try_into_vector(arr).map_err(|e| PyValueError::new_err(format!("{e}")))?,
            );
            Ok(PyCell::new(py, v)?.into())
        }
        ColumnarValue::Scalar(scalar) => scalar_value_to_py_any(py, scalar),
    }
}

/// turn a ScalarValue into a Python Object, currently support
pub fn scalar_value_to_py_any(py: Python<'_>, val: ScalarValue) -> PyResult<PyObject> {
    macro_rules! to_py_any {
        ($val:ident, [$($scalar_ty:ident),*]) => {
            match val{
            ScalarValue::Null => Ok(py.None()),
            $(ScalarValue::$scalar_ty(Some(v)) => Ok(v.to_object(py)),)*
            ScalarValue::List(Some(col), _) => {
                let list:Vec<PyObject> = col
                    .into_iter()
                    .map(|v| scalar_value_to_py_any(py, v))
                    .collect::<PyResult<_>>()?;
                let list = PyList::new(py, list);
                Ok(list.into())
            }
            _ => Err(PyValueError::new_err(format!(
                "Can't cast a Scalar Value `{:#?}` of type {:#?} to a Python Object",
                $val, $val.data_type()
            )))
        }
        };
    }
    to_py_any!(
        val,
        [
            Boolean, Float32, Float64, Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64,
            Utf8, LargeUtf8
        ]
    )
}

pub fn try_into_columnar_value(py: Python<'_>, obj: PyObject) -> PyResult<ColumnarValue> {
    macro_rules! to_rust_types {
        ($obj: ident, $($ty: ty => $scalar_ty: ident),*) => {
            $(
                if let Ok(val) = $obj.extract::<$ty>(py) {
                    Ok(ColumnarValue::Scalar(ScalarValue::$scalar_ty(Some(val))))
                }
            )else*
            else{
                Err(PyValueError::new_err(format!("Can't cast {} into Columnar Value", $obj)))
            }
        };
    }
    if let Ok(v) = obj.extract::<PyVector>(py) {
        Ok(ColumnarValue::Array(v.to_arrow_array()))
    } else if obj.as_ref(py).is_instance_of::<PyList>()
        || obj.as_ref(py).is_instance_of::<PyTuple>()
    {
        let ret: Vec<ScalarValue> = {
            if let Ok(val) = obj.downcast::<PyList>(py) {
                val.iter().map(|v|->PyResult<ScalarValue>{
                    let val = try_into_columnar_value(py, v.into())?;
                    match val{
                        ColumnarValue::Array(arr) => Err(PyValueError::new_err(format!(
                            "Expect only scalar value in a list, found a vector of type {:?} nested in list", arr.data_type()
                        ))),
                        ColumnarValue::Scalar(val) => Ok(val),
                    }
            }).collect::<PyResult<_>>()?
            } else if let Ok(val) = obj.downcast::<PyTuple>(py) {
                val.iter().map(|v|->PyResult<ScalarValue>{
                    let val = try_into_columnar_value(py, v.into())?;
                    match val{
                        ColumnarValue::Array(arr) => Err(PyValueError::new_err(format!(
                            "Expect only scalar value in a tuple, found a vector of type {:?} nested in tuple", arr.data_type()
                        ))),
                        ColumnarValue::Scalar(val) => Ok(val),
                    }
            }).collect::<PyResult<_>>()?
            } else {
                unreachable!()
            }
        };

        if ret.is_empty() {
            return Ok(ColumnarValue::Scalar(ScalarValue::List(
                None,
                Arc::new(new_item_field(ArrowDataType::Null)),
            )));
        }
        let ty = ret[0].data_type();

        if ret.iter().any(|i| i.data_type() != ty) {
            return Err(PyValueError::new_err(format!(
                "All elements in a list should be same type to cast to Datafusion list!\nExpect {ty:?}, found {}",
                collect_diff_types_string(&ret, &ty)
            )));
        }
        Ok(ColumnarValue::Scalar(ScalarValue::List(
            Some(ret),
            Arc::new(new_item_field(ty)),
        )))
    } else {
        to_rust_types!(obj,
            bool => Boolean,
            i64 => Int64,
            f64 => Float64,
            String => Utf8
        )
    }
}
