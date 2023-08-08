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

use std::sync::Arc;

use datafusion_common::ScalarValue;
use datafusion_expr::ColumnarValue as DFColValue;
use datatypes::prelude::ScalarVector;
use datatypes::value::Value;
use datatypes::vectors::{
    BooleanVector, Float64Vector, Helper, Int64Vector, NullVector, StringVector, VectorRef,
};
use rustpython_vm::builtins::{PyBaseExceptionRef, PyBool, PyFloat, PyInt, PyList, PyStr};
use rustpython_vm::object::PyObjectPayload;
use rustpython_vm::{PyObjectRef, PyPayload, PyRef, PyResult, VirtualMachine};
use snafu::{OptionExt, ResultExt};

use crate::python::error;
use crate::python::error::ret_other_error_with;
use crate::python::ffi_types::PyVector;
use crate::python::rspython::builtins::try_into_columnar_value;

/// use `rustpython`'s `is_instance` method to check if a PyObject is a instance of class.
/// if `PyResult` is Err, then this function return `false`
pub fn is_instance<T: PyPayload>(obj: &PyObjectRef, vm: &VirtualMachine) -> bool {
    obj.is_instance(T::class(&vm.ctx).into(), vm)
        .unwrap_or(false)
}

pub fn obj_cast_to<T: PyObjectPayload>(
    obj: PyObjectRef,
    vm: &VirtualMachine,
) -> PyResult<PyRef<T>> {
    obj.downcast::<T>().map_err(|e| {
        vm.new_type_error(format!(
            "Can't cast object into {}, actual type: {}",
            std::any::type_name::<T>(),
            e.class().name()
        ))
    })
}

pub fn format_py_error(excep: PyBaseExceptionRef, vm: &VirtualMachine) -> error::Error {
    let mut msg = String::new();
    if let Err(e) = vm.write_exception(&mut msg, &excep) {
        return error::PyRuntimeSnafu {
            msg: format!("Failed to write exception msg, err: {e}"),
        }
        .build();
    }

    error::PyRuntimeSnafu { msg }.build()
}

pub(crate) fn py_obj_to_value(obj: &PyObjectRef, vm: &VirtualMachine) -> PyResult<Value> {
    macro_rules! obj2val {
        ($OBJ: ident, $($PY_TYPE: ident => $RS_TYPE: ident => $VARIANT: ident),*) => {
            $(
                if is_instance::<$PY_TYPE>($OBJ, vm) {
                    let val = $OBJ
                        .to_owned()
                        .try_into_value::<$RS_TYPE>(vm)?;
                    Ok(Value::$VARIANT(val.into()))
                }
            )else*
            else {
                Err(vm.new_runtime_error(format!("can't convert obj {obj:?} to Value")))
            }
        };
    }
    obj2val!(obj,
        PyBool => bool => Boolean,
        PyInt => i64 => Int64,
        PyFloat => f64 => Float64,
        PyStr => String => String
    )
}

/// convert a single PyVector or a number(a constant)(wrapping in PyObjectRef) into a Array(or a constant array)
pub fn py_obj_to_vec(
    obj: &PyObjectRef,
    vm: &VirtualMachine,
    col_len: usize,
) -> Result<VectorRef, error::Error> {
    // It's ugly, but we can't find a better way right now.
    if is_instance::<PyVector>(obj, vm) {
        let pyv = obj
            .payload::<PyVector>()
            .with_context(|| ret_other_error_with(format!("can't cast obj {obj:?} to PyVector")))?;
        Ok(pyv.as_vector_ref())
    } else if is_instance::<PyInt>(obj, vm) {
        let val = obj
            .clone()
            .try_into_value::<i64>(vm)
            .map_err(|e| format_py_error(e, vm))?;
        let ret = Int64Vector::from_iterator(std::iter::repeat(val).take(col_len));
        Ok(Arc::new(ret) as _)
    } else if is_instance::<PyFloat>(obj, vm) {
        let val = obj
            .clone()
            .try_into_value::<f64>(vm)
            .map_err(|e| format_py_error(e, vm))?;
        let ret = Float64Vector::from_iterator(std::iter::repeat(val).take(col_len));
        Ok(Arc::new(ret) as _)
    } else if is_instance::<PyBool>(obj, vm) {
        let val = obj
            .clone()
            .try_into_value::<bool>(vm)
            .map_err(|e| format_py_error(e, vm))?;

        let ret = BooleanVector::from_iterator(std::iter::repeat(val).take(col_len));
        Ok(Arc::new(ret) as _)
    } else if is_instance::<PyStr>(obj, vm) {
        let val = obj
            .clone()
            .try_into_value::<String>(vm)
            .map_err(|e| format_py_error(e, vm))?;

        let ret = StringVector::from_iterator(std::iter::repeat(val.as_str()).take(col_len));
        Ok(Arc::new(ret) as _)
    } else if is_instance::<PyList>(obj, vm) {
        let columnar_value =
            try_into_columnar_value(obj.clone(), vm).map_err(|e| format_py_error(e, vm))?;

        match columnar_value {
            DFColValue::Scalar(ScalarValue::List(scalars, _datatype)) => match scalars {
                Some(scalars) => {
                    let array =
                        ScalarValue::iter_to_array(scalars).context(error::DataFusionSnafu)?;

                    Helper::try_into_vector(array).context(error::TypeCastSnafu)
                }
                None => Ok(Arc::new(NullVector::new(0))),
            },
            _ => unreachable!(),
        }
    } else {
        ret_other_error_with(format!("Expect a vector or a constant, found {obj:?}")).fail()
    }
}
