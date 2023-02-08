use datatypes::prelude::ConcreteDataType;
use datatypes::value::{self, OrderedFloat};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyFloat, PyInt, PyList};

use crate::ffi_types::vector::PyVector;

/// TODO(discord9): add similar methods
#[pymethods]
impl PyVector {
    #[new]
    fn py_new(iterable: &PyList) -> PyResult<Self> {
        todo!()
    }
    fn __add__(&self, other: PyObject) -> PyResult<Self> {
        todo!()
    }
    fn __radd__(&self, other: PyObject) -> PyResult<Self> {
        self.__add__(other)
    }

    fn __sub__(&self, other: PyObject) -> PyResult<Self> {
        todo!()
    }
    fn __rsub__(&self, other: PyObject) -> PyResult<Self> {
        todo!()
    }
    fn __mul__(&self, other: PyObject) -> PyResult<Self> {
        todo!()
    }
    fn __truediv__(&self, other: PyObject) -> PyResult<Self> {
        todo!()
    }
    fn __rtruediv__(&self, other: PyObject) -> PyResult<Self> {
        todo!()
    }
    fn __floordiv__(&self, other: PyObject) -> PyResult<Self> {
        todo!()
    }
    fn __rfloordiv__(&self, other: PyObject) -> PyResult<Self> {
        todo!()
    }
    fn __and__(&self, other: PyObject) -> PyResult<Self> {
        todo!()
    }
    fn __or__(&self, other: PyObject) -> PyResult<Self> {
        todo!()
    }
    fn __invert__(&self) -> PyResult<Self> {
        todo!()
    }
    fn __doc__(&self, other: PyObject) -> PyResult<String> {
        Ok("PyVector is like a Python array, a compact array of elem of same datatype, but Readonly for now".to_string())
    }
}

macro_rules! to_con_type {
    ($dtype:ident,$obj:ident, $($cty:ident => $rty:ty),*$(,)?) => {
        match $dtype {
            $(
                ConcreteDataType::$cty(_) => $obj.extract::<$rty>().map(value::Value::$cty),
            )*
            _ => unreachable!(),
        }
    };
    ($dtype:ident,$obj:ident, $($cty:ident =ord=> $rty:ty),*$(,)?) => {
        match $dtype {
            $(
                ConcreteDataType::$cty(_) => $obj.extract::<$rty>().map(OrderedFloat).map(value::Value::$cty),
            )*
            _ => unreachable!(),
        }
    };
}

/// to int/float/boolean, if dtype is None, then convert to highest prec type
fn py_obj_try_to_typed_val(obj: &PyAny, dtype: Option<ConcreteDataType>) -> PyResult<value::Value> {
    if let Ok(num) = obj.downcast::<PyInt>() {
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
                todo!()
            }
        } else {
            num.extract::<i64>().map(value::Value::Int64)
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
                // return error
                todo!()
            }
        } else {
            num.extract::<f64>()
                .map(|v| value::Value::Float64(OrderedFloat(v)))
        }
    } else if let Ok(b) = obj.downcast::<PyBool>() {
        todo!()
    } else {
        todo!()
    }
}
