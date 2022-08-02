//! Python udf module, that is udf function that you can use in
//! python script(in Python Coprocessor more precisely)

use rustpython_vm::pymodule;

/// GrepTime User Define Function module
///  
/// design to allow Python Coprocessor Function to use already implmented udf functions
#[pymodule]
mod udf_builtins {
    use datafusion_common::ScalarValue;
    use datafusion_expr::ColumnarValue;
    use rustpython_vm::{
        builtins::{PyBaseExceptionRef, PyBool, PyFloat, PyInt},
        AsObject, PyObjectRef, PyPayload, PyRef, PyResult, VirtualMachine,
    };

    use crate::scalars::math::PowFunction;
    use crate::scalars::{function::FunctionContext, python::PyVector, Function};
    type PyVectorRef = PyRef<PyVector>;
    /// use `rustpython`'s `is_instance` method to check if a PyObject is a instance of class.
    /// if `PyResult` is Err, then this function return `false`
    pub fn is_instance<T: PyPayload>(obj: &PyObjectRef, vm: &VirtualMachine) -> bool {
        obj.is_instance(T::class(vm).into(), vm).unwrap_or(false)
    }

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
    fn try_into_columnar_value(obj: PyObjectRef, vm: &VirtualMachine) -> PyResult<ColumnarValue> {
        if is_instance::<PyVector>(&obj, vm) {
            let ret = obj
                .payload::<PyVector>()
                .ok_or_else(|| type_cast_error(&obj.class().name(), "vector", vm))?;
            Ok(ColumnarValue::Array(ret.to_arrow_array()))
        } else if is_instance::<PyInt>(&obj, vm) {
            let ret = obj.try_into_value::<i64>(vm)?;
            Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(ret))))
        } else if is_instance::<PyFloat>(&obj, vm) {
            let ret = obj.try_into_value::<f64>(vm)?;
            Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(ret))))
        } else if is_instance::<PyBool>(&obj, vm) {
            let ret = obj.try_into_value::<bool>(vm)?;
            Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(ret))))
        } else {
            Err(vm.new_type_error(format!(
                "Can't cast object of type {} into vector or scalar",
                obj.class().name()
            )))
        }
    }

    /// Pow function,
    /// TODO: use PyObjectRef to adopt more type
    #[pyfunction]
    fn pow(base: PyObjectRef, pow: PyVectorRef, vm: &VirtualMachine) -> PyResult<PyVector> {
        let res = try_into_columnar_value(base.clone(), vm);
        if let Ok(res) = res {
            match res {
                ColumnarValue::Array(arr) => {
                    dbg!(&arr);
                }
                ColumnarValue::Scalar(val) => {
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
    #[cfg(test)]
    #[allow(clippy::print_stdout)]
    mod test {
        use std::sync::Arc;

        use datatypes::vectors::VectorRef;
        use rustpython_vm::class::PyClassImpl;
        use rustpython_vm::{scope::Scope, AsObject, VirtualMachine};

        use super::*;
        fn set_items_in_scope(
            scope: &Scope,
            vm: &VirtualMachine,
            arg_names: &[&str],
            args: Vec<PyVector>,
        ) -> Result<(), String> {
            let res = arg_names.iter().zip(args).try_for_each(|(name, vector)| {
                scope
                    .locals
                    .as_object()
                    .set_item(name.to_owned(), vm.new_pyobj(vector), vm)
                    .map_err(|err| {
                        format!(
                            "Error in setting var {name} in scope: \n{}",
                            to_serde_excep(err, vm).unwrap_or_else(|double_err| format!(
                                "Another exception occur during serialize exception to string:\n{double_err}"
                            ))
                        )
                    })
            });
            res
        }

        #[test]
        fn test_vm() {
            rustpython_vm::Interpreter::with_init(Default::default(), |vm| {
                vm.add_native_module("udf_builtins", Box::new(super::make_module));
                // this can be in `.enter()` closure, but for clearity, put it in the `with_init()`
                PyVector::make_class(&vm.ctx);
            })
            .enter(|vm| {
                let values = vec![1.0, 2.0, 3.0];
                let pows = vec![0i8, -1i8, 3i8];

                let args: Vec<VectorRef> = vec![
                    Arc::new(datatypes::vectors::Float32Vector::from_vec(values)),
                    Arc::new(datatypes::vectors::Int8Vector::from_vec(pows)),
                ];
                let args: Vec<PyVector> = args.into_iter().map(PyVector::from).collect();

                let scope = vm.new_scope_with_builtins();
                set_items_in_scope(&scope, vm, &["values", "pows"], args).unwrap();
                let code_obj = vm
                    .compile(
                        "
from udf_mod import pow
pow(values, pows)",
                        rustpython_vm::compile::Mode::BlockExpr,
                        "<embedded>".to_owned(),
                    )
                    .map_err(|err| vm.new_syntax_error(&err))
                    .unwrap();
                let res = vm.run_code_obj(code_obj, scope);
                println!("{:#?}", res);
                if let Err(e) = res {
                    let err_res = to_serde_excep(e, vm).unwrap();
                    println!("Error:\n{err_res}");
                }
            });
        }
        fn to_serde_excep(
            excep: rustpython_vm::builtins::PyBaseExceptionRef,
            vm: &VirtualMachine,
        ) -> Result<String, String> {
            let mut chain = String::new();
            let r = vm.write_exception(&mut chain, &excep);
            // FIXME: better error handling, perhaps with chain calls?
            if let Err(r) = r {
                return Err(format!("Fail to write to string, error: {:#?}", r));
            }
            Ok(chain)
        }
    }
}
