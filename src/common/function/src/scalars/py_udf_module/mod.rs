//! Python udf module, that is udf function that you can use in 
//! python script(in Python Coprocessor more precisely)

use rustpython_vm::pymodule;

/// GrepTime User Define Function module
#[pymodule]
mod gt_udf {
    use crate::scalars::{python::PyVector, function::FunctionContext, Function};
    use rustpython_vm::PyRef;
    type PyVectorRef = PyRef<PyVector>;
    use crate::scalars::math::PowFunction;
    
    #[pyfunction]
    fn pow(base: PyVectorRef, pow: PyVectorRef) -> PyVector {
        let args = vec![base.as_vector_ref(), pow.as_vector_ref()];
        let res = PowFunction::default().eval(FunctionContext::default(), &args).unwrap();
        res.into()
    }
}