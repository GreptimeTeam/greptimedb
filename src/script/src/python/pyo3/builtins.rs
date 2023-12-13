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

use common_function::function::FunctionRef;
use common_function::function_registry::FUNCTION_REGISTRY;
use datafusion::arrow::array::{ArrayRef, NullArray};
use datafusion::physical_plan::expressions;
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::{math_expressions, AggregateExpr};
use datatypes::vectors::VectorRef;
use pyo3::exceptions::{PyKeyError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use super::dataframe_impl::PyDataFrame;
use super::utils::scalar_value_to_py_any;
use crate::python::ffi_types::copr::PyQueryEngine;
use crate::python::ffi_types::utils::all_to_f64;
use crate::python::ffi_types::PyVector;
use crate::python::pyo3::dataframe_impl::{col, lit};
use crate::python::pyo3::utils::{
    columnar_value_to_py_any, try_into_columnar_value, val_to_py_any,
};

/// Try to extract a `PyVector` or convert from a `pyarrow.array` object
#[inline]
fn try_into_py_vector(py: Python, obj: PyObject) -> PyResult<PyVector> {
    if let Ok(v) = obj.extract::<PyVector>(py) {
        Ok(v)
    } else {
        PyVector::from_pyarrow(obj.as_ref(py).get_type(), py, obj.clone())
    }
}

#[inline]
fn to_array_of_py_vec(py: Python, obj: &[&PyObject]) -> PyResult<Vec<PyVector>> {
    obj.iter()
        .map(|v| try_into_py_vector(py, v.to_object(py)))
        .collect::<PyResult<_>>()
}

macro_rules! batch_import {
    ($m: ident, [$($fn_name: ident),*]) => {
        $($m.add_function(wrap_pyfunction!($fn_name, $m)?)?;)*
    };
}

#[pymodule]
#[pyo3(name = "greptime")]
pub(crate) fn greptime_builtins(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyVector>()?;
    m.add_class::<PyDataFrame>()?;
    use self::query_engine;
    batch_import!(
        m,
        [
            dataframe,
            query_engine,
            lit,
            col,
            pow,
            clip,
            diff,
            mean,
            polyval,
            argmax,
            argmin,
            percentile,
            scipy_stats_norm_cdf,
            scipy_stats_norm_pdf,
            sqrt,
            sin,
            cos,
            tan,
            asin,
            acos,
            atan,
            floor,
            ceil,
            round,
            // trunc,
            abs,
            signum,
            exp,
            ln,
            log2,
            log10,
            random,
            approx_distinct,
            median,
            approx_percentile_cont,
            array_agg,
            avg,
            correlation,
            count,
            covariance,
            covariance_pop,
            max,
            min,
            stddev,
            stddev_pop,
            sum,
            variance,
            variance_pop
        ]
    );
    Ok(())
}

fn get_globals(py: Python) -> PyResult<&PyDict> {
    // TODO(discord9): check if this is sound(in python)
    let py_main = PyModule::import(py, "__main__")?;
    let globals = py_main.dict();
    Ok(globals)
}

/// In case of not wanting to repeat the same sql statement in sql,
/// this function is still useful even we already have PyDataFrame.from_sql()
#[pyfunction]
fn dataframe(py: Python) -> PyResult<PyDataFrame> {
    let globals = get_globals(py)?;
    let df = globals
        .get_item("__dataframe__")
        .ok_or_else(|| PyKeyError::new_err("No __dataframe__ variable is found"))?
        .extract::<PyDataFrame>()?;
    Ok(df)
}

#[pyfunction]
#[pyo3(name = "query")]
pub(crate) fn query_engine(py: Python) -> PyResult<PyQueryEngine> {
    let globals = get_globals(py)?;
    let query = globals
        .get_item("__query__")
        .ok_or_else(|| PyKeyError::new_err("No __query__ variable is found"))?
        .extract::<PyQueryEngine>()?;
    Ok(query)
}

fn eval_func(py: Python<'_>, name: &str, v: &[&PyObject]) -> PyResult<PyVector> {
    let v = to_array_of_py_vec(py, v)?;
    py.allow_threads(|| {
        let v: Vec<VectorRef> = v.iter().map(|v| v.as_vector_ref()).collect();
        let func: Option<FunctionRef> = FUNCTION_REGISTRY.get_function(name);
        let res = match func {
            Some(f) => f.eval(Default::default(), &v),
            None => return Err(PyValueError::new_err(format!("Can't find function {name}"))),
        };
        match res {
            Ok(v) => Ok(v.into()),
            Err(err) => Err(PyValueError::new_err(format!(
                "Fail to evaluate the function,: {err}"
            ))),
        }
    })
}

fn eval_aggr_func(py: Python<'_>, name: &str, args: &[&PyVector]) -> PyResult<PyObject> {
    let res = py.allow_threads(|| {
        let v: Vec<VectorRef> = args.iter().map(|v| v.as_vector_ref()).collect();
        let func = FUNCTION_REGISTRY.get_aggr_function(name);
        let f = match func {
            Some(f) => f.create().creator(),
            None => return Err(PyValueError::new_err(format!("Can't find function {name}"))),
        };
        let types: Vec<_> = v.iter().map(|v| v.data_type()).collect();
        let acc = f(&types);
        let mut acc = match acc {
            Ok(acc) => acc,
            Err(err) => {
                return Err(PyValueError::new_err(format!(
                    "Failed to create accumulator: {err}"
                )))
            }
        };
        match acc.update_batch(&v) {
            Ok(_) => (),
            Err(err) => {
                return Err(PyValueError::new_err(format!(
                    "Failed to update batch: {err}"
                )))
            }
        };
        let res = match acc.evaluate() {
            Ok(r) => r,
            Err(err) => {
                return Err(PyValueError::new_err(format!(
                    "Failed to evaluate accumulator: {err}"
                )))
            }
        };
        Ok(res)
    })?;
    val_to_py_any(py, res)
}

/// evaluate Aggregate Expr using its backing accumulator
/// TODO(discord9): cast to f64 before use/Provide cast to f64 function?
fn eval_df_aggr_expr<T: AggregateExpr>(
    py: Python<'_>,
    aggr: T,
    values: &[ArrayRef],
) -> PyResult<PyObject> {
    let res = py.allow_threads(|| -> PyResult<_> {
        // acquire the accumulator, where the actual implement of aggregate expr layers
        let mut acc = aggr
            .create_accumulator()
            .map_err(|e| PyValueError::new_err(format!("{e:?}")))?;
        acc.update_batch(values)
            .map_err(|e| PyValueError::new_err(format!("{e:?}")))?;
        let res = acc
            .evaluate()
            .map_err(|e| PyValueError::new_err(format!("{e:?}")))?;
        Ok(res)
    })?;
    scalar_value_to_py_any(py, res)
}

/// use to bind to Data Fusion's UDF function
macro_rules! bind_call_unary_math_function {
    ($($DF_FUNC: ident),*) => {
    $(
        #[pyfunction]
        fn $DF_FUNC(py: Python<'_>, val: PyObject) -> PyResult<PyObject> {
            let args =
                &[all_to_f64(try_into_columnar_value(py, val)?).map_err(PyValueError::new_err)?];
            let res = math_expressions::$DF_FUNC(args).map_err(|e| PyValueError::new_err(format!("{e:?}")))?;
            columnar_value_to_py_any(py, res)
        }
    )*
    };
}

macro_rules! simple_vector_fn {
    ($name: ident, $name_str: tt, [$($arg:ident),*]) => {
        #[pyfunction]
        fn $name(py: Python<'_>, $($arg: PyObject),*) -> PyResult<PyVector> {
            eval_func(py, $name_str, &[$(&$arg),*])
        }
    };
    ($name: ident, $name_str: tt, AGG[$($arg:ident),*]) => {
        #[pyfunction]
        fn $name(py: Python<'_>, $($arg: &PyVector),*) -> PyResult<PyObject> {
            eval_aggr_func(py, $name_str, &[$($arg),*])
        }
    };
}

// TODO(discord9): More Aggr functions& allow threads
simple_vector_fn!(pow, "pow", [v0, v1]);
simple_vector_fn!(clip, "clip", [v0, v1, v2]);
simple_vector_fn!(diff, "diff", AGG[v0]);
simple_vector_fn!(mean, "mean", AGG[v0]);
simple_vector_fn!(polyval, "polyval", AGG[v0, v1]);
simple_vector_fn!(argmax, "argmax", AGG[v0]);
simple_vector_fn!(argmin, "argmin", AGG[v0]);
simple_vector_fn!(percentile, "percentile", AGG[v0, v1]);
simple_vector_fn!(scipy_stats_norm_cdf, "scipystatsnormcdf", AGG[v0, v1]);
simple_vector_fn!(scipy_stats_norm_pdf, "scipystatsnormpdf", AGG[v0, v1]);

/*
This macro basically expand to this code below:
```rust
fn sqrt(py: Python<'_>, val: PyObject) -> PyResult<PyObject> {
    let args = &[all_to_f64(try_into_columnar_value(py, val)?).map_err(PyValueError::new_err)?];
    let res = math_expressions::sqrt(args).map_err(|e| PyValueError::new_err(format!("{e:?}")))?;
    columnar_value_to_py_any(py, res)
}
```
*/
bind_call_unary_math_function!(
    sqrt, sin, cos, tan, asin, acos, atan, floor, ceil, abs, signum, exp, ln, log2,
    log10 // trunc,
);

/// return a random vector range from 0 to 1 and length of len
#[pyfunction]
fn random(py: Python<'_>, len: usize) -> PyResult<PyObject> {
    // This is in a proc macro so using full path to avoid strange things
    // more info at: https://doc.rust-lang.org/reference/procedural-macros.html#procedural-macro-hygiene
    let arg = NullArray::new(len);
    let args = &[ColumnarValue::Array(std::sync::Arc::new(arg) as _)];
    let res =
        math_expressions::random(args).map_err(|e| PyValueError::new_err(format!("{e:?}")))?;

    columnar_value_to_py_any(py, res)
}

#[pyfunction]
fn round(py: Python<'_>, val: PyObject) -> PyResult<PyObject> {
    let value = try_into_columnar_value(py, val)?;
    let array = value.into_array(1);
    let result =
        math_expressions::round(&[array]).map_err(|e| PyValueError::new_err(format!("{e:?}")))?;
    columnar_value_to_py_any(py, ColumnarValue::Array(result))
}

/// The macro for binding function in `datafusion_physical_expr::expressions`(most of them are aggregate function)
macro_rules! bind_aggr_expr {
    ($FUNC_NAME:ident, $AGGR_FUNC: ident, [$($ARG: ident),*], $ARG_TY: ident, $($EXPR:ident => $idx: literal),*) => {
        #[pyfunction]
        fn $FUNC_NAME(py: Python<'_>, $($ARG: &PyVector),*)->PyResult<PyObject>{
            // just a place holder, we just want the inner `XXXAccumulator`'s function
            // so its expr is irrelevant
            return eval_df_aggr_expr(
                py,
                expressions::$AGGR_FUNC::new(
                    $(
                        Arc::new(expressions::Column::new(stringify!($EXPR), $idx)) as _,
                    )*
                        stringify!($AGGR_FUNC),
                        $ARG_TY.arrow_data_type().to_owned()),
                &[$($ARG.to_arrow_array()),*]
            )
        }
    };
}
/*
`bind_aggr_expr!(approx_distinct, ApproxDistinct,[v0], v0, expr0=>0);`
expand into:
```
fn approx_distinct(py: Python<'_>, v0: &PyVector) -> PyResult<PyObject> {
    return eval_df_aggr_expr(
        py,
        expressions::ApproxDistinct::new(
            Arc::new(expressions::Column::new("expr0", 0)) as _,
            "ApproxDistinct",
            v0.arrow_data_type().to_owned(),
        ),
        &[v0.to_arrow_array()],
    );
}
```

 */
bind_aggr_expr!(approx_distinct, ApproxDistinct,[v0], v0, expr0=>0);

bind_aggr_expr!(median, Median,[v0], v0, expr0=>0);

#[pyfunction]
fn approx_percentile_cont(py: Python<'_>, values: &PyVector, percent: f64) -> PyResult<PyObject> {
    let percent = expressions::Literal::new(datafusion_common::ScalarValue::Float64(Some(percent)));
    eval_df_aggr_expr(
        py,
        expressions::ApproxPercentileCont::new(
            vec![
                Arc::new(expressions::Column::new("expr0", 0)) as _,
                Arc::new(percent) as _,
            ],
            "ApproxPercentileCont",
            values.arrow_data_type(),
        )
        .map_err(|e| PyValueError::new_err(format!("{e:?}")))?,
        &[values.to_arrow_array()],
    )
}

bind_aggr_expr!(array_agg, ArrayAgg,[v0], v0, expr0=>0);

bind_aggr_expr!(avg, Avg,[v0], v0, expr0=>0);

bind_aggr_expr!(correlation, Correlation,[v0, v1], v0, expr0=>0, expr1=>1);

bind_aggr_expr!(count, Count,[v0], v0, expr0=>0);

bind_aggr_expr!(covariance, Covariance,[v0, v1], v0, expr0=>0, expr1=>1);

bind_aggr_expr!(covariance_pop, CovariancePop,[v0, v1], v0, expr0=>0, expr1=>1);

bind_aggr_expr!(max, Max,[v0], v0, expr0=>0);

bind_aggr_expr!(min, Min,[v0], v0, expr0=>0);

bind_aggr_expr!(stddev, Stddev,[v0], v0, expr0=>0);

bind_aggr_expr!(stddev_pop, StddevPop,[v0], v0, expr0=>0);

bind_aggr_expr!(sum, Sum,[v0], v0, expr0=>0);

bind_aggr_expr!(variance, Variance,[v0], v0, expr0=>0);

bind_aggr_expr!(variance_pop, VariancePop,[v0], v0, expr0=>0);
