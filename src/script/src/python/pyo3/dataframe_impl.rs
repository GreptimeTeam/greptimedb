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

use common_recordbatch::DfRecordBatch;
use datafusion::dataframe::DataFrame as DfDataFrame;
use datafusion_expr::Expr as DfExpr;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::pyclass::CompareOp;
use pyo3::types::PyList;
use snafu::ResultExt;

use crate::python::error::DataFusionSnafu;
use crate::python::ffi_types::PyVector;
use crate::python::pyo3::utils::pyo3_obj_try_to_typed_scalar_value;
use crate::python::utils::block_on_async;
type PyExprRef = Py<PyExpr>;

#[derive(Debug, Clone)]
#[pyclass]
pub(crate) struct PyDataFrame {
    inner: DfDataFrame,
}

impl From<DfDataFrame> for PyDataFrame {
    fn from(inner: DfDataFrame) -> Self {
        Self { inner }
    }
}

impl PyDataFrame {
    pub(crate) fn from_record_batch(rb: &DfRecordBatch) -> crate::python::error::Result<Self> {
        let ctx = datafusion::execution::context::SessionContext::new();
        let inner = ctx.read_batch(rb.clone()).context(DataFusionSnafu)?;
        Ok(Self { inner })
    }
}

#[pymethods]
impl PyDataFrame {
    fn __call__(&self) -> PyResult<Self> {
        Ok(self.clone())
    }
    fn select_columns(&self, columns: Vec<String>) -> PyResult<Self> {
        Ok(self
            .inner
            .clone()
            .select_columns(&columns.iter().map(AsRef::as_ref).collect::<Vec<&str>>())
            .map_err(|e| PyValueError::new_err(e.to_string()))?
            .into())
    }
    fn select(&self, py: Python<'_>, expr_list: Vec<PyExprRef>) -> PyResult<Self> {
        Ok(self
            .inner
            .clone()
            .select(
                expr_list
                    .iter()
                    .map(|e| e.borrow(py).inner.clone())
                    .collect(),
            )
            .map_err(|e| PyValueError::new_err(e.to_string()))?
            .into())
    }
    fn filter(&self, predicate: &PyExpr) -> PyResult<Self> {
        Ok(self
            .inner
            .clone()
            .filter(predicate.inner.clone())
            .map_err(|e| PyValueError::new_err(e.to_string()))?
            .into())
    }
    fn aggregate(
        &self,
        py: Python<'_>,
        group_expr: Vec<PyExprRef>,
        aggr_expr: Vec<PyExprRef>,
    ) -> PyResult<Self> {
        let ret = self.inner.clone().aggregate(
            group_expr
                .iter()
                .map(|i| i.borrow(py).inner.clone())
                .collect(),
            aggr_expr
                .iter()
                .map(|i| i.borrow(py).inner.clone())
                .collect(),
        );
        Ok(ret
            .map_err(|e| PyValueError::new_err(e.to_string()))?
            .into())
    }
    fn limit(&self, skip: usize, fetch: Option<usize>) -> PyResult<Self> {
        Ok(self
            .inner
            .clone()
            .limit(skip, fetch)
            .map_err(|e| PyValueError::new_err(e.to_string()))?
            .into())
    }
    fn union(&self, df: &PyDataFrame) -> PyResult<Self> {
        Ok(self
            .inner
            .clone()
            .union(df.inner.clone())
            .map_err(|e| PyValueError::new_err(e.to_string()))?
            .into())
    }
    fn union_distinct(&self, df: &PyDataFrame) -> PyResult<Self> {
        Ok(self
            .inner
            .clone()
            .union_distinct(df.inner.clone())
            .map_err(|e| PyValueError::new_err(e.to_string()))?
            .into())
    }
    fn distinct(&self) -> PyResult<Self> {
        Ok(self
            .inner
            .clone()
            .distinct()
            .map_err(|e| PyValueError::new_err(e.to_string()))?
            .into())
    }
    fn sort(&self, py: Python<'_>, expr: Vec<PyExprRef>) -> PyResult<Self> {
        Ok(self
            .inner
            .clone()
            .sort(expr.iter().map(|e| e.borrow(py).inner.clone()).collect())
            .map_err(|e| PyValueError::new_err(e.to_string()))?
            .into())
    }
    fn join(
        &self,
        py: Python<'_>,
        right: &PyDataFrame,
        join_type: String,
        left_cols: Vec<String>,
        right_cols: Vec<String>,
        filter: Option<PyExprRef>,
    ) -> PyResult<Self> {
        use datafusion::prelude::JoinType;
        let join_type = match join_type.as_str() {
            "inner" | "Inner" => JoinType::Inner,
            "left" | "Left" => JoinType::Left,
            "right" | "Right" => JoinType::Right,
            "full" | "Full" => JoinType::Full,
            "leftSemi" | "LeftSemi" => JoinType::LeftSemi,
            "rightSemi" | "RightSemi" => JoinType::RightSemi,
            "leftAnti" | "LeftAnti" => JoinType::LeftAnti,
            "rightAnti" | "RightAnti" => JoinType::RightAnti,
            _ => {
                return Err(PyValueError::new_err(format!(
                    "Unknown join type: {join_type}"
                )))
            }
        };
        let left_cols: Vec<&str> = left_cols.iter().map(AsRef::as_ref).collect();
        let right_cols: Vec<&str> = right_cols.iter().map(AsRef::as_ref).collect();
        let filter = filter.map(|f| f.borrow(py).inner.clone());
        Ok(self
            .inner
            .clone()
            .join(
                right.inner.clone(),
                join_type,
                &left_cols,
                &right_cols,
                filter,
            )
            .map_err(|e| PyValueError::new_err(e.to_string()))?
            .into())
    }
    fn intersect(&self, py: Python<'_>, df: &PyDataFrame) -> PyResult<Self> {
        py.allow_threads(|| {
            Ok(self
                .inner
                .clone()
                .intersect(df.inner.clone())
                .map_err(|e| PyValueError::new_err(e.to_string()))?
                .into())
        })
    }
    fn except(&self, df: &PyDataFrame) -> PyResult<Self> {
        Ok(self
            .inner
            .clone()
            .except(df.inner.clone())
            .map_err(|e| PyValueError::new_err(e.to_string()))?
            .into())
    }
    /// collect `DataFrame` results into `List[List[Vector]]`
    fn collect<'a>(&self, py: Python<'a>) -> PyResult<&'a PyList> {
        let inner = self.inner.clone();
        let res = block_on_async(async { inner.collect().await });
        let res = res
            .map_err(|e| PyValueError::new_err(format!("{e:?}")))?
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        let outer_list: Vec<PyObject> = res
            .iter()
            .map(|elem| -> PyResult<_> {
                let inner_list: Vec<_> = elem
                    .columns()
                    .iter()
                    .map(|arr| -> PyResult<_> {
                        datatypes::vectors::Helper::try_into_vector(arr)
                            .map(PyVector::from)
                            .map(|v| PyCell::new(py, v))
                            .map_err(|e| PyValueError::new_err(e.to_string()))
                            .and_then(|x| x)
                    })
                    .collect::<Result<_, _>>()?;
                let inner_list = PyList::new(py, inner_list);
                Ok(inner_list.into())
            })
            .collect::<Result<_, _>>()?;
        Ok(PyList::new(py, outer_list))
    }
}

/// Convert a Python Object into a `Expr` for use in constructing literal i.e. `col("number") < lit(42)`
#[pyfunction]
pub(crate) fn lit(py: Python<'_>, value: PyObject) -> PyResult<PyExpr> {
    let value = pyo3_obj_try_to_typed_scalar_value(value.as_ref(py), None)?;
    let expr: PyExpr = DfExpr::Literal(value).into();
    Ok(expr)
}

#[derive(Clone)]
#[pyclass]
pub(crate) struct PyExpr {
    inner: DfExpr,
}

impl From<datafusion_expr::Expr> for PyExpr {
    fn from(value: DfExpr) -> Self {
        Self { inner: value }
    }
}

#[pyfunction]
pub(crate) fn col(name: String) -> PyExpr {
    let expr: PyExpr = DfExpr::Column(datafusion_common::Column::from_name(name)).into();
    expr
}

#[pymethods]
impl PyExpr {
    fn __call__(&self) -> PyResult<Self> {
        Ok(self.clone())
    }
    fn __richcmp__(&self, py: Python<'_>, other: PyObject, op: CompareOp) -> PyResult<Self> {
        let other = other.extract::<Self>(py).or_else(|_| lit(py, other))?;
        let op = match op {
            CompareOp::Lt => DfExpr::lt,
            CompareOp::Le => DfExpr::lt_eq,
            CompareOp::Eq => DfExpr::eq,
            CompareOp::Ne => DfExpr::not_eq,
            CompareOp::Gt => DfExpr::gt,
            CompareOp::Ge => DfExpr::gt_eq,
        };
        py.allow_threads(|| Ok(op(self.inner.clone(), other.inner.clone()).into()))
    }
    fn alias(&self, name: String) -> PyResult<PyExpr> {
        Ok(self.inner.clone().alias(name).into())
    }
    fn __and__(&self, py: Python<'_>, other: PyExprRef) -> PyResult<PyExpr> {
        let other = other.borrow(py).inner.clone();
        py.allow_threads(|| Ok(self.inner.clone().and(other).into()))
    }
    fn __or__(&self, py: Python<'_>, other: PyExprRef) -> PyResult<PyExpr> {
        let other = other.borrow(py).inner.clone();
        py.allow_threads(|| Ok(self.inner.clone().or(other).into()))
    }
    fn __invert__(&self) -> PyResult<PyExpr> {
        Ok(self.inner.clone().not().into())
    }
    fn sort(&self, asc: bool, nulls_first: bool) -> PyExpr {
        self.inner.clone().sort(asc, nulls_first).into()
    }
    fn __repr__(&self) -> String {
        format!("{:#?}", &self.inner)
    }
}
