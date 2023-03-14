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

use rustpython_vm::class::PyClassImpl;
use rustpython_vm::{pymodule as rspymodule, VirtualMachine};
pub(crate) fn init_data_frame(module_name: &str, vm: &mut VirtualMachine) {
    data_frame::PyDataFrame::make_class(&vm.ctx);
    data_frame::PyExpr::make_class(&vm.ctx);
    vm.add_native_module(module_name.to_owned(), Box::new(data_frame::make_module));
}
/// with `register_batch`, and then wrap DataFrame API in it
#[rspymodule]
pub(crate) mod data_frame {
    use common_recordbatch::{DfRecordBatch, RecordBatch};
    use datafusion::dataframe::DataFrame as DfDataFrame;
    use datafusion_expr::Expr as DfExpr;
    use rustpython_vm::builtins::{PyList, PyListRef};
    use rustpython_vm::function::PyComparisonValue;
    use rustpython_vm::types::{Comparable, PyComparisonOp};
    use rustpython_vm::{
        pyclass as rspyclass, PyObject, PyObjectRef, PyPayload, PyRef, PyResult, VirtualMachine,
    };
    use snafu::ResultExt;

    use crate::python::error::DataFusionSnafu;
    use crate::python::ffi_types::PyVector;
    use crate::python::rspython::builtins::greptime_builtin::lit;
    use crate::python::utils::block_on_async;
    #[rspyclass(module = "data_frame", name = "DataFrame")]
    #[derive(PyPayload, Debug, Clone)]
    pub struct PyDataFrame {
        pub inner: DfDataFrame,
    }

    impl From<DfDataFrame> for PyDataFrame {
        fn from(inner: DfDataFrame) -> Self {
            Self { inner }
        }
    }
    /// set DataFrame instance into current scope with given name
    pub fn set_dataframe_in_scope(
        scope: &rustpython_vm::scope::Scope,
        vm: &VirtualMachine,
        name: &str,
        rb: &RecordBatch,
    ) -> crate::python::error::Result<()> {
        let df = PyDataFrame::from_record_batch(rb.df_record_batch())?;
        scope
            .locals
            .set_item(name, vm.new_pyobj(df), vm)
            .map_err(|e| crate::python::utils::format_py_error(e, vm))
    }
    #[rspyclass]
    impl PyDataFrame {
        /// TODO(discord9): error handling
        fn from_record_batch(rb: &DfRecordBatch) -> crate::python::error::Result<Self> {
            let ctx = datafusion::execution::context::SessionContext::new();
            let inner = ctx.read_batch(rb.clone()).context(DataFusionSnafu)?;
            Ok(Self { inner })
        }

        #[pymethod]
        fn select_columns(&self, columns: Vec<String>, vm: &VirtualMachine) -> PyResult<Self> {
            Ok(self
                .inner
                .clone()
                .select_columns(&columns.iter().map(AsRef::as_ref).collect::<Vec<&str>>())
                .map_err(|e| vm.new_runtime_error(e.to_string()))?
                .into())
        }

        #[pymethod]
        fn select(&self, expr_list: Vec<PyExprRef>, vm: &VirtualMachine) -> PyResult<Self> {
            Ok(self
                .inner
                .clone()
                .select(expr_list.iter().map(|e| e.inner.clone()).collect())
                .map_err(|e| vm.new_runtime_error(e.to_string()))?
                .into())
        }

        #[pymethod]
        fn filter(&self, predicate: PyExprRef, vm: &VirtualMachine) -> PyResult<Self> {
            Ok(self
                .inner
                .clone()
                .filter(predicate.inner.clone())
                .map_err(|e| vm.new_runtime_error(e.to_string()))?
                .into())
        }

        #[pymethod]
        fn aggregate(
            &self,
            group_expr: Vec<PyExprRef>,
            aggr_expr: Vec<PyExprRef>,
            vm: &VirtualMachine,
        ) -> PyResult<Self> {
            let ret = self.inner.clone().aggregate(
                group_expr.iter().map(|i| i.inner.clone()).collect(),
                aggr_expr.iter().map(|i| i.inner.clone()).collect(),
            );
            Ok(ret.map_err(|e| vm.new_runtime_error(e.to_string()))?.into())
        }

        #[pymethod]
        fn limit(&self, skip: usize, fetch: Option<usize>, vm: &VirtualMachine) -> PyResult<Self> {
            Ok(self
                .inner
                .clone()
                .limit(skip, fetch)
                .map_err(|e| vm.new_runtime_error(e.to_string()))?
                .into())
        }

        #[pymethod]
        fn union(&self, df: PyRef<PyDataFrame>, vm: &VirtualMachine) -> PyResult<Self> {
            Ok(self
                .inner
                .clone()
                .union(df.inner.clone())
                .map_err(|e| vm.new_runtime_error(e.to_string()))?
                .into())
        }

        #[pymethod]
        fn union_distinct(&self, df: PyRef<PyDataFrame>, vm: &VirtualMachine) -> PyResult<Self> {
            Ok(self
                .inner
                .clone()
                .union_distinct(df.inner.clone())
                .map_err(|e| vm.new_runtime_error(e.to_string()))?
                .into())
        }

        #[pymethod]
        fn distinct(&self, vm: &VirtualMachine) -> PyResult<Self> {
            Ok(self
                .inner
                .clone()
                .distinct()
                .map_err(|e| vm.new_runtime_error(e.to_string()))?
                .into())
        }

        #[pymethod]
        fn sort(&self, expr: Vec<PyExprRef>, vm: &VirtualMachine) -> PyResult<Self> {
            Ok(self
                .inner
                .clone()
                .sort(expr.iter().map(|e| e.inner.clone()).collect())
                .map_err(|e| vm.new_runtime_error(e.to_string()))?
                .into())
        }

        #[pymethod]
        fn join(
            &self,
            right: PyRef<PyDataFrame>,
            join_type: String,
            left_cols: Vec<String>,
            right_cols: Vec<String>,
            filter: Option<PyExprRef>,
            vm: &VirtualMachine,
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
                _ => return Err(vm.new_runtime_error(format!("Unknown join type: {join_type}"))),
            };
            let left_cols: Vec<&str> = left_cols.iter().map(AsRef::as_ref).collect();
            let right_cols: Vec<&str> = right_cols.iter().map(AsRef::as_ref).collect();
            let filter = filter.map(|f| f.inner.clone());
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
                .map_err(|e| vm.new_runtime_error(e.to_string()))?
                .into())
        }

        #[pymethod]
        fn intersect(&self, df: PyRef<PyDataFrame>, vm: &VirtualMachine) -> PyResult<Self> {
            Ok(self
                .inner
                .clone()
                .intersect(df.inner.clone())
                .map_err(|e| vm.new_runtime_error(e.to_string()))?
                .into())
        }

        #[pymethod]
        fn except(&self, df: PyRef<PyDataFrame>, vm: &VirtualMachine) -> PyResult<Self> {
            Ok(self
                .inner
                .clone()
                .except(df.inner.clone())
                .map_err(|e| vm.new_runtime_error(e.to_string()))?
                .into())
        }

        #[pymethod]
        /// collect `DataFrame` results into `List[List[Vector]]`
        fn collect(&self, vm: &VirtualMachine) -> PyResult<PyListRef> {
            let inner = self.inner.clone();
            let res = block_on_async(async { inner.collect().await });
            let res = res
                .map_err(|e| vm.new_runtime_error(format!("{e:?}")))?
                .map_err(|e| vm.new_runtime_error(e.to_string()))?;
            let outer_list: Vec<_> = res
                .iter()
                .map(|elem| -> PyResult<_> {
                    let inner_list: Vec<_> = elem
                        .columns()
                        .iter()
                        .map(|arr| -> PyResult<_> {
                            datatypes::vectors::Helper::try_into_vector(arr)
                                .map(PyVector::from)
                                .map(|v| vm.new_pyobj(v))
                                .map_err(|e| vm.new_runtime_error(e.to_string()))
                        })
                        .collect::<Result<_, _>>()?;
                    let inner_list = PyList::new_ref(inner_list, vm.as_ref());
                    Ok(inner_list.into())
                })
                .collect::<Result<_, _>>()?;
            Ok(PyList::new_ref(outer_list, vm.as_ref()))
        }
    }

    #[rspyclass(module = "data_frame", name = "PyExpr")]
    #[derive(PyPayload, Debug, Clone)]
    pub struct PyExpr {
        pub inner: DfExpr,
    }

    // TODO(discord9): lit function that take PyObject and turn it into ScalarValue

    pub(crate) type PyExprRef = PyRef<PyExpr>;

    impl From<datafusion_expr::Expr> for PyExpr {
        fn from(value: DfExpr) -> Self {
            Self { inner: value }
        }
    }

    impl Comparable for PyExpr {
        fn slot_richcompare(
            zelf: &PyObject,
            other: &PyObject,
            op: PyComparisonOp,
            vm: &VirtualMachine,
        ) -> PyResult<rustpython_vm::function::Either<PyObjectRef, PyComparisonValue>> {
            if let Some(zelf) = zelf.downcast_ref::<Self>() {
                let ret = zelf.richcompare(other.to_owned(), op, vm)?;
                let ret = ret.into_pyobject(vm);
                Ok(rustpython_vm::function::Either::A(ret))
            } else {
                Err(vm.new_type_error(format!(
                    "unexpected payload {zelf:?} and {other:?} for op {}",
                    op.method_name(&vm.ctx).as_str()
                )))
            }
        }
        fn cmp(
            _zelf: &rustpython_vm::Py<Self>,
            _other: &PyObject,
            _op: PyComparisonOp,
            _vm: &VirtualMachine,
        ) -> PyResult<PyComparisonValue> {
            Ok(PyComparisonValue::NotImplemented)
        }
    }

    #[rspyclass(with(Comparable))]
    impl PyExpr {
        fn richcompare(
            &self,
            other: PyObjectRef,
            op: PyComparisonOp,
            vm: &VirtualMachine,
        ) -> PyResult<Self> {
            let other = if let Some(other) = other.downcast_ref::<Self>() {
                other.to_owned()
            } else {
                lit(other, vm)?
            };
            let f = match op {
                PyComparisonOp::Eq => DfExpr::eq,
                PyComparisonOp::Ne => DfExpr::not_eq,
                PyComparisonOp::Gt => DfExpr::gt,
                PyComparisonOp::Lt => DfExpr::lt,
                PyComparisonOp::Ge => DfExpr::gt_eq,
                PyComparisonOp::Le => DfExpr::lt_eq,
            };
            Ok(f(self.inner.clone(), other.inner.clone()).into())
        }
        #[pymethod]
        fn alias(&self, name: String) -> PyResult<PyExpr> {
            Ok(self.inner.clone().alias(name).into())
        }

        #[pymethod(magic)]
        fn and(&self, other: PyExprRef) -> PyResult<PyExpr> {
            Ok(self.inner.clone().and(other.inner.clone()).into())
        }
        #[pymethod(magic)]
        fn or(&self, other: PyExprRef) -> PyResult<PyExpr> {
            Ok(self.inner.clone().or(other.inner.clone()).into())
        }

        /// `~` operator, return `!self`
        #[pymethod(magic)]
        fn invert(&self) -> PyResult<PyExpr> {
            Ok(self.inner.clone().not().into())
        }

        /// sort ascending&nulls_first
        #[pymethod]
        fn sort(&self) -> PyExpr {
            self.inner.clone().sort(true, true).into()
        }
    }
}
