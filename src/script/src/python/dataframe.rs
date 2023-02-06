use rustpython_vm::pymodule as rspymodule;
/// TODO: use given RecordBatch to register table in `SessionContext`
/// with `register_batch`, and then wrap DataFrame API in it
/// TODO: wrap DataFrame&Expr
#[rspymodule]
pub(crate) mod data_frame {
    use common_recordbatch::{DfRecordBatch, RecordBatch};
    use datafusion::dataframe::DataFrame as DfDataFrame;
    use datafusion_expr::Expr as DfExpr;
    use rustpython_vm::builtins::{PyList, PyListRef};
    use rustpython_vm::{pyclass as rspyclass, PyPayload, PyRef, PyResult, VirtualMachine};

    use crate::python::utils::block_on_async;
    #[rspyclass(module = "data_frame", name = "DataFrame")]
    #[derive(PyPayload, Debug)]
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
        let df = PyDataFrame::from_record_batch(rb.df_record_batch());
        scope
            .locals
            .set_item(name, vm.new_pyobj(df), vm)
            .map_err(|e| crate::python::utils::format_py_error(e, vm))
    }
    #[rspyclass]
    impl PyDataFrame {
        /// TODO: error handling
        fn from_record_batch(rb: &DfRecordBatch) -> Self {
            let ctx = datafusion::execution::context::SessionContext::new();
            let inner = ctx.read_batch(rb.clone()).unwrap();
            Self { inner }
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
                .select(expr_list.iter().map(|e| e.inner.to_owned()).collect())
                .map_err(|e| vm.new_runtime_error(e.to_string()))?
                .into())
        }

        #[pymethod]
        fn filter(&self, predicate: PyExprRef, vm: &VirtualMachine) -> PyResult<Self> {
            Ok(self
                .inner
                .clone()
                .filter(predicate.inner.to_owned())
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
            let ret = self.inner.to_owned().aggregate(
                group_expr.iter().map(|i| i.inner.to_owned()).collect(),
                aggr_expr.iter().map(|i| i.inner.to_owned()).collect(),
            );
            Ok(ret.map_err(|e| vm.new_runtime_error(e.to_string()))?.into())
        }

        #[pymethod]
        fn limit(&self, skip: usize, fetch: Option<usize>, vm: &VirtualMachine) -> PyResult<Self> {
            Ok(self
                .inner
                .to_owned()
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
        /// collect `DataFrame` results into List[List[Vector]]
        fn collect(&self, vm: &VirtualMachine) -> PyResult<PyListRef> {
            // let res = self.inner.collect().map_err(|e| vm.new_runtime_error(e.to_string()))?;
            let inner = self.inner.to_owned();
            let res = block_on_async(async { inner.collect().await });
            let res = res
                .map_err(|e| vm.new_runtime_error(format!("{e:?}")))?
                .map_err(|e| vm.new_runtime_error(e.to_string()))?;
            let outer_lst: Vec<_> = res
                .iter()
                .map(|elem| -> PyResult<_> {
                    let inner_lst: Vec<_> = elem
                        .columns()
                        .iter()
                        .map(|arr| -> PyResult<_> {
                            datatypes::vectors::Helper::try_into_vector(arr)
                                .map(crate::python::PyVector::from)
                                .map(|v| vm.new_pyobj(v))
                                .map_err(|e| vm.new_runtime_error(e.to_string()))
                        })
                        .collect::<Result<_, _>>()?;
                    let inner_lst = PyList::new_ref(inner_lst, vm.as_ref());
                    Ok(inner_lst.into())
                })
                .collect::<Result<_, _>>()?;
            Ok(PyList::new_ref(outer_lst, vm.as_ref()))
        }
    }

    #[rspyclass(module = "data_frame", name = "Expr")]
    #[derive(PyPayload, Debug)]
    pub struct PyExpr {
        pub inner: DfExpr,
    }

    #[pyfunction]
    fn col(name: String, vm: &VirtualMachine) -> PyExprRef {
        let expr: PyExpr = DfExpr::Column(datafusion_common::Column::from_name(name)).into();
        expr.into_ref(vm)
    }

    // TODO: lit function that take PyObject and turn it into ScalarValue

    type PyExprRef = PyRef<PyExpr>;

    impl From<datafusion_expr::Expr> for PyExpr {
        fn from(value: DfExpr) -> Self {
            Self { inner: value }
        }
    }

    #[rspyclass]
    impl PyExpr {
        #[pymethod]
        fn alias(&self, name: String) -> PyResult<PyExpr> {
            Ok(self.inner.clone().alias(name).into())
        }

        // bunch of binary op function(not use macro for macro expansion order doesn't support)
        #[pymethod(magic)]
        fn eq(&self, other: PyExprRef) -> PyResult<PyExpr> {
            Ok(self.inner.clone().eq(other.inner.clone()).into())
        }

        #[pymethod(magic)]
        fn ne(&self, other: PyExprRef) -> PyResult<PyExpr> {
            Ok(self.inner.clone().not_eq(other.inner.clone()).into())
        }

        #[pymethod(magic)]
        fn le(&self, other: PyExprRef) -> PyResult<PyExpr> {
            Ok(self.inner.clone().lt_eq(other.inner.clone()).into())
        }

        #[pymethod(magic)]
        fn lt(&self, other: PyExprRef) -> PyResult<PyExpr> {
            Ok(self.inner.clone().lt(other.inner.clone()).into())
        }

        #[pymethod(magic)]
        fn ge(&self, other: PyExprRef) -> PyResult<PyExpr> {
            Ok(self.inner.clone().gt_eq(other.inner.clone()).into())
        }

        #[pymethod(magic)]
        fn gt(&self, other: PyExprRef) -> PyResult<PyExpr> {
            Ok(self.inner.clone().gt(other.inner.clone()).into())
        }

        #[pymethod(magic)]
        fn and(&self, other: PyExprRef) -> PyResult<PyExpr> {
            Ok(self.inner.clone().and(other.inner.clone()).into())
        }
        #[pymethod(magic)]
        fn or(&self, other: PyExprRef) -> PyResult<PyExpr> {
            Ok(self.inner.clone().and(other.inner.clone()).into())
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
