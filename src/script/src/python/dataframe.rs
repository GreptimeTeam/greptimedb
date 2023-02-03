use rustpython_vm::{
    pyclass as rspyclass, pymodule as rspymodule, PyPayload, PyRef, PyResult, VirtualMachine,
};
/// TODO: use given RecordBatch to register table in `SessionContext`
/// with `register_batch`, and then wrap DataFrame API in it
/// TODO: wrap DataFrame&Expr
#[rspymodule]
mod data_frame {
    use common_recordbatch::DfRecordBatch;
    use datafusion::dataframe::DataFrame as DfDataFrame;
    use datafusion_expr::Expr as DfExpr;
    use rustpython_vm::{
        pyclass as rspyclass, PyPayload, PyRef, PyResult,
    };
    #[rspyclass(module = "data_frame", name = "DataFrame")]
    #[derive(PyPayload, Debug)]
    pub struct PyDataFrame {
        pub inner: DfDataFrame,
    }

    impl PyDataFrame {
        /// TODO: error handling
        fn from_record_batch(rb: &DfRecordBatch) -> Self {
            let ctx = datafusion::execution::context::SessionContext::new();
            let inner = ctx.read_batch(rb.clone()).unwrap();
            Self { inner }
        }
    }

    #[rspyclass(module = "data_frame", name = "expr")]
    #[derive(PyPayload, Debug)]
    pub struct PyExpr {
        pub inner: DfExpr,
    }

    fn col(name: &str) -> PyExpr {
        DfExpr::Column(datafusion_common::Column::from_name(name)).into()
    }
    fn cols<L>(names: L)
    where
        L: AsRef<[String]>,
    {
    }
    fn lit(t: datafusion_common::ScalarValue) -> PyExpr {
        DfExpr::Literal(t).into()
    }

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
