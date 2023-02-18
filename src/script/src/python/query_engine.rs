use std::sync::{Arc, Weak};

use common_recordbatch::RecordBatches;
use query::parser::QueryLanguageParser;
use query::QueryEngine;
use rustpython_vm as vm;
use rustpython_vm::convert::ToPyObject;
use rustpython_vm::scope::Scope;
use rustpython_vm::AsObject;
use vm::builtins::{PyList, PyListRef};
use vm::{pyclass, PyPayload, PyResult, VirtualMachine};

use crate::python::error::Result;
use crate::python::utils::format_py_error;
use crate::python::PyVector;

#[derive(Clone)]
pub struct QueryEngineWeakRef(pub Weak<dyn QueryEngine>);

impl From<Weak<dyn QueryEngine>> for QueryEngineWeakRef {
    fn from(value: Weak<dyn QueryEngine>) -> Self {
        Self(value)
    }
}

impl From<&Arc<dyn QueryEngine>> for QueryEngineWeakRef {
    fn from(value: &Arc<dyn QueryEngine>) -> Self {
        Self(Arc::downgrade(value))
    }
}

impl std::fmt::Debug for QueryEngineWeakRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("QueryEngineWeakRef")
            .field(&self.0.upgrade().map(|f| f.name().to_string()))
            .finish()
    }
}

#[pyclass(module = false, name = "query_engine")]
#[derive(Debug, PyPayload)]
pub struct PyQueryEngine {
    pub inner: QueryEngineWeakRef,
}

#[pyclass]
impl PyQueryEngine {
    // TODO(discord9): find a better way to call sql query api, now we don't if we are in async contex or not
    /// return sql query results in List[List[PyVector]], or List[usize] for AffectedRows number if no recordbatches is returned
    #[pymethod]
    fn sql(&self, s: String, vm: &VirtualMachine) -> PyResult<PyListRef> {
        enum Either {
            Rb(RecordBatches),
            AffectedRows(usize),
        }
        let query = self.inner.0.upgrade();
        let thread_handle = std::thread::spawn(move || -> std::result::Result<_, String> {
            if let Some(engine) = query {
                let stmt = QueryLanguageParser::parse_sql(s.as_str()).map_err(|e| e.to_string())?;
                let plan = engine
                    .statement_to_plan(stmt, Default::default())
                    .map_err(|e| e.to_string())?;
                // To prevent the error of nested creating Runtime, if is nested, use the parent runtime instead

                let rt = tokio::runtime::Runtime::new().map_err(|e| e.to_string())?;
                let handle = rt.handle().clone();
                let res = handle.block_on(async {
                    let res = engine
                        .clone()
                        .execute(&plan)
                        .await
                        .map_err(|e| e.to_string());
                    match res {
                        Ok(common_query::Output::AffectedRows(cnt)) => {
                            Ok(Either::AffectedRows(cnt))
                        }
                        Ok(common_query::Output::RecordBatches(rbs)) => Ok(Either::Rb(rbs)),
                        Ok(common_query::Output::Stream(s)) => Ok(Either::Rb(
                            common_recordbatch::util::collect_batches(s).await.unwrap(),
                        )),
                        Err(e) => Err(e),
                    }
                })?;
                Ok(res)
            } else {
                Err("Query Engine is already dropped".to_string())
            }
        });
        thread_handle
            .join()
            .map_err(|e| {
                vm.new_system_error(format!("Dedicated thread for sql query panic: {e:?}"))
            })?
            .map_err(|e| vm.new_system_error(e))
            .map(|rbs| match rbs {
                Either::Rb(rbs) => {
                    let mut top_vec = Vec::with_capacity(rbs.iter().count());
                    for rb in rbs.iter() {
                        let mut vec_of_vec = Vec::with_capacity(rb.columns().len());
                        for v in rb.columns() {
                            let v = PyVector::from(v.clone());
                            vec_of_vec.push(v.to_pyobject(vm));
                        }
                        let vec_of_vec = PyList::new_ref(vec_of_vec, vm.as_ref()).to_pyobject(vm);
                        top_vec.push(vec_of_vec);
                    }
                    let top_vec = PyList::new_ref(top_vec, vm.as_ref());
                    top_vec
                }
                Either::AffectedRows(cnt) => {
                    PyList::new_ref(vec![vm.ctx.new_int(cnt).into()], vm.as_ref())
                }
            })
    }
}

pub fn set_query_engine_in_scope(
    scope: &Scope,
    vm: &VirtualMachine,
    query_engine: PyQueryEngine,
) -> Result<()> {
    scope
        .locals
        .as_object()
        .set_item("__query__", query_engine.to_pyobject(vm), vm)
        .map_err(|e| format_py_error(e, vm))
}
