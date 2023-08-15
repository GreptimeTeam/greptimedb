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

pub mod compile;
pub mod parse;

use std::collections::HashMap;
use std::result::Result as StdResult;
use std::sync::{Arc, Weak};

use common_recordbatch::{RecordBatch, RecordBatches};
use datatypes::arrow::compute;
use datatypes::data_type::{ConcreteDataType, DataType};
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use datatypes::vectors::{Helper, VectorRef};
// use crate::python::builtins::greptime_builtin;
use parse::DecoratorArgs;
#[cfg(feature = "pyo3_backend")]
use pyo3::pyclass as pyo3class;
use query::parser::QueryLanguageParser;
use query::QueryEngine;
use rustpython_compiler_core::CodeObject;
use rustpython_vm as vm;
#[cfg(test)]
use serde::Deserialize;
use session::context::QueryContextBuilder;
use snafu::{OptionExt, ResultExt};
use vm::convert::ToPyObject;
use vm::{pyclass as rspyclass, PyObjectRef, PyPayload, PyResult, VirtualMachine};

use super::py_recordbatch::PyRecordBatch;
use crate::python::error::{ensure, ArrowSnafu, OtherSnafu, Result, TypeCastSnafu};
use crate::python::ffi_types::PyVector;
#[cfg(feature = "pyo3_backend")]
use crate::python::pyo3::pyo3_exec_parsed;
use crate::python::rspython::rspy_exec_parsed;

#[cfg_attr(test, derive(Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnnotationInfo {
    /// if None, use types inferred by PyVector
    // TODO(yingwen): We should use our data type. i.e. ConcreteDataType.
    pub datatype: Option<ConcreteDataType>,
    pub is_nullable: bool,
}

#[cfg_attr(test, derive(Deserialize))]
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub enum BackendType {
    #[default]
    RustPython,
    // TODO(discord9): intergral test
    #[allow(unused)]
    CPython,
}

pub type CoprocessorRef = Arc<Coprocessor>;

#[cfg_attr(test, derive(Deserialize))]
#[derive(Debug, Clone)]
pub struct Coprocessor {
    pub name: String,
    pub deco_args: DecoratorArgs,
    /// get from python function args' annotation, first is type, second is is_nullable
    pub arg_types: Vec<Option<AnnotationInfo>>,
    /// get from python function returns' annotation, first is type, second is is_nullable
    pub return_types: Vec<Option<AnnotationInfo>>,
    /// kwargs in coprocessor function's signature
    pub kwarg: Option<String>,
    /// store its corresponding script, also skip serde when in `cfg(test)` to reduce work in compare
    #[cfg_attr(test, serde(skip))]
    pub script: String,
    // We must use option here, because we use `serde` to deserialize coprocessor
    // from ron file and `Deserialize` requires Coprocessor implementing `Default` trait,
    // but CodeObject doesn't.
    #[cfg_attr(test, serde(skip))]
    pub code_obj: Option<CodeObject>,
    #[cfg_attr(test, serde(skip))]
    pub query_engine: Option<QueryEngineWeakRef>,
    /// Use which backend to run this script
    /// Ideally in test both backend should be tested, so skip this
    #[cfg_attr(test, serde(skip))]
    pub backend: BackendType,
}

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

impl PartialEq for Coprocessor {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.deco_args == other.deco_args
            && self.arg_types == other.arg_types
            && self.return_types == other.return_types
            && self.script == other.script
    }
}

impl Eq for Coprocessor {}

impl Coprocessor {
    /// generate [`Schema`] according to return names, types,
    /// if no annotation
    /// the datatypes of the actual columns is used directly
    pub(crate) fn gen_schema(&self, cols: &[VectorRef]) -> Result<SchemaRef> {
        let names = &self.deco_args.ret_names;
        let anno = &self.return_types;
        ensure!(
            cols.len() == names.len() && names.len() == anno.len(),
            OtherSnafu {
                reason: format!(
                    "Unmatched length for cols({}), names({}) and annotation({})",
                    cols.len(),
                    names.len(),
                    anno.len()
                )
            }
        );

        let column_schemas = names
            .iter()
            .enumerate()
            .map(|(idx, name)| {
                let real_ty = cols[idx].data_type();
                let AnnotationInfo {
                    datatype: ty,
                    is_nullable,
                } = anno[idx].clone().unwrap_or_else(|| {
                    // default to be not nullable and use DataType inferred by PyVector itself
                    AnnotationInfo {
                        datatype: Some(real_ty.clone()),
                        is_nullable: false,
                    }
                });
                let column_type = match ty {
                    Some(anno_type) => anno_type,
                    // if type is like `_` or `_ | None`
                    None => real_ty,
                };
                Ok(ColumnSchema::new(name, column_type, is_nullable))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Arc::new(Schema::new(column_schemas)))
    }

    /// check if real types and annotation types(if have) is the same, if not try cast columns to annotated type
    pub(crate) fn check_and_cast_type(&self, cols: &mut [VectorRef]) -> Result<()> {
        let return_types = &self.return_types;
        // allow ignore Return Type Annotation
        if return_types.is_empty() {
            return Ok(());
        }
        ensure!(
            cols.len() == return_types.len(),
            OtherSnafu {
                reason: format!(
                    "The number of return Vector is wrong, expect {}, found {}",
                    return_types.len(),
                    cols.len()
                )
            }
        );
        for (col, anno) in cols.iter_mut().zip(return_types) {
            if let Some(AnnotationInfo {
                datatype: Some(datatype),
                is_nullable: _,
            }) = anno
            {
                let real_ty = col.data_type();
                let anno_ty = datatype;
                if real_ty != *anno_ty {
                    let array = col.to_arrow_array();
                    let array =
                        compute::cast(&array, &anno_ty.as_arrow_type()).context(ArrowSnafu)?;
                    *col = Helper::try_into_vector(array).context(TypeCastSnafu)?;
                }
            }
        }
        Ok(())
    }
}

/// select columns according to `fetch_names` from `rb`
/// and cast them into a Vec of PyVector
pub(crate) fn select_from_rb(rb: &RecordBatch, fetch_names: &[String]) -> Result<Vec<PyVector>> {
    fetch_names
        .iter()
        .map(|name| {
            let vector = rb.column_by_name(name).with_context(|| OtherSnafu {
                reason: format!("Can't find field name {name} in all columns in {rb:?}"),
            })?;
            Ok(PyVector::from(vector.clone()))
        })
        .collect()
}

/// match between arguments' real type and annotation types
/// if type anno is `vector[_]` then use real type(from RecordBatch's schema)
pub(crate) fn check_args_anno_real_type(
    arg_names: &[String],
    args: &[PyVector],
    copr: &Coprocessor,
    rb: &RecordBatch,
) -> Result<()> {
    ensure!(
        arg_names.len() == args.len(),
        OtherSnafu {
            reason: format!("arg_names:{arg_names:?} and args{args:?}'s length is different")
        }
    );
    for (idx, arg) in args.iter().enumerate() {
        let anno_ty = copr.arg_types[idx].clone();
        let real_ty = arg.data_type();
        let arg_name = arg_names[idx].clone();
        let col_idx = rb.schema.column_index_by_name(&arg_name).ok_or_else(|| {
            OtherSnafu {
                reason: format!("Can't find column by name {arg_name}"),
            }
            .build()
        })?;
        let is_nullable: bool = rb.schema.column_schemas()[col_idx].is_nullable();
        ensure!(
            anno_ty
                .clone()
                .map(|v| v.datatype.is_none() // like a vector[_]
                     || v.datatype == Some(real_ty.clone()) && v.is_nullable == is_nullable)
                .unwrap_or(true),
            OtherSnafu {
                reason: format!(
                    "column {}'s Type annotation is {:?}, but actual type is {:?} with nullable=={}",
                    // It's safe to unwrap here, we already ensure the args and types number is the same when parsing
                    copr.deco_args.arg_names.as_ref().unwrap()[idx],
                    anno_ty,
                    real_ty,
                    is_nullable
                )
            }
        )
    }
    Ok(())
}

/// The coprocessor function accept a python script and a Record Batch:
/// ## What it does
/// 1. it take a python script and a [`RecordBatch`], extract columns and annotation info according to `args` given in decorator in python script
/// 2. execute python code and return a vector or a tuple of vector,
/// 3. the returning vector(s) is assembled into a new [`RecordBatch`] according to `returns` in python decorator and return to caller
///
/// # Example
///
/// ```ignore
/// use std::sync::Arc;
/// use common_recordbatch::RecordBatch;
/// use datatypes::prelude::*;
/// use datatypes::schema::{ColumnSchema, Schema};
/// use datatypes::vectors::{Float32Vector, Float64Vector};
/// use common_function::scalars::python::exec_coprocessor;
/// let python_source = r#"
/// @copr(args=["cpu", "mem"], returns=["perf", "what"])
/// def a(cpu, mem):
///     return cpu + mem, cpu - mem
/// "#;
/// let cpu_array = Float32Vector::from_slice([0.9f32, 0.8, 0.7, 0.6]);
/// let mem_array = Float64Vector::from_slice([0.1f64, 0.2, 0.3, 0.4]);
/// let schema = Arc::new(Schema::new(vec![
///  ColumnSchema::new("cpu", ConcreteDataType::float32_datatype(), false),
///  ColumnSchema::new("mem", ConcreteDataType::float64_datatype(), false),
/// ]));
/// let rb =
/// RecordBatch::new(schema, vec![Arc::new(cpu_array), Arc::new(mem_array)]).unwrap();
/// let ret = exec_coprocessor(python_source, &rb).unwrap();
/// assert_eq!(ret.column(0).len(), 4);
/// ```
///
/// # Type Annotation
/// you can use type annotations in args and returns to designate types, so coprocessor will check for corresponding types.
///
/// Currently support types are `u8`, `u16`, `u32`, `u64`, `i8`, `i16`, `i32`, `i64` and `f16`, `f32`, `f64`
///
/// use `f64 | None` to mark if returning column is nullable like in [`RecordBatch`]'s schema's [`ColumnSchema`]'s is_nullable
///
/// you can also use single underscore `_` to let coprocessor infer what type it is, so `_` and `_ | None` are both valid in type annotation.
/// Note: using `_` means not nullable column, using `_ | None` means nullable column
///
/// a example (of python script) given below:
/// ```python
/// @copr(args=["cpu", "mem"], returns=["perf", "minus", "mul", "div"])
/// def a(cpu: vector[f32], mem: vector[f64])->(vector[f64|None], vector[f64], vector[_], vector[_ | None]):
///     return cpu + mem, cpu - mem, cpu * mem, cpu / mem
/// ```
///
/// # Return Constant columns
/// You can return constant in python code like `return 1, 1.0, True`
/// which create a constant array(with same value)(currently support int, float and bool) as column on return
#[cfg(test)]
pub fn exec_coprocessor(script: &str, rb: &Option<RecordBatch>) -> Result<RecordBatch> {
    // 1. parse the script and check if it's only a function with `@coprocessor` decorator, and get `args` and `returns`,
    // 2. also check for exist of `args` in `rb`, if not found, return error
    // cache the result of parse_copr
    let copr = parse::parse_and_compile_copr(script, None)?;
    exec_parsed(&copr, rb, &HashMap::new())
}

#[cfg_attr(feature = "pyo3_backend", pyo3class(name = "query_engine"))]
#[rspyclass(module = false, name = "query_engine")]
#[derive(Debug, PyPayload, Clone)]
pub struct PyQueryEngine {
    inner: QueryEngineWeakRef,
}
pub(crate) enum Either {
    Rb(RecordBatches),
    AffectedRows(usize),
}

impl PyQueryEngine {
    pub(crate) fn sql_to_rb(&self, sql: String) -> StdResult<RecordBatch, String> {
        let res = self.query_with_new_thread(sql.clone())?;
        match res {
            Either::Rb(rbs) => {
                let rb = compute::concat_batches(
                    rbs.schema().arrow_schema(),
                    rbs.iter().map(|r| r.df_record_batch()),
                )
                .map_err(|e| format!("Concat batches failed for query {sql}: {e}"))?;

                RecordBatch::try_from_df_record_batch(rbs.schema(), rb)
                    .map_err(|e| format!("Convert datafusion record batch to record batch failed for query {sql}: {e}"))
            }
            Either::AffectedRows(_) => Err(format!("Expect actual results from query {sql}")),
        }
    }
}

#[rspyclass]
impl PyQueryEngine {
    pub(crate) fn from_weakref(inner: QueryEngineWeakRef) -> Self {
        Self { inner }
    }
    pub(crate) fn query_with_new_thread(&self, s: String) -> StdResult<Either, String> {
        let query = self.inner.0.upgrade();
        let thread_handle = std::thread::spawn(move || -> std::result::Result<_, String> {
            if let Some(engine) = query {
                let stmt = QueryLanguageParser::parse_sql(&s).map_err(|e| e.to_string())?;

                // To prevent the error of nested creating Runtime, if is nested, use the parent runtime instead

                let rt = tokio::runtime::Runtime::new().map_err(|e| e.to_string())?;
                let handle = rt.handle().clone();
                let res = handle.block_on(async {
                    let ctx = QueryContextBuilder::default().build();
                    let plan = engine
                        .planner()
                        .plan(stmt, ctx.clone())
                        .await
                        .map_err(|e| e.to_string())?;
                    let res = engine
                        .clone()
                        .execute(plan, ctx)
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
            .map_err(|e| format!("Dedicated thread for sql query panic: {e:?}"))?
    }
    // TODO(discord9): find a better way to call sql query api, now we don't if we are in async context or not
    /// - return sql query results in `PyRecordBatch`,  or
    /// - a empty `PyDict` if query results is empty
    /// - or number of AffectedRows
    #[pymethod]
    fn sql(&self, s: String, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
        self.query_with_new_thread(s)
            .map_err(|e| vm.new_system_error(e))
            .map(|rbs| match rbs {
                Either::Rb(rbs) => {
                    let rb = compute::concat_batches(
                        rbs.schema().arrow_schema(),
                        rbs.iter().map(|rb| rb.df_record_batch()),
                    )
                    .map_err(|e| {
                        vm.new_runtime_error(format!("Failed to concat batches: {e:#?}"))
                    })?;
                    let rb =
                        RecordBatch::try_from_df_record_batch(rbs.schema(), rb).map_err(|e| {
                            vm.new_runtime_error(format!("Failed to cast recordbatch: {e:#?}"))
                        })?;
                    let rb = PyRecordBatch::new(rb);

                    Ok(rb.to_pyobject(vm))
                }
                Either::AffectedRows(cnt) => Ok(vm.ctx.new_int(cnt).to_pyobject(vm)),
            })?
    }
}

/// using a parsed `Coprocessor` struct as input to execute python code
pub fn exec_parsed(
    copr: &Coprocessor,
    rb: &Option<RecordBatch>,
    params: &HashMap<String, String>,
) -> Result<RecordBatch> {
    match copr.backend {
        BackendType::RustPython => rspy_exec_parsed(copr, rb, params),
        BackendType::CPython => {
            #[cfg(feature = "pyo3_backend")]
            {
                pyo3_exec_parsed(copr, rb, params)
            }
            #[cfg(not(feature = "pyo3_backend"))]
            {
                OtherSnafu {
                    reason: "`pyo3` feature is disabled, therefore can't run scripts in cpython"
                        .to_string(),
                }
                .fail()
            }
        }
    }
}

/// execute script just like [`exec_coprocessor`] do,
/// but instead of return a internal [`Error`] type,
/// return a friendly String format of error
///
/// use `ln_offset` and `filename` to offset line number and mark file name in error prompt
#[cfg(test)]
#[allow(dead_code)]
pub fn exec_copr_print(
    script: &str,
    rb: &Option<RecordBatch>,
    ln_offset: usize,
    filename: &str,
) -> StdResult<RecordBatch, String> {
    let res = exec_coprocessor(script, rb);
    res.map_err(|e| {
        crate::python::error::pretty_print_error_in_src(script, &e, ln_offset, filename)
    })
}

#[cfg(test)]
mod tests {
    use crate::python::ffi_types::copr::parse::parse_and_compile_copr;

    #[test]
    fn test_parse_copr() {
        let script = r#"
def add(a, b):
    return a + b

@copr(args=["a", "b", "c"], returns = ["r"], sql="select number as a,number as b,number as c from numbers limit 100")
def test(a, b, c, **params):
    import greptime as g
    return ( a + b ) / g.sqrt(c)
"#;

        let copr = parse_and_compile_copr(script, None).unwrap();
        assert_eq!(copr.name, "test");
        let deco_args = copr.deco_args.clone();
        assert_eq!(
            deco_args.sql.unwrap(),
            "select number as a,number as b,number as c from numbers limit 100"
        );
        assert_eq!(deco_args.ret_names, vec!["r"]);
        assert_eq!(deco_args.arg_names.unwrap(), vec!["a", "b", "c"]);
        assert_eq!(copr.arg_types, vec![None, None, None]);
        assert_eq!(copr.return_types, vec![None]);
        assert_eq!(copr.kwarg, Some("params".to_string()));
        assert_eq!(copr.script, script);
        let _ = copr.code_obj.unwrap();
    }
}
