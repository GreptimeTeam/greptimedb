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

use std::cell::RefCell;
use std::collections::HashSet;
use std::result::Result as StdResult;
use std::sync::{Arc, Weak};

use common_recordbatch::{RecordBatch, RecordBatches};
use common_telemetry::info;
use datatypes::arrow::array::Array;
use datatypes::arrow::compute;
use datatypes::data_type::{ConcreteDataType, DataType};
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use datatypes::vectors::{Helper, VectorRef};
use query::parser::QueryLanguageParser;
use query::QueryEngine;
use rustpython_compiler_core::CodeObject;
use rustpython_vm as vm;
use rustpython_vm::class::PyClassImpl;
use rustpython_vm::AsObject;
#[cfg(test)]
use serde::Deserialize;
use snafu::{OptionExt, ResultExt};
use vm::builtins::{PyBaseExceptionRef, PyList, PyListRef, PyTuple};
use vm::convert::ToPyObject;
use vm::scope::Scope;
use vm::{pyclass, Interpreter, PyObjectRef, PyPayload, PyResult, VirtualMachine};

use crate::python::builtins::greptime_builtin;
use crate::python::coprocessor::parse::DecoratorArgs;
use crate::python::dataframe::data_frame::{self, set_dataframe_in_scope};
use crate::python::error::{
    ensure, ret_other_error_with, ArrowSnafu, NewRecordBatchSnafu, OtherSnafu, Result,
    TypeCastSnafu,
};
use crate::python::utils::{format_py_error, is_instance, py_vec_obj_to_array};
use crate::python::PyVector;

thread_local!(static INTERPRETER: RefCell<Option<Arc<Interpreter>>> = RefCell::new(None));

#[cfg_attr(test, derive(Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnnotationInfo {
    /// if None, use types inferred by PyVector
    // TODO(yingwen): We should use our data type. i.e. ConcreteDataType.
    pub datatype: Option<ConcreteDataType>,
    pub is_nullable: bool,
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
            .field(&self.0.upgrade().map(|f| f.name().to_owned()))
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
    fn gen_schema(&self, cols: &[VectorRef]) -> Result<SchemaRef> {
        let names = &self.deco_args.ret_names;
        let anno = &self.return_types;
        ensure!(
            cols.len() == names.len() && names.len() == anno.len(),
            OtherSnafu {
                reason: format!(
                    "Unmatched length for cols({}), names({}) and anno({})",
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
                } = anno[idx].to_owned().unwrap_or_else(|| {
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
    fn check_and_cast_type(&self, cols: &mut [VectorRef]) -> Result<()> {
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

/// convert a tuple of `PyVector` or one `PyVector`(wrapped in a Python Object Ref[`PyObjectRef`])
/// to a `Vec<ArrayRef>`
/// by default, a constant(int/float/bool) gives the a constant array of same length with input args
fn try_into_columns(
    obj: &PyObjectRef,
    vm: &VirtualMachine,
    col_len: usize,
) -> Result<Vec<VectorRef>> {
    if is_instance::<PyTuple>(obj, vm) {
        let tuple = obj
            .payload::<PyTuple>()
            .with_context(|| ret_other_error_with(format!("can't cast obj {obj:?} to PyTuple)")))?;
        let cols = tuple
            .iter()
            .map(|obj| py_vec_obj_to_array(obj, vm, col_len))
            .collect::<Result<Vec<VectorRef>>>()?;
        Ok(cols)
    } else {
        let col = py_vec_obj_to_array(obj, vm, col_len)?;
        Ok(vec![col])
    }
}

/// select columns according to `fetch_names` from `rb`
/// and cast them into a Vec of PyVector
fn select_from_rb(rb: &RecordBatch, fetch_names: &[String]) -> Result<Vec<PyVector>> {
    fetch_names
        .iter()
        .map(|name| {
            let vector = rb.column_by_name(name).with_context(|| OtherSnafu {
                reason: format!("Can't find field name {name}"),
            })?;
            Ok(PyVector::from(vector.clone()))
        })
        .collect()
}

/// match between arguments' real type and annotation types
/// if type anno is vector[_] then use real type
fn check_args_anno_real_type(
    args: &[PyVector],
    copr: &Coprocessor,
    rb: &RecordBatch,
) -> Result<()> {
    for (idx, arg) in args.iter().enumerate() {
        let anno_ty = copr.arg_types[idx].to_owned();
        let real_ty = arg.to_arrow_array().data_type().to_owned();
        let real_ty = ConcreteDataType::from_arrow_type(&real_ty);
        let is_nullable: bool = rb.schema.column_schemas()[idx].is_nullable();
        ensure!(
            anno_ty
                .to_owned()
                .map(|v| v.datatype.is_none() // like a vector[_]
                     || v.datatype == Some(real_ty.to_owned()) && v.is_nullable == is_nullable)
                .unwrap_or(true),
            OtherSnafu {
                reason: format!(
                    "column {}'s Type annotation is {:?}, but actual type is {:?}",
                    // It's safe to unwrap here, we already ensure the args and types number is the same when parsing
                    copr.deco_args.arg_names.as_ref().unwrap()[idx],
                    anno_ty,
                    real_ty
                )
            }
        )
    }
    Ok(())
}

/// set arguments with given name and values in python scopes
fn set_items_in_scope(
    scope: &Scope,
    vm: &VirtualMachine,
    arg_names: &[String],
    args: Vec<PyVector>,
) -> Result<()> {
    let _ = arg_names
        .iter()
        .zip(args)
        .map(|(name, vector)| {
            scope
                .locals
                .as_object()
                .set_item(name, vm.new_pyobj(vector), vm)
        })
        .collect::<StdResult<Vec<()>, PyBaseExceptionRef>>()
        .map_err(|e| format_py_error(e, vm))?;
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
pub fn exec_coprocessor(script: &str, rb: &RecordBatch) -> Result<RecordBatch> {
    // 1. parse the script and check if it's only a function with `@coprocessor` decorator, and get `args` and `returns`,
    // 2. also check for exist of `args` in `rb`, if not found, return error
    // TODO(discord9): cache the result of parse_copr
    let copr = parse::parse_and_compile_copr(script, None)?;
    exec_parsed(&copr, rb)
}

#[pyclass(module = false, name = "query_engine")]
#[derive(Debug, PyPayload)]
pub struct PyQueryEngine {
    inner: QueryEngineWeakRef,
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
                            let v = PyVector::from(v.to_owned());
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

fn set_query_engine_in_scope(
    scope: &Scope,
    vm: &VirtualMachine,
    query_engine: PyQueryEngine,
) -> Result<()> {
    scope
        .locals
        .as_object()
        .set_item("query", query_engine.to_pyobject(vm), vm)
        .map_err(|e| format_py_error(e, vm))
}

pub(crate) fn exec_with_cached_vm(
    copr: &Coprocessor,
    rb: &RecordBatch,
    args: Vec<PyVector>,
    vm: &Arc<Interpreter>,
) -> Result<RecordBatch> {
    vm.enter(|vm| -> Result<RecordBatch> {
        PyVector::make_class(&vm.ctx);
        // set arguments with given name and values
        let scope = vm.new_scope_with_builtins();
        set_items_in_scope(&scope, vm, &copr.deco_args.arg_names, args)?;
        set_dataframe_in_scope(&scope, vm, "dataframe", rb)?;

        if let Some(arg_names) = &copr.deco_args.arg_names {
            assert_eq!(arg_names.len(), args.len());
            set_items_in_scope(&scope, vm, arg_names, args)?;
        }

        if let Some(engine) = &copr.query_engine {
            let query_engine = PyQueryEngine {
                inner: engine.clone(),
            };

            // put a object named with query of class PyQueryEngine in scope
            PyQueryEngine::make_class(&vm.ctx);
            set_query_engine_in_scope(&scope, vm, query_engine)?;
        }

        // It's safe to unwrap code_object, it's already compiled before.
        let code_obj = vm.ctx.new_code(copr.code_obj.clone().unwrap());
        let ret = vm
            .run_code_obj(code_obj, scope)
            .map_err(|e| format_py_error(e, vm))?;

        // 5. get returns as either a PyVector or a PyTuple, and naming schema them according to `returns`
        let col_len = rb.num_rows();
        let mut cols = try_into_columns(&ret, vm, col_len)?;
        ensure!(
            cols.len() == copr.deco_args.ret_names.len(),
            OtherSnafu {
                reason: format!(
                    "The number of return Vector is wrong, expect {}, found {}",
                    copr.deco_args.ret_names.len(),
                    cols.len()
                )
            }
        );

        // if cols and schema's data types is not match, try coerce it to given type(if annotated)(if error occur, return relevant error with question mark)
        copr.check_and_cast_type(&mut cols)?;
        // 6. return a assembled DfRecordBatch
        let schema = copr.gen_schema(&cols)?;
        RecordBatch::new(schema, cols).context(NewRecordBatchSnafu)
    })
}

/// init interpreter with type PyVector and Module: greptime
pub(crate) fn init_interpreter() -> Arc<Interpreter> {
    INTERPRETER.with(|i| {
        i.borrow_mut()
            .get_or_insert_with(|| {
                // we limit stdlib imports for safety reason, i.e `fcntl` is not allowed here
                let native_module_allow_list = HashSet::from([
                    "array", "cmath", "gc", "hashlib", "_json", "_random", "math",
                ]);
                // TODO(discord9): edge cases, can't use "..Default::default" because Settings is `#[non_exhaustive]`
                // so more in here: https://internals.rust-lang.org/t/allow-constructing-non-exhaustive-structs-using-default-default/13868
                let mut settings = vm::Settings::default();
                // disable SIG_INT handler so our own binary can take ctrl_c handler
                settings.no_sig_int = true;
                let interpreter = Arc::new(vm::Interpreter::with_init(settings, |vm| {
                    // not using full stdlib to prevent security issue, instead filter out a few simple util module
                    vm.add_native_modules(
                        rustpython_stdlib::get_module_inits()
                            .into_iter()
                            .filter(|(k, _)| native_module_allow_list.contains(k.as_ref())),
                    );

                    // We are freezing the stdlib to include the standard library inside the binary.
                    // so according to this issue:
                    // https://github.com/RustPython/RustPython/issues/4292
                    // add this line for stdlib, so rustpython can found stdlib's python part in bytecode format
                    vm.add_frozen(rustpython_pylib::frozen_stdlib());
                    // add our own custom datatype and module
                    PyVector::make_class(&vm.ctx);
                    vm.add_native_module("greptime", Box::new(greptime_builtin::make_module));

                    data_frame::PyDataFrame::make_class(&vm.ctx);
                    data_frame::PyExpr::make_class(&vm.ctx);
                    vm.add_native_module("data_frame", Box::new(data_frame::make_module));
                }));
                info!("Initialized Python interpreter.");
                interpreter
            })
            .clone()
    })
}

/// using a parsed `Coprocessor` struct as input to execute python code
pub(crate) fn exec_parsed(copr: &Coprocessor, rb: &RecordBatch) -> Result<RecordBatch> {
    // 3. get args from `rb`, and cast them into PyVector
    let args: Vec<PyVector> =
        select_from_rb(rb, copr.deco_args.arg_names.as_ref().unwrap_or(&vec![]))?;
    check_args_anno_real_type(&args, copr, rb)?;
    let interpreter = init_interpreter();
    // 4. then set args in scope and compile then run `CodeObject` which already append a new `Call` node
    exec_with_cached_vm(copr, rb, args, &interpreter)
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
    rb: &RecordBatch,
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
    use crate::python::coprocessor::parse::parse_and_compile_copr;

    #[test]
    fn test_parse_copr() {
        let script = r#"
def add(a, b):
    return a + b

@copr(args=["a", "b", "c"], returns = ["r"], sql="select number as a,number as b,number as c from numbers limit 100")
def test(a, b, c):
    import greptime as g
    return add(a, b) / g.sqrt(c)
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
        assert_eq!(copr.script, script);
        assert!(copr.code_obj.is_some());
    }
}
