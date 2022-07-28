use std::collections::HashMap;
use std::result::Result as StdResult;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, PrimitiveArray, BooleanArray};
use arrow::compute::cast::CastOptions;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::record_batch::RecordBatch as DfRecordBatch;
use datatypes::vectors::{BooleanVector, PrimitiveVector, Vector, VectorRef};
use rustpython_bytecode::CodeObject;
use rustpython_compiler_core::compile;
use rustpython_parser::{
    ast,
    ast::{Located, Location},
    parser,
};
use rustpython_vm as vm;
use rustpython_vm::{class::PyClassImpl, AsObject};
use snafu::{OptionExt, ResultExt};
use vm::builtins::{PyBaseExceptionRef, PyBool, PyFloat, PyInt, PyTuple};
use vm::scope::Scope;
use vm::{PyObjectRef, PyPayload, VirtualMachine};

use crate::scalars::python::copr_parse::{parse_copr, ret_parse_error};
use crate::scalars::python::error::{
    ensure, ArrowSnafu, CoprParseSnafu, OtherSnafu, PyCompileSnafu, PyExceptionSerde, PyParseSnafu,
    PyRuntimeSnafu, Result, TypeCastSnafu,
};
use crate::scalars::python::type_::{is_instance, PyVector};

fn ret_other_error_with(reason: String) -> OtherSnafu<String> {
    OtherSnafu { reason }
}

#[cfg(test)]
use serde::{Deserialize, Serialize};

#[cfg_attr(test, derive(Serialize, Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnnotationInfo {
    /// if None, use types infered by PyVector
    pub datatype: Option<DataType>,
    pub is_nullable: bool,
}

#[cfg_attr(test, derive(Serialize, Deserialize))]
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Coprocessor {
    pub name: String,
    // get from python decorator args&returns
    pub args: Vec<String>,
    pub returns: Vec<String>,
    // get from python function args& returns' annotation, first is type, second is is_nullable
    pub arg_types: Vec<Option<AnnotationInfo>>,
    pub return_types: Vec<Option<AnnotationInfo>>,
    #[cfg_attr(test, serde(skip))]
    pub script: String,
}

impl Coprocessor {
    /// generate a call to the coprocessor function
    /// with arguments given in decorator's `args` list
    /// also set in location in source code to `loc`
    fn gen_call(&self, loc: &Location) -> ast::Stmt<()> {
        let mut loc = loc.to_owned();
        loc.newline();
        let args: Vec<Located<ast::ExprKind>> = self
            .args
            .iter()
            .map(|v| {
                let node = ast::ExprKind::Name {
                    id: v.to_owned(),
                    ctx: ast::ExprContext::Load,
                };
                set_loc(node, loc)
            })
            .collect();
        let func = ast::ExprKind::Call {
            func: Box::new(set_loc(
                ast::ExprKind::Name {
                    id: self.name.to_owned(),
                    ctx: ast::ExprContext::Load,
                },
                loc,
            )),
            args,
            keywords: Vec::new(),
        };
        let stmt = ast::StmtKind::Expr {
            value: Box::new(set_loc(func, loc)),
        };
        set_loc(stmt, loc)
    }

    /// check if `Mod` is of one line of statement
    fn check_before_compile(top: &ast::Mod) -> Result<()> {
        if let ast::Mod::Interactive { body: code } = top {
            ensure!(
                code.len() == 1,
                CoprParseSnafu {
                    reason: format!(
                        "Expect only one statement in script, found {} statement",
                        code.len()
                    ),
                    loc: if code.is_empty() {
                        None
                    } else {
                        Some(code[0].location)
                    }
                }
            );

            if let ast::StmtKind::FunctionDef {
                name: _,
                args: _,
                body: _,
                decorator_list: _,
                returns: _,
                type_comment: __main__,
            } = &code[0].node
            {
            } else {
                return ret_parse_error(
                    format!("Expect the one and only statement in script as a function def, but instead found: {:?}", code[0].node),
                    Some(code[0].location)
                )
                .fail();
            }
        } else {
            return ret_parse_error(
                format!("Expect statement in script, found: {:?}", top),
                None,
            )
            .fail();
        }
        Ok(())
    }

    /// stripe the decorator(`@xxxx`) and type annotation(for type checker is done in rust function), add one line in the ast for call function with given parameter, and compiler into `CodeObject`
    ///
    /// The conside is that rustpython's vm is not very efficient according to [offical benchmark](https://rustpython.github.io/benchmarks),
    /// So we should avoid running too much Python Bytecode, hence in this function we delete `@` decorator(instead of actually write a decorator in python)
    /// And add a function call in the end and also
    /// strip type annotation
    fn strip_append_and_compile(&self) -> Result<CodeObject> {
        let script = &self.script;
        // note that it's important to use `parser::Mode::Interactive` so the ast can be compile to return a result instead of return None in eval mode
        let mut top = parser::parse(script, parser::Mode::Interactive).context(PyParseSnafu)?;
        Self::check_before_compile(&top)?;
        // erase decorator
        if let ast::Mod::Interactive { body } = &mut top {
            let code = body;
            if let ast::StmtKind::FunctionDef {
                name: _,
                args,
                body: _,
                decorator_list,
                returns,
                type_comment: __main__,
            } = &mut code[0].node
            {
                *decorator_list = Vec::new();
                // strip type annotation
                // def a(b: int, c:int) -> int
                // will became
                // def a(b, c)
                *returns = None;
                for arg in &mut args.args {
                    arg.node.annotation = None;
                }
            } else {
                // already done in check function
                unreachable!()
            }
            let loc = code[0].location;

            // This manually construct ast has no corrsponding code
            // in the script, so just give it a random location
            // (which doesn't matter because Location usually only used in pretty print errors)
            code.push(self.gen_call(&loc));
        } else {
            // already done in check function
            unreachable!()
        }
        // use `compile::Mode::BlockExpr` so it return the result of statement
        compile::compile_top(
            &top,
            "<embedded>".to_owned(),
            compile::Mode::BlockExpr,
            compile::CompileOpts { optimize: 0 },
        )
        .context(PyCompileSnafu)
    }

    /// generate [`Schema`] according to return names, types,
    /// if no annotation
    /// the datatypes of the actual columns is used directly
    fn gen_schema(&self, cols: &[ArrayRef]) -> Result<Arc<Schema>> {
        let names = &self.returns;
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
        Ok(Arc::new(Schema::from(
            names
                .iter()
                .enumerate()
                .map(|(idx, name)| {
                    let real_ty = cols[idx].data_type().to_owned();
                    let AnnotationInfo {
                        datatype: ty,
                        is_nullable,
                    } = anno[idx].to_owned().unwrap_or_else(||
                    // default to be not nullable and use DataType infered by PyVector itself
                    AnnotationInfo{
                        datatype: Some(real_ty.to_owned()),
                        is_nullable: false
                    });
                    Field::new(
                        name,
                        // if type is like `_` or `_ | None`
                        if let Some(ty) = ty { ty } else { real_ty },
                        is_nullable,
                    )
                })
                .collect::<Vec<Field>>(),
        )))
    }

    /// check if real types and annotation types(if have) is the same, if not try cast columns to annotated type
    fn check_and_cast_type(&self, cols: &mut [ArrayRef]) -> Result<()> {
        let return_types = &self.return_types;
        for (col, anno) in cols.iter_mut().zip(return_types) {
            if let Some(AnnotationInfo {
                datatype: Some(datatype),
                is_nullable: _,
            }) = anno
            {
                let real_ty = col.data_type();
                let anno_ty = datatype;
                if real_ty != anno_ty {
                    {
                        *col = arrow::compute::cast::cast(
                            col.as_ref(),
                            anno_ty,
                            CastOptions {
                                wrapped: true,
                                partial: true,
                            },
                        )
                        .context(ArrowSnafu)?
                        .into();
                    }
                } else {
                    continue;
                }
            }
        }
        Ok(())
    }
}

fn set_loc<T>(node: T, loc: Location) -> Located<T> {
    Located::new(loc, node)
}

/// cast a `dyn Array` of type unsigned/int/float into a `dyn Vector`
fn into_vector<T: datatypes::types::Primitive + datatypes::types::DataTypeBuilder>(
    arg: Arc<dyn Array>,
) -> Result<Arc<dyn Vector>> {
    PrimitiveVector::<T>::try_from_arrow_array(arg)
        .map(|op| Arc::new(op) as _)
        // to cast datatypes::Error to python::Error
        .context(TypeCastSnafu)
}

/// convert a `Vec<ArrayRef>` into a `Vec<PyVector>` only when they are of supported types
/// PyVector now only support unsigned&int8/16/32/64, float32/64 and bool when doing meanful arithmetics operation
fn into_py_vector(fetch_args: Vec<ArrayRef>) -> Result<Vec<PyVector>> {
    let mut args: Vec<PyVector> = Vec::with_capacity(fetch_args.len());
    for (idx, arg) in fetch_args.into_iter().enumerate() {
        let v: VectorRef = match arg.data_type() {
            DataType::Float32 => into_vector::<f32>(arg)?,
            DataType::Float64 => into_vector::<f64>(arg)?,
            DataType::UInt8 => into_vector::<u8>(arg)?,
            DataType::UInt16 => into_vector::<u16>(arg)?,
            DataType::UInt32 => into_vector::<u32>(arg)?,
            DataType::UInt64 => into_vector::<u64>(arg)?,
            DataType::Int8 => into_vector::<i8>(arg)?,
            DataType::Int16 => into_vector::<i16>(arg)?,
            DataType::Int32 => into_vector::<i32>(arg)?,
            DataType::Int64 => into_vector::<i64>(arg)?,
            DataType::Boolean => {
                let v: VectorRef =
                    Arc::new(BooleanVector::try_from_arrow_array(arg).context(TypeCastSnafu)?);
                v
            }
            _ => {
                return ret_other_error_with(format!(
                    "Unsupport data type at column {idx}: {:?} for coprocessor",
                    arg.data_type()
                ))
                .fail()
            }
        };
        args.push(PyVector::from(v));
    }
    Ok(args)
}

/// convert a single PyVector or a number(a constant) into a Array(or a constant array)
fn py_vec_to_array_ref(obj: &PyObjectRef, vm: &VirtualMachine, col_len: usize) -> Result<ArrayRef> {
    if is_instance(obj, PyVector::class(vm).into(), vm) {
        let pyv = obj
            .payload::<PyVector>()
            .context(ret_other_error_with(format!(
                "can't cast obj {:?} to PyVector",
                obj
            )))?;
        Ok(pyv.to_arrow_array())
    } else if is_instance(obj, PyInt::class(vm).into(), vm) {
        let val = obj.to_owned().try_into_value::<i64>(vm);
        let val = match val {
            Ok(val) => val,
            Err(excep) => return Err(to_serde_excep(excep, vm)?).context(PyRuntimeSnafu)?,
        };
        let ret = PrimitiveArray::from_vec(vec![val;col_len]);
        Ok(Arc::new(ret) as _)
    } else if is_instance(obj, PyFloat::class(vm).into(), vm) {
        let val = obj.to_owned().try_into_value::<f64>(vm);
        let val = match val {
            Ok(val) => val,
            Err(excep) => return Err(to_serde_excep(excep, vm)?).context(PyRuntimeSnafu)?,
        };
        let ret = PrimitiveArray::from_vec(vec![val;col_len]);
        Ok(Arc::new(ret) as _)
    } else if is_instance(obj, PyBool::class(vm).into(), vm) {
        let val = obj.to_owned().try_into_value::<bool>(vm);
        let val = match val {
            Ok(val) => val,
            Err(excep) => return Err(to_serde_excep(excep, vm)?).context(PyRuntimeSnafu)?,
        };
        let ret = BooleanArray::from_iter(std::iter::repeat(Some(val)).take(5));
        Ok(Arc::new(ret) as _)
    } else {
        ret_other_error_with(format!("Expect a vector or a constant, found {:?}", obj)).fail()
    }
}

/// convert a tuple of `PyVector` or one `PyVector`(wrapped in a Python Object Ref[`PyObjectRef`])
/// to a `Vec<ArrayRef>`
///
/// TODO: add support for constant columns
fn into_columns(obj: &PyObjectRef, vm: &VirtualMachine, col_len: usize) -> Result<Vec<ArrayRef>> {
    if is_instance(obj, PyTuple::class(vm).into(), vm) {
        let tuple = obj
            .payload::<PyTuple>()
            .context(ret_other_error_with(format!(
                "can't cast obj {:?} to PyTuple)",
                obj
            )))?;
        let cols = tuple
            .iter()
            .map(|obj| py_vec_to_array_ref(obj, vm, col_len))
            .collect::<Result<Vec<ArrayRef>>>()?;
        Ok(cols)
    } else {
        let col = py_vec_to_array_ref(obj, vm, col_len)?;
        Ok(vec![col])
    }
}

/// select columns according to `fetch_names` from `rb`
/// and cast them into a Vec of PyVector, also return their length
fn select_from_rb(rb: &DfRecordBatch, fetch_names: &[String]) -> Result<Vec<PyVector>> {
    let field_map: HashMap<&String, usize> = rb
        .schema()
        .fields
        .iter()
        .enumerate()
        .map(|(idx, field)| (&field.name, idx))
        .collect();
    let fetch_idx: Vec<usize> = fetch_names
        .iter()
        .map(|field| {
            field_map.get(field).copied().context(OtherSnafu {
                reason: format!("Can't found field name {field}"),
            })
        })
        .collect::<Result<Vec<usize>>>()?;
    let fetch_args: Vec<Arc<dyn Array>> = fetch_idx
        .into_iter()
        .map(|idx| rb.column(idx).clone())
        .collect();
    into_py_vector(fetch_args)
}

/// match between arguments' real type and annotation types
/// so as to double check you extract correct columns(with correct type) from RecordBatch
fn check_args_anno_real_type(
    args: &[PyVector],
    copr: &Coprocessor,
    rb: &DfRecordBatch,
) -> Result<()> {
    for (idx, arg) in args.iter().enumerate() {
        let anno_ty = copr.arg_types[idx].to_owned();
        let real_ty = arg.to_arrow_array().data_type().to_owned();
        let is_nullable: bool = rb.schema().fields[idx].is_nullable;
        ensure!(
            anno_ty
                .to_owned()
                .map(|v| v.datatype == Some(real_ty.to_owned()) && v.is_nullable == is_nullable)
                .unwrap_or(true),
            OtherSnafu {
                reason: format!(
                    "column {}'s Type annotation is {:?}, but actual type is {:?}",
                    copr.args[idx], anno_ty, real_ty
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
    let res = arg_names
        .iter()
        .zip(args)
        .map(|(name, vector)| {
            scope
                .locals
                .as_object()
                .set_item(name, vm.new_pyobj(vector), vm)
        })
        .collect::<StdResult<Vec<()>, PyBaseExceptionRef>>();
    if let Err(res) = res {
        Err(to_serde_excep(res, vm)?).context(PyRuntimeSnafu)
    } else {
        Ok(())
    }
}

/// The coprocessor function accept a python script and a Record Batch:
/// ## What it does
/// 1. it take a python script and a [`DfRecordBatch`], extract columns and annotation info according to `args` given in decorator in python script
/// 2. execute python code and return a vector or a tuple of vector,
/// 3. the returning vector(s) is assembled into a new [`DfRecordBatch`] according to `returns` in python decorator and return to caller
///
/// # Example
///
/// ```rust
/// use std::sync::Arc;
/// use datafusion_common::record_batch::RecordBatch as DfRecordBatch;
/// use arrow::array::PrimitiveArray;
/// use arrow::datatypes::{DataType, Field, Schema};
/// use common_function::scalars::python::exec_coprocessor;
/// let python_source = r#"
/// @copr(args=["cpu", "mem"], returns=["perf", "what"])
/// def a(cpu, mem):
///     return cpu + mem, cpu - mem
/// "#;
/// let cpu_array = PrimitiveArray::from_slice([0.9f32, 0.8, 0.7, 0.6]);
/// let mem_array = PrimitiveArray::from_slice([0.1f64, 0.2, 0.3, 0.4]);
/// let schema = Arc::new(Schema::from(vec![
///  Field::new("cpu", DataType::Float32, false),
///  Field::new("mem", DataType::Float64, false),
/// ]));
/// let rb =
/// DfRecordBatch::try_new(schema, vec![Arc::new(cpu_array), Arc::new(mem_array)]).unwrap();
/// let ret = exec_coprocessor(python_source, &rb).unwrap();
/// assert_eq!(ret.column(0).len(), 4);
/// ```
///
/// # Type Annotation
/// you can use type annotations in args and returns to designate types, so coprocessor will check for corrsponding types.
///
/// Currently support types are `u8`, `u16`, `u32`, `u64`, `i8`, `i16`, `i32`, `i64` and `f16`, `f32`, `f64`
///
/// use `f64 | None` to mark if returning column is nullable like in [`DfRecordBatch`]'s schema's [`Field`]'s is_nullable
///
/// you can also use single underscore `_` to let coprocessor infer what type it is, so `_` and `_ | None` are both valid in type annotation.
///
/// a example (of python script) given below:
/// ```python
/// @copr(args=["cpu", "mem"], returns=["perf", "minus", "mul", "div"])
/// def a(cpu: vector[f32], mem: vector[f64])->(vector[f64|None], vector[f64], vector[_], vector[_ | None]):
///     return cpu + mem, cpu - mem, cpu * mem, cpu / mem
/// ```
pub fn exec_coprocessor(script: &str, rb: &DfRecordBatch) -> Result<DfRecordBatch> {
    // 1. parse the script and check if it's only a function with `@coprocessor` decorator, and get `args` and `returns`,
    // 2. also check for exist of `args` in `rb`, if not found, return error
    // TODO: cache the result of parse_copr
    let copr = parse_copr(script)?;
    // 3. get args from `rb`, and cast them into PyVector
    let args: Vec<PyVector> = select_from_rb(rb, &copr.args)?;
    check_args_anno_real_type(&args, &copr, rb)?;

    // 4. then set args in scope and compile then run `CodeObject` which already append a new `Call` node
    vm::Interpreter::without_stdlib(Default::default()).enter(|vm| -> Result<DfRecordBatch> {
        PyVector::make_class(&vm.ctx);
        // set arguments with given name and values
        let scope = vm.new_scope_with_builtins();
        set_items_in_scope(&scope, vm, &copr.args, args)?;

        let code_obj = copr.strip_append_and_compile()?;
        let code_obj = vm.ctx.new_code(code_obj);
        let run_res = vm.run_code_obj(code_obj, scope);
        // FIXME: better error handling, perhaps with chain calls
        let ret = match run_res {
            Err(excep) => return Err(to_serde_excep(excep, vm)?).context(PyRuntimeSnafu)?,
            Ok(ret) => ret,
        };

        // 5. get returns as either a PyVector or a PyTuple, and naming schema them according to `returns`
        let col_len = rb.num_rows();
        let mut cols: Vec<ArrayRef> = into_columns(&ret, vm, col_len)?;
        ensure!(
            cols.len() == copr.returns.len(),
            OtherSnafu {
                reason: format!(
                    "The number of return Vector is wrong, expect {}, found {}",
                    copr.returns.len(),
                    cols.len()
                )
            }
        );

        // if cols and schema's data types is not match, try coerce it to given type(if annotated)(if error occur, return relevant error with question mark)
        copr.check_and_cast_type(&mut cols)?;
        // 6. return a assembled DfRecordBatch
        let schema = copr.gen_schema(&cols)?;
        let res_rb = DfRecordBatch::try_new(schema, cols).context(ArrowSnafu)?;
        Ok(res_rb)
    })
}

/// transfer a Python Exception into a python call stack in `String` format
fn to_serde_excep(excep: PyBaseExceptionRef, vm: &VirtualMachine) -> Result<PyExceptionSerde> {
    let mut chain = String::new();
    let r = vm.write_exception(&mut chain, &excep);
    // FIXME: better error handling, perhaps with chain calls?
    if let Err(r) = r {
        return ret_other_error_with(format!("Fail to write to string, error: {:#?}", r)).fail();
    }
    Ok(PyExceptionSerde { output: chain })
}
