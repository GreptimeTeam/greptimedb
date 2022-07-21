use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
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
use snafu::ResultExt;
use vm::builtins::PyTuple;
use vm::{PyObjectRef, PyPayload, VirtualMachine};

use crate::scalars::python::copr_parse::parse_copr;
use crate::scalars::python::error::{
    ensure, ArrowSnafu, CoprParseSnafu, InnerError, OtherSnafu, PyExceptionSerde, PyParseSnafu,
    Result,
};
use crate::scalars::python::type_::{is_instance, PyVector};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnnotationInfo {
    /// if None, use types infered by PyVector
    pub datatype: Option<DataType>,
    pub is_nullable: bool,
    /// if the result type need to be coerced to given type in `into(<datatype>)`
    pub coerce_into: bool,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Coprocessor {
    pub name: String,
    // get from python decorator args&returns
    pub args: Vec<String>,
    pub returns: Vec<String>,
    // get from python function args& returns' annotation, first is type, second is is_nullable
    pub arg_types: Vec<Option<AnnotationInfo>>,
    pub return_types: Vec<Option<AnnotationInfo>>,
}

fn set_loc<T>(node: T, loc: Location) -> Located<T> {
    Located::new(loc, node)
}

/// stripe the decorator(`@xxxx`) and type annotation(for type checker is done in rust function), add one line in the ast for call function with given parameter, and compiler into `CodeObject`
///
/// The conside is that rustpython's vm is not very efficient according to [offical benchmark](https://rustpython.github.io/benchmarks),
/// So we should avoid running too much Python Bytecode, hence in this function we delete `@` decorator(instead of actually write a decorator in python)
/// And add a function call in the end and also
/// strip type annotation
fn strip_append_and_compile(script: &str, copr: &Coprocessor) -> Result<CodeObject> {
    // note that it's important to use `parser::Mode::Interactive` so the ast can be compile to return a result instead of return None in eval mode
    let mut top = parser::parse(script, parser::Mode::Interactive).context(PyParseSnafu)?;
    // erase decorator
    if let ast::Mod::Interactive { body } = &mut top {
        let code = body;
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
            return Err(InnerError::CoprParse {
                reason: format!("Expect the one and only statement in script as a function def, but instead found: {:?}", code[0].node),
                loc: Some(code[0].location)
            });
        }
        let mut loc = code[0].location;
        // This manually construct ast has no corrsponding code in the script, so just give it a random location(which doesn't matter because Location usually only used in pretty print errors)
        loc.newline();
        let args: Vec<Located<ast::ExprKind>> = copr
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
                    id: copr.name.to_owned(),
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
        code.push(set_loc(stmt, loc));
    } else {
        return Err(InnerError::CoprParse {
            reason: format!("Expect statement in script, found: {:?}", top),
            loc: None,
        });
    }
    // use `compile::Mode::BlockExpr` so it return the result of statement
    compile::compile_top(
        &top,
        "<embedded>".to_owned(),
        compile::Mode::BlockExpr,
        compile::CompileOpts { optimize: 0 },
    )
    .map_err(|err| err.into())
}

/// cast a `dyn Array` of type unsigned/int/float into a `dyn Vector`
fn into_vector<T: datatypes::types::Primitive + datatypes::types::DataTypeBuilder>(
    arg: Arc<dyn Array>,
) -> Result<Arc<dyn Vector>> {
    PrimitiveVector::<T>::try_from_arrow_array(arg)
        .map(|op| Arc::new(op) as Arc<dyn Vector>)
        // to cast datatypes::Error to python::Error
        .map_err(|err| err.into())
}

/// convert a `Vec<ArrayRef>` into a `Vec<PyVector>` only when they are of supported types
/// PyVector now only support unsigned&int8/16/32/64, float32/64 and bool when doing meanful arithmetics operation
fn into_py_vector(fetch_args: Vec<ArrayRef>) -> Result<Vec<PyVector>> {
    let mut args: Vec<PyVector> = Vec::new();
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
                let v: VectorRef = Arc::new(BooleanVector::try_from_arrow_array(arg)?);
                v
            }
            _ => {
                return Err(InnerError::Other {
                    reason: format!(
                        "Unsupport data type at column {idx}: {:?} for coprocessor",
                        arg.data_type()
                    ),
                })
            }
        };
        args.push(PyVector::from(v));
    }
    Ok(args)
}

/// convert a tuple of `PyVector` or one `PyVector`(wrapped in a Python Object Ref[`PyObjectRef`])
/// to a `Vec<ArrayRef>`
fn into_columns(obj: &PyObjectRef, vm: &VirtualMachine) -> Result<Vec<ArrayRef>> {
    let mut cols: Vec<ArrayRef> = Vec::new();
    if is_instance(obj, PyTuple::class(vm).into(), vm) {
        let tuple = obj.payload::<PyTuple>().ok_or(InnerError::Other {
            reason: format!("can't cast obj {:?} to PyTuple)", obj),
        })?;
        for obj in tuple {
            if is_instance(obj, PyVector::class(vm).into(), vm) {
                let pyv = obj.payload::<PyVector>().ok_or(InnerError::Other {
                    reason: format!("can't cast obj {:?} to PyVector", obj),
                })?;
                cols.push(pyv.to_arrow_array())
            } else {
                return Err(InnerError::Other { reason: format!("Expect all element in returning tuple to be vector, found one of the element is {:?}", obj) });
            }
        }
    } else if is_instance(obj, PyVector::class(vm).into(), vm) {
        let pyv = obj.payload::<PyVector>().ok_or(InnerError::Other {
            reason: format!("can't cast obj {:?} to PyVector", obj),
        })?;
        cols.push(pyv.to_arrow_array())
    }
    Ok(cols)
}

/// generate [`Schema`] according to names, types and
/// datatypes of the actual columns when no annotation
fn gen_schema(
    cols: &[ArrayRef],
    names: &[String],
    anno: &[Option<AnnotationInfo>],
) -> Result<Arc<Schema>> {
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
                    coerce_into: _,
                } = anno[idx].to_owned().unwrap_or_else(||
                    // default to be not nullable and use DataType infered by PyVector itself
                    AnnotationInfo{
                        datatype: Some(real_ty.to_owned()),
                        is_nullable: false,
                        coerce_into: false
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

/// check type to be correct, if not try cast it to annotated type
fn check_cast_type(
    cols: &mut [ArrayRef],
    schema: &Schema,
    return_types: &[Option<AnnotationInfo>],
) -> Result<()> {
    for ((col, field), anno) in cols.iter_mut().zip(&schema.fields).zip(return_types) {
        let real_ty = col.data_type();
        let anno_ty = field.data_type();
        if let Some(anno) = anno {
            if real_ty != anno_ty {
                if anno.coerce_into {
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
                } else {
                    return Err(InnerError::Other {
                        reason: format!(
                            "Anntation type is {:?}, but real type is {:?}(Maybe add a `into()`?)",
                            anno_ty, real_ty
                        ),
                    });
                }
            } else {
                continue;
            }
        }
    }
    Ok(())
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
/// using `into()` can convert whatever types python function returns into given types.
///
/// Currently support types are `u8`, `u16`, `u32`, `u64`, `i8`, `i16`, `i32`, `i64` and `f16`, `f32`, `f64`
///
/// use `f64 | None` to mark if returning column is nullable like in [`DfRecordBatch`]'s schema's [`Field`]'s is_nullable
///
/// use `into(f64)` to convert returning PyVector into float 64. You can also combine them like `into(f64) | None` to declare a nullable column that types is cast to f64
///
/// you can also use single underscore `_` to let coprocessor infer what type it is, so `_` and `_ | None` are both valid in type annotation.
///
/// a example (of python script) given below:
/// ```python
/// @copr(args=["cpu", "mem"], returns=["perf", "minus", "mul", "div"])
/// def a(cpu: vector[f32], mem: vector[f64])->(vector[into(f64)|None], vector[into(f64)], vector[f64], vector[f64 | None]):
///     return cpu + mem, cpu - mem, cpu * mem, cpu / mem
/// ```
pub fn exec_coprocessor(script: &str, rb: &DfRecordBatch) -> Result<DfRecordBatch> {
    // 1. parse the script and check if it's only a function with `@coprocessor` decorator, and get `args` and `returns`,
    // 2. also check for exist of `args` in `rb`, if not found, return error
    let copr = parse_copr(script)?;
    // 3. get args from `rb`, and cast them into PyVector
    let mut fetch_idx = Vec::new();
    for (idx, field) in rb.schema().fields.iter().enumerate() {
        for arg in &copr.args {
            if arg.as_str() == field.name.as_str() {
                fetch_idx.push(idx)
            }
        }
    }
    let mut fetch_args = Vec::new();
    for idx in fetch_idx {
        fetch_args.push(rb.column(idx).clone());
    }
    let args: Vec<PyVector> = into_py_vector(fetch_args)?;

    // match between arguments real type and annotation types
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
    // 4. then set args in scope and call by compiler and run `CodeObject` which already append `Call` node
    vm::Interpreter::without_stdlib(Default::default()).enter(|vm| -> Result<DfRecordBatch> {
        PyVector::make_class(&vm.ctx);
        let scope = vm.new_scope_with_builtins();
        for (name, vector) in copr.args.iter().zip(args) {
            scope
                .locals
                .as_object()
                .set_item(name, vm.new_pyobj(vector), vm)
                .map_err(|err| InnerError::Other {
                    reason: format!(
                        "fail to set item{} in python scope, PyExpection: {:?}",
                        name, err
                    ),
                })?;
        }

        let code_obj = strip_append_and_compile(script, &copr)?;
        let code_obj = vm.ctx.new_code(code_obj);
        let run_res = vm.run_code_obj(code_obj, scope);
        let ret = if let Err(excep) = run_res {
            let mut chain = String::new();
            vm.write_exception(&mut chain, &excep)
                .map_err(|_| InnerError::Other {
                    reason: "Fail to write to string".into(),
                })?;
            return Err(PyExceptionSerde { output: chain }.into());
        } else if let Ok(ret) = run_res {
            ret
        } else {
            unreachable!()
        };

        // 5. get returns as either a PyVector or a PyTuple, and naming schema them according to `returns`
        let mut cols: Vec<ArrayRef> = into_columns(&ret, vm)?;
        ensure!(
            cols.len() == copr.returns.len(),
            OtherSnafu {
                reason: format!(
                    "The number of return Vector is wrong, expect{}, found{}",
                    copr.returns.len(),
                    cols.len()
                )
            }
        );
        let schema = gen_schema(&cols, &copr.returns, &copr.return_types)?;
        // if cols and schema's data types is not match, first try coerced it to given type(if annotated), if not possible, return Err
        check_cast_type(&mut cols, &schema, &copr.return_types)?;
        // 6. return a assembled DfRecordBatch
        let res_rb = DfRecordBatch::try_new(schema, cols).context(ArrowSnafu)?;
        Ok(res_rb)
    })
}
