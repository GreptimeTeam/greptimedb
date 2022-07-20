//! python udf supports
//! use the function `coprocessor` to parse and run a python function with arguments from recordBatch, and return a newly assembled RecordBatch
mod copr_parse;
mod error;
mod type_;
#[cfg(test)]
mod test;
mod coprocessor;

use std::sync::Arc;

use copr_parse::parse_copr;
use coprocessor::AnnotationInfo;
use arrow::array::{Array, ArrayRef};
use arrow::compute::cast::CastOptions;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::record_batch::RecordBatch as DfRecordBatch;
use datatypes::vectors::{
    BooleanVector, Float32Vector, Int32Vector, PrimitiveVector, Vector, VectorRef,
};
use error::{CoprParseSnafu, Error, OtherSnafu, Result};
use rustpython_bytecode::CodeObject;
use rustpython_compiler_core::compile;
use rustpython_parser::{
    ast,
    ast::{Located, Location},
    parser,
};
use rustpython_vm as vm;
use rustpython_vm::{class::PyClassImpl, AsObject};
use snafu::ensure;
use type_::{is_instance, PyVector};
use vm::builtins::PyTuple;
use vm::PyPayload;

use coprocessor::Coprocessor;




fn set_loc<T>(node: T, loc: Location) -> Located<T> {
    Located::new(loc, node)
}

/// stripe the decorator(`@xxxx`) and type annotation(for type checker is done in rust function), add one line in the ast for call function with given parameter, and compiler into `CodeObject`
///
/// The conside is that rustpython's vm is not very efficient according to [offical benchmark](https://rustpython.github.io/benchmarks),
/// So we should avoid running too much Python Bytecode, hence in this function we delete `@` decorator(instead of actually write a decorator in python)
/// And add a function call in the end
/// strip type annotation
fn strip_append_and_compile(script: &str, copr: &Coprocessor) -> Result<CodeObject> {
    // note that it's important to use `parser::Mode::Interactive` so the ast can be compile to return a result instead of return None in eval mode
    let mut top = parser::parse(script, parser::Mode::Interactive)?;
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
            return Err(Error::CoprParse {
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
        return Err(Error::CoprParse {
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
/// use common_function::scalars::python::coprocessor;
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
/// let ret = coprocessor(python_source, &rb).unwrap();
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
    // PyVector now only support unsigned&int8/16/32/64, float32/64 and bool when doing meanful arithmetics operation
    let mut args: Vec<PyVector> = Vec::new();
    // for readability so not using a macro?
    for (idx, arg) in fetch_args.into_iter().enumerate() {
        match arg.data_type() {
            DataType::Float32 => args.push(PyVector::from(into_vector::<f32>(arg)?)),
            DataType::Float64 => args.push(PyVector::from(into_vector::<f64>(arg)?)),
            DataType::UInt8 => args.push(PyVector::from(into_vector::<u8>(arg)?)),
            DataType::UInt16 => args.push(PyVector::from(into_vector::<u16>(arg)?)),
            DataType::UInt32 => args.push(PyVector::from(into_vector::<u32>(arg)?)),
            DataType::UInt64 => args.push(PyVector::from(into_vector::<u64>(arg)?)),
            DataType::Int8 => args.push(PyVector::from(into_vector::<i8>(arg)?)),
            DataType::Int16 => args.push(PyVector::from(into_vector::<i16>(arg)?)),
            DataType::Int32 => args.push(PyVector::from(into_vector::<i32>(arg)?)),
            DataType::Int64 => args.push(PyVector::from(into_vector::<i64>(arg)?)),
            DataType::Boolean => {
                let v: VectorRef = Arc::new(BooleanVector::try_from_arrow_array(arg)?);
                args.push(PyVector::from(v))
            }
            _ => {
                return Err(Error::Other {
                    reason: format!(
                        "Unsupport data type at column {idx}: {:?} for coprocessor",
                        arg.data_type()
                    ),
                })
            }
        }
    }
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
    vm::Interpreter::without_stdlib(Default::default()).enter(
        |vm| -> Result<DfRecordBatch> {
            PyVector::make_class(&vm.ctx);
            let scope = vm.new_scope_with_builtins();
            for (name, vector) in copr.args.iter().zip(args) {
                scope
                    .locals
                    .as_object()
                    .set_item(name, vm.new_pyobj(vector), vm)
                    .map_err(|err|
                        Error::Other { reason: format!("fail to set item{} in python scope, PyExpection: {:?}", name, err) }
                    )?;
            }

            let code_obj = strip_append_and_compile(script, &copr)?;
            let code_obj = vm.ctx.new_code(code_obj);
            let run_res = vm.run_code_obj(code_obj, scope);
            let ret = run_res?;

            // convert a tuple of `PyVector` or one `PyVector` to a `Vec<ArrayRef>` then finailly to a `DfRecordBatch`
            // 5. get returns as either a PyVector or a PyTuple, and naming schema them according to `returns`
            let mut cols: Vec<ArrayRef> = Vec::new();
            if is_instance(&ret, PyTuple::class(vm).into(), vm) {
                let tuple = ret.payload::<PyTuple>().ok_or(
                Error::Other { reason: format!("can't cast obj {:?} to PyTuple)", ret) }
                )?;
                for obj in tuple {
                    if is_instance(obj, PyVector::class(vm).into(), vm) {
                        let pyv = obj.payload::<PyVector>().ok_or(
                            Error::Other { reason: format!("can't cast obj {:?} to PyVector", obj) }
                        )?;
                        cols.push(pyv.to_arrow_array())
                    } else {
                        return Err(Error::Other { reason: format!("Expect all element in returning tuple to be vector, found one of the element is {:?}", obj) })
                    }
                }
            } else if is_instance(&ret, PyVector::class(vm).into(), vm) {
                let pyv = ret.payload::<PyVector>().ok_or(
                    Error::Other { reason: format!("can't cast obj {:?} to PyVector", ret) }
                    )?;
                cols.push(pyv.to_arrow_array())
            }
            let schema = Arc::new(Schema::from(
                copr.returns
                    .iter()
                    .enumerate()
                    .map(|(idx,name)| {
                        let real_ty = cols[idx].data_type().to_owned();
                        let AnnotationInfo { datatype: ty, is_nullable , coerce_into: _} = copr.return_types[idx]
                        .to_owned()
                        .unwrap_or_else(||
                            // default to be not nullable and use DataType infered by PyVector itself
                            AnnotationInfo{
                                datatype: Some(real_ty.to_owned()),
                                is_nullable: false,
                                coerce_into: false
                            }
                        );
                        Field::new(
                            name,
                            // if type is like `_` or `_ | None`
                            if let Some(ty) = ty{ty}else{
                                real_ty
                            },
                            is_nullable
                        )
                    }
                    )
                    .collect::<Vec<Field>>(),
            ));
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
            // if cols and schema's data types is not match, first try coerced it to given type(if annotated), if not possible, return Err
            for ((col, field), anno) in cols.iter_mut().zip(&schema.fields).zip(copr.return_types){
                let real_ty = col.data_type();
                let anno_ty = field.data_type();
                if let Some(anno) = anno{
                    if real_ty != anno_ty{
                        if anno.coerce_into {
                            *col = arrow::compute::cast::cast(
                            col.as_ref(),
                            anno_ty,
                            CastOptions { wrapped: true, partial: true })?.into();
                        }else{
                        return Err(Error::Other {
                            reason: format!("Anntation type is {:?}, but real type is {:?}(Maybe add a `into()`?)", anno_ty, real_ty)
                            });
                        }
                    }
                    else{
                        continue;
                    }
                }
            }
            // 6. return a assembled DfRecordBatch
            let res_rb = DfRecordBatch::try_new(schema, cols)?;
            Ok(res_rb)
        },
    )
}
pub fn execute_script(script: &str) -> vm::PyResult {
    vm::Interpreter::without_stdlib(Default::default()).enter(|vm| {
        PyVector::make_class(&vm.ctx);
        let scope = vm.new_scope_with_builtins();
        let a: VectorRef = Arc::new(Int32Vector::from_vec(vec![1, 2, 3, 4]));
        let a = PyVector::from(a);
        let b: VectorRef = Arc::new(Float32Vector::from_vec(vec![1.2, 2.0, 3.4, 4.5]));
        let b = PyVector::from(b);
        scope
            .locals
            .as_object()
            .set_item("a", vm.new_pyobj(a), vm)
            .expect("failed");
        scope
            .locals
            .as_object()
            .set_item("b", vm.new_pyobj(b), vm)
            .expect("failed");

        let code_obj = vm
            .compile(
                script,
                vm::compile::Mode::BlockExpr,
                "<embedded>".to_owned(),
            )
            .map_err(|err| vm.new_syntax_error(&err))?;
        vm.run_code_obj(code_obj, scope)
    })
}

