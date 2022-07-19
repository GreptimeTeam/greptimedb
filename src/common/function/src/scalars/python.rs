//! python udf supports
//! use the function `coprocessor` to parse and run a python function with arguments from recordBatch, and return a newly assembled RecordBatch
use arrow::compute::cast::CastOptions;
use arrow::error::ArrowError;
use rustpython_vm as vm;
mod type_;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::record_batch::RecordBatch as DfRecordBatch;
use datatypes::vectors::*;
use datatypes::vectors::{BooleanVector, Float32Vector, Int32Vector};
use rustpython_bytecode::CodeObject;
use rustpython_compiler_core::{compile, error::CompileError};
use rustpython_parser::{
    ast,
    ast::{Located, Location},
    error::ParseError,
    parser,
};
use rustpython_vm::{class::PyClassImpl, AsObject};
use snafu::{ensure, prelude::Snafu};
use type_::{is_instance, PyVector};
use vm::builtins::{PyBaseExceptionRef, PyTuple};
use vm::PyPayload;
#[derive(Debug, PartialEq, Eq)]
struct Coprocessor {
    name: String,
    // get from python decorator args&returns
    args: Vec<String>,
    returns: Vec<String>,
    // get from python function args& returns' annotation, first is type, second is is_nullable
    arg_types: Vec<Option<AnnotationInfo>>,
    return_types: Vec<Option<AnnotationInfo>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AnnotationInfo {
    datatype: DataType,
    is_nullable: bool,
    /// if the result type need to be coerced to given type in `into(<datatype>)`
    coerce_into: bool,
}

#[derive(Debug, Snafu)]
pub enum CoprError {
    TypeCast {
        error: datatypes::error::Error,
    },
    PyParse {
        error: ParseError,
    },
    PyCompile {
        error: CompileError,
    },
    PyRuntime {
        error: PyBaseExceptionRef,
    },
    ArrowError {
        error: ArrowError,
    },
    /// errors in coprocessors' parse check for types and etc.
    CoprParse {
        reason: String,
        // location is option because maybe errors can't give a clear location?
        loc: Option<Location>,
    },
    /// Other types of error that isn't any of above
    Other {
        reason: String,
    },
}

// impl from for those error so one can use question mark and implictly cast into `CoprError`
impl From<ArrowError> for CoprError {
    fn from(error: ArrowError) -> Self {
        Self::ArrowError { error }
    }
}
impl From<PyBaseExceptionRef> for CoprError {
    fn from(err: PyBaseExceptionRef) -> Self {
        Self::PyRuntime { error: err }
    }
}
impl From<ParseError> for CoprError {
    fn from(e: ParseError) -> Self {
        Self::PyParse { error: e }
    }
}
impl From<datatypes::error::Error> for CoprError {
    fn from(e: datatypes::error::Error) -> Self {
        Self::TypeCast { error: e }
    }
}
/// for there is multiple CompilerError struct in different crate of `rustpython`]
///（`rustpython_compiler` & `rustpython_compiler_core` both have a `CompilerError` struct) so use full path to differentiate
impl From<rustpython_compiler_core::error::CompileError> for CoprError {
    fn from(err: rustpython_compiler_core::error::CompileError) -> Self {
        Self::PyCompile { error: err }
    }
}
/// turn a python list of string in ast form(a `ast::Expr`) of string into a `Vec<String>`
fn pylist_to_vec(lst: &ast::Expr<()>) -> Result<Vec<String>, CoprError> {
    if let ast::ExprKind::List { elts, ctx: _ } = &lst.node {
        let mut ret = Vec::new();
        for s in elts {
            if let ast::ExprKind::Constant {
                value: ast::Constant::Str(v),
                kind: _,
            } = &s.node
            {
                ret.push(v.to_owned())
            } else {
                return Err(CoprError::CoprParse {
                    reason: format!(
                        "Expect a list of String, found {:?} in list element",
                        &s.node
                    ),
                    loc: Some(lst.location),
                });
            }
        }
        Ok(ret)
    } else {
        Err(CoprError::CoprParse {
            reason: format!("Expect a list, found {:?}", &lst.node),
            loc: Some(lst.location),
        })
    }
}

fn into_datatype(ty: &str) -> Option<DataType> {
    match ty {
        "u8" => Some(DataType::UInt8),
        "u16" => Some(DataType::UInt16),
        "u32" => Some(DataType::UInt32),
        "u64" => Some(DataType::UInt64),
        "i8" => Some(DataType::Int8),
        "i16" => Some(DataType::Int16),
        "i32" => Some(DataType::Int32),
        "i64" => Some(DataType::Int64),
        "f16" => Some(DataType::Float16),
        "f32" => Some(DataType::Float32),
        "f64" => Some(DataType::Float64),
        _ => None,
    }
}

/// return AnnotationInfo with is_nullable and need_coerced both set to false
fn parse_type(node: &ast::Expr<()>) -> Result<AnnotationInfo, CoprError> {
    match &node.node {
        ast::ExprKind::Name { id, ctx: _ } => {
            let ty = into_datatype(id).ok_or(CoprError::CoprParse {
                reason: format!("unknown type: {id}"),
                loc: Some(node.location),
            })?;
            Ok(AnnotationInfo {
                datatype: ty,
                is_nullable: false,
                coerce_into: false,
            })
        }
        _ => Err(CoprError::CoprParse {
            reason: format!("Expect a type's name, found {:?}", node),
            loc: Some(node.location),
        }),
    }
}

/// Item => NativeType | into `(` NativeType `)`
fn parse_item(sub: &ast::Expr<()>) -> Result<AnnotationInfo, CoprError> {
    match &sub.node {
        ast::ExprKind::Name { id: _, ctx: _ } => Ok(parse_type(sub)?),
        ast::ExprKind::Call {
            func,
            args,
            keywords: _,
        } => {
            let need_coerced = if let ast::ExprKind::Name { id, ctx: _ } = &func.node {
                ensure!(
                    id.as_str() == "into",
                    CoprParseSnafu {
                        reason: format!(
                            "Expect only `into(datatype)` or datatype or `None`, found {id}"
                        ),
                        loc: Some(sub.location)
                    }
                );
                true
            } else {
                todo!()
            };
            ensure!(
                args.len() == 1,
                CoprParseSnafu {
                    reason: "Expect only one arguement for `into`",
                    loc: Some(sub.location)
                }
            );
            let mut anno = parse_type(&args[0])?;
            anno.coerce_into = need_coerced;
            Ok(anno)
        }
        _ => todo!(),
    }
}

/// where:
///
/// Start => vector`[`TYPE`]`
///
/// TYPE => Item | Item `|` None
///
/// Item => NativeType | into(NativeType)
fn parse_annotation(sub: &ast::ExprKind) -> Result<AnnotationInfo, CoprError> {
    if let ast::ExprKind::Subscript {
        value,
        slice,
        ctx: _,
    } = sub
    {
        if let ast::ExprKind::Name { id, ctx: _ } = &value.node {
            ensure!(
                id == "vector",
                CoprParseSnafu {
                    reason: format!("Wrong type annotation, expect `vector[...]`, found {}", id),
                    loc: Some(value.location)
                }
            )
        } else {
            todo!()
        }
        // i.e: vector[f64]
        match &slice.node {
            ast::ExprKind::Name { id: _, ctx: _ } => parse_item(slice),
            ast::ExprKind::Call {
                func: _,
                args: _,
                keywords: _,
            } => parse_item(slice),
            ast::ExprKind::BinOp { left, op: _, right } => {
                let mut is_nullable = false;
                let mut tmp_anno = None;
                for i in [left, right] {
                    match &i.node {
                        ast::ExprKind::Constant { value, kind: _ } => {
                            ensure!(
                                matches!(value, ast::Constant::None),
                                CoprParseSnafu {
                                    reason: format!(
                                        "Expect only typenames and `None`, found{:?}",
                                        i.node
                                    ),
                                    loc: Some(i.location)
                                }
                            );
                            is_nullable = true;
                        }
                        ast::ExprKind::Name { id: _, ctx: _ } => {
                            if tmp_anno.is_none() {
                                tmp_anno = Some(parse_item(i)?)
                            } else {
                                todo!()
                            }
                        }
                        ast::ExprKind::Call {
                            func: _,
                            args: _,
                            keywords: _,
                        } => {
                            if tmp_anno.is_none() {
                                tmp_anno = Some(parse_item(i)?)
                            } else {
                                todo!()
                            }
                        }
                        _ => todo!(),
                    }
                }
                ensure!(
                    tmp_anno.is_some(),
                    CoprParseSnafu {
                        reason: "Expect type, not two `None`",
                        loc: Some(slice.location)
                    }
                );
                let mut tmp_anno = tmp_anno.unwrap();
                tmp_anno.is_nullable = is_nullable;
                Ok(tmp_anno)
            }
            _ => todo!(),
        }
    } else {
        todo!()
    }
}
/// parse script and return `Coprocessor` struct with info extract from ast
fn parse_copr(script: &str) -> Result<Coprocessor, CoprError> {
    let python_ast = parser::parse_program(script)?;
    ensure!(
        python_ast.len() == 1,
        CoprParseSnafu {
            reason:
                "Expect one and only one python function with `@coprocessor` or `@cpor` decorator"
                    .to_string(),
            loc: if python_ast.is_empty() {
                None
            } else {
                Some(python_ast[0].location)
            }
        }
    );
    if let ast::StmtKind::FunctionDef {
        name,
        args: fn_args,
        body: _,
        decorator_list,
        returns,
        type_comment: _,
    } = &python_ast[0].node
    {
        //ensure!(args.len() == 2, FormatSnafu{ reason: "Expect two arguments: `args` and `returns`"})
        ensure!(
            decorator_list.len() == 1,
            CoprParseSnafu {
                reason: "Expect one decorator",
                loc: if decorator_list.is_empty() {
                    None
                } else {
                    Some(decorator_list[0].location)
                }
            }
        );
        let decorator = &decorator_list[0];
        if let ast::ExprKind::Call {
            func,
            args,
            keywords,
        } = &decorator.node
        {
            ensure!(
                func.node
                    == ast::ExprKind::Name {
                        id: "copr".to_string(),
                        ctx: ast::ExprContext::Load
                    }
                    || func.node
                        == ast::ExprKind::Name {
                            id: "coprocessor".to_string(),
                            ctx: ast::ExprContext::Load
                        },
                CoprParseSnafu {
                    reason: "Expect decorator with name `copr` or `coprocessor`",
                    loc: Some(func.location)
                }
            );
            ensure!(
                args.is_empty() && keywords.len() == 2,
                CoprParseSnafu {
                    reason: "Expect two keyword argument of `args` and `returns`",
                    loc: Some(func.location)
                }
            );
            let mut arg_names = None;
            let mut ret_names = None;
            for kw in keywords {
                match &kw.node.arg {
                    Some(s) => {
                        let s = s.as_str();
                        match s {
                            "args" => {
                                if arg_names.is_none() {
                                    arg_names = Some(pylist_to_vec(&kw.node.value))
                                } else {
                                    return Err(CoprError::CoprParse {
                                        reason: "`args` occur multiple times in decorator's arguements' list.".to_string(),
                                        loc: Some(kw.location)
                                    });
                                }
                            }
                            "returns" => {
                                if ret_names.is_none() {
                                    ret_names = Some(pylist_to_vec(&kw.node.value))
                                } else {
                                    return Err(CoprError::CoprParse {
                                        reason: "`returns` occur multiple times in decorator's arguements' list.".to_string(),
                                        loc: Some(kw.location)
                                    });
                                }
                            }
                            _ => {
                                return Err(CoprError::CoprParse {
                                    reason: format!("Expect `args` or `returns`, found `{}`", s),
                                    loc: Some(kw.location),
                                })
                            }
                        }
                    }
                    None => {
                        return Err(CoprError::CoprParse {
                            reason: format!(
                                "Expect explictly set both `args` and `returns`, found {:?}",
                                &kw.node
                            ),
                            loc: Some(kw.location),
                        })
                    }
                }
            }
            let arg_names = if let Some(args) = arg_names {
                args?
            } else {
                return Err(CoprError::CoprParse {
                    reason: "Expect `args` keyword".to_string(),
                    loc: Some(decorator.location),
                });
            };
            let ret_names = if let Some(rets) = ret_names {
                rets?
            } else {
                return Err(CoprError::CoprParse {
                    reason: "Expect `rets` keyword".to_string(),
                    loc: Some(decorator.location),
                });
            };
            // get arg types from type annotation
            let mut arg_types = Vec::new();
            for arg in &fn_args.args {
                if let Some(anno) = &arg.node.annotation {
                    arg_types.push(Some(parse_annotation(&anno.node)?))
                } else {
                    arg_types.push(None)
                }
            }

            // get return types from type annotation
            let mut return_types = Vec::new();
            if let Some(rets) = returns {
                if let ast::ExprKind::Tuple { elts, ctx: _ } = &rets.node {
                    for elem in elts {
                        return_types.push(Some(parse_annotation(&elem.node)?))
                    }
                } else if let ast::ExprKind::Subscript {
                    value: _,
                    slice: _,
                    ctx: _,
                } = &rets.node
                {
                    return_types.push(Some(parse_annotation(&rets.node)?))
                }
            } else {
                // if no anntation at all, set it to all None
                return_types.resize(ret_names.len(), None)
            }
            ensure!(
                arg_names.len() == arg_types.len(),
                CoprParseSnafu {
                    reason: format!(
                        "args number in decorator({}) and function({}) doesn't match",
                        arg_names.len(),
                        arg_types.len()
                    ),
                    loc: None
                }
            );
            ensure!(
                ret_names.len() == return_types.len(),
                CoprParseSnafu {
                    reason: format!(
                        "returns number in decorator( {} ) and function annotation( {} ) doesn't match",
                        ret_names.len(),
                        return_types.len()
                    ),
                    loc: None
                }
            );
            Ok(Coprocessor {
                name: name.to_string(),
                args: arg_names,
                returns: ret_names,
                arg_types,
                return_types,
            })
        } else {
            Err(CoprError::CoprParse {
                reason: format!(
                    "Expect decorator to be a function call(like `@copr(...)`), found {:?}",
                    decorator.node
                ),
                loc: Some(decorator.location),
            })
        }
    } else {
        Err(CoprError::CoprParse {
            reason: format!(
                "Expect a function definition, found a {:?}",
                &python_ast[0].node
            ),
            loc: Some(python_ast[0].location),
        })
    }
}

fn default_loc<T>(node: T) -> Located<T> {
    Located::new(Location::new(0, 0), node)
}
fn set_loc<T>(node: T, loc: Location) -> Located<T> {
    Located::new(loc, node)
}
/// stripe the decorator(`@xxxx`) and type annotation(for type checker is done in rust function), add one line in the ast for call function with given parameter, and compiler into `CodeObject`
///
/// The conside is that rustpython's vm is not very efficient according to [offical benchmark](https://rustpython.github.io/benchmarks),
/// So we should avoid running too much Python Bytecode, hence in this function we delete `@` decorator(instead of actually write a decorator in python)
/// And add a function call in the end
/// strip type annotation
fn strip_append_and_compile(script: &str, copr: &Coprocessor) -> Result<CodeObject, CoprError> {
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
            return Err(CoprError::CoprParse {
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
            value: Box::new(default_loc(func)),
        };
        code.push(default_loc(stmt));
    } else {
        return Err(CoprError::CoprParse {
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
) -> Result<Arc<dyn Vector>, datatypes::error::Error> {
    PrimitiveVector::<T>::try_from_arrow_array(arg).map(|op| Arc::new(op) as Arc<dyn Vector>)
}

/// The coprocessor function
/// first it extract columns according to `args` given in python decorator from [`DfRecordBatch`]
///
/// then execute python script given those `args`, return a tuple([`PyTuple`]) or one ([`PyVector`])
/// the return vectors
///
/// in the end those vector is rename according to `returns` in decorator and form a new [`DfRecordBatch`] and return it
///
/// # Example
///
/// ```rust
/// let python_source = r#"
/// @copr(args=["cpu", "mem"], returns=["perf", "what"])
/// def a(cpu, mem):
///     return cpu + mem, cpu - mem
/// "#;
/// let cpu_array = PrimitiveArray::from_slice([0.9f32, 0.8, 0.7, 0.6]);
/// let mem_array = PrimitiveArray::from_slice([0.1f64, 0.2, 0.3, 0.4]);
/// let schema = Arc::new(Schema::from(vec![
/// Field::new("cpu", DataType::Float32, false),
///  Field::new("mem", DataType::Float64, false),
/// ]));
/// let rb =
/// DfRecordBatch::try_new(schema, vec![Arc::new(cpu_array), Arc::new(mem_array)]).unwrap();
/// let ret = coprocessor(python_source, rb).unwrap();
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
/// a example given below:
/// ```python
/// @copr(args=["cpu", "mem"], returns=["perf", "minus", "mul", "div"])
/// def a(cpu: vector[f32], mem: vector[f64])->(vector[into(f64)|None], vector[into(f64)], vector[f64], vector[f64 | None]):
///     return cpu + mem, cpu - mem, cpu * mem, cpu / mem
/// ```
pub fn coprocessor(script: &str, rb: DfRecordBatch) -> Result<DfRecordBatch, CoprError> {
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
                return Err(CoprError::Other {
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
                .map(|v| v.datatype == real_ty && v.is_nullable == is_nullable)
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
        |vm| -> Result<DfRecordBatch, CoprError> {
            PyVector::make_class(&vm.ctx);
            let scope = vm.new_scope_with_builtins();
            for (name, vector) in copr.args.iter().zip(args) {
                scope
                    .locals
                    .as_object()
                    .set_item(name, vm.new_pyobj(vector), vm)
                    .map_err(|err|
                        CoprError::Other { reason: format!("fail to set item{} in python scope, PyExpection: {:?}", name, err) }
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
                CoprError::Other { reason: format!("can't cast obj {:?} to PyTuple)", ret) }
                )?;
                for obj in tuple {
                    if is_instance(obj, PyVector::class(vm).into(), vm) {
                        let pyv = obj.payload::<PyVector>().ok_or(
                            CoprError::Other { reason: format!("can't cast obj {:?} to PyVector", obj) }
                        )?;
                        cols.push(pyv.to_arrow_array())
                    } else {
                        return Err(CoprError::Other { reason: format!("Expect all element in returning tuple to be vector, found one of the element is {:?}", obj) })
                    }
                }
            } else if is_instance(&ret, PyVector::class(vm).into(), vm) {
                let pyv = ret.payload::<PyVector>().ok_or(
                    CoprError::Other { reason: format!("can't cast obj {:?} to PyVector", ret) }
                    )?;
                cols.push(pyv.to_arrow_array())
            }
            let schema = Arc::new(Schema::from(
                copr.returns
                    .iter()
                    .enumerate()
                    .map(|(idx,name)| {
                        let AnnotationInfo { datatype: ty, is_nullable , coerce_into: _} = copr.return_types[idx]
                        .to_owned()
                        .unwrap_or_else(||
                            // default to be not nullable and use DataType infered by PyVector
                            AnnotationInfo{
                                datatype: cols[idx].data_type().to_owned(),
                                is_nullable: false,
                                coerce_into: false
                            }
                        );
                        Field::new(
                            name,
                            ty,
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
                    if anno.coerce_into && real_ty != anno_ty{
                        *col = arrow::compute::cast::cast(
                            col.as_ref(),
                            anno_ty,
                            CastOptions { wrapped: true, partial: true })?.into();
                    }else{
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::PrimitiveArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::record_batch::RecordBatch as DfRecordBatch;

    use super::*;
    type PredicateFn = Option<fn(Result<Coprocessor, CoprError>) -> bool>;
    #[test]
    fn testsuite_parse() {
        let testcases: Vec<(&'static str, PredicateFn)> = vec![
            (
                // for correct parse
                r#"
@copr(args=["cpu", "mem"], returns=["perf", "what", "how", "why"])
def a(cpu: vector[f32], mem: vector[f64])->(vector[into(f64)|None], vector[into(f64)], vector[f64], vector[f64 | None]):
    return cpu + mem, cpu - mem, cpu * mem, cpu / mem
"#,
                Some(|r| {
                    //dbg!(&r);
                    r.is_ok()
                        && r.unwrap()
                            == Coprocessor {
                                name: "a".into(),
                                args: vec!["cpu".into(), "mem".into()],
                                returns: vec![
                                    "perf".into(),
                                    "what".into(),
                                    "how".into(),
                                    "why".into(),
                                ],
                                arg_types: vec![
                                    Some(AnnotationInfo {
                                        datatype: DataType::Float32,
                                        is_nullable: false,
                                        coerce_into: false,
                                    }),
                                    Some(AnnotationInfo {
                                        datatype: DataType::Float64,
                                        is_nullable: false,
                                        coerce_into: false,
                                    }),
                                ],
                                return_types: vec![
                                    Some(AnnotationInfo {
                                        datatype: DataType::Float64,
                                        is_nullable: true,
                                        coerce_into: true,
                                    }),
                                    Some(AnnotationInfo {
                                        datatype: DataType::Float64,
                                        is_nullable: false,
                                        coerce_into: true,
                                    }),
                                    Some(AnnotationInfo {
                                        datatype: DataType::Float64,
                                        is_nullable: false,
                                        coerce_into: false,
                                    }),
                                    Some(AnnotationInfo {
                                        datatype: DataType::Float64,
                                        is_nullable: true,
                                        coerce_into: false,
                                    }),
                                ],
                            }
                }),
            ),
            (
                // missing decrator
                r#"
def a(cpu: vector[f32], mem: vector[f64])->(vector[into(f64)|None], vector[into(f64)], vector[f64], vector[f64 | None]):
    return cpu + mem, cpu - mem, cpu * mem, cpu / mem
"#,
                Some(|r| {
                    //dbg!(&r);
                    r.is_err()
                        && if let CoprError::CoprParse { reason, loc } = r.unwrap_err() {
                            reason == "Expect one decorator" && loc == None
                        } else {
                            false
                        }
                }),
            ),
        ];

        for (script, predicate) in testcases {
            let copr = parse_copr(script);
            if let Some(predicate) = predicate {
                assert!(predicate(copr));
            }
        }
    }

    #[test]
    #[allow(unused)]
    fn test_type_anno() {
        let python_source = r#"
@copr(args=["cpu", 3], returns=["perf", "what", "how", "why"])
def a(cpu: vector[f32], mem: vector[f64])->(vector[into(f64)|None], vector[into(f64)], vector[f64], vector[f64 | None]):
    return cpu + mem, cpu - mem, cpu * mem, cpu / mem
"#;
        let pyast = parser::parse(python_source, parser::Mode::Interactive).unwrap();
        //dbg!(pyast);
        let copr = parse_copr(python_source);
        dbg!(&copr);
        assert!(copr.is_ok());
    }
    #[test]
    fn test_execute_script() {
        let python_source = "
def a(a: int,b: int)->int:
    return 1
a(2,3)
";
        let result = execute_script(python_source);
        assert!(result.is_ok());
    }

    #[test]
    #[allow(clippy::print_stdout)]
    // allow print in test function for debug purpose
    fn test_coprocessor() {
        let python_source = r#"
@copr(args=["cpu", "mem"], returns=["perf", "what"])
def a(cpu: vector[f32], mem: vector[f64])->(vector[f64|None], vector[into(f32)]):
    return cpu + mem, cpu - mem
"#;
        //println!("{}, {:?}", python_source, python_ast);
        let cpu_array = PrimitiveArray::from_slice([0.9f32, 0.8, 0.7, 0.6]);
        let mem_array = PrimitiveArray::from_slice([0.1f64, 0.2, 0.3, 0.4]);
        let schema = Arc::new(Schema::from(vec![
            Field::new("cpu", DataType::Float32, false),
            Field::new("mem", DataType::Float64, false),
        ]));
        let rb =
            DfRecordBatch::try_new(schema, vec![Arc::new(cpu_array), Arc::new(mem_array)]).unwrap();
        let ret = coprocessor(python_source, rb);
        dbg!(&ret);
        assert!(ret.is_ok());
        assert_eq!(ret.unwrap().column(0).len(), 4);
    }
}
