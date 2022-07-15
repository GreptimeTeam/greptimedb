//! python udf supports
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
#[derive(Debug)]
struct Coprocessor {
    name: String,
    args: Vec<String>,
    returns: Vec<String>,
}

#[derive(Debug, Snafu)]
pub enum CoprError {
    TypeCast { error: datatypes::error::Error },
    PyParse { error: ParseError },
    PyCompile { error: CompileError },
    PyRuntime { error: PyBaseExceptionRef },
    ArrowError { error: ArrowError },
    // Consider change to `Whatever`
    Other { reason: String },
}
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
// for there is multiple CompilerError struct in different crate of `rustpython` so use full path to diff
impl From<rustpython_compiler_core::error::CompileError> for CoprError {
    fn from(err: rustpython_compiler_core::error::CompileError) -> Self {
        Self::PyCompile { error: err }
    }
}
/// turn a python list of string into a Vec<String>
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
                return Err(CoprError::Other {
                    reason: format!(
                        "Expect a list of String, found {:?} in list element",
                        &s.node
                    ),
                });
            }
        }
        Ok(ret)
    } else {
        Err(CoprError::Other {
            reason: format!("Expect a list, found {:?}", &lst.node),
        })
    }
}

/// TODO: a lot of error handling, write function body
fn parse_copr(script: &str) -> Result<Coprocessor, CoprError> {
    let python_ast = parser::parse_program(script)?;
    ensure!(
        python_ast.len() == 1,
        OtherSnafu {
            reason:
                "Expect one and only one python function with `@coprocessor` or `@cpor` decorator"
                    .to_string()
        }
    );
    if let ast::StmtKind::FunctionDef {
        name,
        args: _,
        body: _,
        decorator_list,
        returns: __file__,
        type_comment: _,
    } = &python_ast[0].node
    {
        //ensure!(args.len() == 2, FormatSnafu{ reason: "Expect two arguments: `args` and `returns`"})
        ensure!(
            decorator_list.len() == 1,
            OtherSnafu {
                reason: "Expect one decorator"
            }
        );
        if let ast::ExprKind::Call {
            func,
            args,
            keywords,
        } = &decorator_list[0].node
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
                OtherSnafu {
                    reason: "Expect decorator with name `copr` or `coprocessor`"
                }
            );
            ensure!(
                args.is_empty() && keywords.len() == 2,
                OtherSnafu {
                    reason: "Expect two keyword argument of `args` and `returns`"
                }
            );
            let mut args = None;
            let mut rets = None;
            for kw in keywords {
                match &kw.node.arg {
                    Some(s) => {
                        let s = s.as_str();
                        match s {
                            "args" => {
                                if args.is_none() {
                                    args = Some(pylist_to_vec(&kw.node.value))
                                } else {
                                    return Err(CoprError::Other { reason: "`args` occur multiple times in decorator's parameter list.".to_string() });
                                }
                            }
                            "returns" => {
                                if rets.is_none() {
                                    rets = Some(pylist_to_vec(&kw.node.value))
                                } else {
                                    return Err(CoprError::Other { reason: "`returns` occur multiple times in decorator's parameter list.".to_string() });
                                }
                            }
                            _ => {
                                return Err(CoprError::Other {
                                    reason: format!("Expect `args` or `returns`, found `{}`", s),
                                })
                            }
                        }
                    }
                    None => {
                        return Err(CoprError::Other {
                            reason: format!(
                                "Expect explictly set both `args` and `returns`, found {:?}",
                                &kw.node
                            ),
                        })
                    }
                }
            }
            let args = if let Some(args) = args {
                args?
            } else {
                return Err(CoprError::Other {
                    reason: "Expect `args` keyword".to_string(),
                });
            };
            let rets = if let Some(rets) = rets {
                rets?
            } else {
                return Err(CoprError::Other {
                    reason: "Expect `rets` keyword".to_string(),
                });
            };
            Ok(Coprocessor {
                name: name.to_string(),
                args,
                returns: rets,
            })
        } else {
            Err(CoprError::Other {
                reason: format!(
                    "Expect decorator to be a function call(like `@copr(...)`), found {:?}",
                    &decorator_list[0].node
                ),
            })
        }
    } else {
        Err(CoprError::Other {
            reason: format!(
                "Expect a function definition, found a {:?}",
                &python_ast[0].node
            ),
        })
    }
}

fn default_loc<T>(node: T) -> Located<T> {
    Located::new(Location::new(0, 0), node)
}
fn set_loc<T>(node: T, loc: Location) -> Located<T> {
    Located::new(loc, node)
}
/// stripe the decorator(`@xxxx`), add one line in the ast(note ) for call function with given parameter, and compiler into `CodeObject`
fn strip_append_and_compile(script: &str, copr: &Coprocessor) -> Result<CodeObject, CoprError> {
    let mut top = parser::parse(script, parser::Mode::Interactive)?;
    // erase decorator
    if let ast::Mod::Interactive { body } = &mut top {
        let code = body;
        ensure!(
            code.len() == 1,
            OtherSnafu {
                reason: format!(
                    "Expect only one statement in script, found {} statement",
                    code.len()
                )
            }
        );
        if let ast::StmtKind::FunctionDef {
            name: _,
            args: _,
            body: _,
            decorator_list,
            returns: _,
            type_comment: __main__,
        } = &mut code[0].node
        {
            *decorator_list = Vec::new();
        } else {
            return Err(CoprError::Other { reason: format!("Expect the one and only statement in script as a function def, but instead found: {:?}", code[0].node) });
        }
        let mut loc = code[0].location;
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
        return Err(CoprError::Other {
            reason: format!("Expect statement in script, found: {:?}", top),
        });
    }
    compile::compile_top(
        &top,
        "<embedded>".to_owned(),
        compile::Mode::BlockExpr,
        compile::CompileOpts { optimize: 0 },
    )
    .map_err(|err| err.into())
}
/// cast a dyn Array into a dyn Vector
fn into_vector<T: datatypes::types::Primitive + datatypes::types::DataTypeBuilder>(
    arg: Arc<dyn Array>,
) -> Result<Arc<dyn Vector>, datatypes::error::Error> {
    PrimitiveVector::<T>::try_from_arrow_array(arg).map(|op| Arc::new(op) as Arc<dyn Vector>)
}

// TODO: it seem it returns None because it is in `exec` mode?
pub fn coprocessor(script: &str, rb: DfRecordBatch) -> Result<DfRecordBatch, CoprError> {
    // 1. parse the script and check if it's only a function with `@coprocessor` decorator, and get `args` and `returns`,
    // 2. also check for exist of `args` in `rb`, if not found, return error
    let copr = parse_copr(script)?;
    let code_obj = strip_append_and_compile(script, &copr)?;
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
    // PyVector now support int8/16/32/64, float32/64 and bool
    let mut args: Vec<PyVector> = Vec::new();

    for arg in fetch_args {
        match arg.data_type() {
            DataType::Float32 => args.push(PyVector::from(into_vector::<f32>(arg)?)),
            DataType::Float64 => args.push(PyVector::from(into_vector::<f64>(arg)?)),
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
                    reason: format!("Unsupport data type {:?} for coprocessor", arg.data_type()),
                })
            }
        }
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
            let code_obj = vm.ctx.new_code(code_obj);
            let run_res = vm.run_code_obj(code_obj, scope);
            let ret = run_res?;

            let schema = Arc::new(Schema::from(
                copr.returns
                    .iter()
                    .map(|name| Field::new(name, DataType::Float64, false))
                    .collect::<Vec<Field>>(),
            ));
            let mut cols: Vec<ArrayRef> = Vec::new();
            //let mut rb_ret = DfRecordBatch::new_empty(schema);
            if is_instance(&ret, PyTuple::class(vm).into(), vm) {
                let tuple = ret.payload::<PyTuple>().ok_or(
                CoprError::Other { reason: format!("can't cast obj {:?} to tuple", ret) }
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
            let res_rb = DfRecordBatch::try_new(schema, cols)?;
            Ok(res_rb)
        },
    )
    // 5. get returns as either a PyVector or a PyTuple, and assign them according to `returns`
    // 6. return a assembled DfRecordBatch
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

    #[test]
    fn test_execute_script() {
        let python_source = "
def a(a,b):
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
def a(cpu, mem):
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
        println!("{:?}", ret);
        assert!(ret.is_ok());
    }
}
