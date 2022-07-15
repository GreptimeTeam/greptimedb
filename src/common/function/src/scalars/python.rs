//! python udf supports
use rustpython_vm as vm;
mod type_;
use std::sync::Arc;

use datatypes::vectors::*;
use rustpython_vm::{class::PyClassImpl, AsObject};
use type_::PyVector;

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
            .map(|obj| {
                dbg!(&obj.instructions);
                obj
            })
            .map_err(|err| vm.new_syntax_error(&err))?;
        vm.run_code_obj(code_obj, scope)
    })
}

#[cfg(test)]
#[allow(unused)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        Array, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
        Int8Array, PrimitiveArray,
    };
    use arrow::chunk::Chunk;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::types::NativeType;
    use datafusion_common::record_batch::RecordBatch as DfRecordBatch;
    use datatypes::vectors::{
        BooleanVector, Float32Vector, Float64Vector, Int16Vector, Int32Vector, Int64Vector,
        Int8Vector,
    };
    use rustpython_bytecode::CodeObject;
    use rustpython_compiler_core::{compile, error::CompileError};
    use rustpython_parser::{
        ast,
        ast::{Located, Location},
        error::ParseError,
        parser,
    };
    use snafu::{ensure, prelude::Snafu, OptionExt, ResultExt};

    #[derive(Debug)]
    struct Coprocessor {
        name: String,
        args: Vec<String>,
        returns: Vec<String>,
    }

    #[derive(Debug, Snafu)]
    pub enum CoprError {
        Format { reason: String },
        TypeCast { error: datatypes::error::Error },
        PyParse { error: ParseError },
        PyCompile { error: CompileError },
        PyRuntime,
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
    /// TODO: a lot of error handling
    fn pylist_to_vec(lst: &ast::Expr<()>) -> Result<Vec<String>, CoprError> {
        if let ast::ExprKind::List { elts, ctx: _ } = &lst.node {
            let mut ret = Vec::new();
            for s in elts {
                if let ast::ExprKind::Constant { value, kind } = &s.node {
                    if let ast::Constant::Str(v) = value {
                        ret.push(v.to_owned())
                    } else {
                        todo!()
                    }
                } else {
                    todo!()
                }
            }
            Ok(ret)
        } else {
            todo!()
        }
    }

    /// TODO: a lot of error handling, write function body
    fn parse_copr(script: &str) -> Result<Coprocessor, CoprError> {
        let python_ast = parser::parse_program(script).unwrap();
        ensure!(python_ast.len()==1, FormatSnafu { reason: "Expect one and only one python function with `@coprocessor` or `@cpor` decorator".to_string()});
        if let ast::StmtKind::FunctionDef {
            name,
            args,
            body,
            decorator_list,
            returns,
            type_comment,
        } = &python_ast[0].node
        {
            //ensure!(args.len() == 2, FormatSnafu{ reason: "Expect two arguments: `args` and `returns`"})
            ensure!(
                decorator_list.len() == 1,
                FormatSnafu {
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
                    FormatSnafu {
                        reason: "Expect decorator with name `copr` or `coprocessor`"
                    }
                );
                ensure!(
                    args.is_empty() && keywords.len() == 2,
                    FormatSnafu {
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
                                "args" => args = Some(pylist_to_vec(&kw.node.value)),
                                "returns" => rets = Some(pylist_to_vec(&kw.node.value)),
                                _ => todo!(),
                            }
                        }
                        _ => todo!(),
                    }
                }
                return Ok(Coprocessor {
                    name: name.to_string(),
                    args: args.unwrap()?,
                    returns: rets.unwrap()?,
                });
            }
        } else {
            ensure!(
                false,
                FormatSnafu {
                    reason: "Expect a function definition."
                }
            )
        }
        todo!()
    }

    fn default_loc<T>(node: T) -> Located<T> {
        Located::new(Location::new(0, 0), node)
    }
    fn set_loc<T>(node: T, loc: Location) -> Located<T> {
        Located::new(loc, node)
    }
    // stripe the decorator, add one line for call function with given parameter, and compiler into `CodeObject`
    fn strip_append_and_compile(
        script: &str,
        copr: &Coprocessor,
    ) -> Result<CodeObject, CompileError> {
        let mut top = parser::parse(script, parser::Mode::Interactive).unwrap();
        dbg!(&top);
        // erase decorator
        if let ast::Mod::Interactive { body } = &mut top {
            let mut code = body;
            if let ast::StmtKind::FunctionDef {
                name,
                args,
                body,
                decorator_list,
                returns,
                type_comment,
            } = &mut code[0].node
            {
                *decorator_list = Vec::new();
            } else {
                todo!()
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
            todo!()
        }

        dbg!(&top);
        compile::compile_top(
            &top,
            "<embedded>".to_owned(),
            compile::Mode::BlockExpr,
            compile::CompileOpts { optimize: 0 },
        )
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
        dbg!(&code_obj.instructions);
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
                DataType::Int32 => args.push(PyVector::from(into_vector::<i32>(arg)?)),
                DataType::Boolean => {
                    let v: VectorRef = Arc::new(BooleanVector::try_from_arrow_array(arg)?);
                    args.push(PyVector::from(v))
                }
                _ => {
                    return Err(CoprError::Format {
                        reason: format!(
                            "Unsupport data type {:?} for coprocessor",
                            arg.data_type()
                        ),
                    })
                }
            }
        }
        // 4. then `vm.invoke` function with given args, or maybe scope set item? and just call
        let run_res = vm::Interpreter::without_stdlib(Default::default()).enter(|vm| {
            PyVector::make_class(&vm.ctx);
            let scope = vm.new_scope_with_builtins();
            for (name, vector) in copr.args.iter().zip(args) {
                scope
                    .locals
                    .as_object()
                    .set_item(name, vm.new_pyobj(vector), vm)
                    .unwrap_or_else(|_| panic!("fail to set item{} in python scope", name));
            }
            let code_obj = vm.ctx.new_code(code_obj);
            vm.run_code_obj(code_obj, scope)
        });
        dbg!(run_res);
        // 5. get returns as a PyList, and assign them according to `returns`
        // 6. return a assembled DfRecordBatch
        Ok(rb)
    }
    use super::*;

    #[test]
    fn test_execute_script() {
        let python_source = "
def a(a,b):
    return 1
a(2,3)
";
        let result = execute_script(python_source);
        dbg!(result);
        //assert!(result.is_ok());
        let python_ast = parser::parse_program(python_source).unwrap();
        dbg!(python_ast);
    }

    #[test]
    fn test_coprocessor() {
        let python_source = r#"
@copr(args=["cpu", "mem"], returns=["perf"])
def a(cpu, mem):
    return 2
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
        dbg!(coprocessor(python_source, rb));
    }
}
