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
            .map_err(|err| vm.new_syntax_error(&err))?;
        vm.run_code_obj(code_obj, scope)
    })
}

#[cfg(test)]
#[allow(unused)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, PrimitiveArray};
    use arrow::chunk::Chunk;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::record_batch::RecordBatch as DfRecordBatch;
    use rustpython_parser::{ast, error::ParseError, parser};
    use snafu::{ensure, prelude::Snafu, OptionExt, ResultExt};
    use datatypes::vectors::{Float32Vector, Float64Vector, Int16Vector, Int32Vector, Int64Vector};

    #[derive(Debug)]
    struct Coprocessor {
        name: String,
        args: Vec<String>,
        returns: Vec<String>,
    }

    #[derive(Debug, Snafu)]
    pub enum CoprError {
        Format { reason: String },
        PyParse { parse_error: ParseError },
        PyRuntime,
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

    /// TODO: a lot of error handling
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
        // TODO: support int, float and bool
        let mut args: Vec<PyVector> = Vec::new();
        for arg in fetch_args{
             match arg.data_type(){
                DataType::Float32 => todo!(),
                _ => return Err(CoprError::Format { reason: format!("Unsupport data type {:?} for coprocessor", arg.data_type()) })
             }
        };
        // 4. then `vm.invoke` function with given args,
        // 5. get returns as a PyList, and assign them according to `returns`
        // 6. return a assembled DfRecordBatch
        Ok(rb)
    }
    use super::*;

    #[test]
    fn test_execute_script() {
        let result = execute_script("a//b");
        assert!(result.is_ok());
    }

    #[test]
    fn test_coprocessor() {
        let python_source = r#"
@copr(args=["cpu", "mem"], returns=["perf"])
def a():
    return
"#;
        let python_ast = parser::parse_program(python_source).unwrap();
        //println!("{}, {:?}", python_source, python_ast);
        let cpu_array = PrimitiveArray::from_slice([1.0f32, 0.8, 0.7, 0.6]);
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
