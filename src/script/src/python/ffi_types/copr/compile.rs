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

//! compile script to code object
use rustpython_compiler::codegen::compile::compile_top;
use rustpython_compiler::parser::ast::located::Located;
use rustpython_compiler::parser::ast::{located as located_ast, Identifier};
use rustpython_compiler::parser::source_code::{OneIndexed, SourceLocation, SourceRange};
use rustpython_compiler::{CodeObject, CompileOpts, Mode};
use snafu::ResultExt;

use crate::fail_parse_error;
use crate::python::error::{PyCompileSnafu, Result};
use crate::python::ffi_types::copr::parse::{ret_parse_error, DecoratorArgs};

/// generate a call to the coprocessor function
/// with arguments given in decorator's `args` list
/// also set in location in source code to `loc`
fn gen_call(
    name: &str,
    deco_args: &DecoratorArgs,
    kwarg: &Option<String>,
    loc: &SourceLocation,
) -> located_ast::Stmt {
    let mut loc = *loc;
    loc.row = loc.row.saturating_add(1);
    loc.column = OneIndexed::from_zero_indexed(0);

    let range = SourceRange::new(loc.clone(), loc.clone());
    // adding a line to avoid confusing if any error occurs when calling the function
    // then the pretty print will point to the last line in code
    // instead of point to any of existing code written by user.
    let mut args: Vec<located_ast::Expr> = if let Some(arg_names) = &deco_args.arg_names {
        arg_names
            .iter()
            .map(|v| {
                located_ast::Expr::Name(located_ast::ExprName {
                    id: Identifier::from(v.clone()),
                    ctx: located_ast::ExprContext::Load,
                    range,
                })
            })
            .collect()
    } else {
        vec![]
    };

    if let Some(kwarg) = kwarg {
        let node = located_ast::Expr::Name(located_ast::ExprName {
            range,
            id: Identifier::from(kwarg.clone()),
            ctx: located_ast::ExprContext::Load,
        });
        args.push(node);
    }

    let func = located_ast::Expr::Call(located_ast::ExprCall {
        range,
        func: Box::new(located_ast::Expr::Name(located_ast::ExprName {
            range,
            id: Identifier::from(name.to_string()),
            ctx: located_ast::ExprContext::Load,
        })),
        args,
        keywords: Vec::new(),
    });
    located_ast::Stmt::Expr(located_ast::StmtExpr {
        range,
        value: Box::new(func),
    })
}

/// stripe the decorator(`@xxxx`) and type annotation(for type checker is done in rust function), add one line in the ast for call function with given parameter, and compiler into `CodeObject`
///
/// The rationale is that rustpython's vm is not very efficient according to [official benchmark](https://rustpython.github.io/benchmarks),
/// So we should avoid running too much Python Bytecode, hence in this function we delete `@` decorator(instead of actually write a decorator in python)
/// And add a function call in the end and also
/// strip type annotation
pub fn compile_script(
    name: &str,
    deco_args: &DecoratorArgs,
    kwarg: &Option<String>,
    script: &str,
) -> Result<CodeObject> {
    let mut top = super::parse::parse_interactive(script)?;

    // erase decorator

    if let located_ast::ModInteractive { body, .. } = &mut top {
        let stmts = body;
        let mut loc = None;
        for stmt in stmts.iter_mut() {
            if let located_ast::Stmt::FunctionDef(located_ast::StmtFunctionDef {
                name: _,
                args,
                body: _,
                decorator_list,
                returns,
                type_comment: __main__,
                type_params: _,
                range,
            }) = stmt
            {
                // Rewrite kwargs in coprocessor, make it as a positional argument
                if !decorator_list.is_empty() {
                    if let Some(kwarg) = kwarg {
                        args.kwarg = None;
                        let kwarg = located_ast::ArgWithDefault {
                            range: (*range).into(),
                            def: located_ast::Arg {
                                range: *range,
                                arg: Identifier::from(kwarg.clone()),
                                annotation: None,
                                type_comment: Some("kwargs".to_string()),
                            },
                            default: None,
                        };
                        args.args.push(kwarg);
                    }
                }

                *decorator_list = Vec::new();
                // strip type annotation
                // def a(b: int, c:int) -> int
                // will became
                // def a(b, c)
                *returns = None;
                for arg in &mut args.args {
                    arg.def.annotation = None;
                }
            } else if matches!(
                stmt,
                located_ast::Stmt::Import { .. } | located_ast::Stmt::ImportFrom { .. }
            ) {
                // import statements are allowed.
            } else {
                // already checked in parser
                unreachable!()
            }
            loc = Some(stmt.location());

            // This manually construct ast has no corresponding code
            // in the script, so just give it a location that don't exist in original script
            // (which doesn't matter because Location usually only used in pretty print errors)
        }
        // Append statement which calling coprocessor function.
        // It's safe to unwrap loc, it is always exists.
        stmts.push(gen_call(name, deco_args, kwarg, &loc.unwrap()));
    } else {
        return fail_parse_error!(format!("Expect statement in script, found: {top:?}"), None);
    }
    // use `compile::Mode::BlockExpr` so it return the result of statement
    compile_top(
        &located_ast::Mod::Interactive(top),
        "<embedded>".to_string(),
        Mode::BlockExpr,
        CompileOpts { optimize: 0 },
    )
    .context(PyCompileSnafu)
}
