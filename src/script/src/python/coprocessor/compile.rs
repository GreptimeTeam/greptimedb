//! compile script to code object

use rustpython_bytecode::CodeObject;
use rustpython_compiler_core::compile as python_compile;
use rustpython_parser::{
    ast,
    ast::{Located, Location},
    parser,
};
use snafu::ResultExt;

use crate::fail_parse_error;
use crate::python::coprocessor::parse::{ret_parse_error, DecoratorArgs};
use crate::python::error::{PyCompileSnafu, PyParseSnafu, Result};

fn create_located<T>(node: T, loc: Location) -> Located<T> {
    Located::new(loc, node)
}

/// generate a call to the coprocessor function
/// with arguments given in decorator's `args` list
/// also set in location in source code to `loc`
fn gen_call(name: &str, deco_args: &DecoratorArgs, loc: &Location) -> ast::Stmt<()> {
    let mut loc = loc.to_owned();
    // adding a line to avoid confusing if any error occurs when calling the function
    // then the pretty print will point to the last line in code
    // instead of point to any of existing code written by user.
    loc.newline();
    let args: Vec<Located<ast::ExprKind>> = deco_args
        .arg_names
        .iter()
        .map(|v| {
            let node = ast::ExprKind::Name {
                id: v.to_owned(),
                ctx: ast::ExprContext::Load,
            };
            create_located(node, loc)
        })
        .collect();
    let func = ast::ExprKind::Call {
        func: Box::new(create_located(
            ast::ExprKind::Name {
                id: name.to_string(),
                ctx: ast::ExprContext::Load,
            },
            loc,
        )),
        args,
        keywords: Vec::new(),
    };
    let stmt = ast::StmtKind::Expr {
        value: Box::new(create_located(func, loc)),
    };
    create_located(stmt, loc)
}

/// stripe the decorator(`@xxxx`) and type annotation(for type checker is done in rust function), add one line in the ast for call function with given parameter, and compiler into `CodeObject`
///
/// The rationale is that rustpython's vm is not very efficient according to [offical benchmark](https://rustpython.github.io/benchmarks),
/// So we should avoid running too much Python Bytecode, hence in this function we delete `@` decorator(instead of actually write a decorator in python)
/// And add a function call in the end and also
/// strip type annotation
pub fn compile_script(name: &str, deco_args: &DecoratorArgs, script: &str) -> Result<CodeObject> {
    // note that it's important to use `parser::Mode::Interactive` so the ast can be compile to return a result instead of return None in eval mode
    let mut top = parser::parse(script, parser::Mode::Interactive).context(PyParseSnafu)?;
    // erase decorator
    if let ast::Mod::Interactive { body } = &mut top {
        let stmts = body;
        let mut loc = None;
        for stmt in stmts.iter_mut() {
            if let ast::StmtKind::FunctionDef {
                name: _,
                args,
                body: _,
                decorator_list,
                returns,
                type_comment: __main__,
            } = &mut stmt.node
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
            } else if matches!(
                stmt.node,
                ast::StmtKind::Import { .. } | ast::StmtKind::ImportFrom { .. }
            ) {
                // import statements are allowed.
            } else {
                // already checked in parser
                unreachable!()
            }
            loc = Some(stmt.location);

            // This manually construct ast has no corrsponding code
            // in the script, so just give it a location that don't exist in orginal script
            // (which doesn't matter because Location usually only used in pretty print errors)
        }
        // Append statement which calling coprocessor function.
        // It's safe to unwrap loc, it is always exists.
        stmts.push(gen_call(name, deco_args, &loc.unwrap()));
    } else {
        return fail_parse_error!(
            format!("Expect statement in script, found: {:?}", top),
            None,
        );
    }
    // use `compile::Mode::BlockExpr` so it return the result of statement
    python_compile::compile_top(
        &top,
        "<embedded>".to_owned(),
        python_compile::Mode::BlockExpr,
        python_compile::CompileOpts { optimize: 0 },
    )
    .context(PyCompileSnafu)
}
