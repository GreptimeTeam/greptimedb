use std::collections::{HashMap, HashSet};

use arrow::datatypes::DataType;
use rustpython_parser::{
    ast,
    ast::{Arguments, Location},
    parser,
};
use snafu::{OptionExt, ResultExt};

use crate::scalars::python::coprocessor::Coprocessor;
use crate::scalars::python::error::{ensure, CoprParseSnafu, PyParseSnafu, Result};
use crate::scalars::python::AnnotationInfo;
/// Return a CoprParseSnafu for you to chain fail() to return correct err Result type
pub(crate) fn ret_parse_error(
    reason: String,
    loc: Option<Location>,
) -> CoprParseSnafu<String, Option<Location>> {
    CoprParseSnafu { reason, loc }
}

/// append a `.fail()` after `ret_parse_error`, so compiler can return a Err(this error)
#[macro_export]
macro_rules! fail_parse_error {
    ($reason:expr, $loc:expr $(,)*) => {
        ret_parse_error($reason, $loc).fail()
    };
}

/// turn a python list of string in ast form(a `ast::Expr`) of string into a `Vec<String>`
fn pylist_to_vec(lst: &ast::Expr<()>) -> Result<Vec<String>> {
    if let ast::ExprKind::List { elts, ctx: _ } = &lst.node {
        let mut ret = Vec::with_capacity(elts.len());
        for s in elts {
            if let ast::ExprKind::Constant {
                value: ast::Constant::Str(v),
                kind: _,
            } = &s.node
            {
                ret.push(v.to_owned())
            } else {
                return fail_parse_error!(
                    format!(
                        "Expect a list of String, found {:?} in list element",
                        &s.node
                    ),
                    Some(s.location)
                );
            }
        }
        Ok(ret)
    } else {
        fail_parse_error!(
            format!("Expect a list, found {:?}", &lst.node),
            Some(lst.location)
        )
    }
}

fn try_into_datatype(ty: &str, loc: &Location) -> Result<Option<DataType>> {
    match ty {
        "bool" => Ok(Some(DataType::Boolean)),
        "u8" => Ok(Some(DataType::UInt8)),
        "u16" => Ok(Some(DataType::UInt16)),
        "u32" => Ok(Some(DataType::UInt32)),
        "u64" => Ok(Some(DataType::UInt64)),
        "i8" => Ok(Some(DataType::Int8)),
        "i16" => Ok(Some(DataType::Int16)),
        "i32" => Ok(Some(DataType::Int32)),
        "i64" => Ok(Some(DataType::Int64)),
        "f16" => Ok(Some(DataType::Float16)),
        "f32" => Ok(Some(DataType::Float32)),
        "f64" => Ok(Some(DataType::Float64)),
        // for any datatype
        "_" => Ok(None),
        // note the different between "_" and _
        _ => fail_parse_error!(
            format!("Unknown datatype: {ty} at {}", loc),
            Some(loc.to_owned())
        ),
    }
}

/// Item => NativeType
/// default to be not nullable
fn parse_native_type(sub: &ast::Expr<()>) -> Result<AnnotationInfo> {
    match &sub.node {
        ast::ExprKind::Name { id, .. } => Ok(AnnotationInfo {
            datatype: try_into_datatype(id, &sub.location)?,
            is_nullable: false,
        }),
        _ => fail_parse_error!(
            format!("Expect types' name, found {:?}", &sub.node),
            Some(sub.location)
        ),
    }
}

/// check if binary op expr is legal(with one typename and one `None`)
fn check_bin_op(bin_op: &ast::Expr<()>) -> Result<()> {
    if let ast::ExprKind::BinOp { left, op: _, right } = &bin_op.node {
        // 1. first check if this BinOp is legal(Have one typename and(optional) a None)
        let is_none = |node: &ast::Expr<()>| -> bool {
            matches!(
                &node.node,
                ast::ExprKind::Constant {
                    value: ast::Constant::None,
                    kind: _,
                }
            )
        };
        let is_type = |node: &ast::Expr<()>| {
            if let ast::ExprKind::Name { id, ctx: _ } = &node.node {
                try_into_datatype(id, &node.location).is_ok()
            } else {
                false
            }
        };
        let left_is_ty = is_type(left);
        let left_is_none = is_none(left);
        let right_is_ty = is_type(right);
        let right_is_none = is_none(right);
        if left_is_ty && right_is_ty || left_is_none && right_is_none {
            fail_parse_error!(
                "Expect one typenames and one `None`".to_string(),
                Some(bin_op.location)
            )?;
        } else if !(left_is_none && right_is_ty || left_is_ty && right_is_none) {
            fail_parse_error!(
                format!(
                    "Expect a type name and a `None`, found left: {:?} and right: {:?}",
                    &left.node, &right.node
                ),
                Some(bin_op.location)
            )?;
        }
        Ok(())
    } else {
        fail_parse_error!(
            format!(
                "Expect binary ops like `DataType | None`, found {:#?}",
                bin_op.node
            ),
            Some(bin_op.location)
        )
    }
}

/// parse a `DataType | None` or a single `DataType`
fn parse_bin_op(bin_op: &ast::Expr<()>) -> Result<AnnotationInfo> {
    // 1. first check if this BinOp is legal(Have one typename and(optional) a None)
    check_bin_op(bin_op)?;
    if let ast::ExprKind::BinOp { left, op: _, right } = &bin_op.node {
        // then get types from this BinOp
        let left_ty = parse_native_type(left).ok();
        let right_ty = parse_native_type(right).ok();
        let mut ty_anno = if let Some(left_ty) = left_ty {
            left_ty
        } else if let Some(right_ty) = right_ty {
            right_ty
        } else {
            // deal with errors anyway in case code above changed but forget to modify
            return fail_parse_error!(
                "Expect a type name, not two `None`".into(),
                Some(bin_op.location),
            );
        };
        // because check_bin_op assure a `None` exist
        ty_anno.is_nullable = true;
        return Ok(ty_anno);
    }
    unreachable!()
}

/// check for the grammar correctness of annotation, also return the slice of subscript for further parsing
fn check_annotation_ret_slice(sub: &ast::Expr<()>) -> Result<&ast::Expr<()>> {
    if let ast::ExprKind::Subscript {
        value,
        slice,
        ctx: _,
    } = &sub.node
    {
        if let ast::ExprKind::Name { id, ctx: _ } = &value.node {
            ensure!(
                id == "vector",
                ret_parse_error(
                    format!(
                        "Wrong type annotation, expect `vector[...]`, found `{}`",
                        id
                    ),
                    Some(value.location)
                )
            );
        } else {
            return fail_parse_error!(
                format!("Expect \"vector\", found {:?}", &value.node),
                Some(value.location)
            );
        }
        Ok(slice)
    } else {
        fail_parse_error!(
            format!("Expect type annotation, found {:?}", &sub),
            Some(sub.location)
        )
    }
}

/// where:
///
/// Start => vector`[`TYPE`]`
///
/// TYPE => Item | Item `|` None
///
/// Item => NativeType
fn parse_annotation(sub: &ast::Expr<()>) -> Result<AnnotationInfo> {
    let slice = check_annotation_ret_slice(sub)?;

    {
        // i.e: vector[f64]
        match &slice.node {
            ast::ExprKind::Name { .. } => parse_native_type(slice),
            ast::ExprKind::BinOp {
                left: _,
                op: _,
                right: _,
            } => parse_bin_op(slice),
            _ => {
                fail_parse_error!(
                    format!("Expect type in `vector[...]`, found {:?}", &slice.node),
                    Some(slice.location),
                )
            }
        }
    }
}

/// parse a list of keyword and return args and returns list from keywords
fn parse_keywords(keywords: &Vec<ast::Keyword<()>>) -> Result<(Vec<String>, Vec<String>)> {
    // more keys maybe add to this list of `avail_key`(like `sql` for querying and maybe config for connecting to database?), for better extension using a `HashSet` in here
    let avail_key = HashSet::from(["args", "returns"]);
    ensure!(
        keywords.len() == avail_key.len(),
        CoprParseSnafu {
            reason: format!(
                "Expect {} keyword argument, found {}.",
                avail_key.len(),
                keywords.len()
            ),
            loc: keywords.get(0).map(|s| s.location)
        }
    );
    let mut kw_map = HashMap::new();
    for kw in keywords {
        match &kw.node.arg {
            Some(s) => {
                let s = s.as_str();
                if !kw_map.contains_key(s) {
                    if !avail_key.contains(s) {
                        return fail_parse_error!(
                            format!("Expect one of {:?}, found `{}`", &avail_key, s),
                            Some(kw.location),
                        );
                    }
                    kw_map.insert(s, pylist_to_vec(&kw.node.value)?);
                } else {
                    return fail_parse_error!(
                        format!("`{s}` occur multiple times in decorator's arguements' list."),
                        Some(kw.location),
                    );
                }
            }
            None => {
                return fail_parse_error!(
                    format!(
                        "Expect explictly set both `args` and `returns`, found {:?}",
                        &kw.node
                    ),
                    Some(kw.location),
                )
            }
        }
    }
    let loc = keywords[0].location;
    let arg_names = kw_map
        .remove("args")
        .context(ret_parse_error("Expect `args` keyword".into(), Some(loc)))?;
    let ret_names = kw_map
        .remove("returns")
        .context(ret_parse_error("Expect `rets` keyword".into(), Some(loc)))?;
    Ok((arg_names, ret_names))
}

/// returns args and returns in Vec of String
fn parse_decorator(decorator: &ast::Expr<()>) -> Result<(Vec<String>, Vec<String>)> {
    //check_decorator(decorator)?;
    if let ast::ExprKind::Call {
        func,
        args: _,
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
                reason: format!(
                    "Expect decorator with name `copr` or `coprocessor`, found {:?}",
                    &func.node
                ),
                loc: Some(func.location)
            }
        );
        parse_keywords(keywords)
    } else {
        fail_parse_error!(
            format!(
                "Expect decorator to be a function call(like `@copr(...)`), found {:?}",
                decorator.node
            ),
            Some(decorator.location),
        )
    }
}

// get type annotaion in arguments
fn get_arg_annotations(args: &Arguments) -> Result<Vec<Option<AnnotationInfo>>> {
    // get arg types from type annotation>
    args.args
        .iter()
        .map(|arg| {
            if let Some(anno) = &arg.node.annotation {
                // for there is erro handling for parse_annotation
                parse_annotation(anno).map(Some)
            } else {
                Ok(None)
            }
        })
        .collect::<Result<Vec<Option<_>>>>()
}

fn get_return_annotations(rets: &ast::Expr<()>) -> Result<Vec<Option<AnnotationInfo>>> {
    let mut return_types = Vec::with_capacity(match &rets.node {
        ast::ExprKind::Tuple { elts, ctx: _ } => elts.len(),
        ast::ExprKind::Subscript {
            value: _,
            slice: _,
            ctx: _,
        } => 1,
        _ => {
            return fail_parse_error!(
                format!(
                    "Expect `(vector[...], vector[...], ...)` or `vector[...]`, found {:#?}",
                    &rets.node
                ),
                Some(rets.location),
            )
        }
    });
    match &rets.node {
        // python: ->(vector[...], vector[...], ...)
        ast::ExprKind::Tuple { elts, .. } => {
            for elem in elts {
                return_types.push(Some(parse_annotation(elem)?))
            }
        }
        // python: -> vector[...]
        ast::ExprKind::Subscript {
            value: _,
            slice: _,
            ctx: _,
        } => return_types.push(Some(parse_annotation(rets)?)),
        _ => {
            return fail_parse_error!(
                format!(
                    "Expect one or many type annotation for the return type, found {:#?}",
                    &rets.node
                ),
                Some(rets.location),
            )
        }
    }
    Ok(return_types)
}

/// check if the list of statements contain only one statement and
/// that statement is a function call with one decorator
fn check_copr(stmts: &Vec<ast::Stmt<()>>) -> Result<()> {
    ensure!(
        stmts.len() == 1,
        CoprParseSnafu {
            reason:
                "Expect one and only one python function with `@coprocessor` or `@cpor` decorator"
                    .to_string(),
            loc: stmts.first().map(|s| s.location)
        }
    );
    if let ast::StmtKind::FunctionDef {
        name: _,
        args: _,
        body: _,
        decorator_list,
        returns: _,
        type_comment: _,
    } = &stmts[0].node
    {
        ensure!(
            decorator_list.len() == 1,
            CoprParseSnafu {
                reason: "Expect one decorator",
                loc: decorator_list.first().map(|s| s.location)
            }
        );
    } else {
        return fail_parse_error!(
            format!("Expect a function definition, found a {:?}", &stmts[0].node),
            Some(stmts[0].location),
        );
    }
    Ok(())
}

/// parse script and return `Coprocessor` struct with info extract from ast
pub fn parse_copr(script: &str) -> Result<Coprocessor> {
    let python_ast = parser::parse_program(script).context(PyParseSnafu)?;
    check_copr(&python_ast)?;
    if let ast::StmtKind::FunctionDef {
        name,
        args: fn_args,
        body: _,
        decorator_list,
        returns,
        type_comment: _,
    } = &python_ast[0].node
    {
        let decorator = &decorator_list[0];
        let (arg_names, ret_names) = parse_decorator(decorator)?;

        // get arg types from type annotation
        let arg_types = get_arg_annotations(fn_args)?;

        // get return types from type annotation
        let return_types = if let Some(rets) = returns {
            get_return_annotations(rets)?
        } else {
            // if no anntation at all, set it to all None
            std::iter::repeat(None).take(ret_names.len()).collect()
        };

        // make sure both arguments&returns in fucntion
        // and in decorator have same length
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
            script: script.to_owned(),
        })
    } else {
        unreachable!()
    }
}
