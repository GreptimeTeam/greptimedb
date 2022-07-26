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
                return ret_parse_error(
                    format!(
                        "Expect a list of String, found {:?} in list element",
                        &s.node
                    ),
                    Some(lst.location),
                )
                .fail()
                .map_err(|err| err.into());
            }
        }
        Ok(ret)
    } else {
        ret_parse_error(
            format!("Expect a list, found {:?}", &lst.node),
            Some(lst.location),
        )
        .fail()
        .map_err(|err| err.into())
    }
}

fn into_datatype(ty: &str, loc: &Location) -> Result<Option<DataType>> {
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
        _ => ret_parse_error(
            format!("Unknown datatype: {ty} at {}", loc),
            Some(loc.to_owned()),
        )
        .fail()
        .map_err(|err| err.into()),
    }
}

/// Item => NativeType
/// default to be not nullable
fn parse_item(sub: &ast::Expr<()>) -> Result<AnnotationInfo> {
    match &sub.node {
        ast::ExprKind::Name { id, ctx: _ } => Ok(AnnotationInfo {
            datatype: into_datatype(id, &sub.location)?,
            is_nullable: false,
        }),
        _ => ret_parse_error(
            format!("Expect types' name, found {:?}", &sub.node),
            Some(sub.location),
        )
        .fail()
        .map_err(|err| err.into()),
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
                        "Wrong type annotation, expect `vector[...]`, found \"{}\"",
                        id
                    ),
                    Some(value.location)
                )
            )
        } else {
            return ret_parse_error(
                format!("Expect \"vector\", found {:?}", &value.node),
                Some(value.location),
            )
            .fail()
            .map_err(|err| err.into());
        }
        // i.e: vector[f64]
        match &slice.node {
            ast::ExprKind::Name { id: _, ctx: _ }
            | ast::ExprKind::Call {
                func: _,
                args: _,
                keywords: _,
            } => parse_item(slice),
            ast::ExprKind::BinOp { left, op: _, right } => {
                // 1. first check if this BinOp is legal(Have one typename and(optional) a None)
                let mut has_ty = false;
                for i in [left, right] {
                    match &i.node {
                        ast::ExprKind::Constant { value, kind: _ } => {
                            ensure!(
                                matches!(value, ast::Constant::None),
                                ret_parse_error(
                                    format!("Expect only typenames and `None`, found {:?}", i.node),
                                    Some(i.location)
                                )
                            );
                        }
                        ast::ExprKind::Name { id: _, ctx: _ }
                        | ast::ExprKind::Call {
                            func: _,
                            args: _,
                            keywords: _,
                        } => {
                            if has_ty {
                                return ret_parse_error(
                                    "Expect one typenames and one `None`, not two type names"
                                        .into(),
                                    Some(i.location),
                                )
                                .fail()
                                .map_err(|err| err.into());
                            } else {
                                has_ty = true;
                            }
                        }
                        _ => {
                            return ret_parse_error(
                                format!("Expect typename or `None`, found {:?}", &i.node),
                                Some(i.location),
                            )
                            .fail()
                            .map_err(|err| err.into())
                        }
                    }
                }
                if !has_ty {
                    return ret_parse_error(
                        "Expect a type name, not two `None`".into(),
                        Some(slice.location),
                    )
                    .fail()
                    .map_err(|err| err.into());
                }

                // then get types from this BinOp
                let mut is_nullable = false;
                let mut tmp_anno = None;
                for i in [left, right] {
                    match &i.node {
                        ast::ExprKind::Constant { value: _, kind: _ } => {
                            // already check Constant to be `None`
                            is_nullable = true;
                        }
                        ast::ExprKind::Name { id: _, ctx: _ }
                        | ast::ExprKind::Call {
                            func: _,
                            args: _,
                            keywords: _,
                        } => tmp_anno = Some(parse_item(i)?),
                        _ => {
                            return ret_parse_error(
                                format!("Expect typename or `None`, found {:?}", &i.node),
                                Some(i.location),
                            )
                            .fail()
                            .map_err(|err| err.into());
                        }
                    }
                }
                // deal with errors anyway in case code above changed but forget to modify
                let mut tmp_anno = tmp_anno.context(ret_parse_error(
                    "Expect a type name, not two `None`".into(),
                    Some(slice.location),
                ))?;
                tmp_anno.is_nullable = is_nullable;
                Ok(tmp_anno)
            }
            _ => {
                ret_parse_error(
                    format!("Expect type in `vector[...]`, found {:?}", &slice.node),
                    Some(slice.location),
                )
                .fail()
                .map_err(|err| err.into())
            }
        }
    } else {
        ret_parse_error(
            format!("Expect type annotation, found {:?}", &sub),
            Some(sub.location),
        )
        .fail()
        .map_err(|err| err.into())
    }
}

/// returns args and returns in Vec of String
fn parse_decorator(decorator: &ast::Expr<()>) -> Result<(Vec<String>, Vec<String>)> {
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
                reason: format!(
                    "Expect decorator with name `copr` or `coprocessor`, found {:?}",
                    &func.node
                ),
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
        let avail_key = HashSet::from(["args", "returns"]);
        let mut kw_map = HashMap::new();
        for kw in keywords {
            match &kw.node.arg {
                Some(s) => {
                    let s = s.as_str();
                    if !kw_map.contains_key(s) {
                        if !avail_key.contains(s) {
                            return ret_parse_error(
                                format!("Expect one of {:?}, found `{}`", &avail_key, s),
                                Some(kw.location),
                            )
                            .fail()
                            .map_err(|err| err.into());
                        }
                        kw_map.insert(s, pylist_to_vec(&kw.node.value)?);
                    } else {
                        return ret_parse_error(
                            format!("`{s}` occur multiple times in decorator's arguements' list."),
                            Some(kw.location),
                        )
                        .fail()
                        .map_err(|err| err.into());
                    }
                }
                None => {
                    return ret_parse_error(
                        format!(
                            "Expect explictly set both `args` and `returns`, found {:?}",
                            &kw.node
                        ),
                        Some(kw.location),
                    )
                    .fail()
                    .map_err(|err| err.into())
                }
            }
        }
        let arg_names = kw_map.remove("args").context(ret_parse_error(
            "Expect `args` keyword".into(),
            Some(decorator.location),
        ))?;
        let ret_names = kw_map.remove("returns").context(ret_parse_error(
            "Expect `rets` keyword".into(),
            Some(decorator.location),
        ))?;
        Ok((arg_names, ret_names))
    } else {
        ret_parse_error(
            format!(
                "Expect decorator to be a function call(like `@copr(...)`), found {:?}",
                decorator.node
            ),
            Some(decorator.location),
        )
        .fail()
        .map_err(|err| err.into())
    }
}

// get type annotaion in arguments
fn get_arg_annotations(args: &Arguments) -> Result<Vec<Option<AnnotationInfo>>> {
    // get arg types from type annotation>

    args
        .args
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
    let mut return_types = Vec::new();
    match &rets.node {
        // python: ->(vector[...], vector[...], ...)
        ast::ExprKind::Tuple { elts, ctx: _ } => {
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
            return ret_parse_error(
                format!(
                    "Expect one or many type annotation for the return type, found {:#?}",
                    &rets.node
                ),
                Some(rets.location),
            )
            .fail()
            .map_err(|err| err.into())
        }
    }
    Ok(return_types)
}

/// parse script and return `Coprocessor` struct with info extract from ast
pub fn parse_copr(script: &str) -> Result<Coprocessor> {
    let python_ast = parser::parse_program(script).context(PyParseSnafu)?;
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
        ret_parse_error(
            format!(
                "Expect a function definition, found a {:?}",
                &python_ast[0].node
            ),
            Some(python_ast[0].location),
        )
        .fail()
        .map_err(|err| err.into())
    }
}
