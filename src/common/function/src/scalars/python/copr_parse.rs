use std::collections::{HashMap, HashSet};

use arrow::datatypes::DataType;
use rustpython_parser::{
    ast,
    ast::{Arguments, Location},
    parser,
};
use snafu::ResultExt;

use crate::scalars::python::coprocessor::Coprocessor;
use crate::scalars::python::error::{ensure, CoprParseSnafu, InnerError, PyParseSnafu, Result};
use crate::scalars::python::AnnotationInfo;

/// turn a python list of string in ast form(a `ast::Expr`) of string into a `Vec<String>`
fn pylist_to_vec(lst: &ast::Expr<()>) -> Result<Vec<String>> {
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
                return Err(InnerError::CoprParse {
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
        Err(InnerError::CoprParse {
            reason: format!("Expect a list, found {:?}", &lst.node),
            loc: Some(lst.location),
        })
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
        _ => Err(InnerError::CoprParse {
            reason: format!("Unknown datatype: {ty} at {}", loc),
            loc: Some(loc.to_owned()),
        }),
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
        _ => Err(InnerError::CoprParse {
            reason: format!("Expect types' name, found {:?}", &sub.node),
            loc: Some(sub.location),
        }),
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
                CoprParseSnafu {
                    reason: format!(
                        "Wrong type annotation, expect `vector[...]`, found \"{}\"",
                        id
                    ),
                    loc: Some(value.location)
                }
            )
        } else {
            return Err(InnerError::CoprParse {
                reason: format!("Expect \"vector\", found {:?}", &value.node),
                loc: Some(value.location),
            });
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
                                CoprParseSnafu {
                                    reason: format!(
                                        "Expect only typenames and `None`, found {:?}",
                                        i.node
                                    ),
                                    loc: Some(i.location)
                                }
                            );
                        }
                        ast::ExprKind::Name { id: _, ctx: _ }
                        | ast::ExprKind::Call {
                            func: _,
                            args: _,
                            keywords: _,
                        } => {
                            if has_ty {
                                return Err(InnerError::CoprParse {
                                    reason:
                                        "Expect one typenames and one `None`, not two type names"
                                            .into(),
                                    loc: Some(i.location),
                                });
                            } else {
                                has_ty = true;
                            }
                        }
                        _ => {
                            return Err(InnerError::CoprParse {
                                reason: format!("Expect typename or `None`, found {:?}", &i.node),
                                loc: Some(i.location),
                            })
                        }
                    }
                }
                if !has_ty {
                    return Err(InnerError::CoprParse {
                        reason: "Expect a type name, not two `None`".into(),
                        loc: Some(slice.location),
                    });
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
                            return Err(InnerError::CoprParse {
                                reason: format!("Expect typename or `None`, found {:?}", &i.node),
                                loc: Some(i.location),
                            })
                        }
                    }
                }
                // deal with errors anyway in case code above changed but forget to modify
                let mut tmp_anno = tmp_anno.ok_or(InnerError::CoprParse {
                    reason: "Expect a type name, not two `None`".into(),
                    loc: Some(slice.location),
                })?;
                tmp_anno.is_nullable = is_nullable;
                Ok(tmp_anno)
            }
            _ => Err(InnerError::CoprParse {
                reason: format!("Expect type in `vector[...]`, found {:?}", &slice.node),
                loc: Some(slice.location),
            }),
        }
    } else {
        Err(InnerError::CoprParse {
            reason: format!("Expect type annotation, found {:?}", &sub),
            loc: Some(sub.location),
        })
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
                            return Err(InnerError::CoprParse {
                                reason: format!("Expect one of {:?}, found `{}`", &avail_key, s),
                                loc: Some(kw.location),
                            });
                        }
                        kw_map.insert(s, pylist_to_vec(&kw.node.value)?);
                    } else {
                        return Err(InnerError::CoprParse {
                            reason: format!(
                                "`{s}` occur multiple times in decorator's arguements' list."
                            ),
                            loc: Some(kw.location),
                        });
                    }
                }
                None => {
                    return Err(InnerError::CoprParse {
                        reason: format!(
                            "Expect explictly set both `args` and `returns`, found {:?}",
                            &kw.node
                        ),
                        loc: Some(kw.location),
                    })
                }
            }
        }
        let arg_names = if let Some(args) = kw_map.remove("args") {
            args
        } else {
            return Err(InnerError::CoprParse {
                reason: "Expect `args` keyword".into(),
                loc: Some(decorator.location),
            });
        };
        let ret_names = if let Some(rets) = kw_map.remove("returns") {
            rets
        } else {
            return Err(InnerError::CoprParse {
                reason: "Expect `rets` keyword".into(),
                loc: Some(decorator.location),
            });
        };
        Ok((arg_names, ret_names))
    } else {
        Err(InnerError::CoprParse {
            reason: format!(
                "Expect decorator to be a function call(like `@copr(...)`), found {:?}",
                decorator.node
            ),
            loc: Some(decorator.location),
        })
    }
}

// get type annotaion in arguments
fn get_arg_annotations(args: &Arguments) -> Result<Vec<Option<AnnotationInfo>>> {
    // get arg types from type annotation>
    let mut arg_types = Vec::new();
    for arg in &args.args {
        if let Some(anno) = &arg.node.annotation {
            arg_types.push(Some(parse_annotation(anno)?))
        } else {
            arg_types.push(None)
        }
    }
    Ok(arg_types)
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
        _ => todo!(),
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
            let mut tmp = Vec::new();
            tmp.resize(ret_names.len(), None);
            tmp
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
        Err(InnerError::CoprParse {
            reason: format!(
                "Expect a function definition, found a {:?}",
                &python_ast[0].node
            ),
            loc: Some(python_ast[0].location),
        })
    }
}
