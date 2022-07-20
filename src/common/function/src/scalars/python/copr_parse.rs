use arrow::datatypes::DataType;
use rustpython_parser::{
    ast,
    ast::{Arguments, Location},
    parser,
};

use crate::scalars::python::coprocessor::Coprocessor;
use crate::scalars::python::error::{ensure, CoprParseSnafu, Error, Result};
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
                return Err(Error::CoprParse {
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
        Err(Error::CoprParse {
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
        _ => Err(Error::CoprParse {
            reason: format!("Unknown datatype: {ty} at {}", loc),
            loc: Some(loc.to_owned()),
        }),
    }
}

/// return AnnotationInfo with is_nullable and need_coerced both set to false
/// if type is `_` return datatype is None
fn parse_type(node: &ast::Expr<()>) -> Result<AnnotationInfo> {
    match &node.node {
        ast::ExprKind::Name { id, ctx: _ } => Ok(AnnotationInfo {
            datatype: into_datatype(id, &node.location)?,
            is_nullable: false,
            coerce_into: false,
        }),
        _ => Err(Error::CoprParse {
            reason: format!("Expect a type's name, found {:?}", node),
            loc: Some(node.location),
        }),
    }
}

/// Item => NativeType | into `(` NativeType `)`
fn parse_item(sub: &ast::Expr<()>) -> Result<AnnotationInfo> {
    match &sub.node {
        ast::ExprKind::Name { id: _, ctx: _ } => Ok(parse_type(sub)?),
        ast::ExprKind::Call {
            func,
            args,
            keywords: _,
        } => {
            if let ast::ExprKind::Name { id, ctx: _ } = &func.node {
                ensure!(
                    id.as_str() == "into",
                    CoprParseSnafu {
                        reason: format!(
                            "Expect only `into(datatype)` or datatype or `None`, found {id}"
                        ),
                        loc: Some(sub.location)
                    }
                );
            } else {
                return Err(Error::Other {
                    reason: format!("Expect type names, found {:?}", &func.node),
                });
            };
            ensure!(
                args.len() == 1,
                CoprParseSnafu {
                    reason: "Expect only one arguement for `into`",
                    loc: Some(sub.location)
                }
            );
            let mut anno = parse_type(&args[0])?;
            anno.coerce_into = true;
            Ok(anno)
        }
        _ => Err(Error::Other {
            reason: format!(
                "Expect types' name or into(<typename>), found {:?}",
                &sub.node
            ),
        }),
    }
}

/// where:
///
/// Start => vector`[`TYPE`]`
///
/// TYPE => Item | Item `|` None
///
/// Item => NativeType | into(NativeType)
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
            return Err(Error::Other {
                reason: format!("Expect \"vector\", found {:?}", &value.node),
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
                                return Err(Error::Other { reason: "Expect one typenames(or `into(<typename>)`) and one `None`, not two type names".into() });
                            } else {
                                has_ty = true;
                            }
                        }
                        _ => {
                            return Err(Error::Other {
                                reason: format!(
                                    "Expect typename or `None` or `into(<typename>)`, found {:?}",
                                    &i.node
                                ),
                            })
                        }
                    }
                }
                if !has_ty {
                    return Err(Error::Other {
                        reason: "Expect a type name, not two `None`".into(),
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
                            return Err(Error::Other {
                                reason: format!(
                                    "Expect typename or `None` or `into(<typename>)`, found {:?}",
                                    &i.node
                                ),
                            })
                        }
                    }
                }
                // deal with errors anyway in case code above changed but forget to modify
                let mut tmp_anno = tmp_anno.ok_or(Error::Other {
                    reason: "Expect a type name, not two `None`".into(),
                })?;
                tmp_anno.is_nullable = is_nullable;
                Ok(tmp_anno)
            }
            _ => Err(Error::Other {
                reason: format!("Expect type in `vector[...]`, found {:?}", &slice.node),
            }),
        }
    } else {
        Err(Error::Other {
            reason: format!("Expect type annotation, found {:?}", &sub),
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
                                return Err(Error::CoprParse {
                                    reason: "`args` occur multiple times in decorator's arguements' list.".to_string(),
                                    loc: Some(kw.location)
                                });
                            }
                        }
                        "returns" => {
                            if ret_names.is_none() {
                                ret_names = Some(pylist_to_vec(&kw.node.value))
                            } else {
                                return Err(Error::CoprParse {
                                    reason: "`returns` occur multiple times in decorator's arguements' list.".to_string(),
                                    loc: Some(kw.location)
                                });
                            }
                        }
                        _ => {
                            return Err(Error::CoprParse {
                                reason: format!("Expect `args` or `returns`, found `{}`", s),
                                loc: Some(kw.location),
                            })
                        }
                    }
                }
                None => {
                    return Err(Error::CoprParse {
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
            return Err(Error::CoprParse {
                reason: "Expect `args` keyword".to_string(),
                loc: Some(decorator.location),
            });
        };
        let ret_names = if let Some(rets) = ret_names {
            rets?
        } else {
            return Err(Error::CoprParse {
                reason: "Expect `rets` keyword".to_string(),
                loc: Some(decorator.location),
            });
        };
        Ok((arg_names, ret_names))
    } else {
        Err(Error::CoprParse {
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
        })
    } else {
        Err(Error::CoprParse {
            reason: format!(
                "Expect a function definition, found a {:?}",
                &python_ast[0].node
            ),
            loc: Some(python_ast[0].location),
        })
    }
}
