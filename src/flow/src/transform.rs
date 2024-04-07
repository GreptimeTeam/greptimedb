//! Transform Substrait into execution plan

use std::collections::HashMap;

use common_decimal::Decimal128;
use common_time::{Date, Timestamp};
use datafusion_substrait::variation_const::{
    DATE_32_TYPE_REF, DATE_64_TYPE_REF, DEFAULT_TYPE_REF, TIMESTAMP_MICRO_TYPE_REF,
    TIMESTAMP_MILLI_TYPE_REF, TIMESTAMP_NANO_TYPE_REF, TIMESTAMP_SECOND_TYPE_REF,
    UNSIGNED_INTEGER_TYPE_REF,
};
use datatypes::arrow::compute::kernels::window;
use datatypes::arrow::ipc::Binary;
use datatypes::data_type::ConcreteDataType as CDT;
use datatypes::value::Value;
use itertools::Itertools;
use snafu::{OptionExt, ResultExt};
use substrait_proto::proto::aggregate_function::AggregationInvocation;
use substrait_proto::proto::expression::field_reference::ReferenceType::DirectReference;
use substrait_proto::proto::expression::literal::LiteralType;
use substrait_proto::proto::expression::reference_segment::ReferenceType::StructField;
use substrait_proto::proto::expression::{Literal, MaskExpression, RexType};
use substrait_proto::proto::extensions::simple_extension_declaration::MappingType;
use substrait_proto::proto::function_argument::ArgType;
use substrait_proto::proto::r#type::Kind;
use substrait_proto::proto::read_rel::ReadType;
use substrait_proto::proto::rel::RelType;
use substrait_proto::proto::{self, plan_rel, Expression, Plan as SubPlan, Rel};

use crate::adapter::error::{
    DatatypesSnafu, Error, InvalidQuerySnafu, NotImplementedSnafu, PlanSnafu, TableNotFoundSnafu,
};
use crate::expr::{
    AggregateExpr, AggregateFunc, BinaryFunc, GlobalId, MapFilterProject, SafeMfpPlan, ScalarExpr,
    UnaryFunc, UnmaterializableFunc, VariadicFunc,
};
use crate::plan::{AccumulablePlan, KeyValPlan, Plan, ReducePlan, TypedPlan};
use crate::repr::{self, ColumnType, RelationType};

macro_rules! not_impl_err {
    ($($arg:tt)*)  => {
        NotImplementedSnafu {
            reason: format!($($arg)*),
        }.fail()
    };
}

macro_rules! plan_err {
    ($($arg:tt)*)  => {
        PlanSnafu {
            reason: format!($($arg)*),
        }.fail()
    };
}

pub struct DataflowContext {
    id_to_name: HashMap<GlobalId, Vec<String>>,
    name_to_id: HashMap<Vec<String>, GlobalId>,
    schema: HashMap<GlobalId, RelationType>,
}

impl DataflowContext {
    pub fn new() -> Self {
        Self {
            id_to_name: HashMap::new(),
            name_to_id: HashMap::new(),
            schema: HashMap::new(),
        }
    }
    pub fn register_table(&mut self, id: GlobalId, name: Vec<String>) {
        self.id_to_name.insert(id, name.clone());
        self.name_to_id.insert(name, id);
        todo!("Table provider etc.")
    }

    /// Retrieves a GlobalId and table schema representing a table previously registered by calling the [register_table] function.
    ///
    /// Returns an error if no table has been registered with the provided names
    pub fn table(&self, name: &Vec<String>) -> Result<(GlobalId, RelationType), Error> {
        let id = self
            .name_to_id
            .get(name)
            .copied()
            .with_context(|| TableNotFoundSnafu {
                name: name.join("."),
            })?;
        let schema = self
            .schema
            .get(&id)
            .cloned()
            .with_context(|| TableNotFoundSnafu {
                name: name.join("."),
            })?;
        Ok((id, schema))
    }
}

pub fn from_substrait_plan(ctx: &mut DataflowContext, plan: &SubPlan) -> Result<TypedPlan, Error> {
    // Register function extension
    let function_extension = plan
        .extensions
        .iter()
        .map(|e| match &e.mapping_type {
            Some(ext) => match ext {
                MappingType::ExtensionFunction(ext_f) => Ok((ext_f.function_anchor, &ext_f.name)),
                _ => not_impl_err!("Extension type not supported: {ext:?}"),
            },
            None => not_impl_err!("Cannot parse empty extension"),
        })
        .collect::<Result<HashMap<_, _>, Error>>()?;

    // Parse relations
    match plan.relations.len() {
        1 => {
            match plan.relations[0].rel_type.as_ref() {
                Some(rt) => match rt {
                    plan_rel::RelType::Rel(rel) => {
                        Ok(from_substrait_rel(ctx, rel, &function_extension)?)
                    },
                    plan_rel::RelType::Root(root) => {
                        Ok(from_substrait_rel(ctx, root.input.as_ref().unwrap(), &function_extension)?)
                    }
                },
                None => plan_err!("Cannot parse plan relation: None")
            }
        },
        _ => not_impl_err!(
            "Substrait plan with more than 1 relation trees not supported. Number of relation trees: {:?}",
            plan.relations.len()
        )
    }
}

/// TODO: SELECT DISTINCT(does it get compile with something else?)
pub fn from_substrait_rel(
    ctx: &mut DataflowContext,
    rel: &Rel,
    extensions: &HashMap<u32, &String>,
) -> Result<TypedPlan, Error> {
    match &rel.rel_type {
        Some(RelType::Project(p)) => {
            let input = if let Some(input) = p.input.as_ref() {
                from_substrait_rel(ctx, input, extensions)?
            } else {
                return not_impl_err!("Projection without an input is not supported");
            };
            let mut exprs: Vec<(ScalarExpr, ColumnType)> = vec![];
            for e in &p.expressions {
                // TODO（discord9): deal with declare a constant
                let expr = from_substrait_rex(e, &input.typ, extensions)?;
                exprs.push(expr);
            }
            let is_literal = exprs.iter().all(|(expr, _)| expr.is_literal());
            if is_literal {
                let (literals, lit_types): (Vec<_>, Vec<_>) = exprs.into_iter().unzip();
                let typ = RelationType::new(lit_types);
                let row = literals
                    .into_iter()
                    .map(|lit| lit.as_literal().expect("A literal"))
                    .collect_vec();
                let row = repr::Row::new(row);
                let plan = Plan::Constant {
                    rows: vec![(row, repr::Timestamp::MIN, 1)],
                };
                Ok(TypedPlan { typ, plan })
            } else {
                input.projection(exprs)
            }
        }
        Some(RelType::Filter(filter)) => {
            let input = if let Some(input) = filter.input.as_ref() {
                from_substrait_rel(ctx, input, extensions)?
            } else {
                return not_impl_err!("Filter without an input is not supported");
            };

            let expr = if let Some(condition) = filter.condition.as_ref() {
                from_substrait_rex(condition, &input.typ, extensions)?
            } else {
                return not_impl_err!("Filter without an condition is not valid");
            };
            input.filter(expr)
        }
        Some(RelType::Read(read)) => match &read.as_ref().read_type {
            Some(ReadType::NamedTable(nt)) => {
                let table_reference = nt.names.clone();
                let table = ctx.table(&table_reference)?;
                let get_table = Plan::Get {
                    id: crate::expr::Id::Global(table.0),
                };
                let get_table = TypedPlan {
                    typ: table.1,
                    plan: get_table,
                };

                match &read.projection {
                    Some(MaskExpression {
                        select: Some(projection),
                        ..
                    }) => {
                        let column_indices: Vec<usize> = projection
                            .struct_items
                            .iter()
                            .map(|item| item.field as usize)
                            .collect();
                        let input_arity = get_table.typ.column_types.len();
                        let mfp =
                            MapFilterProject::new(input_arity).project(column_indices.clone())?;
                        get_table.mfp(mfp)
                    }
                    _ => Ok(get_table),
                }
            }
            _ => not_impl_err!("Only NamedTable reads are supported"),
        },
        Some(RelType::Aggregate(agg)) => from_substrait_agg_rel(ctx, agg, extensions),
        _ => not_impl_err!("Unsupported relation type: {:?}", rel.rel_type),
    }
}

fn from_substrait_agg_rel(
    ctx: &mut DataflowContext,
    agg: &proto::AggregateRel,
    extensions: &HashMap<u32, &String>,
) -> Result<TypedPlan, Error> {
    let input = if let Some(input) = agg.input.as_ref() {
        from_substrait_rel(ctx, input, extensions)?
    } else {
        return not_impl_err!("Aggregate without an input is not supported");
    };
    let mut group_expr = vec![];
    let mut aggr_exprs = vec![];

    match agg.groupings.len() {
        1 => {
            for e in &agg.groupings[0].grouping_expressions {
                let x = from_substrait_rex(e, &input.typ, extensions)?;
                group_expr.push(x);
            }
        }
        _ => {
            return not_impl_err!(
                "Grouping sets not support yet, use union all with group by instead."
            );
        }
    };

    let key_val_plan = {
        let (group_expr_val, _group_expr_type): (Vec<_>, Vec<_>) =
            group_expr.iter().cloned().unzip();
        let input_arity = input.typ.column_types.len();
        let output_arity = group_expr_val.len();
        let key_plan = MapFilterProject::new(input_arity)
            .map(group_expr_val)?
            .project(input_arity..input_arity + output_arity)?;
        let key_used = key_plan.demand();

        // find out all the columns that are not used in the key plan
        let value_indices = (0..input_arity)
            .filter(|i| !key_used.contains(i))
            .collect::<Vec<_>>();
        let val_plan = MapFilterProject::new(input_arity).project(value_indices.clone())?;
        KeyValPlan {
            key_plan: key_plan.into_safe(),
            val_plan: val_plan.into_safe(),
        }
    };

    for m in &agg.measures {
        let filter = match &m.filter {
            Some(fil) => Some(from_substrait_rex(fil, &input.typ, extensions)?),
            None => None,
        };

        let agg_func = match &m.measure {
            Some(f) => {
                let distinct = match f.invocation {
                    _ if f.invocation == AggregationInvocation::Distinct as i32 => true,
                    _ if f.invocation == AggregationInvocation::All as i32 => false,
                    _ => false,
                };
                from_substrait_agg_func(
                    f, &input.typ, extensions, filter, // TODO(discord9): impl order_by
                    None, distinct,
                )
            }
            None => not_impl_err!("Aggregate without aggregate function is not supported"),
        };
        aggr_exprs.push(agg_func?.clone());
    }
    // add another layer of mfp if aggr_expr's arg is not a direct column ref
    // to input.plan
    let need_mfp = aggr_exprs.iter().any(|agg| agg.expr.as_column().is_none());
    let input_plan = if need_mfp {
        // create mfp from aggr_expr, and modify aggr_expr to use the output column of mfp
        let input_arity = input.typ.column_types.len();
        let input_exprs = aggr_exprs
            .iter_mut()
            .enumerate()
            .map(|(idx, aggr)| {
                let ret = aggr.expr.clone();
                aggr.expr = ScalarExpr::Column(idx);
                ret
            })
            .collect_vec();
        let aggr_arity = aggr_exprs.len();
        let mfp = MapFilterProject::new(input.typ.column_types.len())
            .map(input_exprs.clone())?
            .project(input_arity..input_arity + aggr_arity)?;
        Plan::Mfp {
            input: Box::new(input.plan),
            mfp,
        }
    } else {
        input.plan
    };

    let output_type = {
        let mut output_types = Vec::new();
        for aggr in &aggr_exprs {
            output_types.push(ColumnType::new_nullable(
                aggr.func.signature().output.clone(),
            ));
        }
        RelationType::new(output_types)
    };
    let full_aggrs = aggr_exprs;
    let mut simple_aggrs = Vec::new();
    let mut distinct_aggrs = Vec::new();
    for (output_column, aggr_expr) in full_aggrs.iter().enumerate() {
        // TODO: support using literal as argument
        let input_column = aggr_expr.expr.as_column().with_context(|| PlanSnafu {
            reason: "Expect aggregate argument to be transformed into a column at this point",
        })?;
        if aggr_expr.distinct {
            distinct_aggrs.push((output_column, input_column, aggr_expr.clone()));
        } else {
            simple_aggrs.push((output_column, input_column, aggr_expr.clone()));
        }
    }
    let accum_plan = AccumulablePlan {
        full_aggrs,
        simple_aggrs,
        distinct_aggrs,
    };
    let plan = Plan::Reduce {
        input: Box::new(input_plan),
        key_val_plan,
        reduce_plan: ReducePlan::Accumulable(accum_plan),
    };
    Ok(TypedPlan {
        typ: output_type,
        plan,
    })
}

pub fn from_substrait_agg_func(
    f: &proto::AggregateFunction,
    input_schema: &RelationType,
    extensions: &HashMap<u32, &String>,
    filter: Option<(ScalarExpr, ColumnType)>,
    order_by: Option<Vec<(ScalarExpr, ColumnType)>>,
    distinct: bool,
) -> Result<AggregateExpr, Error> {
    // TODO(discord9): impl filter
    let _ = filter;
    let _ = order_by;
    let mut args = vec![];
    for arg in &f.arguments {
        let arg_expr = match &arg.arg_type {
            Some(ArgType::Value(e)) => from_substrait_rex(e, input_schema, extensions),
            _ => not_impl_err!("Aggregated function argument non-Value type not supported"),
        }?;
        args.push(arg_expr);
    }

    let arg = if let Some(first) = args.first() {
        first
    } else {
        return not_impl_err!("Aggregated function without arguments is not supported");
    };

    let func = match extensions.get(&f.function_reference) {
        Some(function_name) => {
            AggregateFunc::from_str_and_type(function_name, Some(arg.1.scalar_type.clone()))
        }
        None => not_impl_err!(
            "Aggregated function not found: function anchor = {:?}",
            f.function_reference
        ),
    }?;
    Ok(AggregateExpr {
        func,
        expr: arg.0.clone(),
        distinct,
    })
}

/// Convert Substrait Rex into Flow's ScalarExpr
pub fn from_substrait_rex(
    e: &Expression,
    input_schema: &RelationType,
    extensions: &HashMap<u32, &String>,
) -> Result<(ScalarExpr, ColumnType), Error> {
    match &e.rex_type {
        Some(RexType::Literal(lit)) => {
            let lit = from_substrait_literal(lit)?;
            Ok((
                ScalarExpr::Literal(lit.0, lit.1.clone()),
                ColumnType::new_nullable(lit.1),
            ))
        }
        Some(RexType::SingularOrList(s)) => {
            let substrait_expr = s.value.as_ref().unwrap();
            // Note that we didn't impl support to in list expr
            if !s.options.is_empty() {
                return not_impl_err!("In list expression is not supported");
            }
            from_substrait_rex(substrait_expr, input_schema, extensions)
        }
        Some(RexType::Selection(field_ref)) => match &field_ref.reference_type {
            Some(DirectReference(direct)) => match &direct.reference_type.as_ref() {
                Some(StructField(x)) => match &x.child.as_ref() {
                    Some(_) => {
                        not_impl_err!("Direct reference StructField with child is not supported")
                    }
                    None => {
                        let column = x.field as usize;
                        let column_type = input_schema.column_types[column].clone();
                        Ok((ScalarExpr::Column(column), column_type))
                    }
                },
                _ => not_impl_err!(
                    "Direct reference with types other than StructField is not supported"
                ),
            },
            _ => not_impl_err!("unsupported field ref type"),
        },
        Some(RexType::ScalarFunction(f)) => {
            let fn_name = extensions.get(&f.function_reference).ok_or_else(|| {
                NotImplementedSnafu {
                    reason: format!(
                        "Aggregated function not found: function reference = {:?}",
                        f.function_reference
                    ),
                }
                .build()
            })?;
            let arg_len = f.arguments.len();
            let arg_exprs: Vec<_> = f
                .arguments
                .iter()
                .map(|arg| match &arg.arg_type {
                    Some(ArgType::Value(e)) => from_substrait_rex(e, input_schema, extensions),
                    _ => not_impl_err!("Aggregated function argument non-Value type not supported"),
                })
                .try_collect()?;

            // literal's type is determined by the function and type of other args
            let (arg_exprs, arg_types): (Vec<_>, Vec<_>) = arg_exprs
                .into_iter()
                .map(|(arg_val, arg_type)| {
                    if arg_val.is_literal() {
                        (arg_val, None)
                    } else {
                        (arg_val, Some(arg_type.scalar_type))
                    }
                })
                .unzip();

            match arg_len {
                1 => {
                    // TODO: deal with cast(col AS type)
                    let func = UnaryFunc::from_str_and_type(fn_name, None)?;
                    let arg = arg_exprs[0].clone();
                    let ret_type = ColumnType::new_nullable(func.signature().output.clone());

                    Ok((arg.call_unary(func), ret_type))
                }
                2 => {
                    let func = BinaryFunc::from_str_and_types(fn_name, &arg_types[0..2])?;

                    // TODO: cast literal to the type of the other arg
                    let mut arg_exprs = arg_exprs;
                    for (idx, arg_expr) in arg_exprs.iter_mut().enumerate() {
                        if let ScalarExpr::Literal(val, typ) = arg_expr {
                            let dest_type = func.signature().input[idx].clone();
                            // cast val to target_type
                            let dest_val = datatypes::types::cast(val.clone(), &dest_type)
                            .with_context(|_|
                                DatatypesSnafu{
                                    extra: format!("Failed to implicitly cast literal {val:?} to type {dest_type:?}")
                                })?;
                            *val = dest_val;
                            *typ = dest_type;
                        }
                    }

                    let ret_type = ColumnType::new_nullable(func.signature().output.clone());
                    let ret_expr = arg_exprs[0].clone().call_binary(arg_exprs[1].clone(), func);
                    Ok((ret_expr, ret_type))
                }
                _var => {
                    if let Ok(func) = VariadicFunc::from_str_and_types(fn_name, &arg_types) {
                        let ret_type = ColumnType::new_nullable(func.signature().output.clone());
                        Ok((
                            ScalarExpr::CallVariadic {
                                func,
                                exprs: arg_exprs,
                            },
                            ret_type,
                        ))
                    } else if let Ok(func) = UnmaterializableFunc::from_str(fn_name) {
                        let ret_type = ColumnType::new_nullable(func.signature().output.clone());
                        Ok((ScalarExpr::CallUnmaterializable(func), ret_type))
                    } else {
                        not_impl_err!("Unsupported function {fn_name} with {arg_len} arguments")
                    }
                }
            }
        }
        Some(RexType::IfThen(if_then)) => {
            let ifs: Vec<_> = if_then
                .ifs
                .iter()
                .map(|if_clause| {
                    let proto_if = if_clause.r#if.as_ref().unwrap();
                    let proto_then = if_clause.then.as_ref().unwrap();
                    let cond = from_substrait_rex(proto_if, input_schema, extensions)?;
                    let then = from_substrait_rex(proto_then, input_schema, extensions)?;
                    Ok((cond, then))
                })
                .try_collect()?;
            // if no else is presented
            let els = if_then
                .r#else
                .as_ref()
                .map(|e| from_substrait_rex(e, input_schema, extensions))
                .transpose()?
                .unwrap_or_else(|| {
                    (
                        ScalarExpr::literal_null(),
                        ColumnType::new_nullable(CDT::null_datatype()),
                    )
                });
            fn build_if_then(
                mut next_if_then: impl Iterator<
                    Item = ((ScalarExpr, ColumnType), (ScalarExpr, ColumnType)),
                >,
                els: (ScalarExpr, ColumnType),
            ) -> (ScalarExpr, ColumnType) {
                if let Some((cond, then)) = next_if_then.next() {
                    // always assume the type of `if`` expr is the same with the `then`` expr
                    (
                        ScalarExpr::If {
                            cond: Box::new(cond.0),
                            then: Box::new(then.0),
                            els: Box::new(build_if_then(next_if_then, els).0),
                        },
                        then.1,
                    )
                } else {
                    els
                }
            }
            let expr_if = build_if_then(ifs.into_iter(), els);
            Ok(expr_if)
        }
        Some(RexType::Cast(cast)) => {
            let input = from_substrait_rex(cast.input.as_ref().unwrap(), input_schema, extensions)?;
            let cast_type = from_substrait_type(cast.r#type.as_ref().unwrap())?;
            let func = UnaryFunc::from_str_and_type("cast", Some(cast_type.clone()))?;
            Ok((
                input.0.call_unary(func),
                ColumnType::new_nullable(cast_type),
            ))
        }
        Some(RexType::WindowFunction(_)) => PlanSnafu {
            reason:
                "Window function is not supported yet. Please use aggregation function instead."
                    .to_string(),
        }
        .fail(),
        _ => not_impl_err!("unsupported rex_type"),
    }
}

/// Convert Substrait Expressions to DataFusion Exprs
pub async fn from_substrait_rex_vec(
    exprs: &Vec<Expression>,
    input_schema: &RelationType,
    extensions: &HashMap<u32, &String>,
) -> Result<Vec<(ScalarExpr, ColumnType)>, Error> {
    let mut expressions: Vec<_> = vec![];
    for expr in exprs {
        let expression = from_substrait_rex(expr, input_schema, extensions)?;
        expressions.push(expression);
    }
    Ok(expressions)
}

/// Convert a Substrait literal into a Value and its ConcreteDataType (So that we can know type even if the value is null)
pub fn from_substrait_literal(lit: &Literal) -> Result<(Value, CDT), Error> {
    let scalar_value = match &lit.literal_type {
        Some(LiteralType::Boolean(b)) => (Value::from(*b), CDT::boolean_datatype()),
        Some(LiteralType::I8(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_REF => (Value::from(*n as i8), CDT::int8_datatype()),
            UNSIGNED_INTEGER_TYPE_REF => (Value::from(*n as u8), CDT::uint8_datatype()),
            others => not_impl_err!("Unknown type variation reference {others}",)?,
        },
        Some(LiteralType::I16(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_REF => (Value::from(*n as i16), CDT::int16_datatype()),
            UNSIGNED_INTEGER_TYPE_REF => (Value::from(*n as u16), CDT::uint16_datatype()),
            others => not_impl_err!("Unknown type variation reference {others}",)?,
        },
        Some(LiteralType::I32(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_REF => (Value::from(*n), CDT::int32_datatype()),
            UNSIGNED_INTEGER_TYPE_REF => (Value::from(*n as u32), CDT::uint32_datatype()),
            others => not_impl_err!("Unknown type variation reference {others}",)?,
        },
        Some(LiteralType::I64(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_REF => (Value::from(*n), CDT::int64_datatype()),
            UNSIGNED_INTEGER_TYPE_REF => (Value::from(*n as u64), CDT::uint64_datatype()),
            others => not_impl_err!("Unknown type variation reference {others}",)?,
        },
        Some(LiteralType::Fp32(f)) => (Value::from(*f), CDT::float32_datatype()),
        Some(LiteralType::Fp64(f)) => (Value::from(*f), CDT::float64_datatype()),
        Some(LiteralType::Timestamp(t)) => match lit.type_variation_reference {
            TIMESTAMP_SECOND_TYPE_REF => (
                Value::from(Timestamp::new_second(*t)),
                CDT::timestamp_second_datatype(),
            ),
            TIMESTAMP_MILLI_TYPE_REF => (
                Value::from(Timestamp::new_millisecond(*t)),
                CDT::timestamp_millisecond_datatype(),
            ),
            TIMESTAMP_MICRO_TYPE_REF => (
                Value::from(Timestamp::new_microsecond(*t)),
                CDT::timestamp_microsecond_datatype(),
            ),
            TIMESTAMP_NANO_TYPE_REF => (
                Value::from(Timestamp::new_nanosecond(*t)),
                CDT::timestamp_nanosecond_datatype(),
            ),
            others => not_impl_err!("Unknown type variation reference {others}",)?,
        },
        Some(LiteralType::Date(d)) => (Value::from(Date::new(*d)), CDT::date_datatype()),
        Some(LiteralType::String(s)) => (Value::from(s.clone()), CDT::string_datatype()),
        Some(LiteralType::Binary(b)) | Some(LiteralType::FixedBinary(b)) => {
            (Value::from(b.clone()), CDT::binary_datatype())
        }
        Some(LiteralType::Decimal(d)) => {
            let value: [u8; 16] = d.value.clone().try_into().map_err(|e| {
                PlanSnafu {
                    reason: format!("Failed to parse decimal value from {e:?}"),
                }
                .build()
            })?;
            let p: u8 = d.precision.try_into().map_err(|e| {
                PlanSnafu {
                    reason: format!("Failed to parse decimal precision: {e}"),
                }
                .build()
            })?;
            let s: i8 = d.scale.try_into().map_err(|e| {
                PlanSnafu {
                    reason: format!("Failed to parse decimal scale: {e}"),
                }
                .build()
            })?;
            let value = i128::from_le_bytes(value);
            (
                Value::from(Decimal128::new(value, p, s)),
                CDT::decimal128_datatype(p, s),
            )
        }
        Some(LiteralType::Null(ntype)) => (Value::Null, from_substrait_type(ntype)?),
        _ => not_impl_err!("unsupported literal_type")?,
    };
    Ok(scalar_value)
}

fn from_substrait_type(null_type: &substrait_proto::proto::Type) -> Result<CDT, Error> {
    if let Some(kind) = &null_type.kind {
        match kind {
            Kind::Bool(_) => Ok(CDT::boolean_datatype()),
            Kind::I8(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_REF => Ok(CDT::int8_datatype()),
                UNSIGNED_INTEGER_TYPE_REF => Ok(CDT::uint8_datatype()),
                v => not_impl_err!("Unsupported Substrait type variation {v} of type {kind:?}"),
            },
            Kind::I16(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_REF => Ok(CDT::int16_datatype()),
                UNSIGNED_INTEGER_TYPE_REF => Ok(CDT::uint16_datatype()),
                v => not_impl_err!("Unsupported Substrait type variation {v} of type {kind:?}"),
            },
            Kind::I32(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_REF => Ok(CDT::int32_datatype()),
                UNSIGNED_INTEGER_TYPE_REF => Ok(CDT::uint32_datatype()),
                v => not_impl_err!("Unsupported Substrait type variation {v} of type {kind:?}"),
            },
            Kind::I64(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_REF => Ok(CDT::int64_datatype()),
                UNSIGNED_INTEGER_TYPE_REF => Ok(CDT::uint64_datatype()),
                v => not_impl_err!("Unsupported Substrait type variation {v} of type {kind:?}"),
            },
            Kind::Fp32(_) => Ok(CDT::float32_datatype()),
            Kind::Fp64(_) => Ok(CDT::float64_datatype()),
            Kind::Timestamp(ts) => match ts.type_variation_reference {
                TIMESTAMP_SECOND_TYPE_REF => Ok(CDT::timestamp_second_datatype()),
                TIMESTAMP_MILLI_TYPE_REF => Ok(CDT::timestamp_millisecond_datatype()),
                TIMESTAMP_MICRO_TYPE_REF => Ok(CDT::timestamp_microsecond_datatype()),
                TIMESTAMP_NANO_TYPE_REF => Ok(CDT::timestamp_nanosecond_datatype()),
                v => not_impl_err!("Unsupported Substrait type variation {v} of type {kind:?}"),
            },
            Kind::Date(date) => match date.type_variation_reference {
                DATE_32_TYPE_REF => Ok(CDT::date_datatype()),
                DATE_64_TYPE_REF => Ok(CDT::date_datatype()),
                v => not_impl_err!("Unsupported Substrait type variation {v} of type {kind:?}"),
            },
            Kind::Binary(_) => Ok(CDT::binary_datatype()),
            Kind::String(_) => Ok(CDT::string_datatype()),
            Kind::Decimal(d) => Ok(CDT::decimal128_datatype(d.precision as u8, d.scale as i8)),
            _ => not_impl_err!("Unsupported Substrait type: {kind:?}"),
        }
    } else {
        not_impl_err!("Null type without kind is not supported")
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use catalog::RegisterTableRequest;
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, NUMBERS_TABLE_ID};
    use prost::Message;
    use query::parser::QueryLanguageParser;
    use query::plan::LogicalPlan;
    use query::QueryEngine;
    use session::context::QueryContext;
    use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
    use table::table::numbers::{NumbersTable, NUMBERS_TABLE_NAME};

    use super::*;

    fn create_test_ctx() -> DataflowContext {
        let gid = GlobalId::User(0);
        let name = vec!["numbers".to_string()];
        let schema = RelationType::new(vec![ColumnType::new(CDT::uint32_datatype(), false)]);

        DataflowContext {
            id_to_name: HashMap::from([(gid, name.clone())]),
            name_to_id: HashMap::from([(name.clone(), gid)]),
            schema: HashMap::from([(gid, schema)]),
        }
    }

    fn create_test_query_engine() -> Arc<dyn QueryEngine> {
        let catalog_list = catalog::memory::new_memory_catalog_manager().unwrap();
        let req = RegisterTableRequest {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: NUMBERS_TABLE_NAME.to_string(),
            table_id: NUMBERS_TABLE_ID,
            table: NumbersTable::table(NUMBERS_TABLE_ID),
        };
        catalog_list.register_table_sync(req).unwrap();
        let factory = query::QueryEngineFactory::new(catalog_list, None, None, None, false);

        let engine = factory.query_engine();

        assert_eq!("datafusion", engine.name());
        engine
    }

    async fn sql_to_substrait(engine: Arc<dyn QueryEngine>, sql: &str) -> proto::Plan {
        // let engine = create_test_query_engine();
        let stmt = QueryLanguageParser::parse_sql(sql, &QueryContext::arc()).unwrap();
        let plan = engine
            .planner()
            .plan(stmt, QueryContext::arc())
            .await
            .unwrap();
        let LogicalPlan::DfPlan(plan) = plan;
        // encode then decode so to rely on the impl of conversion from logical plan to substrait plan
        let bytes = DFLogicalSubstraitConvertor {}.encode(&plan).unwrap();

        proto::Plan::decode(bytes).unwrap()
    }

    #[tokio::test]
    async fn test_literal() {
        let engine = create_test_query_engine();
        let sql = "SELECT 1 FROM numbers";
        let plan = sql_to_substrait(engine.clone(), sql).await;

        let mut ctx = create_test_ctx();
        let flow_plan = from_substrait_plan(&mut ctx, &plan);

        let expected = TypedPlan {
            typ: RelationType::new(vec![ColumnType::new(CDT::int64_datatype(), true)]),
            plan: Plan::Constant {
                rows: vec![(
                    repr::Row::new(vec![Value::Int64(1)]),
                    repr::Timestamp::MIN,
                    1,
                )],
            },
        };

        assert_eq!(flow_plan.unwrap(), expected);
    }

    /// test if the type of the literal is correctly inferred, i.e. in here literal is decoded to be int64, but need to be uint32,
    #[tokio::test]
    async fn test_implicitly_cast() {
        let engine = create_test_query_engine();
        let sql = "SELECT number+1 FROM numbers";
        let plan = sql_to_substrait(engine.clone(), sql).await;

        let mut ctx = create_test_ctx();
        let flow_plan = from_substrait_plan(&mut ctx, &plan);

        let expected = TypedPlan {
            typ: RelationType::new(vec![ColumnType::new(CDT::uint32_datatype(), true)]),
            plan: Plan::Mfp {
                input: Box::new(Plan::Get {
                    id: crate::expr::Id::Global(GlobalId::User(0)),
                }),
                mfp: MapFilterProject::new(1)
                    .map(vec![ScalarExpr::Column(0).call_binary(
                        ScalarExpr::Literal(Value::from(1u32), CDT::uint32_datatype()),
                        BinaryFunc::AddUInt32,
                    )])
                    .unwrap()
                    .project(vec![1])
                    .unwrap(),
            },
        };
        assert_eq!(flow_plan.unwrap(), expected);
    }

    #[tokio::test]
    async fn test_cast() {
        let engine = create_test_query_engine();
        let sql = "SELECT CAST(1 AS INT16) FROM numbers";
        let plan = sql_to_substrait(engine.clone(), sql).await;

        let mut ctx = create_test_ctx();
        let flow_plan = from_substrait_plan(&mut ctx, &plan);

        let expected = TypedPlan {
            typ: RelationType::new(vec![ColumnType::new(CDT::int16_datatype(), true)]),
            plan: Plan::Mfp {
                input: Box::new(Plan::Get {
                    id: crate::expr::Id::Global(GlobalId::User(0)),
                }),
                mfp: MapFilterProject::new(1)
                    .map(vec![ScalarExpr::Literal(
                        Value::Int64(1),
                        CDT::int64_datatype(),
                    )
                    .call_unary(UnaryFunc::Cast(CDT::int16_datatype()))])
                    .unwrap()
                    .project(vec![1])
                    .unwrap(),
            },
        };
        assert_eq!(flow_plan.unwrap(), expected);
    }

    #[tokio::test]
    async fn test_select() {
        let engine = create_test_query_engine();
        let sql = "SELECT number FROM numbers";
        let plan = sql_to_substrait(engine.clone(), sql).await;

        let mut ctx = create_test_ctx();
        let flow_plan = from_substrait_plan(&mut ctx, &plan);

        let expected = TypedPlan {
            typ: RelationType::new(vec![ColumnType::new(CDT::uint32_datatype(), false)]),
            plan: Plan::Mfp {
                input: Box::new(Plan::Get {
                    id: crate::expr::Id::Global(GlobalId::User(0)),
                }),
                mfp: MapFilterProject::new(1)
                    .map(vec![ScalarExpr::Column(0)])
                    .unwrap()
                    .project(vec![1])
                    .unwrap(),
            },
        };

        assert_eq!(flow_plan.unwrap(), expected);
    }

    #[tokio::test]
    async fn test_select_add() {
        let engine = create_test_query_engine();
        let sql = "SELECT number+number FROM numbers";
        let plan = sql_to_substrait(engine.clone(), sql).await;

        let mut ctx = create_test_ctx();
        let flow_plan = from_substrait_plan(&mut ctx, &plan);

        let expected = TypedPlan {
            typ: RelationType::new(vec![ColumnType::new(CDT::uint32_datatype(), true)]),
            plan: Plan::Mfp {
                input: Box::new(Plan::Get {
                    id: crate::expr::Id::Global(GlobalId::User(0)),
                }),
                mfp: MapFilterProject::new(1)
                    .map(vec![ScalarExpr::Column(0)
                        .call_binary(ScalarExpr::Column(0), BinaryFunc::AddUInt32)])
                    .unwrap()
                    .project(vec![1])
                    .unwrap(),
            },
        };

        assert_eq!(flow_plan.unwrap(), expected);
    }

    #[tokio::test]
    async fn test_sum() {
        let engine = create_test_query_engine();
        let sql = "SELECT sum(number) FROM numbers";
        let plan = sql_to_substrait(engine.clone(), sql).await;

        let mut ctx = create_test_ctx();
        let flow_plan = from_substrait_plan(&mut ctx, &plan);

        let aggr_expr = AggregateExpr {
            func: AggregateFunc::SumUInt32,
            expr: ScalarExpr::Column(0),
            distinct: false,
        };
        let expected = TypedPlan {
            typ: RelationType::new(vec![ColumnType::new(CDT::uint32_datatype(), true)]),
            plan: Plan::Mfp {
                input: Box::new(Plan::Reduce {
                    input: Box::new(Plan::Get {
                        id: crate::expr::Id::Global(GlobalId::User(0)),
                    }),
                    key_val_plan: KeyValPlan {
                        key_plan: MapFilterProject::new(1)
                            .project(vec![])
                            .unwrap()
                            .into_safe(),
                        val_plan: MapFilterProject::new(1)
                            .project(vec![0])
                            .unwrap()
                            .into_safe(),
                    },
                    reduce_plan: ReducePlan::Accumulable(AccumulablePlan {
                        full_aggrs: vec![aggr_expr.clone()],
                        simple_aggrs: vec![(0, 0, aggr_expr.clone())],
                        distinct_aggrs: vec![],
                    }),
                }),
                mfp: MapFilterProject::new(1)
                    .map(vec![ScalarExpr::Column(0)])
                    .unwrap()
                    .project(vec![1])
                    .unwrap(),
            },
        };
        assert_eq!(flow_plan.unwrap(), expected);
    }

    #[tokio::test]
    async fn test_sum_add() {
        let engine = create_test_query_engine();
        let sql = "SELECT sum(number+number) FROM numbers";
        let plan = sql_to_substrait(engine.clone(), sql).await;

        let mut ctx = create_test_ctx();
        let flow_plan = from_substrait_plan(&mut ctx, &plan);

        let aggr_expr = AggregateExpr {
            func: AggregateFunc::SumUInt32,
            expr: ScalarExpr::Column(0),
            distinct: false,
        };
        let expected = TypedPlan {
            typ: RelationType::new(vec![ColumnType::new(CDT::uint32_datatype(), true)]),
            plan: Plan::Mfp {
                input: Box::new(Plan::Reduce {
                    input: Box::new(Plan::Mfp {
                        input: Box::new(Plan::Get {
                            id: crate::expr::Id::Global(GlobalId::User(0)),
                        }),
                        mfp: MapFilterProject::new(1)
                            .map(vec![ScalarExpr::Column(0)
                                .call_binary(ScalarExpr::Column(0), BinaryFunc::AddUInt32)])
                            .unwrap()
                            .project(vec![1])
                            .unwrap(),
                    }),
                    key_val_plan: KeyValPlan {
                        key_plan: MapFilterProject::new(1)
                            .project(vec![])
                            .unwrap()
                            .into_safe(),
                        val_plan: MapFilterProject::new(1)
                            .project(vec![0])
                            .unwrap()
                            .into_safe(),
                    },
                    reduce_plan: ReducePlan::Accumulable(AccumulablePlan {
                        full_aggrs: vec![aggr_expr.clone()],
                        simple_aggrs: vec![(0, 0, aggr_expr.clone())],
                        distinct_aggrs: vec![],
                    }),
                }),
                mfp: MapFilterProject::new(1)
                    .map(vec![ScalarExpr::Column(0)])
                    .unwrap()
                    .project(vec![1])
                    .unwrap(),
            },
        };
        assert_eq!(flow_plan.unwrap(), expected);
    }
}
