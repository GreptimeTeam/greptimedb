use std::collections::VecDeque;
use std::str::FromStr;

use datafusion::logical_plan::{Column, Expr};
use datafusion_expr::{expr_fn, BuiltinScalarFunction, Operator};
use datatypes::schema::Schema;
use snafu::{ensure, OptionExt};
use substrait_proto::protobuf::expression::field_reference::ReferenceType as FieldReferenceType;
use substrait_proto::protobuf::expression::reference_segment::ReferenceType as SegReferenceType;
use substrait_proto::protobuf::expression::{FieldReference, RexType, ScalarFunction};
use substrait_proto::protobuf::function_argument::ArgType;
use substrait_proto::protobuf::Expression;

use crate::context::ConvertorContext;
use crate::error::{EmptyExprSnafu, InvalidParametersSnafu, Result, UnsupportedExprSnafu};

/// Convert substrait's `Expression` to DataFusion's `Expr`.
pub fn to_df_expr(ctx: &ConvertorContext, expression: Expression, schema: &Schema) -> Result<Expr> {
    let expr_rex_type = expression.rex_type.context(EmptyExprSnafu)?;
    match expr_rex_type {
        RexType::Literal(_literal) => todo!(),
        RexType::Selection(selection) => convert_selection_rex(selection, schema),
        RexType::ScalarFunction(scalar_fn) => convert_scalar_function(ctx, scalar_fn, schema),
        RexType::WindowFunction(_) => todo!(),
        RexType::IfThen(_) => todo!(),
        RexType::SwitchExpression(_) => todo!(),
        RexType::SingularOrList(_) => todo!(),
        RexType::MultiOrList(_) => todo!(),
        RexType::Cast(_) => todo!(),
        RexType::Subquery(_) => todo!(),
        RexType::Enum(_) => todo!(),
    }
}

pub fn convert_selection_rex(selection: Box<FieldReference>, schema: &Schema) -> Result<Expr> {
    if let Some(FieldReferenceType::DirectReference(direct_ref)) = selection.reference_type
    && let Some(SegReferenceType::StructField(field))  = direct_ref.reference_type{
        let column_name = schema.column_name_by_index(field.field as _).to_string();
         Ok(Expr::Column(Column{ relation: None, name: column_name }))
    } else{
        InvalidParametersSnafu{reason:"Only support direct struct reference in Selection Rex"}.fail()
    }
}

pub fn convert_scalar_function(
    ctx: &ConvertorContext,
    scalar_fn: ScalarFunction,
    schema: &Schema,
) -> Result<Expr> {
    // convert argument
    let mut inputs = VecDeque::with_capacity(scalar_fn.arguments.len());
    for arg in scalar_fn.arguments {
        if let Some(ArgType::Value(sub_expr)) = arg.arg_type {
            inputs.push_back(to_df_expr(ctx, sub_expr, schema)?);
        } else {
            InvalidParametersSnafu {
                reason: format!("Only value expression arg is supported to be function argument"),
            }
            .fail()?;
        }
    }

    // convert this scalar function
    // map function name
    let anchor = scalar_fn.function_reference;
    let fn_name = ctx.find_scalar_fn(anchor).context(InvalidParametersSnafu {
        reason: format!("Unregistered scalar function reference: {}", anchor),
    })?;

    // convenient util
    let ensure_arg_len = |expected: usize| -> Result<()> {
        ensure!(
            inputs.len() == expected,
            InvalidParametersSnafu {
                reason: format!(
                    "Invalid number of scalar function {}, expected {} but found {}",
                    fn_name,
                    expected,
                    inputs.len()
                )
            }
        );
        Ok(())
    };

    // onstruct DataFusion expr
    let expr = match fn_name {
        // begin binary exprs, with the same order of DF `Operator`'s definition.
        "eq" | "equal" => {
            ensure_arg_len(2)?;
            inputs.pop_front().unwrap().eq(inputs.pop_front().unwrap())
        }
        "not_eq" | "not_equal" => {
            ensure_arg_len(2)?;
            inputs
                .pop_front()
                .unwrap()
                .not_eq(inputs.pop_front().unwrap())
        }
        "lt" => {
            ensure_arg_len(2)?;
            inputs.pop_front().unwrap().lt(inputs.pop_front().unwrap())
        }
        "lt_eq" | "lte" => {
            ensure_arg_len(2)?;
            inputs
                .pop_front()
                .unwrap()
                .lt_eq(inputs.pop_front().unwrap())
        }
        "gt" => {
            ensure_arg_len(2)?;
            inputs.pop_front().unwrap().gt(inputs.pop_front().unwrap())
        }
        "gt_eq" | "gte" => {
            ensure_arg_len(2)?;
            inputs
                .pop_front()
                .unwrap()
                .gt_eq(inputs.pop_front().unwrap())
        }
        "plus" => {
            ensure_arg_len(2)?;
            expr_fn::binary_expr(
                inputs.pop_front().unwrap(),
                Operator::Plus,
                inputs.pop_front().unwrap(),
            )
        }
        "minus" => {
            ensure_arg_len(2)?;
            expr_fn::binary_expr(
                inputs.pop_front().unwrap(),
                Operator::Minus,
                inputs.pop_front().unwrap(),
            )
        }
        "multiply" => {
            ensure_arg_len(2)?;
            expr_fn::binary_expr(
                inputs.pop_front().unwrap(),
                Operator::Multiply,
                inputs.pop_front().unwrap(),
            )
        }
        "divide" => {
            ensure_arg_len(2)?;
            expr_fn::binary_expr(
                inputs.pop_front().unwrap(),
                Operator::Divide,
                inputs.pop_front().unwrap(),
            )
        }
        "modulo" => {
            ensure_arg_len(2)?;
            expr_fn::binary_expr(
                inputs.pop_front().unwrap(),
                Operator::Modulo,
                inputs.pop_front().unwrap(),
            )
        }
        "and" => {
            ensure_arg_len(2)?;
            expr_fn::and(inputs.pop_front().unwrap(), inputs.pop_front().unwrap())
        }
        "or" => {
            ensure_arg_len(2)?;
            expr_fn::or(inputs.pop_front().unwrap(), inputs.pop_front().unwrap())
        }
        "like" => {
            ensure_arg_len(2)?;
            inputs
                .pop_front()
                .unwrap()
                .like(inputs.pop_front().unwrap())
        }
        "not_like" => {
            ensure_arg_len(2)?;
            inputs
                .pop_front()
                .unwrap()
                .not_like(inputs.pop_front().unwrap())
        }
        "is_distinct_from" => {
            ensure_arg_len(2)?;
            expr_fn::binary_expr(
                inputs.pop_front().unwrap(),
                Operator::IsDistinctFrom,
                inputs.pop_front().unwrap(),
            )
        }
        "is_not_distinct_from" => {
            ensure_arg_len(2)?;
            expr_fn::binary_expr(
                inputs.pop_front().unwrap(),
                Operator::IsNotDistinctFrom,
                inputs.pop_front().unwrap(),
            )
        }
        "regex_match" => {
            ensure_arg_len(2)?;
            expr_fn::binary_expr(
                inputs.pop_front().unwrap(),
                Operator::RegexMatch,
                inputs.pop_front().unwrap(),
            )
        }
        "regex_i_match" => {
            ensure_arg_len(2)?;
            expr_fn::binary_expr(
                inputs.pop_front().unwrap(),
                Operator::RegexIMatch,
                inputs.pop_front().unwrap(),
            )
        }
        "regex_not_match" => {
            ensure_arg_len(2)?;
            expr_fn::binary_expr(
                inputs.pop_front().unwrap(),
                Operator::RegexNotMatch,
                inputs.pop_front().unwrap(),
            )
        }
        "regex_not_i_match" => {
            ensure_arg_len(2)?;
            expr_fn::binary_expr(
                inputs.pop_front().unwrap(),
                Operator::RegexNotIMatch,
                inputs.pop_front().unwrap(),
            )
        }
        "bitwise_and" => {
            ensure_arg_len(2)?;
            expr_fn::binary_expr(
                inputs.pop_front().unwrap(),
                Operator::BitwiseAnd,
                inputs.pop_front().unwrap(),
            )
        }
        "bitwise_or" => {
            ensure_arg_len(2)?;
            expr_fn::binary_expr(
                inputs.pop_front().unwrap(),
                Operator::BitwiseOr,
                inputs.pop_front().unwrap(),
            )
        }
        // end binary exprs
        // start other direct expr, with the same order of DF `Expr`'s definition.
        "not" => {
            ensure_arg_len(1)?;
            inputs.pop_front().unwrap().not()
        }
        "is_not_null" => {
            ensure_arg_len(1)?;
            inputs.pop_front().unwrap().is_not_null()
        }
        "is_null" => {
            ensure_arg_len(1)?;
            inputs.pop_front().unwrap().is_null()
        }
        "negative" => {
            ensure_arg_len(1)?;
            Expr::Negative(Box::new(inputs.pop_front().unwrap()))
        }
        // skip GetIndexedField, unimplemented.
        "between" => {
            ensure_arg_len(3)?;
            Expr::Between {
                expr: Box::new(inputs.pop_front().unwrap()),
                negated: false,
                low: Box::new(inputs.pop_front().unwrap()),
                high: Box::new(inputs.pop_front().unwrap()),
            }
        }
        "not_between" => {
            ensure_arg_len(3)?;
            Expr::Between {
                expr: Box::new(inputs.pop_front().unwrap()),
                negated: true,
                low: Box::new(inputs.pop_front().unwrap()),
                high: Box::new(inputs.pop_front().unwrap()),
            }
        }
        // skip Case, is covered in substrait::SwitchExpression.
        // skip Cast and TryCast, is covered in substrait::Cast.
        "sort" | "sort_des" => {
            ensure_arg_len(1)?;
            Expr::Sort {
                expr: Box::new(inputs.pop_front().unwrap()),
                asc: false,
                nulls_first: false,
            }
        }
        "sort_asc" => {
            ensure_arg_len(1)?;
            Expr::Sort {
                expr: Box::new(inputs.pop_front().unwrap()),
                asc: true,
                nulls_first: false,
            }
        }
        // those are datafusion built-in "scalar functions".
        "abs"
        | "acos"
        | "asin"
        | "atan"
        | "atan2"
        | "ceil"
        | "cos"
        | "exp"
        | "floor"
        | "ln"
        | "log"
        | "log10"
        | "log2"
        | "power"
        | "pow"
        | "round"
        | "signum"
        | "sin"
        | "sqrt"
        | "tan"
        | "trunc"
        | "coalesce"
        | "make_array"
        | "ascii"
        | "bit_length"
        | "btrim"
        | "char_length"
        | "character_length"
        | "concat"
        | "concat_ws"
        | "chr"
        | "current_date"
        | "current_time"
        | "date_part"
        | "datepart"
        | "date_trunc"
        | "datetrunc"
        | "date_bin"
        | "initcap"
        | "left"
        | "length"
        | "lower"
        | "lpad"
        | "ltrim"
        | "md5"
        | "nullif"
        | "octet_length"
        | "random"
        | "regexp_replace"
        | "repeat"
        | "replace"
        | "reverse"
        | "right"
        | "rpad"
        | "rtrim"
        | "sha224"
        | "sha256"
        | "sha384"
        | "sha512"
        | "digest"
        | "split_part"
        | "starts_with"
        | "strpos"
        | "substr"
        | "to_hex"
        | "to_timestamp"
        | "to_timestamp_millis"
        | "to_timestamp_micros"
        | "to_timestamp_seconds"
        | "now"
        | "translate"
        | "trim"
        | "upper"
        | "uuid"
        | "regexp_match"
        | "struct"
        | "from_unixtime"
        | "arrow_typeof" => Expr::ScalarFunction {
            fun: BuiltinScalarFunction::from_str(fn_name).unwrap(),
            args: inputs.into(),
        },
        // skip ScalarUDF, unimplemented.
        // skip AggregateFunction, is covered in substrait::AggregateRel
        // skip WindowFunction, is covered in substrait WindowFunction
        // skip AggregateUDF, unimplemented.
        // skip InList, unimplemented
        // skip Wildcard, unimplemented.
        // end other direct expr
        _ => UnsupportedExprSnafu {
            name: format!("scalar function {}", fn_name),
        }
        .fail()?,
    };

    Ok(expr)
}

/// Convert DataFusion's `Expr` to substrait's `Expression`
pub fn from_df_expr(expr: &Expr) -> Result<Expression> {
    todo!()
}
