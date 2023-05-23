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

use std::collections::VecDeque;
use std::str::FromStr;

use datafusion::common::Column;
use datafusion_expr::expr::Sort;
use datafusion_expr::{expr_fn, lit, Between, BinaryExpr, BuiltinScalarFunction, Expr, Operator};
use datatypes::schema::Schema;
use snafu::{ensure, OptionExt};
use substrait_proto::proto::expression::field_reference::ReferenceType as FieldReferenceType;
use substrait_proto::proto::expression::reference_segment::{
    ReferenceType as SegReferenceType, StructField,
};
use substrait_proto::proto::expression::{
    FieldReference, Literal, ReferenceSegment, RexType, ScalarFunction,
};
use substrait_proto::proto::function_argument::ArgType;
use substrait_proto::proto::Expression;

use crate::context::ConvertorContext;
use crate::error::{
    EmptyExprSnafu, InvalidParametersSnafu, MissingFieldSnafu, Result, UnsupportedExprSnafu,
};
use crate::types::{literal_type_to_scalar_value, scalar_value_as_literal_type};

/// Convert substrait's `Expression` to DataFusion's `Expr`.
pub(crate) fn to_df_expr(
    ctx: &ConvertorContext,
    expression: Expression,
    schema: &Schema,
) -> Result<Expr> {
    let expr_rex_type = expression.rex_type.context(EmptyExprSnafu)?;
    match expr_rex_type {
        RexType::Literal(l) => {
            let t = l.literal_type.context(MissingFieldSnafu {
                field: "LiteralType",
                plan: "Literal",
            })?;
            let v = literal_type_to_scalar_value(t)?;
            Ok(lit(v))
        }
        RexType::Selection(selection) => convert_selection_rex(*selection, schema),
        RexType::ScalarFunction(scalar_fn) => convert_scalar_function(ctx, scalar_fn, schema),
        RexType::WindowFunction(_)
        | RexType::IfThen(_)
        | RexType::SwitchExpression(_)
        | RexType::SingularOrList(_)
        | RexType::MultiOrList(_)
        | RexType::Cast(_)
        | RexType::Subquery(_)
        | RexType::Nested(_)
        | RexType::Enum(_) => UnsupportedExprSnafu {
            name: format!("substrait expression {expr_rex_type:?}"),
        }
        .fail()?,
    }
}

/// Convert Substrait's `FieldReference` - `DirectReference` - `StructField` to Datafusion's
/// `Column` expr.
pub fn convert_selection_rex(selection: FieldReference, schema: &Schema) -> Result<Expr> {
    if let Some(FieldReferenceType::DirectReference(direct_ref)) = selection.reference_type
    && let Some(SegReferenceType::StructField(field)) = direct_ref.reference_type {
        let column_name = schema.column_name_by_index(field.field as _).to_string();
        Ok(Expr::Column(Column {
            relation: None,
            name: column_name,
        }))
    } else {
        InvalidParametersSnafu {
            reason: "Only support direct struct reference in Selection Rex",
        }
        .fail()
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
                reason: "Only value expression arg is supported to be function argument",
            }
            .fail()?;
        }
    }

    // convert this scalar function
    // map function name
    let anchor = scalar_fn.function_reference;
    let fn_name = ctx
        .find_scalar_fn(anchor)
        .with_context(|| InvalidParametersSnafu {
            reason: format!("Unregistered scalar function reference: {anchor}"),
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

    // construct DataFusion expr
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
            Expr::Between(Between {
                expr: Box::new(inputs.pop_front().unwrap()),
                negated: false,
                low: Box::new(inputs.pop_front().unwrap()),
                high: Box::new(inputs.pop_front().unwrap()),
            })
        }
        "not_between" => {
            ensure_arg_len(3)?;
            Expr::Between(Between {
                expr: Box::new(inputs.pop_front().unwrap()),
                negated: true,
                low: Box::new(inputs.pop_front().unwrap()),
                high: Box::new(inputs.pop_front().unwrap()),
            })
        }
        // skip Case, is covered in substrait::SwitchExpression.
        // skip Cast and TryCast, is covered in substrait::Cast.
        "sort" | "sort_des" => {
            ensure_arg_len(1)?;
            Expr::Sort(Sort {
                expr: Box::new(inputs.pop_front().unwrap()),
                asc: false,
                nulls_first: false,
            })
        }
        "sort_asc" => {
            ensure_arg_len(1)?;
            Expr::Sort(Sort {
                expr: Box::new(inputs.pop_front().unwrap()),
                asc: true,
                nulls_first: false,
            })
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
            name: format!("scalar function {fn_name}"),
        }
        .fail()?,
    };

    Ok(expr)
}

/// Convert DataFusion's `Expr` to substrait's `Expression`
pub fn expression_from_df_expr(
    ctx: &mut ConvertorContext,
    expr: &Expr,
    schema: &Schema,
) -> Result<Expression> {
    let expression = match expr {
        // Don't merge them with other unsupported expr arms to preserve the ordering.
        Expr::Alias(..) => UnsupportedExprSnafu {
            name: expr.to_string(),
        }
        .fail()?,
        Expr::Column(column) => {
            let field_reference = convert_column(column, schema)?;
            Expression {
                rex_type: Some(RexType::Selection(Box::new(field_reference))),
            }
        }
        // Don't merge them with other unsupported expr arms to preserve the ordering.
        Expr::ScalarVariable(..) => UnsupportedExprSnafu {
            name: expr.to_string(),
        }
        .fail()?,
        Expr::Literal(v) => {
            let t = scalar_value_as_literal_type(v)?;
            let l = Literal {
                nullable: true,
                type_variation_reference: 0,
                literal_type: Some(t),
            };
            Expression {
                rex_type: Some(RexType::Literal(l)),
            }
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let left = expression_from_df_expr(ctx, left, schema)?;
            let right = expression_from_df_expr(ctx, right, schema)?;
            let arguments = utils::expression_to_argument(vec![left, right]);
            let op_name = utils::name_df_operator(op);
            let function_reference = ctx.register_scalar_fn(op_name);
            utils::build_scalar_function_expression(function_reference, arguments)
        }
        Expr::Not(e) => {
            let arg = expression_from_df_expr(ctx, e, schema)?;
            let arguments = utils::expression_to_argument(vec![arg]);
            let op_name = "not";
            let function_reference = ctx.register_scalar_fn(op_name);
            utils::build_scalar_function_expression(function_reference, arguments)
        }
        Expr::IsNotNull(e) => {
            let arg = expression_from_df_expr(ctx, e, schema)?;
            let arguments = utils::expression_to_argument(vec![arg]);
            let op_name = "is_not_null";
            let function_reference = ctx.register_scalar_fn(op_name);
            utils::build_scalar_function_expression(function_reference, arguments)
        }
        Expr::IsNull(e) => {
            let arg = expression_from_df_expr(ctx, e, schema)?;
            let arguments = utils::expression_to_argument(vec![arg]);
            let op_name = "is_null";
            let function_reference = ctx.register_scalar_fn(op_name);
            utils::build_scalar_function_expression(function_reference, arguments)
        }
        Expr::Negative(e) => {
            let arg = expression_from_df_expr(ctx, e, schema)?;
            let arguments = utils::expression_to_argument(vec![arg]);
            let op_name = "negative";
            let function_reference = ctx.register_scalar_fn(op_name);
            utils::build_scalar_function_expression(function_reference, arguments)
        }
        // Don't merge them with other unsupported expr arms to preserve the ordering.
        Expr::GetIndexedField { .. } => UnsupportedExprSnafu {
            name: expr.to_string(),
        }
        .fail()?,
        Expr::Between(Between {
            expr,
            negated,
            low,
            high,
        }) => {
            let expr = expression_from_df_expr(ctx, expr, schema)?;
            let low = expression_from_df_expr(ctx, low, schema)?;
            let high = expression_from_df_expr(ctx, high, schema)?;
            let arguments = utils::expression_to_argument(vec![expr, low, high]);
            let op_name = if *negated { "not_between" } else { "between" };
            let function_reference = ctx.register_scalar_fn(op_name);
            utils::build_scalar_function_expression(function_reference, arguments)
        }
        // Don't merge them with other unsupported expr arms to preserve the ordering.
        Expr::Case { .. } | Expr::Cast { .. } | Expr::TryCast { .. } => UnsupportedExprSnafu {
            name: expr.to_string(),
        }
        .fail()?,
        Expr::Sort(Sort {
            expr,
            asc,
            nulls_first: _,
        }) => {
            let expr = expression_from_df_expr(ctx, expr, schema)?;
            let arguments = utils::expression_to_argument(vec![expr]);
            let op_name = if *asc { "sort_asc" } else { "sort_des" };
            let function_reference = ctx.register_scalar_fn(op_name);
            utils::build_scalar_function_expression(function_reference, arguments)
        }
        Expr::ScalarFunction { fun, args } => {
            let arguments = utils::expression_to_argument(
                args.iter()
                    .map(|e| expression_from_df_expr(ctx, e, schema))
                    .collect::<Result<Vec<_>>>()?,
            );
            let op_name = utils::name_builtin_scalar_function(fun);
            let function_reference = ctx.register_scalar_fn(op_name);
            utils::build_scalar_function_expression(function_reference, arguments)
        }
        // Don't merge them with other unsupported expr arms to preserve the ordering.
        Expr::ScalarUDF { .. }
        | Expr::AggregateFunction { .. }
        | Expr::WindowFunction { .. }
        | Expr::AggregateUDF { .. }
        | Expr::InList { .. }
        | Expr::Wildcard
        | Expr::Like(_)
        | Expr::ILike(_)
        | Expr::SimilarTo(_)
        | Expr::IsTrue(_)
        | Expr::IsFalse(_)
        | Expr::IsUnknown(_)
        | Expr::IsNotTrue(_)
        | Expr::IsNotFalse(_)
        | Expr::IsNotUnknown(_)
        | Expr::Exists { .. }
        | Expr::InSubquery { .. }
        | Expr::ScalarSubquery(..)
        | Expr::Placeholder { .. }
        | Expr::QualifiedWildcard { .. } => todo!(),
        Expr::GroupingSet(_) | Expr::OuterReferenceColumn(_, _) => UnsupportedExprSnafu {
            name: expr.to_string(),
        }
        .fail()?,
    };

    Ok(expression)
}

/// Convert DataFusion's `Column` expr into substrait's `FieldReference` -
/// `DirectReference` - `StructField`.
pub fn convert_column(column: &Column, schema: &Schema) -> Result<FieldReference> {
    let column_name = &column.name;
    let field_index =
        schema
            .column_index_by_name(column_name)
            .with_context(|| MissingFieldSnafu {
                field: format!("{column:?}"),
                plan: format!("schema: {schema:?}"),
            })?;

    Ok(FieldReference {
        reference_type: Some(FieldReferenceType::DirectReference(ReferenceSegment {
            reference_type: Some(SegReferenceType::StructField(Box::new(StructField {
                field: field_index as _,
                child: None,
            }))),
        })),
        root_type: None,
    })
}

/// Some utils special for this `DataFusion::Expr` and `Substrait::Expression` conversion.
mod utils {
    use datafusion_expr::{BuiltinScalarFunction, Operator};
    use substrait_proto::proto::expression::{RexType, ScalarFunction};
    use substrait_proto::proto::function_argument::ArgType;
    use substrait_proto::proto::{Expression, FunctionArgument};

    pub(crate) fn name_df_operator(op: &Operator) -> &str {
        match op {
            Operator::Eq => "equal",
            Operator::NotEq => "not_equal",
            Operator::Lt => "lt",
            Operator::LtEq => "lte",
            Operator::Gt => "gt",
            Operator::GtEq => "gte",
            Operator::Plus => "plus",
            Operator::Minus => "minus",
            Operator::Multiply => "multiply",
            Operator::Divide => "divide",
            Operator::Modulo => "modulo",
            Operator::And => "and",
            Operator::Or => "or",
            Operator::IsDistinctFrom => "is_distinct_from",
            Operator::IsNotDistinctFrom => "is_not_distinct_from",
            Operator::RegexMatch => "regex_match",
            Operator::RegexIMatch => "regex_i_match",
            Operator::RegexNotMatch => "regex_not_match",
            Operator::RegexNotIMatch => "regex_not_i_match",
            Operator::BitwiseAnd => "bitwise_and",
            Operator::BitwiseOr => "bitwise_or",
            Operator::BitwiseXor => "bitwise_xor",
            Operator::BitwiseShiftRight => "bitwise_shift_right",
            Operator::BitwiseShiftLeft => "bitwise_shift_left",
            Operator::StringConcat => "string_concat",
        }
    }

    /// Convert list of [Expression] to [FunctionArgument] vector.
    pub(crate) fn expression_to_argument<I: IntoIterator<Item = Expression>>(
        expressions: I,
    ) -> Vec<FunctionArgument> {
        expressions
            .into_iter()
            .map(|expr| FunctionArgument {
                arg_type: Some(ArgType::Value(expr)),
            })
            .collect()
    }

    /// Convenient builder for [Expression]
    pub(crate) fn build_scalar_function_expression(
        function_reference: u32,
        arguments: Vec<FunctionArgument>,
    ) -> Expression {
        Expression {
            rex_type: Some(RexType::ScalarFunction(ScalarFunction {
                function_reference,
                arguments,
                output_type: None,
                ..Default::default()
            })),
        }
    }

    pub(crate) fn name_builtin_scalar_function(fun: &BuiltinScalarFunction) -> &str {
        match fun {
            BuiltinScalarFunction::Abs => "abs",
            BuiltinScalarFunction::Acos => "acos",
            BuiltinScalarFunction::Asin => "asin",
            BuiltinScalarFunction::Atan => "atan",
            BuiltinScalarFunction::Ceil => "ceil",
            BuiltinScalarFunction::Cos => "cos",
            BuiltinScalarFunction::Digest => "digest",
            BuiltinScalarFunction::Exp => "exp",
            BuiltinScalarFunction::Floor => "floor",
            BuiltinScalarFunction::Ln => "ln",
            BuiltinScalarFunction::Log => "log",
            BuiltinScalarFunction::Log10 => "log10",
            BuiltinScalarFunction::Log2 => "log2",
            BuiltinScalarFunction::Round => "round",
            BuiltinScalarFunction::Signum => "signum",
            BuiltinScalarFunction::Sin => "sin",
            BuiltinScalarFunction::Sqrt => "sqrt",
            BuiltinScalarFunction::Tan => "tan",
            BuiltinScalarFunction::Trunc => "trunc",
            BuiltinScalarFunction::Ascii => "ascii",
            BuiltinScalarFunction::BitLength => "bit_length",
            BuiltinScalarFunction::Btrim => "btrim",
            BuiltinScalarFunction::CharacterLength => "character_length",
            BuiltinScalarFunction::Chr => "chr",
            BuiltinScalarFunction::Concat => "concat",
            BuiltinScalarFunction::ConcatWithSeparator => "concat_ws",
            BuiltinScalarFunction::DatePart => "date_part",
            BuiltinScalarFunction::DateTrunc => "date_trunc",
            BuiltinScalarFunction::InitCap => "initcap",
            BuiltinScalarFunction::Left => "left",
            BuiltinScalarFunction::Lpad => "lpad",
            BuiltinScalarFunction::Lower => "lower",
            BuiltinScalarFunction::Ltrim => "ltrim",
            BuiltinScalarFunction::MD5 => "md5",
            BuiltinScalarFunction::NullIf => "nullif",
            BuiltinScalarFunction::OctetLength => "octet_length",
            BuiltinScalarFunction::Random => "random",
            BuiltinScalarFunction::RegexpReplace => "regexp_replace",
            BuiltinScalarFunction::Repeat => "repeat",
            BuiltinScalarFunction::Replace => "replace",
            BuiltinScalarFunction::Reverse => "reverse",
            BuiltinScalarFunction::Right => "right",
            BuiltinScalarFunction::Rpad => "rpad",
            BuiltinScalarFunction::Rtrim => "rtrim",
            BuiltinScalarFunction::SHA224 => "sha224",
            BuiltinScalarFunction::SHA256 => "sha256",
            BuiltinScalarFunction::SHA384 => "sha384",
            BuiltinScalarFunction::SHA512 => "sha512",
            BuiltinScalarFunction::SplitPart => "split_part",
            BuiltinScalarFunction::StartsWith => "starts_with",
            BuiltinScalarFunction::Strpos => "strpos",
            BuiltinScalarFunction::Substr => "substr",
            BuiltinScalarFunction::ToHex => "to_hex",
            BuiltinScalarFunction::ToTimestamp => "to_timestamp",
            BuiltinScalarFunction::ToTimestampMillis => "to_timestamp_millis",
            BuiltinScalarFunction::ToTimestampMicros => "to_timestamp_macros",
            BuiltinScalarFunction::ToTimestampSeconds => "to_timestamp_seconds",
            BuiltinScalarFunction::Now => "now",
            BuiltinScalarFunction::Translate => "translate",
            BuiltinScalarFunction::Trim => "trim",
            BuiltinScalarFunction::Upper => "upper",
            BuiltinScalarFunction::RegexpMatch => "regexp_match",
            BuiltinScalarFunction::Atan2 => "atan2",
            BuiltinScalarFunction::Coalesce => "coalesce",
            BuiltinScalarFunction::Power => "power",
            BuiltinScalarFunction::MakeArray => "make_array",
            BuiltinScalarFunction::DateBin => "date_bin",
            BuiltinScalarFunction::FromUnixtime => "from_unixtime",
            BuiltinScalarFunction::CurrentDate => "current_date",
            BuiltinScalarFunction::CurrentTime => "current_time",
            BuiltinScalarFunction::Uuid => "uuid",
            BuiltinScalarFunction::Struct => "struct",
            BuiltinScalarFunction::ArrowTypeof => "arrow_type_of",
            BuiltinScalarFunction::Acosh => "acosh",
            BuiltinScalarFunction::Asinh => "asinh",
            BuiltinScalarFunction::Atanh => "atanh",
            BuiltinScalarFunction::Cbrt => "cbrt",
            BuiltinScalarFunction::Cosh => "cosh",
            BuiltinScalarFunction::Pi => "pi",
            BuiltinScalarFunction::Sinh => "sinh",
            BuiltinScalarFunction::Tanh => "tanh",
        }
    }
}

#[cfg(test)]
mod test {
    use datatypes::schema::ColumnSchema;

    use super::*;

    #[test]
    fn expr_round_trip() {
        let expr = expr_fn::and(
            expr_fn::col("column_a").lt_eq(expr_fn::col("column_b")),
            expr_fn::col("column_a").gt(expr_fn::col("column_b")),
        );

        let schema = Schema::new(vec![
            ColumnSchema::new(
                "column_a",
                datatypes::data_type::ConcreteDataType::int64_datatype(),
                true,
            ),
            ColumnSchema::new(
                "column_b",
                datatypes::data_type::ConcreteDataType::float64_datatype(),
                true,
            ),
        ]);

        let mut ctx = ConvertorContext::default();
        let substrait_expr = expression_from_df_expr(&mut ctx, &expr, &schema).unwrap();
        let converted_expr = to_df_expr(&ctx, substrait_expr, &schema).unwrap();

        assert_eq!(expr, converted_expr);
    }
}
