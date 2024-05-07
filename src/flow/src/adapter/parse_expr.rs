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

//! parse expr like "ts <= now() - interval '5 min'"

use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::{alphanumeric1, digit0, multispace0};
use nom::combinator::peek;
use nom::sequence::tuple;
use nom::IResult;

use crate::expr::ScalarExpr;
use crate::repr;

enum Expr {
    Col(String),
    Now,
    Duration(repr::Duration),
    Binary {
        left: Box<Expr>,
        op: String,
        right: Box<Expr>,
    },
}

fn parse_expr(input: &str) -> IResult<&str, Expr> {
    parse_expr_bp(input, 0)
}

/// a simple pratt parser
fn parse_expr_bp(input: &str, min_bp: u8) -> IResult<&str, Expr> {
    let (mut input, mut lhs): (&str, Expr) = parse_item(input)?;
    loop {
        let (r, op) = parse_op(input)?;
        let (_, (l_bp, r_bp)) = infix_binding_power(op)?;
        if l_bp < min_bp {
            return Ok((input, lhs));
        }
        let (r, rhs) = parse_expr_bp(r, r_bp)?;
        input = r;
        lhs = Expr::Binary {
            left: Box::new(lhs),
            op: op.to_string(),
            right: Box::new(rhs),
        };
    }
}

fn parse_op(input: &str) -> IResult<&str, &str> {
    alt((parse_add_sub, parse_cmp))(input)
}

fn parse_item(input: &str) -> IResult<&str, Expr> {
    if let Ok((r, name)) = parse_col_name(input) {
        Ok((r, Expr::Col(name.to_string())))
    } else if let Ok((r, _now)) = parse_now(input) {
        Ok((r, Expr::Now))
    } else if let Ok((r, _num)) = parse_quality(input) {
        let _ = parse_unit(r)?;
        todo!()
    } else {
        todo!()
    }
}

fn infix_binding_power(op: &str) -> IResult<&str, (u8, u8)> {
    let ret = match op {
        "<" | ">" | "<=" | ">=" => (1, 2),
        "+" | "-" => (3, 4),
        _ => {
            return Err(nom::Err::Error(nom::error::Error::new(
                op,
                nom::error::ErrorKind::Fail,
            )))
        }
    };
    Ok((op, ret))
}

fn parse_col_name(input: &str) -> IResult<&str, &str> {
    tuple((multispace0, alphanumeric1, multispace0))(input).map(|(r, (_, name, _))| (r, name))
}

fn parse_now(input: &str) -> IResult<&str, &str> {
    tag_no_case("now()")(input)
}

fn parse_add_sub(input: &str) -> IResult<&str, &str> {
    tuple((multispace0, alt((tag("+"), tag("-"))), multispace0))(input)
        .map(|(r, (_, op, _))| (r, op))
}

fn parse_cmp(input: &str) -> IResult<&str, &str> {
    tuple((
        multispace0,
        alt((tag("<="), tag(">="), tag("<"), tag(">"))),
        multispace0,
    ))(input)
    .map(|(r, (_, op, _))| (r, op))
}

/// parse a number with optional sign
fn parse_quality(input: &str) -> IResult<&str, isize> {
    tuple((
        multispace0,
        alt((tag("+"), tag("-"), tag(""))),
        digit0,
        multispace0,
    ))(input)
    .map(|(r, (_, sign, name, _))| (r, sign, name))
    .and_then(|(r, sign, name)| {
        let num = name.parse::<isize>().map_err(|_| {
            nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Digit))
        })?;
        let num = match sign {
            "+" => num,
            "-" => -num,
            _ => num,
        };
        Ok((r, num))
    })
}

enum TimeUnit {
    Second,
    Minute,
    Hour,
    Day,
    Month,
    Year,
}

fn parse_unit(input: &str) -> IResult<&str, &str> {
    tuple((
        multispace0,
        alt((
            tag_no_case("second"),
            tag_no_case("seconds"),
            tag_no_case("S"),
            tag_no_case("minute"),
            tag_no_case("minutes"),
            tag_no_case("m"),
            tag_no_case("hour"),
            tag_no_case("hours"),
            tag_no_case("h"),
            tag_no_case("day"),
            tag_no_case("days"),
            tag_no_case("d"),
            tag_no_case("month"),
            tag_no_case("months"),
            tag_no_case("m"),
            tag_no_case("year"),
            tag_no_case("years"),
            tag_no_case("y"),
        )),
        multispace0,
    ))(input)
    .map(|(r, (_, unit, _))| (r, unit))
}
