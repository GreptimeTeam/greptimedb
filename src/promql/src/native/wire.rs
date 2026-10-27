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

//! Native prefix wire format, protocol version 1.
//!
//! Layout: `u32 version`, then recursive node. Node tags are 1 Input, 2 Instant, 3 Window,
//! 4 Map; all integers are little-endian. Input is source index (u64), ordering tag (2 means
//! KeyThenTime), followed by a raw samples schema: field count (u32), each UTF-8 field name
//! (u32 byte length + bytes). Instant stores lookback i64, offset i64, optional-at marker + i64,
//! timestamp bool byte, then output grid. Window stores function tag and its arguments, range i64,
//! offset i64, optional-anchor marker + i64, modifier tag (0 none, 1 anchored, 2 smoothed) and
//! modifier lookback where present, input-grid marker + grid where present, output grid. Grid is
//! start_ms i64, step_ms i64, len u64. Map stores op count u32 and math operations. Math tags and
//! range-function tags below are wire-stable and MUST NOT follow Rust discriminants. Floating
//! values are raw `f64::to_bits()` u64s. Every payload ends exactly after the root node.
//!
//! Stable RangeFunction tags: 1 Rate, 2 Increase, 3 Delta, 4 Irate, 5 Idelta, 6 AvgOverTime,
//! 7 MinOverTime, 8 MaxOverTime, 9 SumOverTime, 10 CountOverTime, 11 LastOverTime,
//! 12 PresentOverTime, 13 StddevOverTime, 14 StdvarOverTime, 15 QuantileOverTime(f64),
//! 16 Resets, 17 Changes, 18 Deriv, 19 PredictLinear(f64), 20 DoubleExponentialSmoothing
//! (smoothing, trend), 21 FirstOverTime, 22 MadOverTime, 23 TsOfFirstOverTime,
//! 24 TsOfLastOverTime, 25 TsOfMinOverTime, 26 TsOfMaxOverTime. Stable MathOp tags:
//! 1 Abs, 2 Ceil, 3 Floor, 4 Sqrt, 5 Round(nearest), 6 Exp, 7 Ln, 8 Log2, 9 Log10,
//! 10 Sin, 11 Cos, 12 Tan, 13 Asin, 14 Acos, 15 Atan, 16 Sinh, 17 Cosh, 18 Tanh,
//! 19 Asinh, 20 Acosh, 21 Atanh, 22 Deg, 23 Rad, 24 Sgn, 25 Clamp(min,max),
//! 26 ClampMin(min), 27 ClampMax(max), 28-35 Date(DayOfMonth, DayOfWeek, DayOfYear,
//! DaysInMonth, Hour, Minute, Month, Year), in that order.

//! Maximum payload is 1 MiB and maximum recursive node depth / operation count is 128.

use crate::{
    map::MathOp,
    map_time::DateFunction,
    native::plan::{
        BoundInput, BoundInstant, BoundMap, BoundWindow, MapOp, NativeExpr, NativeNode, NativeTree,
        NativeType,
    },
    range_eval::{RangeFunction, RangeModifier, RangeWindow},
};
use datatypes::series::{
    SeriesOrdering,
    samples::{FieldSpec, SeriesSchema},
    time::{EvalGrid, GridSpec},
};
use std::{convert::TryInto, sync::Arc};

pub(crate) const PROTOCOL_VERSION: u32 = 1;
const MAX_BYTES: usize = 1_048_576;
const MAX_COUNT: usize = 128;

struct W(Vec<u8>);
impl W {
    fn u8(&mut self, v: u8) {
        self.0.push(v)
    }
    fn u32(&mut self, v: u32) {
        self.0.extend(v.to_le_bytes())
    }
    fn u64(&mut self, v: u64) {
        self.0.extend(v.to_le_bytes())
    }
    fn i64(&mut self, v: i64) {
        self.0.extend(v.to_le_bytes())
    }
    fn f(&mut self, v: f64) {
        self.u64(v.to_bits())
    }
    fn text(&mut self, s: &str) -> Result<(), String> {
        self.u32(u32::try_from(s.len()).map_err(|_| "string too long")?);
        self.0.extend(s.as_bytes());
        Ok(())
    }
    fn grid(&mut self, g: &EvalGrid) {
        let s = g.spec();
        self.i64(s.start_ms);
        self.i64(s.step_ms);
        self.u64(s.len)
    }
}
struct R<'a> {
    b: &'a [u8],
    p: usize,
}
impl<'a> R<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        let e = self.p.checked_add(n).ok_or("length overflow")?;
        if e > self.b.len() {
            return Err("truncated payload".into());
        }
        let x = &self.b[self.p..e];
        self.p = e;
        Ok(x)
    }
    fn u8(&mut self) -> Result<u8, String> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, String> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, String> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn i64(&mut self) -> Result<i64, String> {
        Ok(i64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn f(&mut self) -> Result<f64, String> {
        Ok(f64::from_bits(self.u64()?))
    }
    fn grid(&mut self) -> Result<Arc<EvalGrid>, String> {
        let s =
            GridSpec::try_new(self.i64()?, self.i64()?, self.u64()?).map_err(|e| e.to_string())?;
        Ok(Arc::new(
            EvalGrid::try_deferred(s).map_err(|e| e.to_string())?,
        ))
    }
    fn text(&mut self) -> Result<Arc<str>, String> {
        let n = self.u32()? as usize;
        Ok(std::str::from_utf8(self.take(n)?)
            .map_err(|e| e.to_string())?
            .into())
    }
}

/// Encode only the documented Input→Instant/Window→Math-Map forms.
pub(crate) fn encode_prefix(tree: &NativeTree) -> Result<Vec<u8>, String> {
    check_shape(tree.root())?;
    let mut w = W(Vec::new());
    w.u32(PROTOCOL_VERSION);
    enc_node(tree.root(), &mut w, 0)?;
    if w.0.len() > MAX_BYTES {
        return Err("payload exceeds MAX_BYTES".into());
    }
    Ok(w.0)
}
/// Accepts exactly the supported shapes: a selection over raw input, optionally under one map.
///
/// The map is optional because a bare selection is a prefix too — it is what a query like `x`
/// transports. When present there is one, and it holds only math operations: a label rewrite or a
/// histogram function would change the identity or the value type in ways this format does not
/// describe.
fn check_shape(n: &NativeNode) -> Result<(), String> {
    let selection = match &n.expr {
        NativeExpr::Map { input, op } => {
            if op.ops.is_empty() || op.ops.iter().any(|x| !matches!(x, MapOp::Math(_))) {
                return Err("unsupported Map operation (only Math)".into());
            }
            input
        }
        NativeExpr::Instant { .. } | NativeExpr::Window { .. } => n,
        other => return Err(format!("unsupported operation: {}", node_name(other))),
    };
    let (NativeExpr::Instant { input: child, .. } | NativeExpr::Window { input: child, .. }) =
        &selection.expr
    else {
        return Err(format!(
            "unsupported operation: {}",
            node_name(&selection.expr)
        ));
    };
    if !matches!(child.expr, NativeExpr::Input(_)) {
        return Err(format!("unsupported operation: {}", node_name(&child.expr)));
    }
    Ok(())
}
fn enc_node(n: &NativeNode, w: &mut W, d: usize) -> Result<(), String> {
    if d > MAX_COUNT {
        return Err("nesting maximum exceeded".into());
    }
    match &n.expr {
        NativeExpr::Input(i) => {
            w.u8(1);
            w.u64(i.source_index as u64);
            if i.ordering != SeriesOrdering::KeyThenTime {
                return Err("unsupported Input ordering".into());
            }
            w.u8(2);
            let NativeType::Samples(s) = &i.output else {
                return Err("unsupported Input output type".into());
            };
            if s.eval_grid.is_some() {
                return Err("unsupported evaluated Input".into());
            }
            w.u32(s.fields.len() as u32);
            for f in s.fields.iter() {
                w.text(&f.name)?
            }
        }
        NativeExpr::Instant { input, op } => {
            w.u8(2);
            enc_node(input, w, d + 1)?;
            w.i64(op.lookback_ms);
            w.i64(op.offset_ms);
            opt_i(w, op.at_ms);
            w.u8(op.timestamp as u8);
            w.grid(&op.output_grid)
        }
        NativeExpr::Window { input, op } => {
            w.u8(3);
            enc_node(input, w, d + 1)?;
            enc_range(op.function, w)?;
            w.i64(op.window.range_ms);
            w.i64(op.window.offset_ms);
            opt_i(w, op.window.anchor_ms);
            match op.window.modifier {
                None => w.u8(0),
                Some(RangeModifier::Anchored { lookback_ms }) => {
                    w.u8(1);
                    w.i64(lookback_ms)
                }
                Some(RangeModifier::Smoothed { lookback_ms }) => {
                    w.u8(2);
                    w.i64(lookback_ms)
                }
            }
            match &op.input_grid {
                None => w.u8(0),
                Some(g) => {
                    w.u8(1);
                    w.grid(g)
                }
            }
            w.grid(&op.output_grid)
        }
        NativeExpr::Map { input, op } => {
            w.u8(4);
            enc_node(input, w, d + 1)?;
            if op.ops.is_empty() || op.ops.len() > MAX_COUNT {
                return Err("operation count exceeds supported maximum or is zero".into());
            }
            w.u32(op.ops.len() as u32);
            for x in op.ops.iter() {
                if let MapOp::Math(m) = x {
                    enc_math(*m, w)?
                } else {
                    return Err("unsupported Map operation (only Math)".into());
                }
            }
        }
        _ => return Err(format!("unsupported operation: {}", node_name(&n.expr))),
    }
    Ok(())
}
fn opt_i(w: &mut W, x: Option<i64>) {
    if let Some(v) = x {
        w.u8(1);
        w.i64(v)
    } else {
        w.u8(0)
    }
}
fn enc_math(m: MathOp, w: &mut W) -> Result<(), String> {
    let (t, vals): (u8, &[f64]) = match m {
        MathOp::Abs => (1, &[]),
        MathOp::Ceil => (2, &[]),
        MathOp::Floor => (3, &[]),
        MathOp::Sqrt => (4, &[]),
        MathOp::Round { nearest } => (5, &[nearest]),
        MathOp::Exp => (6, &[]),
        MathOp::Ln => (7, &[]),
        MathOp::Log2 => (8, &[]),
        MathOp::Log10 => (9, &[]),
        MathOp::Sin => (10, &[]),
        MathOp::Cos => (11, &[]),
        MathOp::Tan => (12, &[]),
        MathOp::Asin => (13, &[]),
        MathOp::Acos => (14, &[]),
        MathOp::Atan => (15, &[]),
        MathOp::Sinh => (16, &[]),
        MathOp::Cosh => (17, &[]),
        MathOp::Tanh => (18, &[]),
        MathOp::Asinh => (19, &[]),
        MathOp::Acosh => (20, &[]),
        MathOp::Atanh => (21, &[]),
        MathOp::Deg => (22, &[]),
        MathOp::Rad => (23, &[]),
        MathOp::Sgn => (24, &[]),
        MathOp::Clamp { min, max } => (25, &[min, max]),
        MathOp::ClampMin { min } => (26, &[min]),
        MathOp::ClampMax { max } => (27, &[max]),
        MathOp::Date(f) => (
            28 + match f {
                DateFunction::DayOfMonth => 0,
                DateFunction::DayOfWeek => 1,
                DateFunction::DayOfYear => 2,
                DateFunction::DaysInMonth => 3,
                DateFunction::Hour => 4,
                DateFunction::Minute => 5,
                DateFunction::Month => 6,
                DateFunction::Year => 7,
            },
            &[],
        ),
    };
    w.u8(t);
    for x in vals {
        w.f(*x)
    }
    Ok(())
}
fn enc_range(f: RangeFunction, w: &mut W) -> Result<(), String> {
    let (tag, vs): (u8, &[f64]) = match f {
        RangeFunction::Rate => (1, &[]),
        RangeFunction::Increase => (2, &[]),
        RangeFunction::Delta => (3, &[]),
        RangeFunction::Irate => (4, &[]),
        RangeFunction::Idelta => (5, &[]),
        RangeFunction::AvgOverTime => (6, &[]),
        RangeFunction::MinOverTime => (7, &[]),
        RangeFunction::MaxOverTime => (8, &[]),
        RangeFunction::SumOverTime => (9, &[]),
        RangeFunction::CountOverTime => (10, &[]),
        RangeFunction::LastOverTime => (11, &[]),
        RangeFunction::PresentOverTime => (12, &[]),
        RangeFunction::StddevOverTime => (13, &[]),
        RangeFunction::StdvarOverTime => (14, &[]),
        RangeFunction::QuantileOverTime(x) => (15, &[x]),
        RangeFunction::Resets => (16, &[]),
        RangeFunction::Changes => (17, &[]),
        RangeFunction::Deriv => (18, &[]),
        RangeFunction::PredictLinear(x) => (19, &[x]),
        RangeFunction::DoubleExponentialSmoothing { smoothing, trend } => (20, &[smoothing, trend]),
        RangeFunction::FirstOverTime => (21, &[]),
        RangeFunction::MadOverTime => (22, &[]),
        RangeFunction::TsOfFirstOverTime => (23, &[]),
        RangeFunction::TsOfLastOverTime => (24, &[]),
        RangeFunction::TsOfMinOverTime => (25, &[]),
        RangeFunction::TsOfMaxOverTime => (26, &[]),
    };
    w.u8(tag);
    for x in vs {
        w.f(*x)
    }
    Ok(())
}
fn node_name(e: &NativeExpr) -> &'static str {
    match e {
        NativeExpr::Binary { .. } => "Binary",
        NativeExpr::Aggregate { .. } => "Aggregate",
        NativeExpr::Vector { .. } => "Vector",
        NativeExpr::Scalar(_) => "Scalar",
        NativeExpr::Timestamp { .. } => "Timestamp",
        _ => "unsupported",
    }
}
/// Decode and validate a supported prefix tree.
pub(crate) fn decode_prefix(bytes: &[u8]) -> Result<NativeTree, String> {
    if bytes.len() > MAX_BYTES {
        return Err("payload exceeds MAX_BYTES".into());
    }
    let mut r = R { b: bytes, p: 0 };
    let v = r.u32()?;
    if v != PROTOCOL_VERSION {
        return Err(format!("unknown version {v}"));
    }
    let n = dec_node(&mut r, 0)?;
    check_shape(&n)?;
    if r.p != bytes.len() {
        return Err("trailing bytes after complete payload".into());
    }
    NativeTree::try_new(n).map_err(|e| e.to_string())
}
fn dec_node(r: &mut R<'_>, d: usize) -> Result<NativeNode, String> {
    if d > MAX_COUNT {
        return Err("nesting maximum exceeded".into());
    }
    match r.u8()? {
        1 => {
            let ix = usize::try_from(r.u64()?).map_err(|_| "source index overflow")?;
            if r.u8()? != 2 {
                return Err("unsupported Input ordering".into());
            }
            let n = r.u32()? as usize;
            if n > MAX_COUNT {
                return Err("operation count exceeds maximum".into());
            }
            let mut fs = Vec::with_capacity(n);
            for _ in 0..n {
                fs.push(FieldSpec { name: r.text()? })
            }
            let ty = NativeType::Samples(Arc::new(SeriesSchema {
                fields: fs.into(),
                eval_grid: None,
            }));
            Ok(NativeNode {
                expr: NativeExpr::Input(BoundInput {
                    source_index: ix,
                    output: ty.clone(),
                    ordering: SeriesOrdering::KeyThenTime,
                }),
                output: ty,
            })
        }
        2 => {
            let input = Box::new(dec_node(r, d + 1)?);
            let lookback_ms = r.i64()?;
            let offset_ms = r.i64()?;
            let at_ms = read_opt(r)?;
            let timestamp = match r.u8()? {
                0 => false,
                1 => true,
                _ => return Err("invalid timestamp flag".into()),
            };
            let output_grid = r.grid()?;
            let schema = match &input.output {
                NativeType::Samples(s) => Arc::new(SeriesSchema {
                    fields: s.fields.clone(),
                    eval_grid: Some(output_grid.clone()),
                }),
                _ => return Err("unsupported Instant input type".into()),
            };
            let output = NativeType::Samples(schema);
            Ok(NativeNode {
                expr: NativeExpr::Instant {
                    input,
                    op: BoundInstant {
                        lookback_ms,
                        offset_ms,
                        at_ms,
                        timestamp,
                        output_grid,
                    },
                },
                output,
            })
        }
        3 => {
            let input = Box::new(dec_node(r, d + 1)?);
            let function = dec_range(r)?;
            let range_ms = r.i64()?;
            let offset_ms = r.i64()?;
            let anchor_ms = read_opt(r)?;
            let modifier = match r.u8()? {
                0 => None,
                1 => Some(RangeModifier::Anchored {
                    lookback_ms: r.i64()?,
                }),
                2 => Some(RangeModifier::Smoothed {
                    lookback_ms: r.i64()?,
                }),
                _ => return Err("unknown range modifier".into()),
            };
            let input_grid = match r.u8()? {
                0 => None,
                1 => Some(r.grid()?),
                _ => return Err("invalid input grid marker".into()),
            };
            let output_grid = r.grid()?;
            let fields = match &input.output {
                NativeType::Samples(s) => s.fields.clone(),
                _ => return Err("unsupported Window input type".into()),
            };
            let output = NativeType::Samples(Arc::new(SeriesSchema {
                fields,
                eval_grid: Some(output_grid.clone()),
            }));
            Ok(NativeNode {
                expr: NativeExpr::Window {
                    input,
                    op: BoundWindow {
                        function,
                        window: RangeWindow {
                            range_ms,
                            offset_ms,
                            anchor_ms,
                            modifier,
                        },
                        input_grid,
                        output_grid,
                    },
                },
                output,
            })
        }
        4 => {
            let input = Box::new(dec_node(r, d + 1)?);
            let n = r.u32()? as usize;
            if n == 0 || n > MAX_COUNT {
                return Err("operation count exceeds maximum".into());
            }
            let mut ops = Vec::new();
            for _ in 0..n {
                ops.push(MapOp::Math(dec_math(r)?))
            }
            let output = match &input.output {
                NativeType::Samples(s) => NativeType::Samples(s.clone()),
                _ => return Err("unsupported Map input type".into()),
            };
            Ok(NativeNode {
                expr: NativeExpr::Map {
                    input,
                    op: BoundMap { ops: ops.into() },
                },
                output,
            })
        }
        x => Err(format!("unsupported operation tag {x}")),
    }
}
fn read_opt(r: &mut R<'_>) -> Result<Option<i64>, String> {
    match r.u8()? {
        0 => Ok(None),
        1 => Ok(Some(r.i64()?)),
        _ => Err("invalid optional integer marker".into()),
    }
}
fn dec_range(r: &mut R<'_>) -> Result<RangeFunction, String> {
    Ok(match r.u8()? {
        1 => RangeFunction::Rate,
        2 => RangeFunction::Increase,
        3 => RangeFunction::Delta,
        4 => RangeFunction::Irate,
        5 => RangeFunction::Idelta,
        6 => RangeFunction::AvgOverTime,
        7 => RangeFunction::MinOverTime,
        8 => RangeFunction::MaxOverTime,
        9 => RangeFunction::SumOverTime,
        10 => RangeFunction::CountOverTime,
        11 => RangeFunction::LastOverTime,
        12 => RangeFunction::PresentOverTime,
        13 => RangeFunction::StddevOverTime,
        14 => RangeFunction::StdvarOverTime,
        15 => RangeFunction::QuantileOverTime(r.f()?),
        16 => RangeFunction::Resets,
        17 => RangeFunction::Changes,
        18 => RangeFunction::Deriv,
        19 => RangeFunction::PredictLinear(r.f()?),
        20 => RangeFunction::DoubleExponentialSmoothing {
            smoothing: r.f()?,
            trend: r.f()?,
        },
        21 => RangeFunction::FirstOverTime,
        22 => RangeFunction::MadOverTime,
        23 => RangeFunction::TsOfFirstOverTime,
        24 => RangeFunction::TsOfLastOverTime,
        25 => RangeFunction::TsOfMinOverTime,
        26 => RangeFunction::TsOfMaxOverTime,
        x => return Err(format!("unsupported RangeFunction tag {x}")),
    })
}
fn dec_math(r: &mut R<'_>) -> Result<MathOp, String> {
    Ok(match r.u8()? {
        1 => MathOp::Abs,
        2 => MathOp::Ceil,
        3 => MathOp::Floor,
        4 => MathOp::Sqrt,
        5 => MathOp::Round { nearest: r.f()? },
        6 => MathOp::Exp,
        7 => MathOp::Ln,
        8 => MathOp::Log2,
        9 => MathOp::Log10,
        10 => MathOp::Sin,
        11 => MathOp::Cos,
        12 => MathOp::Tan,
        13 => MathOp::Asin,
        14 => MathOp::Acos,
        15 => MathOp::Atan,
        16 => MathOp::Sinh,
        17 => MathOp::Cosh,
        18 => MathOp::Tanh,
        19 => MathOp::Asinh,
        20 => MathOp::Acosh,
        21 => MathOp::Atanh,
        22 => MathOp::Deg,
        23 => MathOp::Rad,
        24 => MathOp::Sgn,
        25 => MathOp::Clamp {
            min: r.f()?,
            max: r.f()?,
        },
        26 => MathOp::ClampMin { min: r.f()? },
        27 => MathOp::ClampMax { max: r.f()? },
        28 => MathOp::Date(DateFunction::DayOfMonth),
        29 => MathOp::Date(DateFunction::DayOfWeek),
        30 => MathOp::Date(DateFunction::DayOfYear),
        31 => MathOp::Date(DateFunction::DaysInMonth),
        32 => MathOp::Date(DateFunction::Hour),
        33 => MathOp::Date(DateFunction::Minute),
        34 => MathOp::Date(DateFunction::Month),
        35 => MathOp::Date(DateFunction::Year),
        x => return Err(format!("unsupported MathOp tag {x}")),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn input() -> NativeNode {
        let t = NativeType::Samples(Arc::new(SeriesSchema {
            fields: vec![FieldSpec { name: "v".into() }].into(),
            eval_grid: None,
        }));
        NativeNode {
            expr: NativeExpr::Input(BoundInput {
                source_index: 0,
                output: t.clone(),
                ordering: SeriesOrdering::KeyThenTime,
            }),
            output: t,
        }
    }
    fn grid() -> Arc<EvalGrid> {
        Arc::new(EvalGrid::try_deferred(GridSpec::try_new(100, 10, 3).unwrap()).unwrap())
    }
    /// `rate` over the raw input on the fixture grid: the simplest window a prefix transports.
    fn rate_window() -> NativeTree {
        let input = Box::new(input());
        let g = grid();
        let output = NativeType::Samples(Arc::new(SeriesSchema {
            fields: match &input.output {
                NativeType::Samples(s) => s.fields.clone(),
                _ => unreachable!(),
            },
            eval_grid: Some(g.clone()),
        }));
        NativeTree::try_new(NativeNode {
            expr: NativeExpr::Window {
                input,
                op: BoundWindow {
                    function: RangeFunction::Rate,
                    window: RangeWindow {
                        range_ms: 3_000,
                        offset_ms: 0,
                        anchor_ms: None,
                        modifier: None,
                    },
                    input_grid: None,
                    output_grid: g,
                },
            },
            output,
        })
        .unwrap()
    }

    fn instant() -> NativeTree {
        let input = Box::new(input());
        let g = grid();
        let output = NativeType::Samples(Arc::new(SeriesSchema {
            fields: match &input.output {
                NativeType::Samples(s) => s.fields.clone(),
                _ => unreachable!(),
            },
            eval_grid: Some(g.clone()),
        }));
        NativeTree::try_new(NativeNode {
            expr: NativeExpr::Instant {
                input,
                op: BoundInstant {
                    lookback_ms: 300_000,
                    offset_ms: 7,
                    at_ms: Some(88),
                    timestamp: true,
                    output_grid: g,
                },
            },
            output,
        })
        .unwrap()
    }
    fn window() -> NativeTree {
        let input = Box::new(input());
        let g = grid();
        let output = NativeType::Samples(Arc::new(SeriesSchema {
            fields: match &input.output {
                NativeType::Samples(s) => s.fields.clone(),
                _ => unreachable!(),
            },
            eval_grid: Some(g.clone()),
        }));
        NativeTree::try_new(NativeNode {
            expr: NativeExpr::Window {
                input,
                op: BoundWindow {
                    function: RangeFunction::QuantileOverTime(f64::from_bits(
                        0x7ff8_0000_0000_0042,
                    )),
                    window: RangeWindow {
                        range_ms: 500,
                        offset_ms: 9,
                        anchor_ms: Some(-4),
                        modifier: Some(RangeModifier::Smoothed { lookback_ms: 99 }),
                    },
                    input_grid: None,
                    output_grid: g,
                },
            },
            output,
        })
        .unwrap()
    }
    fn mapped() -> NativeTree {
        let instant = instant();
        let input = Box::new(instant.root().clone());
        let output = input.output.clone();
        NativeTree::try_new(NativeNode {
            expr: NativeExpr::Map {
                input,
                op: BoundMap {
                    ops: vec![
                        MapOp::Math(MathOp::Round { nearest: 0.5 }),
                        MapOp::Math(MathOp::Clamp {
                            min: -1.0,
                            max: 2.0,
                        }),
                        MapOp::Math(MathOp::Abs),
                    ]
                    .into(),
                },
            },
            output,
        })
        .unwrap()
    }
    #[test]
    fn instant_roundtrip_and_fields() {
        let a = instant();
        let b = encode_prefix(&a).unwrap();
        let d = decode_prefix(&b).unwrap();
        assert_eq!(encode_prefix(&d).unwrap(), b);
        let NativeExpr::Instant { op, .. } = &d.root().expr else {
            panic!()
        };
        assert_eq!(op.lookback_ms, 300_000);
        assert_eq!(op.offset_ms, 7);
        assert_eq!(op.at_ms, Some(88));
        assert!(op.timestamp);
        assert_eq!(op.output_grid.spec(), grid().spec());
    }
    #[test]
    fn window_roundtrip_and_fields() {
        let a = window();
        let b = encode_prefix(&a).unwrap();
        let d = decode_prefix(&b).unwrap();
        assert_eq!(encode_prefix(&d).unwrap(), b);
        let NativeExpr::Window { op, .. } = &d.root().expr else {
            panic!()
        };
        assert!(
            matches!(op.function,RangeFunction::QuantileOverTime(x) if x.to_bits()==0x7ff8_0000_0000_0042)
        );
        assert_eq!(op.window.range_ms, 500);
        assert_eq!(op.output_grid.spec(), grid().spec());
    }
    #[test]
    fn map_roundtrip_and_ops() {
        let a = mapped();
        let b = encode_prefix(&a).unwrap();
        let d = decode_prefix(&b).unwrap();
        assert_eq!(encode_prefix(&d).unwrap(), b);
        let NativeExpr::Map { op, .. } = &d.root().expr else {
            panic!()
        };
        assert_eq!(op.ops.len(), 3);
        assert!(
            matches!(op.ops[0],MapOp::Math(MathOp::Round{nearest}) if nearest.to_bits()==0.5f64.to_bits())
        );
    }
    #[test]
    fn floats_preserve_bits() {
        let values = [
            f64::from_bits(0x7ff8_0000_0000_0042),
            -0.0,
            0.0,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::from_bits(1),
        ];
        for x in values {
            let mut w = W(Vec::new());
            w.f(x);
            let mut r = R { b: &w.0, p: 0 };
            assert_eq!(r.f().unwrap().to_bits(), x.to_bits());
        }
    }
    fn labels() -> Option<datatypes::series::SeriesLabels> {
        datatypes::series::SeriesLabels::try_new(
            Some("m".into()),
            vec![datatypes::series::Label {
                name: "job".into(),
                value: "x".into(),
            }],
        )
        .ok()
    }

    /// The decoded tree must compute what the original computes, not merely encode back to the
    /// same bytes.
    ///
    /// Both trees are driven through the real executor over the same raw input, and their answers
    /// are compared. A codec that dropped a parameter — a lookback, a window offset, a clamp bound
    /// — would still round-trip its own bytes, so only executing them can tell.
    #[test]
    fn decoded_tree_computes_the_same_answer() {
        use crate::native::execute::{InputId, NativeExecutionState, NativeExecutor};
        use datafusion::execution::memory_pool::UnboundedMemoryPool;
        use datatypes::arrow::array::Float64Array;
        use datatypes::arrow::buffer::{Buffer, ScalarBuffer};
        use datatypes::series::raw::{RawSeriesChunk, RawTimestamps};
        use datatypes::series::samples::{Samples, SeriesFields};

        // A plain rate window over raw input: a legal tree the executor accepts. The float-bits
        // fixture carries a `QuantileOverTime(NaN)` under a smoothed modifier, which the executor
        // rejects on purpose, so it cannot be the tree this test runs.
        let tree = rate_window();
        let decoded = decode_prefix(&encode_prefix(&tree).unwrap()).unwrap();

        let answer = |tree: &NativeTree| {
            let mut execution = NativeExecutor::new(
                NativeExecutionState::try_new(tree, Arc::new(UnboundedMemoryPool::default()))
                    .unwrap(),
            );
            let schema = tree
                .root()
                .output
                .grid()
                .map(|_| ())
                .and_then(|()| match &tree.root().expr {
                    NativeExpr::Window { input, .. } => match &input.output {
                        NativeType::Samples(schema) => Some(Arc::clone(schema)),
                        other => panic!("a window reads samples, not {other:?}"),
                    },
                    other => panic!("the fixture is a window, not {other:?}"),
                })
                .unwrap();
            let chunk = RawSeriesChunk::try_new(
                &schema,
                Buffer::from_slice_ref(b"series-a"),
                labels(),
                // Samples before, inside and after the fixture grid (100, 110, 120) so the
                // window has a counter to measure across every evaluation point.
                RawTimestamps::try_new(ScalarBuffer::from(vec![60i64, 100, 110, 120, 140]))
                    .unwrap(),
                SeriesFields::one(Samples::from_float(Float64Array::from(vec![
                    0.0, 10.0, 20.0, 30.0, 50.0,
                ]))),
                true,
            )
            .unwrap();
            execution.push_raw(InputId::new(0), chunk).unwrap();
            execution.end_of_input().unwrap();
            let mut values = Vec::new();
            while let Some(chunk) = execution.next_chunk().unwrap() {
                for sample in chunk.series.fields.get(0).unwrap().iter() {
                    match sample {
                        datatypes::series::samples::SampleRef::Float(value) => {
                            values.push(Some(value))
                        }
                        _ => values.push(None),
                    }
                }
            }
            values
        };

        let original = answer(&tree);
        let round_tripped = answer(&decoded);
        assert_eq!(
            original, round_tripped,
            "the decoded tree answered differently from the original"
        );
        assert!(
            original.iter().any(Option::is_some),
            "the fixture produced no values, so it proves nothing"
        );
    }

    #[test]
    fn rejects_unknown_version() {
        let mut b = encode_prefix(&instant()).unwrap();
        b[0] = 2;
        assert!(decode_prefix(&b).unwrap_err().contains("unknown version"));
    }
    #[test]
    fn rejects_unsupported_node_kind() {
        let n = NativeNode {
            expr: NativeExpr::Scalar(crate::native::plan::BoundScalar { value: 1.0 }),
            output: NativeType::Scalar(grid()),
        };
        let t = NativeTree::try_new(n).unwrap();
        assert!(encode_prefix(&t).unwrap_err().contains("Scalar"));
    }
    #[test]
    fn rejects_truncated_payload() {
        let mut b = encode_prefix(&instant()).unwrap();
        b.pop();
        assert!(decode_prefix(&b).unwrap_err().contains("truncated"));
    }
    #[test]
    fn rejects_trailing_bytes() {
        let mut b = encode_prefix(&instant()).unwrap();
        b.push(0);
        assert!(decode_prefix(&b).unwrap_err().contains("trailing"));
    }
    #[test]
    fn rejects_oversized_payload() {
        let b = vec![0; MAX_BYTES + 1];
        assert!(decode_prefix(&b).unwrap_err().contains("MAX_BYTES"));
    }
    #[test]
    fn rejects_excess_operation_count() {
        let mut w = W(Vec::new());
        w.u32(PROTOCOL_VERSION);
        w.u8(4);
        w.u8(1);
        w.u64(0);
        w.u8(2);
        w.u32(1);
        w.text("v").unwrap();
        w.u8(2);
        w.i64(1);
        w.i64(0);
        opt_i(&mut w, None);
        w.u8(0);
        w.grid(&grid());
        w.u32((MAX_COUNT + 1) as u32);
        assert!(decode_prefix(&w.0).unwrap_err().contains("operation count"));
    }
}
