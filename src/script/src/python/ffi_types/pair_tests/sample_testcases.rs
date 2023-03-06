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

use std::collections::HashMap;
use std::f64::consts;
use std::sync::Arc;

use datatypes::prelude::ScalarVector;
use datatypes::vectors::{BooleanVector, Float64Vector, Int32Vector, Int64Vector, VectorRef};

use super::CoprTestCase;
use crate::python::ffi_types::pair_tests::CodeBlockTestCase;
macro_rules! vector {
    ($ty: ident, $slice: expr) => {
        Arc::new($ty::from_slice($slice)) as VectorRef
    };
}

macro_rules! ronish {
    ($($key: literal : $expr: expr),*$(,)?) => {
        HashMap::from([
            $(($key.to_string(), $expr)),*
        ])
    };
}
pub(super) fn generate_copr_intgrate_tests() -> Vec<CoprTestCase> {
    vec![
        CoprTestCase {
            script: r#"
@copr(args=["number", "number"],
    returns=["value"],
    sql="select number from numbers limit 5", backend="rspy")
def add_vecs(n1, n2) -> vector[i32]:
    return n1 + n2
"#
            .to_string(),
            expect: Some(ronish!("value": vector!(Int32Vector, [0, 2, 4, 6, 8]))),
        },
        #[cfg(feature = "pyo3_backend")]
        CoprTestCase {
            script: r#"
@copr(args=["number", "number"],
    returns=["value"],
    sql="select number from numbers limit 5", backend="pyo3")
def add_vecs(n1, n2) -> vector[i32]:
    return n1 + n2
"#
            .to_string(),
            expect: Some(ronish!("value": vector!(Int32Vector, [0, 2, 4, 6, 8]))),
        },
        CoprTestCase {
            script: r#"
@copr(returns=["value"])
def answer() -> vector[i64]:
    from greptime import vector
    return vector([42, 43, 44])
"#
            .to_string(),
            expect: Some(ronish!("value": vector!(Int64Vector, [42, 43, 44]))),
        },
        #[cfg(feature = "pyo3_backend")]
        CoprTestCase {
            script: r#"
@copr(returns=["value"], backend="pyo3")
def answer() -> vector[i64]:
    from greptime import vector
    return vector([42, 43, 44])
"#
            .to_string(),
            expect: Some(ronish!("value": vector!(Int64Vector, [42, 43, 44]))),
        },
        #[cfg(feature = "pyo3_backend")]
        CoprTestCase {
            script: r#"
@copr(returns=["value"], backend="pyo3")
def answer() -> vector[i64]:
    from greptime import vector
    return vector.from_pyarrow(vector([42, 43, 44]).to_pyarrow())
"#
            .to_string(),
            expect: Some(ronish!("value": vector!(Int64Vector, [42, 43, 44]))),
        },
        #[cfg(feature = "pyo3_backend")]
        CoprTestCase {
            script: r#"
@copr(returns=["value"], backend="pyo3")
def answer() -> vector[i64]:
    from greptime import vector
    import pyarrow as pa
    return vector.from_pyarrow(pa.array([42, 43, 44]))
"#
            .to_string(),
            expect: Some(ronish!("value": vector!(Int64Vector, [42, 43, 44]))),
        },
        CoprTestCase {
            script: r#"
@copr(args=[], returns = ["number"], sql = "select * from numbers", backend="rspy")
def answer() -> vector[i64]:
    from greptime import vector, col, lit
    expr_0 = (col("number")<lit(3)) & (col("number")>0)
    ret = dataframe.select([col("number")]).filter(expr_0).collect()[0][0]
    return ret
"#
            .to_string(),
            expect: Some(ronish!("number": vector!(Int64Vector, [1, 2]))),
        },
        #[cfg(feature = "pyo3_backend")]
        CoprTestCase {
            script: r#"
@copr(args=[], returns = ["number"], sql = "select * from numbers", backend="pyo3")
def answer() -> vector[i64]:
    from greptime import vector, col, lit
    # Bitwise Operator  pred comparison operator
    expr_0 = (col("number")<lit(3)) & (col("number")>0)
    ret = dataframe.select([col("number")]).filter(expr_0).collect()[0][0]
    return ret
"#
            .to_string(),
            expect: Some(ronish!("number": vector!(Int64Vector, [1, 2]))),
        },
    ]
}

/// Generate tests for basic vector operations and basic builtin functions
/// Using a function to generate testcase instead of `.ron` configure file because it's more flexible and we are in #[cfg(test)] so no binary bloat worrying
#[allow(clippy::approx_constant)]
pub(super) fn sample_test_case() -> Vec<CodeBlockTestCase> {
    vec![
        CodeBlockTestCase {
            input: ronish! {
                "a": vector!(Float64Vector, [1.0f64, 2.0, 3.0])
            },
            script: r#"
from greptime import *
ret = a+3.0
ret = ret * 2.0
ret = ret / 2.0
ret = ret - 3.0
ret"#
                .to_string(),
            expect: vector!(Float64Vector, [1.0f64, 2.0, 3.0]),
        },
        CodeBlockTestCase {
            input: ronish! {
                "a": vector!(Float64Vector, [1.0f64, 2.0, 3.0]),
                "b": vector!(Float64Vector, [3.0f64, 2.0, 1.0])
            },
            script: r#"
from greptime import *
ret = a+b
ret"#
                .to_string(),
            expect: vector!(Float64Vector, [4.0f64, 4.0, 4.0]),
        },
        CodeBlockTestCase {
            input: ronish! {
                "a": vector!(Float64Vector, [1.0f64, 2.0, 3.0]),
                "b": vector!(Float64Vector, [3.0f64, 2.0, 1.0])
            },
            script: r#"
from greptime import *
ret = a-b
ret"#
                .to_string(),
            expect: vector!(Float64Vector, [-2.0f64, 0.0, 2.0]),
        },
        CodeBlockTestCase {
            input: ronish! {
                "a": vector!(Float64Vector, [1.0f64, 2.0, 3.0]),
                "b": vector!(Float64Vector, [3.0f64, 2.0, 1.0])
            },
            script: r#"
from greptime import *
ret = a*b
ret"#
                .to_string(),
            expect: vector!(Float64Vector, [3.0f64, 4.0, 3.0]),
        },
        CodeBlockTestCase {
            input: ronish! {
                "a": vector!(Float64Vector, [1.0f64, 2.0, 3.0]),
                "b": vector!(Float64Vector, [3.0f64, 2.0, 1.0])
            },
            script: r#"
from greptime import *
ret = a/b
ret"#
                .to_string(),
            expect: vector!(Float64Vector, [1. / 3., 1.0, 3.0]),
        },
        CodeBlockTestCase {
            input: ronish! {
                "values": vector!(Float64Vector, [1.0f64, 2.0, 3.0])
            },
            script: r#"
from greptime import *
ret = sqrt(values)
ret"#
                .to_string(),
            expect: vector!(
                Float64Vector,
                [1.0f64, std::f64::consts::SQRT_2, 1.7320508075688772,]
            ),
        },
        CodeBlockTestCase {
            input: ronish! {
                "values": vector!(Float64Vector, [1.0, 2.0, 3.0])
            },
            script: r#"
from greptime import *
ret = sin(values)
ret"#
                .to_string(),
            expect: vector!(
                Float64Vector,
                [0.8414709848078965, 0.9092974268256817, 0.1411200080598672,]
            ),
        },
        CodeBlockTestCase {
            input: ronish! {
                "values": vector!(Float64Vector, [1.0, 2.0, 3.0])
            },
            script: r#"
from greptime import *
ret = cos(values)
ret"#
                .to_string(),
            expect: vector!(
                Float64Vector,
                [0.5403023058681398, -0.4161468365471424, -0.9899924966004454,]
            ),
        },
        CodeBlockTestCase {
            input: ronish! {
                "values": vector!(Float64Vector, [1.0, 2.0, 3.0])
            },
            script: r#"
from greptime import *
ret = tan(values)
ret"#
                .to_string(),
            expect: vector!(
                Float64Vector,
                [1.5574077246549023, -2.185039863261519, -0.1425465430742778,]
            ),
        },
        CodeBlockTestCase {
            input: ronish! {
                "values": vector!(Float64Vector, [0.3, 0.5, 1.0])
            },
            script: r#"
from greptime import *
ret = asin(values)
ret"#
                .to_string(),
            expect: vector!(
                Float64Vector,
                [0.3046926540153975, 0.5235987755982989, 1.5707963267948966,]
            ),
        },
        CodeBlockTestCase {
            input: ronish! {
                "values": vector!(Float64Vector, [0.3, 0.5, 1.0])
            },
            script: r#"
from greptime import *
ret = acos(values)
ret"#
                .to_string(),
            expect: vector!(
                Float64Vector,
                [1.2661036727794992, 1.0471975511965979, 0.0,]
            ),
        },
        CodeBlockTestCase {
            input: ronish! {
                "values": vector!(Float64Vector, [0.3, 0.5, 1.1])
            },
            script: r#"
from greptime import *
ret = atan(values)
ret"#
                .to_string(),
            expect: vector!(
                Float64Vector,
                [0.2914567944778671, 0.4636476090008061, 0.8329812666744317,]
            ),
        },
        CodeBlockTestCase {
            input: ronish! {
                "values": vector!(Float64Vector, [0.3, 0.5, 1.1])
            },
            script: r#"
from greptime import *
ret = floor(values)
ret"#
                .to_string(),
            expect: vector!(Float64Vector, [0.0, 0.0, 1.0,]),
        },
        CodeBlockTestCase {
            input: ronish! {
                "values": vector!(Float64Vector, [0.3, 0.5, 1.1])
            },
            script: r#"
from greptime import *
ret = ceil(values)
ret"#
                .to_string(),
            expect: vector!(Float64Vector, [1.0, 1.0, 2.0,]),
        },
        CodeBlockTestCase {
            input: ronish! {
                "values": vector!(Float64Vector, [0.3, 0.5, 1.1])
            },
            script: r#"
from greptime import *
ret = round(values)
ret"#
                .to_string(),
            expect: vector!(Float64Vector, [0.0, 1.0, 1.0,]),
        },
        CodeBlockTestCase {
            input: ronish! {
                "values": vector!(Float64Vector, [0.3, 0.5, 1.1])
            },
            script: r#"
from greptime import *
ret = trunc(values)
ret"#
                .to_string(),
            expect: vector!(Float64Vector, [0.0, 0.0, 1.0,]),
        },
        CodeBlockTestCase {
            input: ronish! {
                "values": vector!(Float64Vector, [-0.3, 0.5, -1.1])
            },
            script: r#"
from greptime import *
ret = abs(values)
ret"#
                .to_string(),
            expect: vector!(Float64Vector, [0.3, 0.5, 1.1,]),
        },
        CodeBlockTestCase {
            input: ronish! {
                "values": vector!(Float64Vector, [-0.3, 0.5, -1.1])
            },
            script: r#"
from greptime import *
ret = signum(values)
ret"#
                .to_string(),
            expect: vector!(Float64Vector, [-1.0, 1.0, -1.0,]),
        },
        CodeBlockTestCase {
            input: ronish! {
                "values": vector!(Float64Vector, [0., 1.0, 2.0])
            },
            script: r#"
from greptime import *
ret = exp(values)
ret"#
                .to_string(),
            expect: vector!(Float64Vector, [1.0, consts::E, 7.38905609893065,]),
        },
        CodeBlockTestCase {
            input: ronish! {
                "values": vector!(Float64Vector, [1.0, 2.0, 3.0])
            },
            script: r#"
from greptime import *
ret = ln(values)
ret"#
                .to_string(),
            expect: vector!(Float64Vector, [0.0, consts::LN_2, 1.0986122886681098,]),
        },
        CodeBlockTestCase {
            input: ronish! {
                "values": vector!(Float64Vector, [1.0, 2.0, 3.0])
            },
            script: r#"
from greptime import *
ret = log2(values)
ret"#
                .to_string(),
            expect: vector!(Float64Vector, [0.0, 1.0, 1.584962500721156,]),
        },
        CodeBlockTestCase {
            input: ronish! {
                "values": vector!(Float64Vector, [1.0, 2.0, 3.0])
            },
            script: r#"
from greptime import *
ret = log10(values)
ret"#
                .to_string(),
            expect: vector!(Float64Vector, [0.0, consts::LOG10_2, 0.47712125471966244,]),
        },
        CodeBlockTestCase {
            input: ronish! {},
            script: r#"
from greptime import *
ret = 0.0<=random(3)<=1.0
ret"#
                .to_string(),
            expect: vector!(BooleanVector, &[true, true, true]),
        },
        CodeBlockTestCase {
            input: ronish! {
                "values": vector!(Int64Vector, [1, 2, 2, 3])
            },
            script: r#"
from greptime import *
ret = vector([approx_distinct(values)])
ret"#
                .to_string(),
            expect: vector!(Int64Vector, [3]),
        },
        CodeBlockTestCase {
            input: ronish! {
                "values": vector!(Int64Vector, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
            },
            script: r#"
from greptime import *
ret = vector([approx_percentile_cont(values, 0.6)])
ret"#
                .to_string(),
            expect: vector!(Int64Vector, [6]),
        },
        CodeBlockTestCase {
            input: ronish! {
                "values": vector!(Float64Vector, [1.0, 2.0, 3.0])
            },
            script: r#"
from greptime import *
ret = vector(array_agg(values))
ret"#
                .to_string(),
            expect: vector!(Float64Vector, [1.0, 2.0, 3.0]),
        },
        CodeBlockTestCase {
            input: ronish! {
                "values": vector!(Float64Vector, [1.0, 2.0, 3.0])
            },
            script: r#"
from greptime import *
ret = vector([avg(values)])
ret"#
                .to_string(),
            expect: vector!(Float64Vector, [2.0]),
        },
        CodeBlockTestCase {
            input: ronish! {
                "a": vector!(Float64Vector, [1.0, 2.0, 3.0]),
                "b": vector!(Float64Vector, [1.0, 0.0, -1.0])
            },
            script: r#"
from greptime import *
ret = vector([correlation(a, b)])
ret"#
                .to_string(),
            expect: vector!(Float64Vector, [-1.0]),
        },
        CodeBlockTestCase {
            input: ronish! {
                "values": vector!(Int64Vector, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
            },
            script: r#"
from greptime import *
ret = vector([count(values)])
ret"#
                .to_string(),
            expect: vector!(Int64Vector, [10]),
        },
        CodeBlockTestCase {
            input: ronish! {
                "a": vector!(Float64Vector, [1.0, 2.0, 3.0]),
                "b": vector!(Float64Vector, [1.0, 0.0, -1.0])
            },
            script: r#"
from greptime import *
ret = vector([covariance(a, b)])
ret"#
                .to_string(),
            expect: vector!(Float64Vector, [-1.0]),
        },
        CodeBlockTestCase {
            input: ronish! {
                "a": vector!(Float64Vector, [1.0, 2.0, 3.0]),
                "b": vector!(Float64Vector, [1.0, 0.0, -1.0])
            },
            script: r#"
from greptime import *
ret = vector([covariance_pop(a, b)])
ret"#
                .to_string(),
            expect: vector!(Float64Vector, [-0.6666666666666666]),
        },
        CodeBlockTestCase {
            input: ronish! {
                "a": vector!(Float64Vector, [1.0, 2.0, 3.0]),
            },
            script: r#"
from greptime import *
ret = vector([max(a)])
ret"#
                .to_string(),
            expect: vector!(Float64Vector, [3.0]),
        },
        CodeBlockTestCase {
            input: ronish! {
                "a": vector!(Float64Vector, [1.0, 2.0, 3.0]),
            },
            script: r#"
from greptime import *
ret = vector([min(a)])
ret"#
                .to_string(),
            expect: vector!(Float64Vector, [1.0]),
        },
        CodeBlockTestCase {
            input: ronish! {
                "values": vector!(Float64Vector, [1., 2., 3., 4., 5., 6., 7., 8., 9., 10.]),
            },
            script: r#"
from greptime import *
ret = vector([stddev(values)])
ret"#
                .to_string(),
            expect: vector!(Float64Vector, [3.0276503540974917]),
        },
        CodeBlockTestCase {
            input: ronish! {
                "values": vector!(Float64Vector, [1., 2., 3., 4., 5., 6., 7., 8., 9., 10.]),
            },
            script: r#"
from greptime import *
ret = vector([stddev_pop(values)])
ret"#
                .to_string(),
            expect: vector!(Float64Vector, [2.8722813232690143]),
        },
        CodeBlockTestCase {
            input: ronish! {
                "values": vector!(Float64Vector, [1., 2., 3., 4., 5., 6., 7., 8., 9., 10.]),
            },
            script: r#"
from greptime import *
ret = vector([sum(values)])
ret"#
                .to_string(),
            expect: vector!(Float64Vector, [55.0]),
        },
        CodeBlockTestCase {
            input: ronish! {
                "values": vector!(Float64Vector, [1., 2., 3., 4., 5., 6., 7., 8., 9., 10.]),
            },
            script: r#"
from greptime import *
ret = vector([variance(values)])
ret"#
                .to_string(),
            expect: vector!(Float64Vector, [9.166666666666666]),
        },
        CodeBlockTestCase {
            input: ronish! {
                "values": vector!(Float64Vector, [1., 2., 3., 4., 5., 6., 7., 8., 9., 10.]),
            },
            script: r#"
from greptime import *
ret = vector([variance_pop(values)])
ret"#
                .to_string(),
            expect: vector!(Float64Vector, [8.25]),
        },
        // TODO(discord9): GrepTime's Own UDF
    ]
}
