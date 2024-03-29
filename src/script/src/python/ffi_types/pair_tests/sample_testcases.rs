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
#[cfg(feature = "pyo3_backend")]
use datatypes::vectors::UInt32Vector;
use datatypes::vectors::{
    BooleanVector, Float64Vector, Int32Vector, Int64Vector, StringVector, VectorRef,
};

use crate::python::ffi_types::pair_tests::{CodeBlockTestCase, CoprTestCase};
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
        // first is examples in docs
// hello.py: test return a single string
CoprTestCase {
    script: r#"
@coprocessor(returns=['msg'])
def hello() -> vector[str]:
    return "hello, GreptimeDB"
"#
    .to_string(),
    expect: Some(ronish!("msg": vector!(StringVector, &["hello, GreptimeDB"]))),
},
#[cfg(feature = "pyo3_backend")]
CoprTestCase {
    script: r#"
@coprocessor(returns=['msg'], backend="pyo3")
def hello() -> vector[str]:
    return "hello, GreptimeDB"
"#
    .to_string(),
    expect: Some(ronish!("msg": vector!(StringVector, &["hello, GreptimeDB"]))),
},
// add_vectors.py
CoprTestCase {
    script: r#"
@copr(args=["n1", "n2"],
    returns=["value"],
    sql="select number as n1,number as n2 from numbers limit 5")
def add_vectors(n1, n2) -> vector[i32]:
    return n1 + n2
"#
    .to_string(),
    expect: Some(ronish!("value": vector!(Int32Vector, [0, 2, 4, 6, 8]))),
},
#[cfg(feature = "pyo3_backend")]
CoprTestCase {
    script: r#"
@copr(args=["n1", "n2"],
    returns=["value"],
    sql="select number as n1,number as n2 from numbers limit 5",
    backend="pyo3")
def add_vectors(n1, n2) -> vector[i32]:
    return n1 + n2
"#
    .to_string(),
    expect: Some(ronish!("value": vector!(Int32Vector, [0, 2, 4, 6, 8]))),
},
// answer.py
CoprTestCase {
    script: r#"
@copr(returns=["value"])
def answer() -> vector[i64]:
    return 42
"#
    .to_string(),
    expect: Some(ronish!("value": vector!(Int64Vector, [42]))),
},
#[cfg(feature = "pyo3_backend")]
CoprTestCase {
    script: r#"
@copr(returns=["value"], backend="pyo3")
def answer() -> vector[i64]:
    return 42
"#
    .to_string(),
    expect: Some(ronish!("value": vector!(Int64Vector, [42]))),
},
// answer_list.py
CoprTestCase {
    script: r#"
from greptime import vector

@copr(returns=["value"])
def answer() -> (vector[i64]):
    return vector([42, 43, 44])
"#
    .to_string(),
    expect: Some(ronish!("value": vector!(Int64Vector, [42, 43, 44]))),
},
#[cfg(feature = "pyo3_backend")]
CoprTestCase {
    script: r#"
from greptime import vector

@copr(returns=["value"], backend="pyo3")
def answer() -> (vector[i64]):
    return vector([42, 43, 44])
"#
    .to_string(),
    expect: Some(ronish!("value": vector!(Int64Vector, [42, 43, 44]))),
},
// boolean_array.py
CoprTestCase {
    script: r#"
from greptime import vector

@copr(returns=["value"])
def boolean_array() -> vector[f64]:
    v = vector([1.0, 2.0, 3.0])
    # This returns a vector([2.0])
    return v[(v > 1) & (v< 3)]
"#
    .to_string(),
    expect: Some(ronish!("value": vector!(Float64Vector, [2.0]))),
},
#[cfg(feature = "pyo3_backend")]
CoprTestCase {
    script: r#"
from greptime import vector

@copr(returns=["value"], backend="pyo3")
def boolean_array() -> vector[f64]:
    v = vector([1.0, 2.0, 3.0])
    # This returns a vector([2.0])
    return v[(v > 1) & (v< 3)]
"#
    .to_string(),
    expect: Some(ronish!("value": vector!(Float64Vector, [2.0]))),
},
// compare.py
CoprTestCase {
    script: r#"
from greptime import vector

@copr(returns=["value"])
def compare() -> vector[bool]:
    # This returns a vector([False, False, True])
    return vector([1.0, 2.0, 3.0]) > 2.0
"#
    .to_string(),
    expect: Some(ronish!("value": vector!(BooleanVector, &[false, false, true]))),
},
#[cfg(feature = "pyo3_backend")]
CoprTestCase {
    script: r#"
from greptime import vector

@copr(returns=["value"], backend="pyo3")
def compare() -> vector[bool]:
    # This returns a vector([False, False, True])
    return vector([1.0, 2.0, 3.0]) > 2.0
"#
    .to_string(),
    expect: Some(ronish!("value": vector!(BooleanVector, &[false, false, true]))),
},
// compare_vectors.py
CoprTestCase {
    script: r#"
from greptime import vector

@copr(returns=["value"])
def compare_vectors() -> vector[bool]:
    # This returns a vector([False, False, True])
    return vector([1.0, 2.0, 3.0]) > vector([1.0, 2.0, 2.0])
"#
    .to_string(),
    expect: Some(ronish!("value": vector!(BooleanVector, &[false, false, true]))),
},
#[cfg(feature = "pyo3_backend")]
CoprTestCase {
    script: r#"
from greptime import vector

@copr(returns=["value"], backend="pyo3")
def compare_vectors() -> vector[bool]:
    # This returns a vector([False, False, True])
    return vector([1.0, 2.0, 3.0]) > vector([1.0, 2.0, 2.0])
"#
    .to_string(),
    expect: Some(ronish!("value": vector!(BooleanVector, &[false, false, true]))),
},
// list_comprehension.py
CoprTestCase {
    script: r#"
from greptime import vector

@copr(returns=["value"])
def list_comprehension() -> (vector[f64]):
    a = vector([1.0, 2.0, 3.0])
    # This returns a vector([3.0, 4.0])
    return [x+1 for x in a if x >= 2.0]
"#
    .to_string(),
    expect: Some(ronish!("value": vector!(Float64Vector, &[3.0 ,4.0]))),
},
#[cfg(feature = "pyo3_backend")]
CoprTestCase {
    script: r#"
from greptime import vector

@copr(returns=["value"], backend="pyo3")
def list_comprehension() -> (vector[f64]):
    a = vector([1.0, 2.0, 3.0])
    # This returns a vector([3.0, 4.0])
    return [x+1 for x in a if x >= 2.0]
"#
    .to_string(),
    expect: Some(ronish!("value": vector!(Float64Vector, &[3.0 ,4.0]))),
},
// select_elements.py
CoprTestCase {
    script: r#"
from greptime import vector

@copr(returns=["value"])
def select_elements() -> (vector[f64]):
    a = vector([1.0, 2.0, 3.0])
    # This returns a vector([2.0, 3.0])
    return a[a>=2.0]
"#
    .to_string(),
    expect: Some(ronish!("value": vector!(Float64Vector, &[2.0, 3.0]))),
},
#[cfg(feature = "pyo3_backend")]
CoprTestCase {
    script: r#"
from greptime import vector

@copr(returns=["value"], backend="pyo3")
def select_elements() -> (vector[f64]):
    a = vector([1.0, 2.0, 3.0])
    # This returns a vector([2.0, 3.0])
    return a[a>=2.0]
"#
    .to_string(),
    expect: Some(ronish!("value": vector!(Float64Vector, &[2.0, 3.0]))),
},
// args.py
CoprTestCase {
    script: r#"
@coprocessor(args=["a", "b"],
returns=["value"],
sql="select number as a,number as b from numbers limit 5")
def add_vectors(a, b) -> vector[i64]:
    return a + b
"#
    .to_string(),
    expect: Some(ronish!("value": vector!(Int64Vector, &[0, 2, 4, 6, 8]))),
},
#[cfg(feature = "pyo3_backend")]
CoprTestCase {
    script: r#"
@coprocessor(args=["a", "b"],
returns=["value"],
sql="select number as a,number as b from numbers limit 5",
backend="pyo3")
def add_vectors(a, b) -> vector[i64]:
    return a + b
"#
    .to_string(),
    expect: Some(ronish!("value": vector!(Int64Vector, &[0, 2, 4, 6, 8]))),
},
// numbers.py
CoprTestCase {
    script: r#"
@coprocessor(args=["number", "number", "number"],
sql="select number from numbers limit 5",
returns=["value"])
def normalize(n1, n2, n3) -> vector[i64]:
    # returns [0,1,8,27,64]
    return n1 * n2 * n3
"#
    .to_string(),
    expect: Some(ronish!("value": vector!(Int64Vector, &[0, 1, 8, 27, 64]))),
},
#[cfg(feature = "pyo3_backend")]
CoprTestCase {
    script: r#"
@coprocessor(args=["number", "number", "number"],
sql="select number from numbers limit 5",
returns=["value"],
backend="pyo3")
def normalize(n1, n2, n3) -> vector[i64]:
    # returns [0,1,8,27,64]
    return n1 * n2 * n3
"#
    .to_string(),
    expect: Some(ronish!("value": vector!(Int64Vector, &[0, 1, 8, 27, 64]))),
},
// return_multi_vectors1.py
CoprTestCase {
    script: r#"
from greptime import vector

@coprocessor(returns=["a", "b", "c"])
def return_vectors() -> (vector[i64], vector[str], vector[f64]):
    a = vector([1, 2, 3])
    b = vector(["a", "b", "c"])
    c = vector([42.0, 43.0, 44.0])
    return a, b, c
"#
    .to_string(),
    expect: Some(ronish!(
        "a": vector!(Int64Vector, &[1, 2,  3]),
        "b": vector!(StringVector, &["a", "b", "c"]),
        "c": vector!(Float64Vector, &[42.0, 43.0, 44.0])
    )),
},
#[cfg(feature = "pyo3_backend")]
CoprTestCase {
    script: r#"
from greptime import vector

@coprocessor(returns=["a", "b", "c"], backend="pyo3")
def return_vectors() -> (vector[i64], vector[str], vector[f64]):
    a = vector([1, 2, 3])
    b = vector(["a", "b", "c"])
    c = vector([42.0, 43.0, 44.0])
    return a, b, c
"#
    .to_string(),
    expect: Some(ronish!(
        "a": vector!(Int64Vector, &[1, 2, 3]),
        "b": vector!(StringVector, &["a", "b", "c"]),
        "c": vector!(Float64Vector, &[42.0, 43.0, 44.0])
    )),
},
// return_multi_vectors2.py
CoprTestCase {
    script: r#"
from greptime import vector

@coprocessor(returns=["a", "b", "c"])
def return_vectors() -> (vector[i64], vector[str], vector[i64]):
    a = 1
    b = "Hello, GreptimeDB!"
    c = 42
    return a, b, c
"#
    .to_string(),
    expect: Some(ronish!(
        "a": vector!(Int64Vector, &[1]),
        "b": vector!(StringVector, &["Hello, GreptimeDB!"]),
        "c": vector!(Float64Vector, &[42.0])
    )),
},
#[cfg(feature = "pyo3_backend")]
CoprTestCase {
    script: r#"
from greptime import vector

@coprocessor(returns=["a", "b", "c"], backend="pyo3")
def return_vectors() -> (vector[i64], vector[str], vector[i64]):
    a = 1
    b = "Hello, GreptimeDB!"
    c = 42
    return [a], [b], [c]
"#
    .to_string(),
    expect: Some(ronish!(
        "a": vector!(Int64Vector, &[1]),
        "b": vector!(StringVector, &["Hello, GreptimeDB!"]),
        "c": vector!(Float64Vector, &[42.0])
    )),
},


        // following is some random tests covering most features of coprocessor
        CoprTestCase {
            script: r#"
from greptime import vector
@copr(returns=["value"])
def boolean_array() -> vector[f64]:
    v = vector([1.0, 2.0, 3.0])
    # This returns a vector([2.0])
    return v[(v > 1) & (v < 3)]
"#
            .to_string(),
            expect: Some(ronish!("value": vector!(Float64Vector, [2.0f64]))),
        },
        #[cfg(feature = "pyo3_backend")]
        CoprTestCase {
            script: r#"
@copr(returns=["value"], backend="pyo3")
def boolean_array() -> vector[f64]:
    from greptime import vector
    from greptime import query
    
    print("query()=", query())
    assert "query_engine object at" in str(query())
    rb = query().sql(
        "select number from numbers limit 5"
    )
    print(rb)
    assert len(rb) == 5


    v = vector([1.0, 2.0, 3.0])
    # This returns a vector([2.0])
    return v[(v > 1) & (v < 3)]
"#
            .to_string(),
            expect: Some(ronish!("value": vector!(Float64Vector, [2.0f64]))),
        },
        CoprTestCase {
            script: r#"
@copr(returns=["value"], backend="rspy")
def boolean_array() -> vector[f64]:
    from greptime import vector, col
    from greptime import query, PyDataFrame
    
    df = PyDataFrame.from_sql("select number from numbers limit 5")
    print("df from sql=", df)
    collected = df.collect()
    print("df.collect()=", collected)
    assert len(collected[0]) == 5
    df = PyDataFrame.from_sql("select number from numbers limit 5").filter(col("number") > 2)
    collected = df.collect()
    assert len(collected[0]) == 2
    assert collected[0] == collected["number"]
    print("query()=", query())

    assert "query_engine object at" in repr(query())
    rb = query().sql(
        "select number from numbers limit 5"
    )
    print(rb)
    assert len(rb) == 5

    v = vector([1.0, 2.0, 3.0])
    # This returns a vector([2.0])
    return v[(v > 1) & (v < 3)]
"#
            .to_string(),
            expect: Some(ronish!("value": vector!(Float64Vector, [2.0f64]))),
        },
        #[cfg(feature = "pyo3_backend")]
        CoprTestCase {
            script: r#"
@copr(returns=["value"], backend="pyo3")
def boolean_array() -> vector[f64]:
    from greptime import vector
    from greptime import query, PyDataFrame, col
    df = PyDataFrame.from_sql("select number from numbers limit 5")
    print("df from sql=", df)
    ret = df.collect()
    print("df.collect()=", ret)
    assert len(ret[0]) == 5
    df = PyDataFrame.from_sql("select number from numbers limit 5").filter(col("number") > 2)
    collected = df.collect()
    assert len(collected[0]) == 2
    assert collected[0] == collected["number"]
    return ret[0]
"#
            .to_string(),
            expect: Some(ronish!("value": vector!(UInt32Vector, [0, 1, 2, 3, 4]))),
        },
        #[cfg(feature = "pyo3_backend")]
        CoprTestCase {
            script: r#"
@copr(returns=["value"], backend="pyo3")
def boolean_array() -> vector[f64]:
    from greptime import vector
    v = vector([1.0, 2.0, 3.0])
    # This returns a vector([2.0])
    return v[(v > 1) & (v < 3)]
"#
            .to_string(),
            expect: Some(ronish!("value": vector!(Float64Vector, [2.0f64]))),
        },
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
    try:
        import pyarrow as pa
    except ImportError:
        # Python didn't have pyarrow
        print("Warning: no pyarrow in current python")
        return vector([42, 43, 44])
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
    try:
        import pyarrow as pa
    except ImportError:
        # Python didn't have pyarrow
        print("Warning: no pyarrow in current python")
        return vector([42, 43, 44])
    return vector.from_pyarrow(pa.array([42, 43, 44]))
"#
            .to_string(),
            expect: Some(ronish!("value": vector!(Int64Vector, [42, 43, 44]))),
        },
        CoprTestCase {
            script: r#"
@copr(args=[], returns = ["number"], sql = "select * from numbers", backend="rspy")
def answer() -> vector[i64]:
    from greptime import vector, col, lit, PyDataFrame
    expr_0 = (col("number")<lit(3)) & (col("number")>0)
    ret = PyDataFrame.from_sql("select * from numbers").select([col("number")]).filter(expr_0).collect()[0]
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
    from greptime import vector, col, lit, PyDataFrame
    # Bitwise Operator  pred comparison operator
    expr_0 = (col("number")<lit(3)) & (col("number")>0)
    ret = PyDataFrame.from_sql("select * from numbers").select([col("number")]).filter(expr_0).collect()[0]
    return ret
"#
            .to_string(),
            expect: Some(ronish!("number": vector!(Int64Vector, [1, 2]))),
        },
        #[cfg(feature = "pyo3_backend")]
        CoprTestCase {
            script: r#"
@copr(returns=["value"], backend="pyo3")
def answer() -> vector[i64]:
    from greptime import vector
    try:
        import pyarrow as pa
    except ImportError:
        # Python didn't have pyarrow
        print("Warning: no pyarrow in current python")
        return vector([42])
    a = vector.from_pyarrow(pa.array([42]))
    return a[0:1]
"#
            .to_string(),
            expect: Some(ronish!("value": vector!(Int64Vector, [42]))),
        },
        #[cfg(feature = "pyo3_backend")]
        CoprTestCase {
            script: r#"
@copr(returns=["value"], backend="pyo3")
def answer() -> vector[i64]:
    from greptime import vector
    a = vector([42, 43, 44])
    # slicing test
    assert a[0:2] == a[:-1]
    assert len(a[:-1]) == vector([42,44])
    assert a[0:1] == a[:-2] 
    assert a[0:1] == vector([42])
    assert a[:-2] == vector([42])
    assert a[:-1:2] == vector([42])
    assert a[::2] == vector([42,44])
    # negative step
    assert a[-1::-2] == vector([44, 42])
    assert a[-2::-2] == vector([44])
    return a[0:1]
"#
            .to_string(),
            expect: Some(ronish!("value": vector!(Int64Vector, [42]))),
        },
        CoprTestCase {
            script: r#"
@copr(returns=["value"], backend="rspy")
def answer() -> vector[i64]:
    from greptime import vector
    a = vector([42, 43, 44])
    # slicing test
    assert a[0:2] == a[:-1]
    assert len(a[:-1]) == vector([42,44])
    assert a[0:1] == a[:-2] 
    assert a[0:1] == vector([42])
    assert a[:-2] == vector([42])
    assert a[:-1:2] == vector([42])
    assert a[::2] == vector([42,44])
    # negative step
    assert a[-1::-2] == vector([44, 42])
    assert a[-2::-2] == vector([44])
    return a[-2:-1]
"#
            .to_string(),
            expect: Some(ronish!("value": vector!(Int64Vector, [43]))),
        },
        // normalize.py
        CoprTestCase {
            script: r#"
import math

def normalize0(x):
    if x is None or math.isnan(x):
        return 0
    elif x > 100:
        return 100
    elif x < 0:
        return 0
    else:
        return x

@coprocessor(args=["number"], sql="select number from numbers limit 10", returns=["value"], backend="rspy")
def normalize(v) -> vector[i64]:
    return [normalize0(x) for x in v]
            
"#
            .to_string(),
            expect: Some(ronish!(
                "value": vector!(Int64Vector, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9,])
            )),
        },
        #[cfg(feature = "pyo3_backend")]
        CoprTestCase {
            script: r#"
import math

def normalize0(x):
    if x is None or math.isnan(x):
        return 0
    elif x > 100:
        return 100
    elif x < 0:
        return 0
    else:
        return x

@coprocessor(args=["number"], sql="select number from numbers limit 10", returns=["value"], backend="pyo3")
def normalize(v) -> vector[i64]:
    return [normalize0(x) for x in v]
            
"#
            .to_string(),
            expect: Some(ronish!(
                "value": vector!(Int64Vector, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9,])
            )),
        },
        #[cfg(feature = "pyo3_backend")]
        CoprTestCase {
            script: r#"
import math

@coprocessor(args=[], returns=["value"], backend="pyo3")
def test_numpy() -> vector[i64]:
    try:
        import numpy as np
        import pyarrow as pa
    except ImportError as e:
        # Python didn't have numpy or pyarrow
        print("Warning: no pyarrow or numpy found in current python", e)
        return vector([0, 1, 2, 3, 4, 5, 6, 7, 8, 9,])
    from greptime import vector
    v = np.array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9,])
    v = pa.array(v)
    v = vector.from_pyarrow(v)
    v = vector.from_numpy(v.numpy())
    v = v.to_pyarrow()
    v = v.to_numpy()
    v = vector.from_numpy(v)
    return v
            
"#
            .to_string(),
            expect: Some(ronish!(
                "value": vector!(Int64Vector, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9,])
            )),
        },
    ]
}

/// Generate tests for basic vector operations and basic builtin functions
/// Using a function to generate testcase instead of `.ron` configure file because it's more flexible and we are in #[cfg(test)] so no binary bloat worrying
#[allow(clippy::approx_constant)]
pub(super) fn sample_test_case() -> Vec<CodeBlockTestCase> {
    // TODO(discord9): detailed tests for slicing vector
    vec![
        CodeBlockTestCase {
            input: ronish! {
                "a": vector!(Float64Vector, [1.0f64, 2.0, 3.0])
            },
            script: r#"
from greptime import *
ret = a[0:1]
ret"#
                .to_string(),
            expect: vector!(Float64Vector, [1.0f64]),
        },
        CodeBlockTestCase {
            input: ronish! {
                "a": vector!(Float64Vector, [1.0f64, 2.0, 3.0])
            },
            script: r#"
from greptime import *
ret = a[0:1:1]
ret"#
                .to_string(),
            expect: vector!(Float64Vector, [1.0f64]),
        },
        CodeBlockTestCase {
            input: ronish! {
                "a": vector!(Float64Vector, [1.0f64, 2.0, 3.0])
            },
            script: r#"
from greptime import *
ret = a[-2:-1]
ret"#
                .to_string(),
            expect: vector!(Float64Vector, [2.0f64]),
        },
        CodeBlockTestCase {
            input: ronish! {
                "a": vector!(Float64Vector, [1.0f64, 2.0, 3.0])
            },
            script: r#"
from greptime import *
ret = a[-1:-2:-1]
ret"#
                .to_string(),
            expect: vector!(Float64Vector, [3.0f64]),
        },
        CodeBlockTestCase {
            input: ronish! {
                "a": vector!(Float64Vector, [1.0f64, 2.0, 3.0])
            },
            script: r#"
from greptime import *
ret = a[-1:-4:-1]
ret"#
                .to_string(),
            expect: vector!(Float64Vector, [3.0f64, 2.0, 1.0]),
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
