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

use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use std::sync::Arc;

use common_recordbatch::RecordBatch;
use common_telemetry::{error, info};
use console::style;
use datatypes::arrow::datatypes::DataType as ArrowDataType;
use datatypes::data_type::{ConcreteDataType, DataType};
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::vectors::{Float32Vector, Float64Vector, Int64Vector, VectorRef};
use ron::from_str as from_ron_string;
use rustpython_parser::{parse, Mode};
use serde::{Deserialize, Serialize};

use crate::python::error::{get_error_reason_loc, pretty_print_error_in_src, visualize_loc, Error};
use crate::python::ffi_types::copr::parse::parse_and_compile_copr;
use crate::python::ffi_types::copr::{exec_coprocessor, AnnotationInfo};
use crate::python::ffi_types::Coprocessor;

#[derive(Deserialize, Debug)]
struct TestCase {
    name: String,
    code: String,
    predicate: Predicate,
}

#[derive(Deserialize, Debug)]
enum Predicate {
    ParseIsOk {
        result: Box<Coprocessor>,
    },
    ParseIsErr {
        /// used to check if after serialize [`Error`] into a String, that string contains `reason`
        reason: String,
    },
    ExecIsOk {
        fields: Vec<AnnotationInfo>,
        columns: Vec<ColumnInfo>,
    },
    ExecIsErr {
        /// used to check if after serialize [`Error`] into a String, that string contains `reason`
        reason: String,
    },
}

#[derive(Serialize, Deserialize, Debug)]
struct ColumnInfo {
    pub ty: ArrowDataType,
    pub len: usize,
}

fn create_sample_recordbatch() -> RecordBatch {
    let cpu_array = Float32Vector::from_slice([0.9f32, 0.8, 0.7, 0.6]);
    let mem_array = Float64Vector::from_slice([0.1f64, 0.2, 0.3, 0.4]);
    let schema = Arc::new(Schema::new(vec![
        ColumnSchema::new("cpu", ConcreteDataType::float32_datatype(), false),
        ColumnSchema::new("mem", ConcreteDataType::float64_datatype(), false),
    ]));

    RecordBatch::new(
        schema,
        [
            Arc::new(cpu_array) as VectorRef,
            Arc::new(mem_array) as VectorRef,
        ],
    )
    .unwrap()
}

/// test cases which read from a .ron file, deser,
///
/// and exec/parse (depending on the type of predicate) then decide if result is as expected
#[test]
fn run_ron_testcases() {
    common_telemetry::init_default_ut_logging();

    let loc = Path::new("src/python/rspython/testcases.ron");
    let loc = loc.to_str().expect("Fail to parse path");
    let mut file = File::open(loc).expect("Fail to open file");
    let mut buf = String::new();
    let _ = file.read_to_string(&mut buf).unwrap();
    let testcases: Vec<TestCase> = from_ron_string(&buf).expect("Fail to convert to testcases");
    info!("Read {} testcases from {}", testcases.len(), loc);
    for testcase in testcases {
        info!(".ron test {}", testcase.name);
        match testcase.predicate {
            Predicate::ParseIsOk { result } => {
                let copr = parse_and_compile_copr(&testcase.code, None);
                let mut copr = copr.unwrap();
                copr.script = "".into();
                assert_eq!(copr, *result);
            }
            Predicate::ParseIsErr { reason } => {
                let copr = parse_and_compile_copr(&testcase.code, None);
                assert!(copr.is_err(), "Expect to be err, actual {copr:#?}");

                let res = &copr.unwrap_err();
                error!(
                    "{}",
                    pretty_print_error_in_src(&testcase.code, res, 0, "<embedded>")
                );
                let (res, _) = get_error_reason_loc(res);
                assert!(
                    res.contains(&reason),
                    "{} Parse Error, expect \"{reason}\" in \"{res}\", actual not found.",
                    testcase.code,
                );
            }
            Predicate::ExecIsOk { fields, columns } => {
                let rb = create_sample_recordbatch();
                let res = exec_coprocessor(&testcase.code, &Some(rb)).unwrap();
                fields
                    .iter()
                    .zip(res.schema.column_schemas())
                    .for_each(|(anno, real)| {
                        assert!(
                            anno.datatype.as_ref().unwrap() == &real.data_type
                                && anno.is_nullable == real.is_nullable(),
                            "Fields expected to be {anno:#?}, actual {real:#?}"
                        );
                    });
                columns.iter().zip(res.columns()).for_each(|(anno, real)| {
                    assert!(
                        anno.ty == real.data_type().as_arrow_type() && anno.len == real.len(),
                        "Type or length not match! Expect [{:#?}; {}], actual [{:#?}; {}]",
                        anno.ty,
                        anno.len,
                        real.data_type(),
                        real.len()
                    );
                });
            }
            Predicate::ExecIsErr {
                reason: part_reason,
            } => {
                let rb = create_sample_recordbatch();
                let res = exec_coprocessor(&testcase.code, &Some(rb));
                assert!(res.is_err(), "{res:#?}\nExpect Err(...), actual Ok(...)");
                if let Err(res) = res {
                    error!(
                        "{}",
                        pretty_print_error_in_src(&testcase.code, &res, 1120, "<embedded>")
                    );
                    let (reason, _) = get_error_reason_loc(&res);
                    assert!(
                        reason.contains(&part_reason),
                        "{}\nExecute error, expect \"{reason}\" in \"{res}\", actual not found.",
                        testcase.code,
                        reason = style(reason).green(),
                        res = style(res).red()
                    )
                }
            }
        }
        info!(" ... {}", style("ok✅").green());
    }
}

#[test]
#[allow(unused)]
fn test_type_anno() {
    let python_source = r#"
@copr(args=["cpu", "mem"], returns=["perf", "what", "how", "why"])
def a(cpu, mem: vector[f64])->(vector[f64|None], vector[f64], vector[_], vector[ _ | None]):
    return cpu + mem, cpu - mem, cpu * mem, cpu / mem
"#;
    let pyast = parse(python_source, Mode::Interactive, "<embedded>").unwrap();
    let copr = parse_and_compile_copr(python_source, None);
    dbg!(copr);
}

#[test]
#[allow(clippy::print_stdout, unused_must_use)]
// allow print in test function for debug purpose(like for quick testing a syntax&ideas)
fn test_calc_rvs() {
    let python_source = r#"
@coprocessor(args=["open_time", "close"], returns=[
    "rv_7d",
    "rv_15d",
    "rv_30d",
    "rv_60d",
    "rv_90d",
    "rv_180d"
])
def calc_rvs(open_time, close):
    from greptime import vector, log, prev, sqrt, datetime, pow, sum, last
    import greptime as g
    def calc_rv(close, open_time, time, interval):
        mask = (open_time < time) & (open_time > time - interval)
        close = close[mask]
        open_time = open_time[mask]
        close = g.interval(open_time, close, datetime("10m"), lambda x:last(x))

        avg_time_interval = (open_time[-1] - open_time[0])/(len(open_time)-1)
        ref = log(close/prev(close))
        var = sum(pow(ref, 2)/(len(ref)-1))
        return sqrt(var/avg_time_interval)

    # how to get env var,
    # maybe through accessing scope and serde then send to remote?
    timepoint = open_time[-1]
    rv_7d = vector([calc_rv(close, open_time, timepoint, datetime("7d"))])
    rv_15d = vector([calc_rv(close, open_time, timepoint, datetime("15d"))])
    rv_30d = vector([calc_rv(close, open_time, timepoint, datetime("30d"))])
    rv_60d = vector([calc_rv(close, open_time, timepoint, datetime("60d"))])
    rv_90d = vector([calc_rv(close, open_time, timepoint, datetime("90d"))])
    rv_180d = vector([calc_rv(close, open_time, timepoint, datetime("180d"))])
    return rv_7d, rv_15d, rv_30d, rv_60d, rv_90d, rv_180d
"#;
    let close_array = Float32Vector::from_slice([
        10106.79f32,
        10106.09,
        10108.73,
        10106.38,
        10106.95,
        10107.55,
        10104.68,
        10108.8,
        10115.96,
        10117.08,
        10120.43,
    ]);
    let open_time_array = Int64Vector::from_slice([
        300i64, 900i64, 1200i64, 1800i64, 2400i64, 3000i64, 3600i64, 4200i64, 4800i64, 5400i64,
        6000i64,
    ]);
    let schema = Arc::new(Schema::new(vec![
        ColumnSchema::new("close", ConcreteDataType::float32_datatype(), false),
        ColumnSchema::new("open_time", ConcreteDataType::int64_datatype(), false),
    ]));
    let rb = RecordBatch::new(
        schema,
        [
            Arc::new(close_array) as VectorRef,
            Arc::new(open_time_array) as VectorRef,
        ],
    )
    .unwrap();
    let ret = exec_coprocessor(python_source, &Some(rb));
    if let Err(Error::PyParse { location: _, error }) = ret {
        let res = visualize_loc(
            python_source,
            &error.location,
            "unknown tokens",
            error.error.to_string().as_str(),
            0,
            "copr.py",
        );
        info!("{res}");
    } else if let Ok(res) = ret {
        dbg!(&res);
    } else {
        dbg!(ret);
    }
}

#[test]
#[allow(clippy::print_stdout, unused_must_use)]
// allow print in test function for debug purpose(like for quick testing a syntax&ideas)
fn test_coprocessor() {
    let python_source = r#"
@copr(args=["cpu", "mem"], returns=["ref"])
def a(cpu, mem):
    import greptime as gt
    from greptime import vector, log2, prev, sum, pow, sqrt, datetime
    abc = vector([v[0] > v[1] for v in zip(cpu, mem)])
    fed = cpu.filter(abc)
    ref = log2(fed/prev(fed))
    return cpu[(cpu > 0.5) & ~( cpu >= 0.75)]
"#;
    let cpu_array = Float32Vector::from_slice([0.9f32, 0.8, 0.7, 0.3]);
    let mem_array = Float64Vector::from_slice([0.1f64, 0.2, 0.3, 0.4]);
    let schema = Arc::new(Schema::new(vec![
        ColumnSchema::new("cpu", ConcreteDataType::float32_datatype(), false),
        ColumnSchema::new("mem", ConcreteDataType::float64_datatype(), false),
    ]));
    let rb = RecordBatch::new(
        schema,
        [
            Arc::new(cpu_array) as VectorRef,
            Arc::new(mem_array) as VectorRef,
        ],
    )
    .unwrap();
    let ret = exec_coprocessor(python_source, &Some(rb));
    if let Err(Error::PyParse { location: _, error }) = ret {
        let res = visualize_loc(
            python_source,
            &error.location,
            "unknown tokens",
            error.error.to_string().as_str(),
            0,
            "copr.py",
        );
        info!("{res}");
    } else if let Ok(res) = ret {
        dbg!(&res);
    } else {
        dbg!(ret);
    }
}
