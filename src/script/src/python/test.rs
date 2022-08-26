#![allow(clippy::print_stdout, clippy::print_stderr)]
// for debug purpose, also this is already a
// test module so allow print_stdout shouldn't be a problem?
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use std::sync::Arc;

use arrow::array::PrimitiveArray;
use arrow::datatypes::{DataType, Field, Schema};
use console::style;
use datafusion_common::record_batch::RecordBatch as DfRecordBatch;
use ron::from_str as from_ron_string;
use rustpython_parser::parser;
use serde::{Deserialize, Serialize};

use super::error::{get_error_reason_loc, visualize_loc};
use crate::python::coprocessor::AnnotationInfo;
use crate::python::error::pretty_print_error_in_src;
use crate::python::{
    coprocessor, coprocessor::parse::parse_copr, coprocessor::Coprocessor, error::Error,
};

#[derive(Deserialize, Debug)]
struct TestCase {
    name: String,
    code: String,
    predicate: Predicate,
}

#[derive(Deserialize, Debug)]
enum Predicate {
    ParseIsOk {
        result: Coprocessor,
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
    pub ty: DataType,
    pub len: usize,
}

fn create_sample_recordbatch() -> DfRecordBatch {
    let cpu_array = PrimitiveArray::from_slice([0.9f32, 0.8, 0.7, 0.6]);
    let mem_array = PrimitiveArray::from_slice([0.1f64, 0.2, 0.3, 0.4]);
    let schema = Arc::new(Schema::from(vec![
        Field::new("cpu", DataType::Float32, false),
        Field::new("mem", DataType::Float64, false),
    ]));

    DfRecordBatch::try_new(schema, vec![Arc::new(cpu_array), Arc::new(mem_array)]).unwrap()
}

/// test cases which read from a .ron file, deser,
///
/// and exec/parse (depending on the type of predicate) then decide if result is as expected
#[test]
fn run_ron_testcases() {
    let loc = Path::new("src/python/testcases.ron");
    let loc = loc.to_str().expect("Fail to parse path");
    let mut file = File::open(loc).expect("Fail to open file");
    let mut buf = String::new();
    file.read_to_string(&mut buf)
        .expect("Fail to read to string");
    let testcases: Vec<TestCase> = from_ron_string(&buf).expect("Fail to convert to testcases");
    println!("Read {} testcases from {}", testcases.len(), loc);
    for testcase in testcases {
        print!(".ron test {}", testcase.name);
        match testcase.predicate {
            Predicate::ParseIsOk { result } => {
                let copr = parse_copr(&testcase.code);
                let mut copr = copr.unwrap();
                copr.script = "".into();
                assert_eq!(copr, result);
            }
            Predicate::ParseIsErr { reason } => {
                let copr = parse_copr(&testcase.code);
                if copr.is_ok() {
                    eprintln!("Expect to be err, found{copr:#?}");
                    panic!()
                }
                let res = &copr.unwrap_err();
                println!(
                    "{}",
                    pretty_print_error_in_src(&testcase.code, res, 0, "<embedded>")
                );
                let (res, _) = get_error_reason_loc(res);
                if !res.contains(&reason) {
                    eprintln!("{}", testcase.code);
                    eprintln!("Parse Error, expect \"{reason}\" in \"{res}\", but not found.");
                    panic!()
                }
            }
            Predicate::ExecIsOk { fields, columns } => {
                let rb = create_sample_recordbatch();
                let res = coprocessor::exec_coprocessor(&testcase.code, &rb);
                if res.is_err() {
                    dbg!(&res);
                }
                assert!(res.is_ok());
                let res = res.unwrap();
                fields
                    .iter()
                    .zip(&res.schema.arrow_schema().fields)
                    .map(|(anno, real)| {
                        if !(anno.datatype.clone().unwrap() == real.data_type
                            && anno.is_nullable == real.is_nullable)
                        {
                            eprintln!("fields expect to be {anno:#?}, found to be {real:#?}.");
                            panic!()
                        }
                    })
                    .count();
                columns
                    .iter()
                    .zip(res.df_recordbatch.columns())
                    .map(|(anno, real)| {
                        if !(&anno.ty == real.data_type() && anno.len == real.len()) {
                            eprintln!(
                                "Unmatch type or length!Expect [{:#?}; {}], found [{:#?}; {}]",
                                anno.ty,
                                anno.len,
                                real.data_type(),
                                real.len()
                            );
                            panic!()
                        }
                    })
                    .count();
            }
            Predicate::ExecIsErr {
                reason: part_reason,
            } => {
                let rb = create_sample_recordbatch();
                let res = coprocessor::exec_coprocessor(&testcase.code, &rb);
                if let Err(res) = res {
                    println!(
                        "{}",
                        pretty_print_error_in_src(&testcase.code, &res, 1120, "<embedded>")
                    );
                    let (reason, _) = get_error_reason_loc(&res);
                    if !reason.contains(&part_reason) {
                        eprintln!(
                            "{}\nExecute error, expect \"{reason}\" in \"{res}\", but not found.",
                            testcase.code,
                            reason = style(reason).green(),
                            res = style(res).red()
                        );
                        panic!()
                    }
                } else {
                    eprintln!("{:#?}\nExpect Err(...), found Ok(...)", res);
                    panic!();
                }
            }
        }
        println!(" ... {}", style("ok✅").green());
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
    let pyast = parser::parse(python_source, parser::Mode::Interactive).unwrap();
    let copr = parse_copr(python_source);
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
    from greptime import vector, log2, prev, sqrt, datetime, pow, sum
    def calc_rv(close, open_time, time, interval):
        mask = (open_time < time) & (open_time > time - interval)
        close = close.filter(mask)

        avg_time_interval = (open_time[-1] - open_time[0])/(len(open_time)-1)
        ref = log2(close/prev(close))/log2(2.7)
        var = sum(pow(ref, 2)/(len(ref)-1))
        return sqrt(var/avg_time_interval)

    # how to get env var, maybe through closure?
    timepoint = open_time[-1]
    rv_7d = calc_rv(close, open_time, timepoint, datetime("7d"))
    rv_15d = calc_rv(close, open_time, timepoint, datetime("15d"))
    rv_30d = calc_rv(close, open_time, timepoint, datetime("30d"))
    rv_60d = calc_rv(close, open_time, timepoint, datetime("60d"))
    rv_90d = calc_rv(close, open_time, timepoint, datetime("90d"))
    rv_180d = calc_rv(close, open_time, timepoint, datetime("180d"))
    return rv_7d, rv_15d, rv_30d, rv_60d, rv_90d, rv_180d
"#;
    let close_array = PrimitiveArray::from_slice([10106.79f32, 10106.09, 10108.73, 10106.38, 10106.95, 10107.55,
        10104.68, 10108.8 , 10115.96, 10117.08, 10120.43]);
    let open_time_array = PrimitiveArray::from_slice([1581231300i64, 1581231360, 1581231420, 1581231480, 1581231540,
        1581231600, 1581231660, 1581231720, 1581231780, 1581231840,
        1581231900]);
    let schema = Arc::new(Schema::from(vec![
        Field::new("close", DataType::Float32, false),
        Field::new("open_time", DataType::Int64, false),
    ]));
    let rb =
        DfRecordBatch::try_new(schema, vec![Arc::new(close_array), Arc::new(open_time_array)]).unwrap();
    let ret = coprocessor::exec_coprocessor(python_source, &rb);
    if let Err(Error::PyParse {
        backtrace: _,
        source,
    }) = ret
    {
        let res = visualize_loc(
            python_source,
            &source.location,
            "unknown tokens",
            source.error.to_string().as_str(),
            0,
            "copr.py",
        );
        println!("{res}");
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
    return (0.5 < cpu) & (cpu < 0.75)
"#;
    let cpu_array = PrimitiveArray::from_slice([0.9f32, 0.8, 0.7, 0.3]);
    let mem_array = PrimitiveArray::from_slice([0.1f64, 0.2, 0.3, 0.4]);
    let schema = Arc::new(Schema::from(vec![
        Field::new("cpu", DataType::Float32, false),
        Field::new("mem", DataType::Float64, false),
    ]));
    let rb =
        DfRecordBatch::try_new(schema, vec![Arc::new(cpu_array), Arc::new(mem_array)]).unwrap();
    let ret = coprocessor::exec_coprocessor(python_source, &rb);
    if let Err(Error::PyParse {
        backtrace: _,
        source,
    }) = ret
    {
        let res = visualize_loc(
            python_source,
            &source.location,
            "unknown tokens",
            source.error.to_string().as_str(),
            0,
            "copr.py",
        );
        println!("{res}");
    } else if let Ok(res) = ret {
        dbg!(&res);
    } else {
        dbg!(ret);
    }
}
