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
use super::*;
use crate::scalars::python::error::pretty_print_error_in_src;
use crate::scalars::python::{copr_parse::parse_copr, coprocessor::Coprocessor, error::Error};

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
    let loc = Path::new("src/scalars/python/copr_testcases/testcases.ron");
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
                let res = exec_coprocessor(&testcase.code, &rb);
                if res.is_err() {
                    dbg!(&res);
                }
                assert!(res.is_ok());
                let res = res.unwrap();
                fields
                    .iter()
                    .zip(&res.schema().fields)
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
                    .zip(res.columns())
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
                let res = exec_coprocessor(&testcase.code, &rb);
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
def a(cpu: vector[_], mem: vector[f64])->(vector[f64|None], vector[f64], vector[_], vector[ _ | None]):
    return cpu + mem, cpu - mem, cpu * mem, cpu / mem
"#;
    let pyast = parser::parse(python_source, parser::Mode::Interactive).unwrap();
    let copr = parse_copr(python_source);
}

#[test]
#[allow(clippy::print_stdout)]
// allow print in test function for debug purpose
fn test_coprocessor() {
    let python_source = r#"
@copr(args=["cpu", "mem"], returns=["perf", "what"])
def a(cpu: vector[f32], mem: vector[f64])->(vector[f64|None], 
    vector[bool]):
    abc = cpu
    abc *= 2
    return abc, cpu.__gt__(mem)
"#;
    let cpu_array = PrimitiveArray::from_slice([0.9f32, 0.8, 0.7, 0.3]);
    let mem_array = PrimitiveArray::from_slice([0.1f64, 0.2, 0.3, 0.4]);
    let schema = Arc::new(Schema::from(vec![
        Field::new("cpu", DataType::Float32, false),
        Field::new("mem", DataType::Float64, false),
    ]));
    let rb =
        DfRecordBatch::try_new(schema, vec![Arc::new(cpu_array), Arc::new(mem_array)]).unwrap();
    let ret = exec_coprocessor(python_source, &rb);
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
    }else if let Ok(res) = ret{
        dbg!(&res);
    }else{
        dbg!(ret);
    }
}
