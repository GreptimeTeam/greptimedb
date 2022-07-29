#![allow(clippy::print_stdout)]
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

use super::error::{get_error_reason, visualize_loc};
use super::*;
use crate::scalars::python::{copr_parse::parse_copr, coprocessor::Coprocessor, error::Error};

#[derive(Serialize, Deserialize, Debug)]
struct TestCase {
    name: String,
    code: String,
    predicate: Predicate,
}

#[derive(Serialize, Deserialize, Debug)]
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
        print!("Testing {}", testcase.name);
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
                    panic!("Expect to be err, found{copr:#?}")
                }
                let res = get_error_reason(&copr.unwrap_err());
                if !res.contains(&reason) {
                    println!("{}", testcase.code);
                    panic!("Parse Error, expect \"{reason}\" in \"{res}\", but not found.")
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
                            panic!("fields expect to be {anno:#?}, found to be {real:#?}.");
                        }
                    })
                    .count();
                columns
                    .iter()
                    .zip(res.columns())
                    .map(|(anno, real)| {
                        if !(&anno.ty == real.data_type() && anno.len == real.len()) {
                            panic!(
                                "Unmatch type or length!Expect [{:#?}; {}], found [{:#?}; {}]",
                                anno.ty,
                                anno.len,
                                real.data_type(),
                                real.len()
                            )
                        }
                    })
                    .count();
            }
            Predicate::ExecIsErr { reason } => {
                let rb = create_sample_recordbatch();
                let res = exec_coprocessor(&testcase.code, &rb);
                let res = get_error_reason(&res.unwrap_err());
                if !res.contains(&reason) {
                    println!("{}", testcase.code);
                    panic!("Execute error, expect \"{reason}\" in \"{res}\", but not found.")
                }
            }
        }
        println!(" ... {}", style("ok").green());
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
    //dbg!(pyast);
    let copr = parse_copr(python_source);
    // dbg!(&copr);
    //assert!(copr.is_ok());
}

#[test]
#[allow(clippy::print_stdout)]
// allow print in test function for debug purpose
fn test_coprocessor() {
    let python_source = r#"
@copr(args=["cpu", "mem"], returns=["perf", "what"])
def a(cpu: vector[f32], mem: vector[f64])->(vector[f64|None], 
    vector[f32]):
    abc = cpu
    abc *= 2
    return abc, cpu - mem
"#;
    //println!("{}, {:?}", python_source, python_ast);
    let cpu_array = PrimitiveArray::from_slice([0.9f32, 0.8, 0.7, 0.6]);
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
    }
}
