use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use std::sync::Arc;

use arrow::array::PrimitiveArray;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::record_batch::RecordBatch as DfRecordBatch;
use ron::from_str as from_ron_string;
use rustpython_parser::parser;
use serde::{Deserialize, Serialize};

use super::*;
use crate::scalars::python::{
    copr_parse::parse_copr,
    coprocessor::Coprocessor,
    error::{Error, Result},
};

#[derive(Serialize, Deserialize, Debug)]
struct TestCase {
    code: String,
    predicate: Predicate,
}

#[derive(Serialize, Deserialize, Debug)]
enum Predicate {
    ParseIsOk {
        result: Coprocessor,
    },
    ParseIsErr {
        reason: String,
    },
    ExecIsOk {
        fields: Vec<AnnotationInfo>,
        columns: Vec<ColumnInfo>,
    },
    ExecIsErr {
        reason: String,
    },
}

#[derive(Serialize, Deserialize, Debug)]
struct ColumnInfo {
    pub ty: DataType,
    pub len: usize,
}

type ExecResPredicateFn = Option<fn(Result<DfRecordBatch>)>;

fn create_sample_recordbatch() -> DfRecordBatch {
    let cpu_array = PrimitiveArray::from_slice([0.9f32, 0.8, 0.7, 0.6]);
    let mem_array = PrimitiveArray::from_slice([0.1f64, 0.2, 0.3, 0.4]);
    let schema = Arc::new(Schema::from(vec![
        Field::new("cpu", DataType::Float32, false),
        Field::new("mem", DataType::Float64, false),
    ]));

    DfRecordBatch::try_new(schema, vec![Arc::new(cpu_array), Arc::new(mem_array)]).unwrap()
}

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
        match testcase.predicate {
            Predicate::ParseIsOk { result } => {
                let copr = parse_copr(&testcase.code);
                let mut copr = copr.unwrap();
                copr.script = "".into();
                assert_eq!(copr, result);
            }
            Predicate::ParseIsErr { reason } => {
                let copr = parse_copr(&testcase.code);
                assert!(copr.is_err());
                let res = get_error_reason(&copr.unwrap_err());
                if !res.contains(&reason) {
                    println!("{}", testcase.code);
                    panic!("Parse Error, expect \"{reason}\" in \"{res}\", but not found.")
                }
            }
            Predicate::ExecIsOk { fields, columns } => {
                let rb = create_sample_recordbatch();
                let res = exec_coprocessor(&testcase.code, &rb);
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
    }
}

fn get_error_reason(err: &Error) -> String {
    match err {
        Error::CoprParse {
            backtrace: _,
            reason,
            loc: _,
        } => reason.clone(),
        Error::Other {
            backtrace: _,
            reason,
        } => reason.clone(),
        Error::PyRuntime {
            backtrace: _,
            source,
        } => source.output.clone(),
        Error::PyParse { backtrace:_, source } => format!("{}", source.error),
        _ => {
            unimplemented!()
        }
    }
}

#[test]
fn test_all_types_error() {
    let test_cases: Vec<(&'static str, ExecResPredicateFn)> = vec![(
        // cast to bool
        r#"
@copr(args=["cpu", "mem"], returns=["perf", "what", "how", "why", "whatever", "nihilism"])
def a(cpu: vector[f32], mem: vector[f64])->(vector[f64|None], vector[f64], vector[f64], vector[f64 | None], vector[bool], vector[_ | None]):
    return cpu + mem, cpu - mem, cpu * mem, cpu / mem, cpu, mem
"#,
        Some(|r| {
            // assert in here seems to be more readable
            assert!(r.is_ok() && *r.unwrap().column(4).data_type() == DataType::Boolean);
        }),
    )];
    let rb = create_sample_recordbatch();

    for (script, predicate) in test_cases {
        if let Some(predicate) = predicate {
            predicate(exec_coprocessor(script, &rb));
        }
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
fn test_execute_script() {
    let python_source = "
def a(a: int,b: int)->int:
    return 1
a(2,3)
";
    let result = execute_script(python_source);
    assert!(result.is_ok());
}

#[test]
#[allow(clippy::print_stdout)]
// allow print in test function for debug purpose
fn test_coprocessor() {
    let python_source = r#"
@copr(args=["cpu", "mem"], returns=["perf", "what"])
def a(cpu: vector[f32], mem: vector[f64])->(vector[f64|None], 
    vector[f32]):
    return cpu + mem, cpu - mem***
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
    // println!("{}", ret.unwrap_err());
    dbg!(&ret);
    let ret = ret.unwrap_err();
    dbg!(get_error_reason(&ret));
    if let Error::PyParse {
        backtrace: _,
        source,
    } = ret
    {
        let res = pretty_print_loc_in_src(
            python_source, 
            &source.location, 
            format!("{}", source.error).as_str(),
            0,
            "copr.py"
        );
        println!("{res}");
    }
    // assert!(ret.is_ok());
    // assert_eq!(ret.unwrap().column(0).len(), 4);
}
