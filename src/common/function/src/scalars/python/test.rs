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

    }
}

#[derive(Serialize, Deserialize, Debug)]
struct ColumnInfo {
    pub ty: DataType,
    pub len: usize,
}

type PredicateFn = Option<fn(Result<Coprocessor>) -> bool>;
type ExecResPredicateFn = Option<fn(Result<DfRecordBatch>)>;

#[test]
fn run_ron_testcases() {
    let loc = Path::new("src/scalars/python/copr_testcases/testcases.ron");
    let mut file =
        File::open(loc.to_str().expect("Fail to parse path")).expect("Fail to open file");
    let mut buf = String::new();
    file.read_to_string(&mut buf)
        .expect("Fail to read to string");
    let testcases: Vec<TestCase> = from_ron_string(&buf).expect("Fail to convert to testcases");
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
                    panic!("Expect \"{reason}\" in \"{res}\", but not found.")
                }
            }
            Predicate::ExecIsOk { fields, columns } => {
                let cpu_array = PrimitiveArray::from_slice([0.9f32, 0.8, 0.7, 0.6]);
                let mem_array = PrimitiveArray::from_slice([0.1f64, 0.2, 0.3, 0.4]);
                let schema = Arc::new(Schema::from(vec![
                    Field::new("cpu", DataType::Float32, false),
                    Field::new("mem", DataType::Float64, false),
                ]));
                let rb =
                    DfRecordBatch::try_new(schema, vec![Arc::new(cpu_array), Arc::new(mem_array)])
                        .unwrap();
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
            _ => todo!(),
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
        _ => {
            unimplemented!()
        }
    }
}

#[test]
fn testsuite_parse() {
    let correct_script = r#"
@copr(args=["cpu", "mem"], returns=["perf", "what", "how", "why"])
def a(cpu: vector[f32], mem: vector[f64])->(vector[f64], vector[f64|None], vector[_], vector[_ | None]):
    return cpu + mem, cpu - mem, cpu * mem, cpu / mem
"#;
    let testcases: Vec<(&'static str, PredicateFn)> = vec![
        (
            // for correct parse with all possible type annotation
            correct_script,
            Some(|r| {
                r.is_ok()
                    && r.unwrap()
                        == Coprocessor {
                            name: "a".into(),
                            args: vec!["cpu".into(), "mem".into()],
                            returns: vec!["perf".into(), "what".into(), "how".into(), "why".into()],
                            arg_types: vec![
                                Some(AnnotationInfo {
                                    datatype: Some(DataType::Float32),
                                    is_nullable: false,
                                }),
                                Some(AnnotationInfo {
                                    datatype: Some(DataType::Float64),
                                    is_nullable: false,
                                }),
                            ],
                            return_types: vec![
                                Some(AnnotationInfo {
                                    datatype: Some(DataType::Float64),
                                    is_nullable: false,
                                }),
                                Some(AnnotationInfo {
                                    datatype: Some(DataType::Float64),
                                    is_nullable: true,
                                }),
                                Some(AnnotationInfo {
                                    datatype: None,
                                    is_nullable: false,
                                }),
                                Some(AnnotationInfo {
                                    datatype: None,
                                    is_nullable: true,
                                }),
                            ],
                            script: r#"
@copr(args=["cpu", "mem"], returns=["perf", "what", "how", "why"])
def a(cpu: vector[f32], mem: vector[f64])->(vector[f64], vector[f64|None], vector[_], vector[_ | None]):
    return cpu + mem, cpu - mem, cpu * mem, cpu / mem
"#.to_owned()
                        }
            }),
        ),
        (
            // missing decrator
            r#"
def a(cpu: vector[f32], mem: vector[f64])->(vector[f64], vector[f64|None], vector[_], vector[_ | None]):
    return cpu + mem, cpu - mem, cpu * mem, cpu / mem
"#,
            Some(|r| {
                //dbg!(&r);
                r.is_err() && get_error_reason(&r.unwrap_err()).contains("Expect one decorator")
            }),
        ),
        (
            // illegal list(not all is string)
            r#"
@copr(args=["cpu", 3], returns=["perf", "what", "how", "why"])
def a(cpu: vector[f32], mem: vector[f64])->(vector[f64], vector[f64|None], vector[_], vector[_ | None]):
    return cpu + mem, cpu - mem, cpu * mem, cpu / mem
"#,
            Some(|r| {
                r.is_err()
                    && get_error_reason(&r.unwrap_err()).contains("Expect a list of String, found ")
            }),
        ),
        (
            // not even a list
            r#"
@copr(args=42, returns=["perf", "what", "how", "why"])
def a(cpu: vector[f32], mem: vector[f64])->(vector[f64], vector[f64|None], vector[_], vector[_ | None]):
    return cpu + mem, cpu - mem, cpu * mem, cpu / mem
"#,
            Some(|r| {
                if r.is_err() {
                    let err = if let Err(r) = r { r } else { unreachable!() };
                    if !get_error_reason(&err).contains("Expect a list, found ") {
                        dbg!(err);
                        // don't
                        panic!();
                    }
                } else {
                    panic!("Expect error, found {r:#?}");
                }
                true
            }),
        ),
        (
            // unknown type names
            r#"
@copr(args=["cpu", "mem"], returns=["perf", "what", "how", "why"])
def a(cpu: vector[g32], mem: vector[f64])->(vector[f64], vector[f64|None], vector[_], vector[_ | None]):
    return cpu + mem, cpu - mem, cpu * mem, cpu / mem
"#,
            Some(|r| {
                assert!(
                    r.is_err() && get_error_reason(&r.unwrap_err()).contains("Unknown datatype:")
                );
                true
            }),
        ),
        (
            // two type name
            r#"
@copr(args=["cpu", "mem"], returns=["perf", "what", "how", "why"])
def a(cpu: vector[f32 | f64], mem: vector[f64])->(vector[f64], vector[f64|None], vector[_], vector[_ | None]):
    return cpu + mem, cpu - mem, cpu * mem, cpu / mem
"#,
            Some(|r| {
                assert!(
                    r.is_err() && get_error_reason(&r.unwrap_err()).contains("not two type names")
                );
                true
            }),
        ),
        (
            // two `None`
            r#"
@copr(args=["cpu", "mem"], returns=["perf", "what", "how", "why"])
def a(cpu: vector[None | None], mem: vector[f64])->(vector[f64], vector[None|None], vector[_], vector[_ | None]):
    return cpu + mem, cpu - mem, cpu * mem, cpu / mem
"#,
            Some(|r| {
                assert!(
                    r.is_err()
                        && get_error_reason(&r.unwrap_err())
                            .contains("Expect a type name, not two `None`")
                );
                true
            }),
        ),
        (
            // Expect a Types name
            r#"
@copr(args=["cpu", "mem"], returns=["perf", "what", "how", "why"])
def a(cpu: vector[f64|None], mem: vector[f64])->(vector[g64], vector[f64|None], vector[_], vector[_ | None]):
    return cpu + mem, cpu - mem, cpu * mem, cpu / mem
"#,
            Some(|r| {
                assert!(
                    r.is_err() && get_error_reason(&r.unwrap_err()).contains("Unknown datatype:")
                );
                true
            }),
        ),
        (
            // no more `into`
            r#"
@copr(args=["cpu", "mem"], returns=["perf", "what", "how", "why"])
def a(cpu: vector[cast(f64)], mem: vector[f64])->(vector[f64], vector[f64|None], vector[_], vector[_ | None]):
    return cpu + mem, cpu - mem, cpu * mem, cpu / mem
"#,
            Some(|r| {
                assert!(
                    r.is_err()
                        && get_error_reason(&r.unwrap_err()).contains("Expect types' name, found")
                );
                true
            }),
        ),
        (
            // Expect `vector` not `vec`
            r#"
@copr(args=["cpu", "mem"], returns=["perf", "what", "how", "why"])
def a(cpu: vec[f64], mem: vector[f64])->(vector[f64|None], vector[f64], vector[_], vector[_ | None]):
    return cpu + mem, cpu - mem, cpu * mem, cpu / mem
"#,
            Some(|r| {
                assert!(
                    r.is_err()
                        && get_error_reason(&r.unwrap_err())
                            .contains("Wrong type annotation, expect `vector[...]`, found")
                );
                true
            }),
        ),
        (
            // Expect `None`
            r#"
@copr(args=["cpu", "mem"], returns=["perf", "what", "how", "why"])
def a(cpu: vector[f64|1], mem: vector[f64])->(vector[f64|None], vector[f64], vector[_], vector[_ | None]):
    return cpu + mem, cpu - mem, cpu * mem, cpu / mem
"#,
            Some(|r| {
                assert!(
                    r.is_err()
                        && get_error_reason(&r.unwrap_err())
                            .contains("Expect only typenames and `None`, found")
                );
                true
            }),
        ),
        (
            // more than one statement
            r#"
print("hello world")
@copr(args=["cpu", "mem"], returns=["perf", "what", "how", "why"])
def a(cpu: vector[f64], mem: vector[f64])->(vector[None|None], vector[into(f64)], vector[f64], vector[f64 | None]):
    return cpu + mem, cpu - mem, cpu * mem, cpu / mem
"#,
            Some(|r| {
                // assert in here seems to be more readable
                assert!(
                    r.is_err() && get_error_reason(&r.unwrap_err()).contains("Expect one and only one python function with `@coprocessor` or `@cpor` decorator")
                );
                true
            }),
        ),
        (
            // wrong decorator name
            r#"
@corp(args=["cpu", "mem"], returns=["perf", "what", "how", "why"])
def a(cpu: vector[f64], mem: vector[f64])->(vector[None|None], vector[into(f64)], vector[f64], vector[f64 | None]):
    return cpu + mem, cpu - mem, cpu * mem, cpu / mem
"#,
            Some(|r| {
                // assert in here seems to be more readable
                assert!(
                    r.is_err()
                        && get_error_reason(&r.unwrap_err())
                            .contains("Expect decorator with name `copr` or `coprocessor`, found",)
                );
                true
            }),
        ),
        (
            // not enough keyword arguements
            r#"
@copr(args=["cpu", "mem"])
def a(cpu: vector[f64], mem: vector[f64])->(vector[f64|None], vector[into(f64)], vector[f64], vector[f64 | None]):
    return cpu + mem, cpu - mem, cpu * mem, cpu / mem
"#,
            Some(|r| {
                // assert in here seems to be more readable
                assert!(
                    r.is_err()
                        && get_error_reason(&r.unwrap_err())
                            .contains("Expect two keyword argument of `args` and `returns`")
                );
                true
            }),
        ),
        // ... More `Other` errors
    ];

    for (script, predicate) in testcases {
        let copr = parse_copr(script);
        if let Some(predicate) = predicate {
            if !predicate(copr) {
                panic!("Error on {script}");
            }
        }
    }
}

#[test]
fn test_all_types_error() {
    let test_cases: Vec<(&'static str, ExecResPredicateFn)> = vec![
        (
            // cast errors
            r#"
@copr(args=["cpu", "mem"], returns=["perf", "what", "how", "why", "whatever", "nihilism"])
def a(cpu: vector[f32], mem: vector[f64])->(vector[f64|None], vector[f64], vector[f64], vector[f64 | None], vector[bool], vector[_ | None]):
    return cpu + mem, cpu - mem, cpu * mem, cpu / mem, cpu, mem
"#,
            Some(|r| {
                // assert in here seems to be more readable
                assert!(r.is_ok());
            }),
        ),
        (
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
        ),
    ];
    let cpu_array = PrimitiveArray::from_slice([0.9f32, 0.8, 0.7, 0.6]);
    let mem_array = PrimitiveArray::from_slice([0.1f64, 0.2, 0.3, 0.4]);
    let schema = Arc::new(Schema::from(vec![
        Field::new("cpu", DataType::Float32, false),
        Field::new("mem", DataType::Float64, false),
    ]));
    let rb =
        DfRecordBatch::try_new(schema, vec![Arc::new(cpu_array), Arc::new(mem_array)]).unwrap();

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
    return cpu + mem, cpu - mem
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
    assert!(ret.is_ok());
    assert_eq!(ret.unwrap().column(0).len(), 4);
}
