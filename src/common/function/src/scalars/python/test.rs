use std::sync::Arc;

use arrow::array::PrimitiveArray;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::record_batch::RecordBatch as DfRecordBatch;
use rustpython_parser::parser;

use super::*;
use crate::scalars::python::{
    copr_parse::parse_copr,
    coprocessor::Coprocessor,
    error::{InnerError, Result},
};
type PredicateFn = Option<fn(Result<Coprocessor>) -> bool>;
type ExecResPredicateFn = Option<fn(Result<DfRecordBatch>)>;

#[test]
fn testsuite_parse() {
    let testcases: Vec<(&'static str, PredicateFn)> = vec![
        (
            // for correct parse with all possible type annotation
            r#"
@copr(args=["cpu", "mem"], returns=["perf", "what", "how", "why", "whatever", "nihilism"])
def a(cpu: vector[f32], mem: vector[f64])->(vector[into(f64)|None], vector[into(f64)], vector[f64], vector[f64 | None], vector[_], vector[_ | None]):
    return cpu + mem, cpu - mem, cpu * mem, cpu / mem, cpu, mem
"#,
            Some(|r| {
                //dbg!(&r);
                r.is_ok()
                    && r.unwrap()
                        == Coprocessor {
                            name: "a".into(),
                            args: vec!["cpu".into(), "mem".into()],
                            returns: vec![
                                "perf".into(),
                                "what".into(),
                                "how".into(),
                                "why".into(),
                                "whatever".into(),
                                "nihilism".into(),
                            ],
                            arg_types: vec![
                                Some(AnnotationInfo {
                                    datatype: Some(DataType::Float32),
                                    is_nullable: false,
                                    coerce_into: false,
                                }),
                                Some(AnnotationInfo {
                                    datatype: Some(DataType::Float64),
                                    is_nullable: false,
                                    coerce_into: false,
                                }),
                            ],
                            return_types: vec![
                                Some(AnnotationInfo {
                                    datatype: Some(DataType::Float64),
                                    is_nullable: true,
                                    coerce_into: true,
                                }),
                                Some(AnnotationInfo {
                                    datatype: Some(DataType::Float64),
                                    is_nullable: false,
                                    coerce_into: true,
                                }),
                                Some(AnnotationInfo {
                                    datatype: Some(DataType::Float64),
                                    is_nullable: false,
                                    coerce_into: false,
                                }),
                                Some(AnnotationInfo {
                                    datatype: Some(DataType::Float64),
                                    is_nullable: true,
                                    coerce_into: false,
                                }),
                                Some(AnnotationInfo {
                                    datatype: None,
                                    is_nullable: false,
                                    coerce_into: false,
                                }),
                                Some(AnnotationInfo {
                                    datatype: None,
                                    is_nullable: true,
                                    coerce_into: false,
                                }),
                            ],
                        }
            }),
        ),
        (
            // missing decrator
            r#"
def a(cpu: vector[f32], mem: vector[f64])->(vector[into(f64)|None], vector[into(f64)], vector[f64], vector[f64 | None]):
    return cpu + mem, cpu - mem, cpu * mem, cpu / mem
"#,
            Some(|r| {
                //dbg!(&r);
                r.is_err()
                    && if let InnerError::CoprParse { reason, loc } = r.unwrap_err() {
                        reason.contains("Expect one decorator") && loc == None
                    } else {
                        false
                    }
            }),
        ),
        (
            // illegal list(not all is string)
            r#"
@copr(args=["cpu", 3], returns=["perf", "what", "how", "why"])
def a(cpu: vector[f32], mem: vector[f64])->(vector[into(f64)|None], vector[into(f64)], vector[f64], vector[f64 | None]):
    return cpu + mem, cpu - mem, cpu * mem, cpu / mem
"#,
            Some(|r| {
                r.is_err()
                    && if let InnerError::CoprParse { reason, loc } = r.unwrap_err() {
                        reason.contains("Expect a list of String, found ") && loc.is_some()
                    } else {
                        false
                    }
            }),
        ),
        (
            // not even a list
            r#"
@copr(args=42, returns=["perf", "what", "how", "why"])
def a(cpu: vector[f32], mem: vector[f64])->(vector[into(f64)|None], vector[into(f64)], vector[f64], vector[f64 | None]):
    return cpu + mem, cpu - mem, cpu * mem, cpu / mem
"#,
            Some(|r| {
                r.is_err()
                    && if let InnerError::CoprParse { reason, loc } = r.unwrap_err() {
                        reason.contains("Expect a list, found ") && loc.is_some()
                    } else {
                        false
                    }
            }),
        ),
        (
            // unknown type names
            r#"
@copr(args=["cpu", "mem"], returns=["perf", "what", "how", "why"])
def a(cpu: vector[g32], mem: vector[f64])->(vector[into(f64)|None], vector[into(f64)], vector[f64], vector[f64 | None]):
    return cpu + mem, cpu - mem, cpu * mem, cpu / mem
"#,
            Some(|r| {
                assert!(
                    r.is_err()
                        && if let InnerError::CoprParse { reason, loc: _ } = r.unwrap_err() {
                            reason.contains("Unknown datatype:")
                        } else {
                            false
                        }
                );
                true
            }),
        ),
        (
            // two type name
            r#"
@copr(args=["cpu", "mem"], returns=["perf", "what", "how", "why"])
def a(cpu: vector[f32 | f64], mem: vector[f64])->(vector[into(f64)|None], vector[into(f64)], vector[f64], vector[f64 | None]):
    return cpu + mem, cpu - mem, cpu * mem, cpu / mem
"#,
            Some(|r| {
                assert!(
                    r.is_err()
                        && if let InnerError::Other { reason } = r.unwrap_err() {
                            reason.contains("Expect one typenames(or `into(<typename>)`) and one `None`, not two type names")
                        } else {
                            false
                        }
                );
                true
            }),
        ),
        (
            // two `None`
            r#"
@copr(args=["cpu", "mem"], returns=["perf", "what", "how", "why"])
def a(cpu: vector[None | None], mem: vector[f64])->(vector[into(f64)|None], vector[into(f64)], vector[f64], vector[f64 | None]):
    return cpu + mem, cpu - mem, cpu * mem, cpu / mem
"#,
            Some(|r| {
                assert!(
                    r.is_err()
                        && if let InnerError::Other { reason } = r.unwrap_err() {
                            reason.contains("Expect a type name, not two `None`")
                        } else {
                            false
                        }
                );
                true
            }),
        ),
        (
            // Expect a Types name
            r#"
@copr(args=["cpu", "mem"], returns=["perf", "what", "how", "why"])
def a(cpu: vector[into(f64|None)], mem: vector[f64])->(vector[into(f64)|None], vector[into(f64)], vector[f64], vector[f64 | None]):
    return cpu + mem, cpu - mem, cpu * mem, cpu / mem
"#,
            Some(|r| {
                r.is_err()
                    && if let InnerError::CoprParse { reason, loc } = r.unwrap_err() {
                        reason.contains("Expect a type's name, found ") && loc.is_some()
                    } else {
                        false
                    }
            }),
        ),
        (
            // not into
            r#"
@copr(args=["cpu", "mem"], returns=["perf", "what", "how", "why"])
def a(cpu: vector[cast(f64)], mem: vector[f64])->(vector[into(f64)|None], vector[into(f64)], vector[f64], vector[f64 | None]):
    return cpu + mem, cpu - mem, cpu * mem, cpu / mem
"#,
            Some(|r| {
                r.is_err()
                    && if let InnerError::CoprParse { reason, loc } = r.unwrap_err() {
                        reason
                            .contains("Expect only `into(datatype)` or datatype or `None`, found ")
                            && loc.is_some()
                    } else {
                        false
                    }
            }),
        ),
        (
            // Expect one arg in into
            r#"
@copr(args=["cpu", "mem"], returns=["perf", "what", "how", "why"])
def a(cpu: vector[into(f64, f32)], mem: vector[f64])->(vector[into(f64)|None], vector[into(f64)], vector[f64], vector[f64 | None]):
    return cpu + mem, cpu - mem, cpu * mem, cpu / mem
"#,
            Some(|r| {
                r.is_err()
                    && if let InnerError::CoprParse { reason, loc } = r.unwrap_err() {
                        reason.contains("Expect only one arguement for `into`") && loc.is_some()
                    } else {
                        false
                    }
            }),
        ),
        (
            // Expect `vector` not `vec`
            r#"
@copr(args=["cpu", "mem"], returns=["perf", "what", "how", "why"])
def a(cpu: vec[into(f64)], mem: vector[f64])->(vector[into(f64)|None], vector[into(f64)], vector[f64], vector[f64 | None]):
    return cpu + mem, cpu - mem, cpu * mem, cpu / mem
"#,
            Some(|r| {
                r.is_err()
                    && if let InnerError::CoprParse { reason, loc } = r.unwrap_err() {
                        reason.contains("Wrong type annotation, expect `vector[...]`, found")
                            && loc.is_some()
                    } else {
                        false
                    }
            }),
        ),
        (
            // Expect `None`
            r#"
@copr(args=["cpu", "mem"], returns=["perf", "what", "how", "why"])
def a(cpu: vector[into(f64)|1], mem: vector[f64])->(vector[into(f64)|None], vector[into(f64)], vector[f64], vector[f64 | None]):
    return cpu + mem, cpu - mem, cpu * mem, cpu / mem
"#,
            Some(|r| {
                assert!(
                    r.is_err()
                        && if let InnerError::CoprParse { reason, loc } = r.unwrap_err() {
                            reason.contains("Expect only typenames and `None`, found")
                                && loc.is_some()
                        } else {
                            false
                        }
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
                    r.is_err()
                        && if let InnerError::CoprParse { reason, loc } = r.unwrap_err() {
                            reason.contains("Expect one and only one python function with `@coprocessor` or `@cpor` decorator")
                                    && loc.is_some()
                        } else {
                            false
                        }
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
                        && if let InnerError::CoprParse { reason, loc } = r.unwrap_err() {
                            reason.contains(
                                "Expect decorator with name `copr` or `coprocessor`, found",
                            ) && loc.is_some()
                        } else {
                            false
                        }
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
                        && if let InnerError::CoprParse { reason, loc } = r.unwrap_err() {
                            reason.contains("Expect two keyword argument of `args` and `returns`")
                                && loc.is_some()
                        } else {
                            false
                        }
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
def a(cpu: vector[f32], mem: vector[f64])->(vector[into(f64)|None], vector[into(f64)], vector[f64], vector[f64 | None], vector[bool], vector[_ | None]):
    return cpu + mem, cpu - mem, cpu * mem, cpu / mem, cpu, mem
"#,
            Some(|r| {
                // dbg!(&r);
                // assert in here seems to be more readable
                assert!(
                    r.is_err()
                        && if let InnerError::Other { reason } = r.unwrap_err() {
                            reason.contains("Anntation type is ")
                        } else {
                            false
                        }
                );
            }),
        ),
        (
            // cast to bool
            r#"
@copr(args=["cpu", "mem"], returns=["perf", "what", "how", "why", "whatever", "nihilism"])
def a(cpu: vector[f32], mem: vector[f64])->(vector[into(f64)|None], vector[into(f64)], vector[f64], vector[f64 | None], vector[into(bool)], vector[_ | None]):
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
def a(cpu: vector[_], mem: vector[f64])->(vector[into(f64)|None], vector[into(f64)], vector[_], vector[ _ | None]):
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
    vector[into(f32)]):
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
    // dbg!(&ret);
    assert!(ret.is_ok());
    assert_eq!(ret.unwrap().column(0).len(), 4);
}
