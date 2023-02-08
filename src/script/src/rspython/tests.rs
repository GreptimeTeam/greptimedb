use std::sync::Arc;

use common_telemetry::info;
use datatypes::prelude::Value;
use datatypes::value::{self, OrderedFloat};
use datatypes::vectors::{Float32Vector, Int32Vector, NullVector, VectorRef};
use rustpython_vm::builtins::PyList;
use rustpython_vm::class::PyClassImpl;
use rustpython_vm::protocol::PySequence;
use rustpython_vm::{AsObject, PyObjectRef, PyPayload, PyRef, PyResult, VirtualMachine};

use crate::ffi_types::vector::{is_pyobj_scalar, val_to_pyobj, PyVector};
use crate::rspython::vector_impl::pyobj_try_to_typed_val;
type PredicateFn = Option<fn(PyResult<PyObjectRef>, &VirtualMachine) -> bool>;
/// test the paired `val_to_obj` and `pyobj_to_val` func
#[test]
fn test_val2pyobj2val() {
    rustpython_vm::Interpreter::without_stdlib(Default::default()).enter(|vm| {
        let i = value::Value::Float32(OrderedFloat(2.0));
        let j = value::Value::Int32(1);
        let dtype = i.data_type();
        let obj = val_to_pyobj(i, vm);
        assert!(is_pyobj_scalar(&obj, vm));
        let obj_1 = obj.clone();
        let obj_2 = obj.clone();
        let ri = pyobj_try_to_typed_val(obj, vm, Some(dtype));
        let rj = pyobj_try_to_typed_val(obj_1, vm, Some(j.data_type()));
        let rn = pyobj_try_to_typed_val(obj_2, vm, None);
        assert_eq!(rj, None);
        assert_eq!(rn, Some(value::Value::Float64(OrderedFloat(2.0))));
        assert_eq!(ri, Some(value::Value::Float32(OrderedFloat(2.0))));
        let typed_lst = {
            [
                Value::Null,
                Value::Boolean(true),
                Value::Boolean(false),
                // PyInt is Big Int
                Value::Int16(2),
                Value::Int32(2),
                Value::Int64(2),
                Value::UInt16(2),
                Value::UInt32(2),
                Value::UInt64(2),
                Value::Float32(OrderedFloat(2.0)),
                Value::Float64(OrderedFloat(2.0)),
                Value::String("123".into()),
                // TODO(discord9): test Bytes and Date/DateTime
            ]
        };
        for val in typed_lst {
            let obj = val_to_pyobj(val.clone(), vm);
            let ret = pyobj_try_to_typed_val(obj, vm, Some(val.data_type()));
            assert_eq!(ret, Some(val));
        }
    })
}

#[test]
fn test_getitem_by_index_in_vm() {
    rustpython_vm::Interpreter::without_stdlib(Default::default()).enter(|vm| {
        PyVector::make_class(&vm.ctx);
        let a: VectorRef = Arc::new(Int32Vector::from_vec(vec![1, 2, 3, 4]));
        let a = PyVector::from(a);
        assert_eq!(
            1,
            a.getitem_by_index(0, vm)
                .map(|v| v.try_into_value::<i32>(vm).unwrap_or(0))
                .unwrap_or(0)
        );
        assert!(a.getitem_by_index(4, vm).ok().is_none());
        assert_eq!(
            4,
            a.getitem_by_index(-1, vm)
                .map(|v| v.try_into_value::<i32>(vm).unwrap_or(0))
                .unwrap_or(0)
        );
        assert!(a.getitem_by_index(-5, vm).ok().is_none());

        let a: VectorRef = Arc::new(NullVector::new(42));
        let a = PyVector::from(a);
        let a = a.into_pyobject(vm);
        assert!(PySequence::find_methods(&a, vm).is_some());
        assert!(PySequence::try_protocol(&a, vm).is_ok());
    })
}

pub fn execute_script(
    interpreter: &rustpython_vm::Interpreter,
    script: &str,
    test_vec: Option<PyVector>,
    predicate: PredicateFn,
) -> Result<(PyObjectRef, Option<bool>), PyRef<rustpython_vm::builtins::PyBaseException>> {
    let mut pred_res = None;
    interpreter
        .enter(|vm| {
            PyVector::make_class(&vm.ctx);
            let scope = vm.new_scope_with_builtins();
            let a: VectorRef = Arc::new(Int32Vector::from_vec(vec![1, 2, 3, 4]));
            let a = PyVector::from(a);
            let b: VectorRef = Arc::new(Float32Vector::from_vec(vec![1.2, 2.0, 3.4, 4.5]));
            let b = PyVector::from(b);
            scope
                .locals
                .as_object()
                .set_item("a", vm.new_pyobj(a), vm)
                .expect("failed");

            scope
                .locals
                .as_object()
                .set_item("b", vm.new_pyobj(b), vm)
                .expect("failed");

            if let Some(v) = test_vec {
                scope
                    .locals
                    .as_object()
                    .set_item("test_vec", vm.new_pyobj(v), vm)
                    .expect("failed");
            }

            let code_obj = vm
                .compile(
                    script,
                    rustpython_compiler_core::Mode::BlockExpr,
                    "<embedded>".to_owned(),
                )
                .map_err(|err| vm.new_syntax_error(&err))?;
            let ret = vm.run_code_obj(code_obj, scope);
            pred_res = predicate.map(|f| f(ret.clone(), vm));
            ret
        })
        .map(|r| (r, pred_res))
}

#[test]
// for debug purpose, also this is already a test function so allow print_stdout shouldn't be a problem?
fn test_execute_script() {
    common_telemetry::init_default_ut_logging();

    fn is_eq<T: std::cmp::PartialEq + rustpython_vm::TryFromObject>(
        v: PyResult,
        i: T,
        vm: &VirtualMachine,
    ) -> bool {
        v.and_then(|v| v.try_into_value::<T>(vm))
            .map(|v| v == i)
            .unwrap_or(false)
    }

    let snippet: Vec<(&str, PredicateFn)> = vec![
        ("1", Some(|v, vm| is_eq(v, 1i32, vm))),
        ("len(a)", Some(|v, vm| is_eq(v, 4i32, vm))),
        ("a[-1]", Some(|v, vm| is_eq(v, 4i32, vm))),
        ("a[0]*5", Some(|v, vm| is_eq(v, 5i32, vm))),
        (
            "list(a)",
            Some(|v, vm| {
                v.map_or(false, |obj| {
                    obj.is_instance(PyList::class(vm).into(), vm)
                        .unwrap_or(false)
                })
            }),
        ),
        (
            "len(a[1:-1])#elem in [1,3)",
            Some(|v, vm| is_eq(v, 2i64, vm)),
        ),
        ("(a+1)[0]", Some(|v, vm| is_eq(v, 2i32, vm))),
        ("(a-1)[0]", Some(|v, vm| is_eq(v, 0i32, vm))),
        ("(a*2)[0]", Some(|v, vm| is_eq(v, 2i64, vm))),
        ("(a/2.0)[2]", Some(|v, vm| is_eq(v, 1.5f64, vm))),
        ("(a/2)[2]", Some(|v, vm| is_eq(v, 1.5f64, vm))),
        ("(a//2)[2]", Some(|v, vm| is_eq(v, 1i32, vm))),
        ("(2-a)[0]", Some(|v, vm| is_eq(v, 1i32, vm))),
        ("(3/a)[1]", Some(|v, vm| is_eq(v, 1.5, vm))),
        ("(3//a)[1]", Some(|v, vm| is_eq(v, 1, vm))),
        ("(3/a)[2]", Some(|v, vm| is_eq(v, 1.0, vm))),
        (
            "(a+1)[0] + (a-1)[0] * (a/2.0)[2]",
            Some(|v, vm| is_eq(v, 2.0, vm)),
        ),
    ];

    let interpreter = rustpython_vm::Interpreter::without_stdlib(Default::default());
    for (code, pred) in snippet {
        let result = execute_script(&interpreter, code, None, pred);

        info!(
            "\u{001B}[35m{code}\u{001B}[0m: {:?}{}",
            result.clone().map(|v| v.0),
            result
                .clone()
                .map(|v| if let Some(v) = v.1 {
                    if v {
                        "\u{001B}[32m...[ok]\u{001B}[0m".to_string()
                    } else {
                        "\u{001B}[31m...[failed]\u{001B}[0m".to_string()
                    }
                } else {
                    "\u{001B}[36m...[unapplicable]\u{001B}[0m".to_string()
                })
                .unwrap()
        );

        if let Ok(p) = result {
            if let Some(v) = p.1 {
                if !v {
                    panic!("{code}: {:?}\u{001B}[12m...[failed]\u{001B}[0m", p.0)
                }
            }
        } else {
            panic!("{code}: {result:?}")
        }
    }
}
