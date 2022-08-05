use rustpython_vm::class::PyClassImpl;

use super::*;
#[test]
fn convert_scalar_to_py_obj_and_back() {
    rustpython_vm::Interpreter::with_init(Default::default(), |vm| {
        // this can be in `.enter()` closure, but for clearity, put it in the `with_init()`
        PyVector::make_class(&vm.ctx);
    })
    .enter(|vm| {
        let col = DFColValue::Scalar(ScalarValue::Float64(Some(1.0)));
        let to = try_into_py_obj(col, vm).unwrap();
        let back = try_into_columnar_value(to, vm).unwrap();
        if let DFColValue::Scalar(ScalarValue::Float64(Some(v))) = back {
            if (v - 1.0).abs() > 2.0 * f64::EPSILON {
                panic!("Expect 1.0, found {v}")
            }
        } else {
            panic!("Convert errors, expect 1.0")
        }
        let col = DFColValue::Scalar(ScalarValue::Int64(Some(1)));
        let to = try_into_py_obj(col, vm).unwrap();
        let back = try_into_columnar_value(to, vm).unwrap();
        if let DFColValue::Scalar(ScalarValue::Int64(Some(v))) = back {
            assert_eq!(v, 1);
        } else {
            panic!("Convert errors, expect 1")
        }
        let col = DFColValue::Scalar(ScalarValue::UInt64(Some(1)));
        let to = try_into_py_obj(col, vm).unwrap();
        let back = try_into_columnar_value(to, vm).unwrap();
        if let DFColValue::Scalar(ScalarValue::Int64(Some(v))) = back {
            assert_eq!(v, 1);
        } else {
            panic!("Convert errors, expect 1")
        }
        let col = DFColValue::Scalar(ScalarValue::List(
            Some(Box::new(vec![
                ScalarValue::Int64(Some(1)),
                ScalarValue::Int64(Some(2)),
            ])),
            Box::new(DataType::Int64),
        ));
        let to = try_into_py_obj(col, vm).unwrap();
        let back = try_into_columnar_value(to, vm).unwrap();
        if let DFColValue::Scalar(ScalarValue::List(Some(list), ty)) = back {
            assert_eq!(list.len(), 2);
            assert_eq!(ty.as_ref(), &DataType::Int64);
        }
        let list: Vec<PyObjectRef> = vec![vm.ctx.new_int(1).into(), vm.ctx.new_int(2).into()];
        let nested_list: Vec<PyObjectRef> =
            vec![vm.ctx.new_list(list).into(), vm.ctx.new_int(3).into()];
        let list_obj = vm.ctx.new_list(nested_list).into();
        let col = try_into_columnar_value(list_obj, vm).unwrap();
        if let DFColValue::Scalar(ScalarValue::List(Some(list), _))= col{
            assert_eq!(list.len(), 2);
            if let ScalarValue::List(Some(inner_list), _) = &list[0]{
                assert_eq!(inner_list.len(), 2);
            }else{
                panic!("Expect a inner list!");
            }
        }else{
            panic!("Expect a list!");
        }
    })
}
