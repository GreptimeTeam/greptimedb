use std::sync::Arc;

use arrow::array::PrimitiveArray;
use rustpython_vm::class::PyClassImpl;

use super::*;
use crate::python::utils::format_py_error;
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
        let col = try_into_columnar_value(list_obj, vm);
        if let Err(err) = col {
            let reason = format_py_error(err, vm);
            assert!(format!("{}", reason).contains(
                "TypeError: All elements in a list should be same type to cast to Datafusion list!"
            ));
        }

        let list: PyVector = PyVector::from(
            HelperVec::try_into_vector(
                Arc::new(PrimitiveArray::from_slice([0.1f64, 0.2, 0.3, 0.4])) as ArrayRef,
            )
            .unwrap(),
        );
        let nested_list: Vec<PyObjectRef> = vec![list.into_pyobject(vm), vm.ctx.new_int(3).into()];
        let list_obj = vm.ctx.new_list(nested_list).into();
        let expect_err = try_into_columnar_value(list_obj, vm);
        assert!(expect_err.is_err());
    })
}
