use arrow::bitmap::MutableBitmap;

use crate::data_type::ConcreteDataType;
use crate::error::Result;
use crate::scalars::ScalarVector;
use crate::scalars::ScalarVectorBuilder;
use crate::vectors::{
    BooleanVector, BooleanVectorBuilder, ListVector, NullVector, Vector, VectorRef,
};

macro_rules! with_match_scalar_vector {
    ($key: expr, $KeyType: ident, | $_: tt $T: ident | $body: tt, $nbody: tt) => {{
        macro_rules! __with_ty__ {
            ( $_ $T:ident ) => {
                $body
            };
        }

        use crate::vectors::all::*;
        match $key {
            $KeyType::Boolean(_) => __with_ty__! { BooleanVector },
            $KeyType::Int8(_) => __with_ty__! { Int8Vector },
            $KeyType::Int16(_) => __with_ty__! { Int16Vector },
            $KeyType::Int32(_) => __with_ty__! { Int32Vector },
            $KeyType::Int64(_) => __with_ty__! { Int64Vector },
            $KeyType::UInt8(_) => __with_ty__! { UInt8Vector },
            $KeyType::UInt16(_) => __with_ty__! { UInt16Vector },
            $KeyType::UInt32(_) => __with_ty__! { UInt32Vector },
            $KeyType::UInt64(_) => __with_ty__! { UInt64Vector },
            $KeyType::Float32(_) => __with_ty__! { Float32Vector },
            $KeyType::Float64(_) => __with_ty__! { Float64Vector },
            $KeyType::Binary(_) => __with_ty__! { BinaryVector },
            $KeyType::String(_) => __with_ty__! { StringVector },
            $KeyType::Date(_) => __with_ty__! { DateVector },
            $KeyType::DateTime(_) => __with_ty__! { DateTimeVector },
            _ => $nbody,
        }
    }};
}

// TODO(yingwen): Allow pass closure.
macro_rules! dispatch_vector_compute2 {
    ($vector: ident, | $_1: tt $ScalarVector: ident | $sbody: tt, | $_2: tt $NullVector: ident | $nbody: tt, | $_3: tt $ListVector: ident | $lbody: tt) => {{
        use $crate::data_type::ConcreteDataType;
        use $crate::vectors::{all::*, Vector};

        macro_rules! __with_ty_s__ {
            ( $_1 $ScalarVector: ident ) => {
                $sbody
            };
        }
        macro_rules! __with_ty_n__ {
            ( $_2 $NullVector: ident ) => {
                $nbody
            };
        }
        macro_rules! __with_ty_l__ {
            ( $_3 $ListVector: ident ) => {
                $lbody
            };
        }

        match $vector.data_type() {
            ConcreteDataType::Null(_) => {
                // let v = $vector.as_any().downcast_ref::<NullVector>().unwrap();
                // $compute.compute_null(v)
                //
                __with_ty_n__! { NullVector }
            }
            ConcreteDataType::Boolean(_) => {
                __with_ty_s__! { BooleanVector }
            }
            ConcreteDataType::Int8(_) => {
                __with_ty_s__! { Int8Vector }
            }
            ConcreteDataType::Int16(_) => {
                __with_ty_s__! { Int16Vector }
            }
            ConcreteDataType::Int32(_) => {
                __with_ty_s__! { Int32Vector }
            }
            ConcreteDataType::Int64(_) => {
                __with_ty_s__! { Int64Vector }
            }
            ConcreteDataType::UInt8(_) => {
                __with_ty_s__! { UInt8Vector }
            }
            ConcreteDataType::UInt16(_) => {
                __with_ty_s__! { UInt16Vector }
            }
            ConcreteDataType::UInt32(_) => {
                __with_ty_s__! { UInt32Vector }
            }
            ConcreteDataType::UInt64(_) => {
                __with_ty_s__! { UInt64Vector }
            }
            ConcreteDataType::Float32(_) => {
                __with_ty_s__! { Float32Vector }
            }
            ConcreteDataType::Float64(_) => {
                __with_ty_s__! { Float64Vector }
            }
            ConcreteDataType::Binary(_) => {
                __with_ty_s__! { BinaryVector }
            }
            ConcreteDataType::String(_) => {
                __with_ty_s__! { StringVector }
            }
            ConcreteDataType::Date(_) => {
                __with_ty_s__! { DateVector }
            }
            ConcreteDataType::DateTime(_) => {
                __with_ty_s__! { DateTimeVector }
            }
            ConcreteDataType::Timestamp(_) => {
                __with_ty_s__! { TimestampVector }
            }
            ConcreteDataType::List(_) => {
                __with_ty_l__! { ListVector }
            }
        }
    }};
}

macro_rules! dispatch_vector_compute {
    ($vector: ident, $compute: ident) => {
        use $crate::data_type::ConcreteDataType;
        use $crate::vectors::{all::*, Vector};

        match $vector.data_type() {
            ConcreteDataType::Null(_) => {
                let v = $vector.as_any().downcast_ref::<NullVector>().unwrap();
                $compute.compute_null(v)
            }
            ConcreteDataType::Boolean(_) => {
                let v = $vector.as_any().downcast_ref::<BooleanVector>().unwrap();
                $compute.compute_scalar(v)
            }
            ConcreteDataType::Int8(_) => {
                let v = $vector.as_any().downcast_ref::<Int8Vector>().unwrap();
                $compute.compute_scalar(v)
            }
            ConcreteDataType::Int16(_) => {
                let v = $vector.as_any().downcast_ref::<Int16Vector>().unwrap();
                $compute.compute_scalar(v)
            }
            ConcreteDataType::Int32(_) => {
                let v = $vector.as_any().downcast_ref::<Int32Vector>().unwrap();
                $compute.compute_scalar(v)
            }
            ConcreteDataType::Int64(_) => {
                let v = $vector.as_any().downcast_ref::<Int64Vector>().unwrap();
                $compute.compute_scalar(v)
            }
            ConcreteDataType::UInt8(_) => {
                let v = $vector.as_any().downcast_ref::<UInt8Vector>().unwrap();
                $compute.compute_scalar(v)
            }
            ConcreteDataType::UInt16(_) => {
                let v = $vector.as_any().downcast_ref::<UInt16Vector>().unwrap();
                $compute.compute_scalar(v)
            }
            ConcreteDataType::UInt32(_) => {
                let v = $vector.as_any().downcast_ref::<UInt32Vector>().unwrap();
                $compute.compute_scalar(v)
            }
            ConcreteDataType::UInt64(_) => {
                let v = $vector.as_any().downcast_ref::<UInt64Vector>().unwrap();
                $compute.compute_scalar(v)
            }
            ConcreteDataType::Float32(_) => {
                let v = $vector.as_any().downcast_ref::<Float32Vector>().unwrap();
                $compute.compute_scalar(v)
            }
            ConcreteDataType::Float64(_) => {
                let v = $vector.as_any().downcast_ref::<Float64Vector>().unwrap();
                $compute.compute_scalar(v)
            }
            ConcreteDataType::Binary(_) => {
                let v = $vector.as_any().downcast_ref::<BinaryVector>().unwrap();
                $compute.compute_scalar(v)
            }
            ConcreteDataType::String(_) => {
                let v = $vector.as_any().downcast_ref::<StringVector>().unwrap();
                $compute.compute_scalar(v)
            }
            ConcreteDataType::Date(_) => {
                let v = $vector.as_any().downcast_ref::<DateVector>().unwrap();
                $compute.compute_scalar(v)
            }
            ConcreteDataType::DateTime(_) => {
                let v = $vector.as_any().downcast_ref::<DateTimeVector>().unwrap();
                $compute.compute_scalar(v)
            }
            ConcreteDataType::Timestamp(_) => {
                let v = $vector.as_any().downcast_ref::<TimestampVector>().unwrap();
                $compute.compute_scalar(v)
            }
            ConcreteDataType::List(_) => {
                let v = $vector.as_any().downcast_ref::<ListVector>().unwrap();
                $compute.compute_list(v)
            }
        }
    };
}

fn dedup2(vector: &dyn Vector) {
    let mut compute = Dedup {
        prev_vector: None,
        // FIXME(yingwen): Initialize bitmap.
        selected: MutableBitmap::new(),
    };
    dispatch_vector_compute2!(vector, |$S| {
        let v = vector.as_any().downcast_ref::<$S>().unwrap();
        compute.compute_scalar(v)
    },
    |$N| {
        let v = vector.as_any().downcast_ref::<$N>().unwrap();
        compute.compute_null(v)
    },
    |$L| {
        let v = vector.as_any().downcast_ref::<$L>().unwrap();
        compute.compute_list(v)
    })
}

struct Dedup {
    prev_vector: Option<VectorRef>,
    // selected is filled by false at initialization.
    selected: MutableBitmap,
}

impl Dedup {
    fn compute_scalar<'a: 'b, 'b, T: ScalarVector>(&'a mut self, vector: &'b T)
    where
        T::RefItem<'b>: PartialEq,
    {
        if vector.is_empty() {
            return;
        }

        for ((i, current), next) in vector
            .iter_data()
            .enumerate()
            .zip(vector.iter_data().skip(1))
        {
            if current != next {
                // If next element is a different element, we mark it as selected.
                self.selected.set(i + 1, true);
            }
        }

        // Always retain the first element.
        self.selected.set(0, true);

        // Then check whether still keep the first element based last element in previous vector.
        if let Some(prev_vector) = &self.prev_vector {
            let prev_vector = prev_vector.as_any().downcast_ref::<T>().unwrap();
            if !prev_vector.is_empty() {
                let last = prev_vector.get_data(prev_vector.len() - 1);
                if last == vector.get_data(0) {
                    self.selected.set(0, false);
                }
            }
        }
    }

    fn compute_null(&mut self, vector: &NullVector) {
        if vector.is_empty() {
            return;
        }

        if self.prev_vector.is_none() {
            // Retain first element.
            self.selected.set(0, true);
        }
    }

    fn compute_list(&mut self, vector: &ListVector) {
        if vector.is_empty() {
            return;
        }
        unimplemented!()
    }
}

fn dedup(vector: &dyn Vector) {
    let mut compute = Dedup {
        prev_vector: None,
        // FIXME(yingwen): Initialize bitmap.
        selected: MutableBitmap::new(),
    };

    dispatch_vector_compute!(vector, compute);
}
