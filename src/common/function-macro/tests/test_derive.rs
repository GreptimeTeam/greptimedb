use common_function_macro::{as_aggr_func_creator, AggrFuncTypeStore};
use static_assertions::{assert_fields, assert_impl_all};

#[as_aggr_func_creator]
#[derive(Debug, Default, AggrFuncTypeStore)]
struct Foo {}

#[test]
fn test_derive() {
    Foo::default();
    assert_fields!(Foo: input_types);
    assert_impl_all!(Foo: std::fmt::Debug, Default, AggrFuncTypeStore);
}
