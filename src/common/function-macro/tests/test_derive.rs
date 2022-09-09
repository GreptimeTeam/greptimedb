use common_function_macro::as_aggr_func_creator;
use common_function_macro::AggrFuncTypeStore;

#[as_aggr_func_creator]
#[derive(Debug, Default, AggrFuncTypeStore)]
struct Foo {}

#[test]
fn test_derive() {
    Foo::default();
}
