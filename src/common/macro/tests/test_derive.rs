// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use common_macro::{as_aggr_func_creator, AggrFuncTypeStore};
use static_assertions::{assert_fields, assert_impl_all};

#[as_aggr_func_creator]
#[derive(Debug, Default, AggrFuncTypeStore)]
struct Foo {}

#[test]
#[allow(clippy::extra_unused_type_parameters)]
fn test_derive() {
    let _ = Foo::default();
    assert_fields!(Foo: input_types);
    assert_impl_all!(Foo: std::fmt::Debug, Default, AggrFuncTypeStore);
}
