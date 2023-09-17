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

mod aggr_func;
mod print_caller;
mod range_fn;

use aggr_func::{impl_aggr_func_type_store, impl_as_aggr_func_creator};
use print_caller::process_print_caller;
use proc_macro::TokenStream;
use range_fn::process_range_fn;
use syn::{parse_macro_input, DeriveInput};

/// Make struct implemented trait [AggrFuncTypeStore], which is necessary when writing UDAF.
/// This derive macro is expect to be used along with attribute macro [macro@as_aggr_func_creator].
#[proc_macro_derive(AggrFuncTypeStore)]
pub fn aggr_func_type_store_derive(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    impl_aggr_func_type_store(&ast)
}

/// A struct can be used as a creator for aggregate function if it has been annotated with this
/// attribute first. This attribute add a necessary field which is intended to store the input
/// data's types to the struct.
/// This attribute is expected to be used along with derive macro [AggrFuncTypeStore].
#[proc_macro_attribute]
pub fn as_aggr_func_creator(args: TokenStream, input: TokenStream) -> TokenStream {
    impl_as_aggr_func_creator(args, input)
}

/// Attribute macro to convert an arithimetic function to a range function. The annotated function
/// should accept servaral arrays as input and return a single value as output. This procedure
/// macro can works on any number of input parameters. Return type can be either primitive type
/// or wrapped in `Option`.
///
/// # Example
/// Take `count_over_time()` in PromQL as an example:
/// ```rust, ignore
/// /// The count of all values in the specified interval.
/// #[range_fn(
///     name = "CountOverTime",
///     ret = "Float64Array",
///     display_name = "prom_count_over_time"
/// )]
/// pub fn count_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> f64 {
///      values.len() as f64
/// }
/// ```
///
/// # Arguments
/// - `name`: The name of the generated `ScalarUDF` struct.
/// - `ret`: The return type of the generated UDF function.
/// - `display_name`: The display name of the generated UDF function.
#[proc_macro_attribute]
pub fn range_fn(args: TokenStream, input: TokenStream) -> TokenStream {
    process_range_fn(args, input)
}

/// Attribute macro to print the caller to the annotated function.
/// The caller is printed as its filename and the call site line number.
///
/// This macro works like this: inject the tracking codes as the first statement to the annotated
/// function body. The tracking codes use [backtrace-rs](https://crates.io/crates/backtrace) to get
/// the callers. So you must dependent on the `backtrace-rs` crate.
///
/// # Arguments
/// - `depth`: The max depth of call stack to print. Optional, defaults to 1.
///
/// # Example
/// ```rust, ignore
///
/// #[print_caller(depth = 3)]
/// fn foo() {}
/// ```
#[proc_macro_attribute]
pub fn print_caller(args: TokenStream, input: TokenStream) -> TokenStream {
    process_print_caller(args, input)
}
