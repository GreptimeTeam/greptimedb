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

use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::token::Comma;
use syn::{FnArg, Type};

/// Extract the argument list from the annotated function.
pub(crate) fn extract_input_types(
    inputs: &Punctuated<FnArg, Comma>,
) -> Result<Vec<Type>, syn::Error> {
    inputs
        .iter()
        .map(|arg| match arg {
            FnArg::Receiver(receiver) => Err(syn::Error::new(receiver.span(), "expected bool")),
            FnArg::Typed(pat_type) => Ok(*pat_type.ty.clone()),
        })
        .collect()
}
