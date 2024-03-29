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

use std::collections::HashMap;

use proc_macro2::Span;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::token::Comma;
use syn::{FnArg, Ident, Meta, MetaNameValue, NestedMeta, Type};

/// Extract a String <-> Ident map from the attribute args.
pub(crate) fn extract_arg_map(args: Vec<NestedMeta>) -> Result<HashMap<String, Ident>, syn::Error> {
    args.into_iter()
        .map(|meta| {
            if let NestedMeta::Meta(Meta::NameValue(MetaNameValue { path, lit, .. })) = meta {
                let name = path.get_ident().unwrap().to_string();
                let ident = match lit {
                    syn::Lit::Str(lit_str) => lit_str.parse::<Ident>(),
                    _ => Err(syn::Error::new(
                        lit.span(),
                        "Unexpected attribute format. Expected `name = \"value\"`",
                    )),
                }?;
                Ok((name, ident))
            } else {
                Err(syn::Error::new(
                    meta.span(),
                    "Unexpected attribute format. Expected `name = \"value\"`",
                ))
            }
        })
        .collect::<Result<HashMap<String, Ident>, syn::Error>>()
}

/// Helper function to get an Ident from the previous arg map.
pub(crate) fn get_ident(
    map: &HashMap<String, Ident>,
    key: &str,
    span: Span,
) -> Result<Ident, syn::Error> {
    map.get(key)
        .cloned()
        .ok_or_else(|| syn::Error::new(span, format!("Expect attribute {key} but not found")))
}

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
