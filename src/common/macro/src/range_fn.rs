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

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::token::Comma;
use syn::{
    parse_macro_input, Attribute, AttributeArgs, FnArg, Ident, ItemFn, Meta, MetaNameValue,
    NestedMeta, Signature, Type, TypeReference, Visibility,
};

/// Internal util macro to early return on error.
macro_rules! ok {
    ($item:expr) => {
        match $item {
            Ok(item) => item,
            Err(e) => return e.into_compile_error().into(),
        }
    };
}

pub(crate) fn process_range_fn(args: TokenStream, input: TokenStream) -> TokenStream {
    let mut result = TokenStream::new();

    // extract arg map
    let arg_pairs = parse_macro_input!(args as AttributeArgs);
    let arg_span = arg_pairs[0].span();
    let arg_map = ok!(extract_arg_map(arg_pairs));

    // decompose the fn block
    let compute_fn = parse_macro_input!(input as ItemFn);
    let ItemFn {
        attrs,
        vis,
        sig,
        block,
    } = compute_fn;

    // extract fn arg list
    let Signature {
        inputs,
        ident: fn_name,
        ..
    } = &sig;
    let arg_types = ok!(extract_input_types(inputs));

    // build the struct and its impl block
    // only do this when `display_name` is specified
    if let Ok(display_name) = get_ident(&arg_map, "display_name", arg_span) {
        let struct_code = build_struct(
            attrs,
            vis,
            ok!(get_ident(&arg_map, "name", arg_span)),
            display_name,
        );
        result.extend(struct_code);
    }

    let calc_fn_code = build_calc_fn(
        ok!(get_ident(&arg_map, "name", arg_span)),
        arg_types,
        fn_name.clone(),
        ok!(get_ident(&arg_map, "ret", arg_span)),
    );
    // preserve this fn, but remove its `pub` modifier
    let input_fn_code: TokenStream = quote! {
        #sig { #block }
    }
    .into();

    result.extend(calc_fn_code);
    result.extend(input_fn_code);
    result
}

/// Extract a String <-> Ident map from the attribute args.
fn extract_arg_map(args: Vec<NestedMeta>) -> Result<HashMap<String, Ident>, syn::Error> {
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
fn get_ident(map: &HashMap<String, Ident>, key: &str, span: Span) -> Result<Ident, syn::Error> {
    map.get(key)
        .cloned()
        .ok_or_else(|| syn::Error::new(span, format!("Expect attribute {key} but not found")))
}

/// Extract the argument list from the annotated function.
fn extract_input_types(inputs: &Punctuated<FnArg, Comma>) -> Result<Vec<Type>, syn::Error> {
    inputs
        .iter()
        .map(|arg| match arg {
            FnArg::Receiver(receiver) => Err(syn::Error::new(receiver.span(), "expected bool")),
            FnArg::Typed(pat_type) => Ok(*pat_type.ty.clone()),
        })
        .collect()
}

fn build_struct(
    attrs: Vec<Attribute>,
    vis: Visibility,
    name: Ident,
    display_name_ident: Ident,
) -> TokenStream {
    let display_name = display_name_ident.to_string();
    quote! {
        #(#attrs)*
        #[derive(Debug)]
        #vis struct #name {}

        impl #name {
            pub const fn name() -> &'static str {
                #display_name
            }

            pub fn scalar_udf() -> ScalarUDF {
                ScalarUDF {
                    name: Self::name().to_string(),
                    signature: Signature::new(
                        TypeSignature::Exact(Self::input_type()),
                        Volatility::Immutable,
                    ),
                    return_type: Arc::new(|_| Ok(Arc::new(Self::return_type()))),
                    fun: Arc::new(Self::calc),
                }
            }

            // TODO(ruihang): this should be parameterized
            // time index column and value column
            fn input_type() -> Vec<DataType> {
                vec![
                    RangeArray::convert_data_type(DataType::Timestamp(TimeUnit::Millisecond, None)),
                    RangeArray::convert_data_type(DataType::Float64),
                ]
            }

            // TODO(ruihang): this should be parameterized
            fn return_type() -> DataType {
                DataType::Float64
            }
        }
    }
    .into()
}

fn build_calc_fn(
    name: Ident,
    param_types: Vec<Type>,
    fn_name: Ident,
    ret_type: Ident,
) -> TokenStream {
    let param_names = param_types
        .iter()
        .enumerate()
        .map(|(i, ty)| Ident::new(&format!("param_{}", i), ty.span()))
        .collect::<Vec<_>>();
    let unref_param_types = param_types
        .iter()
        .map(|ty| {
            if let Type::Reference(TypeReference { elem, .. }) = ty {
                elem.as_ref().clone()
            } else {
                ty.clone()
            }
        })
        .collect::<Vec<_>>();
    let num_params = param_types.len();
    let param_numbers = (0..num_params).collect::<Vec<_>>();
    let range_array_names = param_names
        .iter()
        .map(|name| Ident::new(&format!("{}_range_array", name), name.span()))
        .collect::<Vec<_>>();
    let first_range_array_name = range_array_names.first().unwrap().clone();

    quote! {
        impl #name {
            fn calc(input: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
                assert_eq!(input.len(), #num_params);

                #( let #range_array_names = RangeArray::try_new(extract_array(&input[#param_numbers])?.to_data().into())?; )*

                // TODO(ruihang): add ensure!() 

                let mut result_array = Vec::new();
                for index in 0..#first_range_array_name.len(){
                    #( let #param_names = #range_array_names.get(index).unwrap().as_any().downcast_ref::<#unref_param_types>().unwrap().clone(); )*

                    // TODO(ruihang): add ensure!() to check length

                    let result = #fn_name(#( &#param_names, )*);
                    result_array.push(result);
                }

                let result = ColumnarValue::Array(Arc::new(#ret_type::from_iter(result_array)));
                Ok(result)
            }
        }
    }
    .into()
}
