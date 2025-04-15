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

use proc_macro::TokenStream;
use quote::quote;
use syn::spanned::Spanned;
use syn::{
    parse_macro_input, Attribute, Ident, ItemFn, Signature, Type, TypeReference, Visibility,
};

use crate::utils::extract_input_types;

macro_rules! ok {
    ($item:expr) => {
        match $item {
            Ok(item) => item,
            Err(e) => return e.into_compile_error().into(),
        }
    };
}

pub(crate) fn process_range_fn(args: TokenStream, input: TokenStream) -> TokenStream {
    let mut name: Option<Ident> = None;
    let mut display_name: Option<Ident> = None;
    let mut ret: Option<Ident> = None;

    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("name") {
            name = Some(meta.value()?.parse()?);
            Ok(())
        } else if meta.path.is_ident("display_name") {
            display_name = Some(meta.value()?.parse()?);
            Ok(())
        } else if meta.path.is_ident("ret") {
            ret = Some(meta.value()?.parse()?);
            Ok(())
        } else {
            Err(meta.error("unsupported property"))
        }
    });

    // extract arg map
    parse_macro_input!(args with parser);

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

    // with format like Float64Array
    let array_types = arg_types
        .iter()
        .map(|ty| {
            if let Type::Reference(TypeReference { elem, .. }) = ty {
                elem.as_ref().clone()
            } else {
                ty.clone()
            }
        })
        .collect::<Vec<_>>();

    let mut result = TokenStream::new();

    // build the struct and its impl block
    // only do this when `display_name` is specified
    if let Some(display_name) = display_name {
        let struct_code = build_struct(
            attrs,
            vis,
            name.clone().expect("name required"),
            display_name,
            array_types,
            ret.clone().expect("ret required"),
        );
        result.extend(struct_code);
    }

    let calc_fn_code = build_calc_fn(
        name.expect("name required"),
        arg_types,
        fn_name.clone(),
        ret.expect("ret required"),
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

fn build_struct(
    attrs: Vec<Attribute>,
    vis: Visibility,
    name: Ident,
    display_name_ident: Ident,
    array_types: Vec<Type>,
    return_array_type: Ident,
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
                datafusion_expr::create_udf(
                    Self::name(),
                    Self::input_type(),
                    Self::return_type(),
                    Volatility::Volatile,
                    Arc::new(Self::calc) as _,
                )
            }

            fn input_type() -> Vec<DataType> {
                vec![#( RangeArray::convert_data_type(#array_types::new_null(0).data_type().clone()), )*]
            }

            fn return_type() -> DataType {
                #return_array_type::new_null(0).data_type().clone()
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
    let first_param_name = param_names.first().unwrap().clone();

    quote! {
        impl #name {
            fn calc(input: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
                assert_eq!(input.len(), #num_params);

                #( let #range_array_names = RangeArray::try_new(extract_array(&input[#param_numbers])?.to_data().into())?; )*

                // check arrays len
                {
                    let len_first = #first_range_array_name.len();
                    #(
                        if len_first != #range_array_names.len() {
                            return Err(DataFusionError::Execution(format!("RangeArray have different lengths in PromQL function {}: array1={}, array2={}", #name::name(), len_first, #range_array_names.len())));
                        }
                    )*
                }

                let mut result_array = Vec::new();
                for index in 0..#first_range_array_name.len(){
                    #( let #param_names = #range_array_names.get(index).unwrap().as_any().downcast_ref::<#unref_param_types>().unwrap().clone(); )*

                    // check element len
                    {
                        let len_first = #first_param_name.len();
                        #(
                            if len_first != #param_names.len() {
                                return Err(DataFusionError::Execution(format!("RangeArray's element {} have different lengths in PromQL function {}: array1={}, array2={}", index, #name::name(), len_first, #param_names.len())));
                            }
                        )*
                    }

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
