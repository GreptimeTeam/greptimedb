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
    parse_macro_input, Attribute, Ident, ItemFn, Signature, Type, TypePath, TypeReference,
    Visibility,
};

use crate::utils::extract_input_types;

/// Internal util macro to early return on error.
macro_rules! ok {
    ($item:expr) => {
        match $item {
            Ok(item) => item,
            Err(e) => return e.into_compile_error().into(),
        }
    };
}

/// Internal util macro to create an error.
macro_rules! error {
    ($span:expr, $msg: expr) => {
        Err(syn::Error::new($span, $msg))
    };
}

pub(crate) fn process_admin_fn(args: TokenStream, input: TokenStream) -> TokenStream {
    let mut name: Option<Ident> = None;
    let mut display_name: Option<Ident> = None;
    let mut sig_fn: Option<Ident> = None;
    let mut ret: Option<Ident> = None;

    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("name") {
            name = Some(meta.value()?.parse()?);
            Ok(())
        } else if meta.path.is_ident("display_name") {
            display_name = Some(meta.value()?.parse()?);
            Ok(())
        } else if meta.path.is_ident("sig_fn") {
            sig_fn = Some(meta.value()?.parse()?);
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
    if arg_types.len() < 2 {
        ok!(error!(
            sig.span(),
            "Expect at least two argument for admin fn: (handler, query_ctx)"
        ));
    }
    let handler_type = ok!(extract_handler_type(&arg_types));

    let mut result = TokenStream::new();
    // build the struct and its impl block
    // only do this when `display_name` is specified
    if let Some(display_name) = display_name {
        let struct_code = build_struct(
            attrs,
            vis,
            fn_name,
            name.expect("name required"),
            sig_fn.expect("sig_fn required"),
            ret.expect("ret required"),
            handler_type,
            display_name,
        );
        result.extend(struct_code);
    }

    // preserve this fn
    let input_fn_code: TokenStream = quote! {
        #sig { #block }
    }
    .into();

    result.extend(input_fn_code);
    result
}

/// Retrieve the handler type, `ProcedureServiceHandlerRef` or `TableMutationHandlerRef`.
fn extract_handler_type(arg_types: &[Type]) -> Result<&Ident, syn::Error> {
    match &arg_types[0] {
        Type::Reference(TypeReference { elem, .. }) => match &**elem {
            Type::Path(TypePath { path, .. }) => Ok(&path
                .segments
                .first()
                .expect("Expected a reference of handler")
                .ident),
            other => {
                error!(other.span(), "Expected a reference of handler")
            }
        },
        other => {
            error!(other.span(), "Expected a reference of handler")
        }
    }
}

/// Build the function struct
#[allow(clippy::too_many_arguments)]
fn build_struct(
    attrs: Vec<Attribute>,
    vis: Visibility,
    fn_name: &Ident,
    name: Ident,
    sig_fn: Ident,
    ret: Ident,
    handler_type: &Ident,
    display_name_ident: Ident,
) -> TokenStream {
    let display_name = display_name_ident.to_string();
    let ret = Ident::new(&format!("{ret}_datatype"), ret.span());
    let uppcase_display_name = display_name.to_uppercase();
    // Get the handler name in function state by the argument ident
    // TODO(discord9): consider simple depend injection if more handlers are needed
    let (handler, snafu_type) = match handler_type.to_string().as_str() {
        "ProcedureServiceHandlerRef" => (
            Ident::new("procedure_service_handler", handler_type.span()),
            Ident::new("MissingProcedureServiceHandlerSnafu", handler_type.span()),
        ),

        "TableMutationHandlerRef" => (
            Ident::new("table_mutation_handler", handler_type.span()),
            Ident::new("MissingTableMutationHandlerSnafu", handler_type.span()),
        ),

        "FlowServiceHandlerRef" => (
            Ident::new("flow_service_handler", handler_type.span()),
            Ident::new("MissingFlowServiceHandlerSnafu", handler_type.span()),
        ),
        handler => ok!(error!(
            handler_type.span(),
            format!("Unknown handler type: {handler}")
        )),
    };

    quote! {
        #(#attrs)*
        #[derive(Debug)]
        #vis struct #name;

        impl std::fmt::Display for #name {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, #uppcase_display_name)
            }
        }


        impl crate::function::Function for #name {
            fn name(&self) -> &'static str {
                #display_name
            }

            fn return_type(&self, _input_types: &[store_api::storage::ConcreteDataType]) -> common_query::error::Result<store_api::storage::ConcreteDataType> {
                Ok(store_api::storage::ConcreteDataType::#ret())
            }

            fn signature(&self) -> Signature {
                #sig_fn()
            }

            fn eval(&self, func_ctx: crate::function::FunctionContext, columns: &[datatypes::vectors::VectorRef]) ->  common_query::error::Result<datatypes::vectors::VectorRef> {
                // Ensure under the `greptime` catalog for security
                crate::ensure_greptime!(func_ctx);

                let columns_num = columns.len();
                let rows_num = if columns.is_empty() {
                    1
                } else {
                    columns[0].len()
                };
                let columns = Vec::from(columns);

                // TODO(dennis): DataFusion doesn't support async UDF currently
                std::thread::spawn(move || {
                    use snafu::OptionExt;
                    use datatypes::data_type::DataType;

                    let query_ctx = &func_ctx.query_ctx;
                    let handler = func_ctx
                        .state
                        .#handler
                        .as_ref()
                        .context(#snafu_type)?;

                    let mut builder = store_api::storage::ConcreteDataType::#ret()
                        .create_mutable_vector(rows_num);

                    if columns_num == 0 {
                        let result = common_runtime::block_on_global(async move {
                            #fn_name(handler, query_ctx, &[]).await
                        })?;

                        builder.push_value_ref(result.as_value_ref());
                    } else {
                        for i in 0..rows_num {
                            let args: Vec<_> = columns.iter()
                                .map(|vector| vector.get_ref(i))
                                .collect();

                            let result = common_runtime::block_on_global(async move {
                                #fn_name(handler, query_ctx, &args).await
                            })?;

                            builder.push_value_ref(result.as_value_ref());
                        }
                    }

                    Ok(builder.to_vector())
                })
                    .join()
                    .map_err(|e| {
                        common_telemetry::error!(e; "Join thread error");
                        common_query::error::Error::ThreadJoin {
                            location: snafu::Location::default(),
                        }
                    })?

            }

        }
    }
    .into()
}
