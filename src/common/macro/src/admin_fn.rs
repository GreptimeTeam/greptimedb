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
    Attribute, Ident, ItemFn, Path, Signature, Type, TypePath, TypeReference, Visibility,
    parse_macro_input,
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
    let mut user_path: Option<Path> = None;

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
        } else if meta.path.is_ident("user_path") {
            user_path = Some(meta.value()?.parse()?);
            Ok(())
        } else {
            Err(meta.error("unsupported property"))
        }
    });

    // extract arg map
    parse_macro_input!(args with parser);

    if user_path.is_none() {
        user_path = Some(syn::parse_str("crate").expect("failed to parse user path"));
    }

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
            user_path.expect("user_path required"),
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
    user_path: Path,
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
        #vis struct #name {
            signature: datafusion_expr::Signature,
            func_ctx: #user_path::function::FunctionContext,
        }

        impl #name {
            /// Creates a new instance of the function with function context.
            fn create(signature: datafusion_expr::Signature, func_ctx: #user_path::function::FunctionContext) -> Self {
                Self {
                    signature,
                    func_ctx,
                }
            }

            /// Returns the [`ScalarFunctionFactory`] of the function.
            pub fn factory() -> impl Into< #user_path::function_factory::ScalarFunctionFactory>  {
                Self {
                    signature: #sig_fn().into(),
                    func_ctx: #user_path::function::FunctionContext::default(),
                }
            }
        }

        impl std::fmt::Display for #name {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, #uppcase_display_name)
            }
        }

        impl std::fmt::Debug for #name {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "{}({})", #uppcase_display_name, self.func_ctx)
            }
        }

        // Implement DataFusion's ScalarUDFImpl trait
        impl datafusion::logical_expr::ScalarUDFImpl for #name {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn name(&self) -> &str {
                #display_name
            }

            fn signature(&self) -> &datafusion_expr::Signature {
                &self.signature
            }

            fn return_type(&self, _arg_types: &[datafusion::arrow::datatypes::DataType]) -> datafusion_common::Result<datafusion::arrow::datatypes::DataType> {
                use datatypes::data_type::DataType;
                Ok(store_api::storage::ConcreteDataType::#ret().as_arrow_type())
            }

            fn invoke_with_args(
                &self,
                _args: datafusion::logical_expr::ScalarFunctionArgs,
            ) -> datafusion_common::Result<datafusion_expr::ColumnarValue> {
                Err(datafusion_common::DataFusionError::NotImplemented(
                    format!("{} can only be called from async contexts", #display_name)
                ))
            }
        }

        /// Implement From trait for ScalarFunctionFactory
        impl From<#name> for  #user_path::function_factory::ScalarFunctionFactory {
            fn from(func: #name) -> Self {
                 use std::sync::Arc;
                 use datafusion_expr::ScalarUDFImpl;
                 use datafusion_expr::async_udf::AsyncScalarUDF;

                let name = func.name().to_string();

                let func = Arc::new(move |ctx: #user_path::function::FunctionContext| {
                    // create the UDF dynamically with function context
                    let udf_impl = #name::create(func.signature.clone(), ctx);
                    let async_udf = AsyncScalarUDF::new(Arc::new(udf_impl));
                    async_udf.into_scalar_udf()
                });
                Self {
                    name,
                    factory: func,
                }
            }
        }

        // Implement DataFusion's AsyncScalarUDFImpl trait
        #[async_trait::async_trait]
        impl datafusion_expr::async_udf::AsyncScalarUDFImpl for #name {
            async fn invoke_async_with_args(
                &self,
                args: datafusion::logical_expr::ScalarFunctionArgs,
            ) -> datafusion_common::Result<datafusion_expr::ColumnarValue> {
                use common_error::ext::ErrorExt;

                let columns = args.args
                    .iter()
                    .map(|arg| {
                        common_query::prelude::ColumnarValue::try_from(arg)
                            .and_then(|cv| match cv {
                                common_query::prelude::ColumnarValue::Vector(v) => Ok(v),
                                common_query::prelude::ColumnarValue::Scalar(s) => {
                                    datatypes::vectors::Helper::try_from_scalar_value(s, args.number_rows)
                                        .context(common_query::error::FromScalarValueSnafu)
                                }
                            })
                    })
                    .collect::<common_query::error::Result<Vec<_>>>()
                    .map_err(|e| datafusion_common::DataFusionError::Execution(format!("Column conversion error: {}", e.output_msg())))?;

                // Safety check: Ensure under the `greptime` catalog for security
                #user_path::ensure_greptime!(self.func_ctx);

                let columns_num = columns.len();
                let rows_num = if columns.is_empty() {
                    1
                } else {
                    columns[0].len()
                };

                use snafu::{OptionExt, ResultExt};
                use datatypes::data_type::DataType;

                let query_ctx = &self.func_ctx.query_ctx;
                let handler = self.func_ctx
                    .state
                    .#handler
                    .as_ref()
                    .context(#snafu_type)
                    .map_err(|e| datafusion_common::DataFusionError::Execution(format!("Handler error: {}", e.output_msg())))?;

                let mut builder = store_api::storage::ConcreteDataType::#ret()
                    .create_mutable_vector(rows_num);

                if columns_num == 0 {
                    let result = #fn_name(handler, query_ctx, &[]).await
                        .map_err(|e| datafusion_common::DataFusionError::Execution(format!("Function execution error: {}", e.output_msg())))?;

                    builder.push_value_ref(&result.as_value_ref());
                } else {
                    for i in 0..rows_num {
                        let args: Vec<_> = columns.iter()
                            .map(|vector| vector.get_ref(i))
                            .collect();

                        let result = #fn_name(handler, query_ctx, &args).await
                            .map_err(|e| datafusion_common::DataFusionError::Execution(format!("Function execution error: {}", e.output_msg())))?;

                        builder.push_value_ref(&result.as_value_ref());
                    }
                }

                let result_vector = builder.to_vector();

                // Convert result back to DataFusion ColumnarValue
                Ok(datafusion_expr::ColumnarValue::Array(result_vector.to_arrow_array()))
            }
        }

        impl PartialEq for #name {
            fn eq(&self, other: &Self) -> bool {
                self.signature == other.signature
            }
        }

        impl Eq for #name {}

        impl std::hash::Hash for #name {
            fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
                self.signature.hash(state)
            }
        }
    }
    .into()
}
