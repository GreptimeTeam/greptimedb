use proc_macro::TokenStream;
use quote::{quote, quote_spanned};
use syn::parse::Parser;
use syn::spanned::Spanned;
use syn::{parse_macro_input, DeriveInput, ItemStruct};

/// Make struct implemented trait [AggrFuncTypeStore], which is necessary when writing UDAF.
/// This derive macro is expect to be used along with attribute macro [as_aggr_func_creator].
#[proc_macro_derive(AggrFuncTypeStore)]
pub fn aggr_func_type_store_derive(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    impl_aggr_func_type_store(&ast)
}

fn impl_aggr_func_type_store(ast: &DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let gen = quote! {
        use common_query::logical_plan::accumulator::AggrFuncTypeStore;
        use common_query::error::{InvalidInputStateSnafu, Error as QueryError};
        use datatypes::prelude::ConcreteDataType;

        impl AggrFuncTypeStore for #name {
            fn input_types(&self) -> std::result::Result<Vec<ConcreteDataType>, QueryError> {
                let input_types = self.input_types.load();
                snafu::ensure!(input_types.is_some(), InvalidInputStateSnafu);
                Ok(input_types.as_ref().unwrap().as_ref().clone())
            }

            fn set_input_types(&self, input_types: Vec<ConcreteDataType>) -> std::result::Result<(), QueryError> {
                let old = self.input_types.swap(Some(std::sync::Arc::new(input_types.clone())));
                if let Some(old) = old {
                    snafu::ensure!(old.len() == input_types.len(), InvalidInputStateSnafu);
                    for (x, y) in old.iter().zip(input_types.iter()) {
                        snafu::ensure!(x == y, InvalidInputStateSnafu);
                    }
                }
                Ok(())
            }
        }
    };
    gen.into()
}

/// A struct can be used as a creator for aggregate function if it has been annotated with this
/// attribute first. This attribute add a necessary field which is intended to store the input
/// data's types to the struct.
/// This attribute is expected to be used along with derive macro [AggrFuncTypeStore].
#[proc_macro_attribute]
pub fn as_aggr_func_creator(_args: TokenStream, input: TokenStream) -> TokenStream {
    let mut item_struct = parse_macro_input!(input as ItemStruct);
    if let syn::Fields::Named(ref mut fields) = item_struct.fields {
        let result = syn::Field::parse_named.parse2(quote! {
            input_types: arc_swap::ArcSwapOption<Vec<ConcreteDataType>>
        });
        match result {
            Ok(field) => fields.named.push(field),
            Err(e) => return e.into_compile_error().into(),
        }
    } else {
        return quote_spanned!(
            item_struct.fields.span() => compile_error!(
                "This attribute macro needs to add fields to the its annotated struct, \
                so the struct must have \"{}\".")
        )
        .into();
    }
    quote! {
        #item_struct
    }
    .into()
}
