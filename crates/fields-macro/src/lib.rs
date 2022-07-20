extern crate proc_macro;
use self::proc_macro::TokenStream;

use quote::quote;
use syn::parse::{Parse, ParseStream, Result};
use syn::{parse_macro_input, Data, DataStruct, DeriveInput, Fields, LitStr, Token};

#[proc_macro_attribute]
pub fn selected_fields(args: TokenStream, input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let fields = match &input.data {
        Data::Struct(DataStruct {
            fields: Fields::Named(fields),
            ..
        }) => &fields.named,
        _ => panic!("expected a struct with named fields"),
    };
    let field_name = fields.iter().map(|field| &field.ident);
    let field_type = fields.iter().map(|field| &field.ty);

    let struct_name = &input.ident;

    TokenStream::from(quote! {
        // Preserve the input struct unchanged in the output.
        #input

        impl #struct_name {
            fn all_fields() -> Vec<String> {
                println!("Hello from macro");

                let mut t_fields: Vec<String> = vec![];

                // #field_name.iter().map(|field_one| println!("{:?}", field_one));

                // The following repetition expands to, for example:
                //
                //    let id: u32 = Default::default();
                //    let car_name: String = Default::default();
                // #(
                //     println!("{:?}", #field_name);
                // )*;

                t_fields
            }
        }
    })
}
