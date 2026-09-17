use proc_macro2::{Ident, Span, TokenStream};
use proc_macro_crate::{crate_name, FoundCrate};
use quote::quote;

pub(crate) struct RuntimePath {
    pub(crate) tokens: TokenStream,
    pub(crate) attribute_prefix: String,
}

pub(crate) fn lily_queue() -> syn::Result<RuntimePath> {
    match crate_name("lily_queue") {
        Ok(FoundCrate::Itself) => Ok(RuntimePath {
            tokens: quote!(crate),
            attribute_prefix: "crate".to_string(),
        }),
        Ok(FoundCrate::Name(name)) => {
            let name = name.replace('-', "_");
            let identifier = Ident::new(&name, Span::call_site());
            Ok(RuntimePath {
                tokens: quote!(::#identifier),
                attribute_prefix: name,
            })
        }
        Err(error) => Err(syn::Error::new(
            Span::call_site(),
            format!(
                "could not locate the `lily_queue` runtime dependency for queue handler expansion: {error}"
            ),
        )),
    }
}
