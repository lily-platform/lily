use proc_macro2::{Ident, Span, TokenStream};
use proc_macro_crate::{crate_name, FoundCrate};
use quote::quote;

pub(crate) fn lily_http_api() -> syn::Result<TokenStream> {
    match crate_name("lily_http_api") {
        Ok(FoundCrate::Itself) => Ok(quote!(::lily_http_api)),
        Ok(FoundCrate::Name(name)) => {
            let name = name.replace('-', "_");
            let identifier = Ident::new(&name, Span::call_site());
            Ok(quote!(::#identifier))
        }
        Err(error) => Err(syn::Error::new(
            Span::call_site(),
            format!(
                "could not locate the `lily_http_api` runtime dependency for Lily HTTP macro expansion: {error}"
            ),
        )),
    }
}
