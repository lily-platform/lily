use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

pub(crate) fn lily_websocket() -> syn::Result<TokenStream> {
    match crate_name("lily_websocket") {
        Ok(FoundCrate::Itself) => Ok(quote!(::lily_websocket)),
        Ok(FoundCrate::Name(name)) => {
            let name = name.replace('-', "_");
            let identifier = Ident::new(&name, Span::call_site());
            Ok(quote!(::#identifier))
        }
        Err(error) => Err(syn::Error::new(
            Span::call_site(),
            format!(
                "could not locate the `lily_websocket` runtime dependency for Lily WebSocket macro expansion: {error}"
            ),
        )),
    }
}
