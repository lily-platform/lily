//! Locate the DI expansion API exposed by the caller's direct dependencies.

use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

pub(crate) fn lily_injection() -> syn::Result<TokenStream> {
    if let Some(runtime) = dependency("lily_injection") {
        return Ok(runtime);
    }

    // These facades preserve their root-level application API. Only generated
    // code traverses the hidden bridge to the same DI runtime and registry.
    for facade in ["lily_http_api", "lily_websocket", "lily_consumer"] {
        if let Some(runtime) = dependency(facade) {
            return Ok(quote!(#runtime::__private::lily_injection));
        }
    }

    if let Some(runtime) = dependency("lily") {
        return Ok(quote!(#runtime::__private::lily_injection));
    }

    Err(syn::Error::new(
        Span::call_site(),
        "Injectable requires a direct dependency on `lily_injection` or a Lily DI facade (`lily_http_api`, `lily_websocket`, `lily_consumer`, or `lily` with `injection` or a framework feature)",
    ))
}

fn dependency(package: &str) -> Option<TokenStream> {
    let name = match crate_name(package).ok()? {
        // The runtime has a self alias, and its integration tests/doctests
        // refer to it as an external crate. An absolute path works for both.
        FoundCrate::Itself => package.to_owned(),
        FoundCrate::Name(name) => name,
    };
    let identifier = Ident::new(&name.replace('-', "_"), Span::call_site());
    Some(quote!(::#identifier))
}
