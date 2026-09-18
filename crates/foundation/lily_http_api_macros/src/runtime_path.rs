//! Resolve the direct component or its umbrella re-export.

use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

pub(crate) fn lily_http_api() -> syn::Result<TokenStream> {
    if let Some(identifier) = dependency("lily_http_api") {
        return Ok(quote!(::#identifier));
    }
    if let Some(identifier) = dependency("lilyrs") {
        return Ok(quote!(::#identifier::http_api));
    }
    Err(syn::Error::new(
        Span::call_site(),
        "Lily http_api macros require `lily_http_api` or `lilyrs` with its `http-api` feature",
    ))
}

fn dependency(package: &str) -> Option<Ident> {
    let name = match crate_name(package).ok()? {
        FoundCrate::Itself => package.to_owned(),
        FoundCrate::Name(name) => name,
    };
    Some(Ident::new(&name.replace('-', "_"), Span::call_site()))
}
