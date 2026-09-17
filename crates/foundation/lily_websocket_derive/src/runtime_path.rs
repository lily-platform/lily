//! Resolve the direct component or its umbrella re-export.

use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

pub(crate) fn lily_websocket() -> syn::Result<TokenStream> {
    if let Some(identifier) = dependency("lily_websocket") {
        return Ok(quote!(::#identifier));
    }
    if let Some(identifier) = dependency("lily") {
        return Ok(quote!(::#identifier::websocket));
    }
    Err(syn::Error::new(
        Span::call_site(),
        "Lily websocket macros require `lily_websocket` or `lily` with its `websocket` feature",
    ))
}

fn dependency(package: &str) -> Option<Ident> {
    let name = match crate_name(package).ok()? {
        FoundCrate::Itself => package.to_owned(),
        FoundCrate::Name(name) => name,
    };
    Some(Ident::new(&name.replace('-', "_"), Span::call_site()))
}
