//! Locate the runtime without requiring callers to use its Cargo package name.

use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::{Ident, Span};

pub(crate) fn lily_clickhouse() -> syn::Path {
    let name = match crate_name("lily_clickhouse") {
        Ok(FoundCrate::Name(name)) => name,
        // The runtime exposes a self alias, also usable from integration tests.
        // Standalone derive contracts may supply that alias themselves.
        Ok(FoundCrate::Itself) | Err(_) => "lily_clickhouse".to_owned(),
    };
    let identifier = Ident::new(&name.replace('-', "_"), Span::call_site());
    syn::parse_quote!(::#identifier)
}
