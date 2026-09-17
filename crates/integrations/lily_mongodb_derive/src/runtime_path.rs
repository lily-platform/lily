//! Locate the component runtime, including renamed umbrella dependencies.

use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::{Ident, Span};

pub(crate) fn lily_mongodb() -> syn::Path {
    if let Some(identifier) = dependency("lily_mongodb") {
        return syn::parse_quote!(::#identifier);
    }
    if let Some(identifier) = dependency("lily") {
        return syn::parse_quote!(::#identifier::mongodb);
    }
    // Standalone derive contracts may supply the runtime self alias themselves.
    syn::parse_quote!(::lily_mongodb)
}

fn dependency(package: &str) -> Option<Ident> {
    let name = match crate_name(package).ok()? {
        FoundCrate::Itself => package.to_owned(),
        FoundCrate::Name(name) => name,
    };
    Some(Ident::new(&name.replace('-', "_"), Span::call_site()))
}
