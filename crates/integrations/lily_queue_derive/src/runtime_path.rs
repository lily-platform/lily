use proc_macro2::{Ident, Span, TokenStream};
use proc_macro_crate::{crate_name, FoundCrate};
use quote::quote;

pub(crate) struct RuntimePath {
    pub(crate) tokens: TokenStream,
    pub(crate) attribute_prefixes: Vec<String>,
}

pub(crate) fn lily_queue() -> syn::Result<RuntimePath> {
    let mut paths = Vec::new();
    if let Ok(found) = crate_name("lily_queue") {
        let name = match found {
            FoundCrate::Itself => "crate".to_owned(),
            FoundCrate::Name(name) => name.replace('-', "_"),
        };
        let identifier = Ident::new(&name, Span::call_site());
        let tokens = if name == "crate" {
            quote!(crate)
        } else {
            quote!(::#identifier)
        };
        paths.push((tokens, name));
    }
    if let Ok(found) = crate_name("lilyrs") {
        let name = match found {
            FoundCrate::Itself => "lilyrs".to_owned(),
            FoundCrate::Name(name) => name.replace('-', "_"),
        };
        let identifier = Ident::new(&name, Span::call_site());
        paths.push((quote!(::#identifier::queue), format!("{name}::queue")));
    }
    let Some((tokens, _)) = paths.first() else {
        return Err(syn::Error::new(
            Span::call_site(),
            "queue macros require `lily_queue` or `lilyrs` with its `queue` or `consumer` feature",
        ));
    };
    let tokens = tokens.clone();
    Ok(RuntimePath {
        tokens,
        attribute_prefixes: paths.into_iter().map(|(_, prefix)| prefix).collect(),
    })
}
