#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]
#![doc = include_str!("docs/overview.md")]

use proc_macro::TokenStream;

mod injectable;
mod runtime_path;
mod service_args;
mod utils;

#[doc = include_str!("docs/injectable.md")]
#[proc_macro_derive(Injectable, attributes(service, inject))]
pub fn derive_injectable(input: TokenStream) -> TokenStream {
    injectable::derive_impl(input)
}
