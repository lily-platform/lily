//! Service attribute argument parsing
//!
//! Handles parsing of #[service(...)] attributes for the Injectable derive macro.

use syn::{
    Ident, LitBool, LitStr, Result, Token, Type,
    parse::{Parse, ParseStream},
};

/// Arguments for the #[service(...)] attribute
pub(crate) struct ServiceArgs {
    /// Service lifetime (Singleton, Scoped, Transient)
    pub(crate) lifetime: Option<String>,
    /// Optional trait interface for trait-based resolution
    pub(crate) interface: Option<Type>,
    /// Whether this service participates in the link-time DI registry.
    pub(crate) enabled: bool,
}

impl Default for ServiceArgs {
    fn default() -> Self {
        Self {
            lifetime: None,
            interface: None,
            enabled: true,
        }
    }
}

impl Parse for ServiceArgs {
    fn parse(input: ParseStream) -> Result<Self> {
        let mut args = ServiceArgs::default();
        let mut activation_seen = false;

        while !input.is_empty() {
            let ident: Ident = input.parse()?;

            match ident.to_string().as_str() {
                "disabled" => {
                    if activation_seen {
                        return Err(syn::Error::new(
                            ident.span(),
                            "service activation may be specified only once",
                        ));
                    }
                    if input.peek(Token![=]) {
                        return Err(syn::Error::new(
                            ident.span(),
                            "`disabled` is a flag; use `disabled` without a value",
                        ));
                    }
                    args.enabled = false;
                    activation_seen = true;
                }
                "lifetime" => {
                    input.parse::<Token![=]>()?;
                    let lit: LitStr = input.parse()?;
                    args.lifetime = Some(lit.value());
                }
                "interface" => {
                    input.parse::<Token![=]>()?;
                    let ty: Type = input.parse()?;
                    args.interface = Some(ty);
                }
                "enabled" => {
                    if activation_seen {
                        return Err(syn::Error::new(
                            ident.span(),
                            "service activation may be specified only once",
                        ));
                    }
                    input.parse::<Token![=]>()?;
                    let enabled: LitBool = input.parse()?;
                    args.enabled = enabled.value;
                    activation_seen = true;
                }
                _ => {
                    return Err(syn::Error::new(
                        ident.span(),
                        format!("Unknown service parameter: {ident}"),
                    ));
                }
            }

            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            }
        }

        Ok(args)
    }
}

/// Parse service lifetime string to ServiceLifetime enum
pub(crate) fn parse_lifetime(
    lifetime_str: &str,
    runtime: &proc_macro2::TokenStream,
) -> syn::Result<proc_macro2::TokenStream> {
    use quote::quote;

    match lifetime_str {
        "Singleton" => Ok(quote! { #runtime::ServiceLifetime::Singleton }),
        "Scoped" => Ok(quote! { #runtime::ServiceLifetime::Scoped }),
        "Transient" => Ok(quote! { #runtime::ServiceLifetime::Transient }),
        _ => Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            format!(
                "Invalid lifetime: '{lifetime_str}'. Valid options: Singleton, Scoped, Transient"
            ),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn services_are_enabled_by_default() {
        let args: ServiceArgs = syn::parse_str("interface = dyn PaymentService").unwrap();
        assert!(args.enabled);
        assert!(matches!(args.interface, Some(Type::TraitObject(_))));
    }

    #[test]
    fn accepts_canonical_disabled_flag() {
        let args: ServiceArgs = syn::parse_str("lifetime = \"Singleton\", disabled").unwrap();
        assert!(!args.enabled);
    }

    #[test]
    fn accepts_backward_friendly_enabled_false() {
        let args: ServiceArgs = syn::parse_str("enabled = false").unwrap();
        assert!(!args.enabled);
    }

    #[test]
    fn disabled_flag_rejects_a_value() {
        let error = match syn::parse_str::<ServiceArgs>("disabled = true") {
            Ok(_) => panic!("disabled flag unexpectedly accepted a value"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("without a value"));
    }

    #[test]
    fn removed_config_parameter_is_rejected() {
        let error = match syn::parse_str::<ServiceArgs>("config = AppConfig") {
            Ok(_) => panic!("removed config parameter unexpectedly accepted"),
            Err(error) => error,
        };
        assert_eq!(error.to_string(), "Unknown service parameter: config");
    }
}
