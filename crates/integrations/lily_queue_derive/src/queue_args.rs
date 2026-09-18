//! Queue attribute argument parsing
//!
//! Handles parsing of #[queue(...)] attributes for queue handler methods.

use syn::{
    Ident, LitInt, LitStr, Result, Token,
    parse::{Parse, ParseStream},
};

const MAX_QUEUE_TRANSPORT_IDENTITY_BYTES: usize = 200;

fn queue_transport_identity_is_valid(value: &str) -> bool {
    (1..=MAX_QUEUE_TRANSPORT_IDENTITY_BYTES).contains(&value.len())
        && value.trim() == value
        && !value.chars().any(char::is_control)
}

/// Arguments for the #[queue(...)] attribute
///
/// Supports the following syntax:
/// - `#[queue("queue.name", version = 1, content = "json")]`
/// - `#[queue("queue.name", version = 1, content = "json", delivery_guarantee = "transactional_inbox")]`
#[derive(Debug, Clone)]
pub(crate) struct QueueArgs {
    /// Queue name (required)
    pub(crate) name: LitStr,

    /// Exact schema version accepted by this handler.
    pub(crate) version: u16,

    /// Exact content-kind token accepted by this handler.
    pub(crate) content: LitStr,

    /// Database-neutral delivery guarantee requested by the handler.
    pub(crate) delivery_guarantee: DeliveryGuaranteeArg,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeliveryGuaranteeArg {
    AtLeastOnce,
    TransactionalInbox,
}

impl Parse for QueueArgs {
    fn parse(input: ParseStream) -> Result<Self> {
        let name: LitStr = input.parse()?;
        let value = name.value();
        if !queue_transport_identity_is_valid(&value) {
            return Err(syn::Error::new_spanned(
                &name,
                "queue name must contain 1..=200 trimmed, control-free bytes",
            ));
        }
        if input.is_empty() {
            return Err(input.error(
                "#[queue] requires `version = 1..=65535` and exact `content = \"...\"` arguments",
            ));
        }
        input.parse::<Token![,]>()?;

        let mut version = None;
        let mut content = None;
        let mut delivery_guarantee = None;
        while !input.is_empty() {
            let key: Ident = input.parse()?;
            input.parse::<Token![=]>()?;
            match key.to_string().as_str() {
                "version" => {
                    if version.is_some() {
                        return Err(syn::Error::new_spanned(key, "duplicate `version` argument"));
                    }
                    let literal: LitInt = input.parse()?;
                    let parsed = literal.base10_parse::<u16>().map_err(|_| {
                        syn::Error::new_spanned(
                            &literal,
                            "queue schema version must be an integer in 1..=65535",
                        )
                    })?;
                    if parsed == 0 {
                        return Err(syn::Error::new_spanned(
                            literal,
                            "queue schema version must be an integer in 1..=65535",
                        ));
                    }
                    version = Some(parsed);
                }
                "content" => {
                    if content.is_some() {
                        return Err(syn::Error::new_spanned(key, "duplicate `content` argument"));
                    }
                    let literal: LitStr = input.parse()?;
                    validate_content_kind(&literal)?;
                    content = Some(literal);
                }
                "delivery_guarantee" => {
                    if delivery_guarantee.is_some() {
                        return Err(syn::Error::new_spanned(
                            key,
                            "duplicate `delivery_guarantee` argument",
                        ));
                    }
                    let literal: LitStr = input.parse()?;
                    delivery_guarantee = Some(match literal.value().as_str() {
                        "at_least_once" => DeliveryGuaranteeArg::AtLeastOnce,
                        "transactional_inbox" => DeliveryGuaranteeArg::TransactionalInbox,
                        _ => {
                            return Err(syn::Error::new_spanned(
                                literal,
                                "delivery_guarantee must be `at_least_once` or `transactional_inbox`",
                            ));
                        }
                    });
                }
                _ => {
                    return Err(syn::Error::new_spanned(
                        key,
                        "unknown #[queue] argument; policy belongs in rabbitmq.topology.queues",
                    ));
                }
            }

            if input.is_empty() {
                break;
            }
            input.parse::<Token![,]>()?;
            if input.is_empty() {
                return Err(input.error("trailing comma is not accepted in #[queue]"));
            }
        }

        let version =
            version.ok_or_else(|| input.error("#[queue] requires `version = 1..=65535`"))?;
        let content = content
            .ok_or_else(|| input.error("#[queue] requires an exact `content = \"...\"` token"))?;

        Ok(QueueArgs {
            name,
            version,
            content,
            delivery_guarantee: delivery_guarantee.unwrap_or(DeliveryGuaranteeArg::AtLeastOnce),
        })
    }
}

fn validate_content_kind(content: &LitStr) -> Result<()> {
    let value = content.value();
    let bytes = value.as_bytes();
    let valid = (1..=64).contains(&bytes.len())
        && (bytes[0].is_ascii_lowercase() || bytes[0].is_ascii_digit())
        && bytes.iter().all(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(byte, b'.' | b'_' | b'+' | b'-')
        });
    if valid {
        Ok(())
    } else {
        Err(syn::Error::new_spanned(
            content,
            "content kind must contain 1..=64 bytes and match [a-z0-9][a-z0-9._+-]*",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;
    use syn::parse2;

    #[test]
    fn supported_schema_versions_are_positive_u16_values() {
        let tokens = quote! { "user.created", version = 1, content = "json" };
        let args: QueueArgs = parse2(tokens).unwrap();

        assert_eq!(args.name.value(), "user.created");
        assert_eq!(args.version, 1);
        assert_eq!(args.content.value(), "json");
        assert_eq!(args.delivery_guarantee, DeliveryGuaranteeArg::AtLeastOnce);

        let version_two: QueueArgs =
            parse2(quote! { "user.created", version = 2, content = "json" }).unwrap();
        assert_eq!(version_two.version, 2);

        let maximum: QueueArgs =
            parse2(quote! { "user.created", version = 65535, content = "json" }).unwrap();
        assert_eq!(maximum.version, u16::MAX);
    }

    #[test]
    fn transactional_delivery_guarantee_is_explicit_and_strict() {
        let args: QueueArgs = parse2(quote! {
            "user.created", version = 1, content = "json",
            delivery_guarantee = "transactional_inbox"
        })
        .unwrap();
        assert_eq!(
            args.delivery_guarantee,
            DeliveryGuaranteeArg::TransactionalInbox
        );

        assert!(
            parse2::<QueueArgs>(quote! {
                "user.created", version = 1, content = "json",
                delivery_guarantee = "exactly_once"
            })
            .is_err()
        );
        assert!(
            parse2::<QueueArgs>(quote! {
                "user.created", version = 1, content = "json",
                delivery_guarantee = "at_least_once",
                delivery_guarantee = "transactional_inbox"
            })
            .is_err()
        );
    }

    #[test]
    fn test_rejects_retry_policy() {
        let tokens = quote! { "user.created", version = 1, content = "json", retry = 3 };
        let error = parse2::<QueueArgs>(tokens).unwrap_err();
        assert!(error.to_string().contains("policy belongs"));
    }

    #[test]
    fn test_rejects_batch_policy() {
        let tokens = quote! { "user.created", version = 1, content = "json", batch = true };
        assert!(parse2::<QueueArgs>(tokens).is_err());
    }

    #[test]
    fn queue_name_only_zero_and_overflow_versions_are_rejected() {
        assert!(parse2::<QueueArgs>(quote! { "user.created" }).is_err());
        assert!(
            parse2::<QueueArgs>(quote! { "user.created", version = 0, content = "json" }).is_err()
        );
        assert!(
            parse2::<QueueArgs>(quote! { "user.created", version = 65536, content = "json" })
                .is_err()
        );
    }

    #[test]
    fn queue_identity_bounds_are_exact_and_control_free() {
        let exact = "q".repeat(MAX_QUEUE_TRANSPORT_IDENTITY_BYTES);
        assert!(parse2::<QueueArgs>(quote! { #exact, version = 1, content = "json" }).is_ok());

        for invalid in [
            " queue".to_string(),
            "queue ".to_string(),
            "queue\nforged".to_string(),
            "q".repeat(MAX_QUEUE_TRANSPORT_IDENTITY_BYTES + 1),
        ] {
            assert!(
                parse2::<QueueArgs>(quote! { #invalid, version = 1, content = "json" }).is_err(),
                "{invalid:?}"
            );
        }
    }

    #[test]
    fn invalid_content_tokens_are_rejected() {
        for tokens in [
            quote! { "user.created", version = 1, content = "" },
            quote! { "user.created", version = 1, content = "Json" },
            quote! { "user.created", version = 1, content = "json value" },
        ] {
            assert!(parse2::<QueueArgs>(tokens).is_err());
        }
    }
}
