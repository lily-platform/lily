use syn::punctuated::Punctuated;
use syn::{Attribute, Ident, LitInt, LitStr, Token, Type};

pub(crate) fn is_named(attribute: &Attribute, name: &str) -> bool {
    attribute.path().is_ident(name)
}

pub(crate) fn named_attributes<'a>(attributes: &'a [Attribute], name: &str) -> Vec<&'a Attribute> {
    attributes
        .iter()
        .filter(|attribute| is_named(attribute, name))
        .collect()
}

pub(crate) fn reject_duplicate_authority(
    attributes: &[&Attribute],
    authority: &str,
) -> syn::Result<()> {
    if let Some(duplicate) = attributes.get(1) {
        return Err(syn::Error::new_spanned(
            duplicate,
            format!("duplicate {authority} attribute"),
        ));
    }
    Ok(())
}

pub(crate) fn parse_string(attribute: &Attribute, context: &str) -> syn::Result<LitStr> {
    attribute.parse_args::<LitStr>().map_err(|_| {
        syn::Error::new_spanned(
            attribute,
            format!("{context} must contain exactly one string literal"),
        )
    })
}

pub(crate) fn parse_type_list(attribute: &Attribute, context: &str) -> syn::Result<Vec<Type>> {
    let types = attribute.parse_args_with(Punctuated::<Type, Token![,]>::parse_terminated)?;
    if types.is_empty() {
        return Err(syn::Error::new_spanned(
            attribute,
            format!("{context} requires at least one type"),
        ));
    }
    Ok(types.into_iter().collect())
}

pub(crate) fn parse_optional_single_type(
    attributes: &[Attribute],
    name: &str,
    context: &str,
) -> syn::Result<Option<Type>> {
    let attributes = named_attributes(attributes, name);
    reject_duplicate_authority(&attributes, context)?;
    let Some(attribute) = attributes.first() else {
        return Ok(None);
    };
    let types = parse_type_list(attribute, context)?;
    if types.len() != 1 {
        return Err(syn::Error::new_spanned(
            attribute,
            format!("{context} requires exactly one type"),
        ));
    }
    Ok(types.into_iter().next())
}

struct TimeoutArgument {
    name: Ident,
    _equals: Token![=],
    value: LitInt,
}

impl syn::parse::Parse for TimeoutArgument {
    fn parse(input: syn::parse::ParseStream<'_>) -> syn::Result<Self> {
        Ok(Self {
            name: input.parse()?,
            _equals: input.parse()?,
            value: input.parse()?,
        })
    }
}

pub(crate) fn parse_timeout_seconds(attribute: &Attribute) -> syn::Result<u64> {
    const MAX_EXECUTION_TIMEOUT_SECS: u64 = 300;
    let argument = attribute.parse_args::<TimeoutArgument>().map_err(|_| {
        syn::Error::new_spanned(attribute, "timeout must use `#[timeout(seconds = N)]`")
    })?;
    if argument.name != "seconds" {
        return Err(syn::Error::new_spanned(
            argument.name,
            "timeout accepts only the `seconds` argument",
        ));
    }
    let value = argument.value;
    let value = value.base10_parse::<u64>()?;
    if value == 0 {
        return Err(syn::Error::new_spanned(
            attribute,
            "timeout must be greater than zero seconds",
        ));
    }
    if value > MAX_EXECUTION_TIMEOUT_SECS {
        return Err(syn::Error::new_spanned(
            attribute,
            format!("timeout must not exceed {MAX_EXECUTION_TIMEOUT_SECS} seconds"),
        ));
    }
    Ok(value)
}

pub(crate) fn validate_route_token(
    value: &LitStr,
    kind: &str,
    maximum_bytes: usize,
) -> syn::Result<()> {
    let text = value.value();
    if text.is_empty() {
        return Err(syn::Error::new(
            value.span(),
            format!("{kind} cannot be empty"),
        ));
    }
    if text.len() > maximum_bytes {
        return Err(syn::Error::new(
            value.span(),
            format!("{kind} cannot exceed {maximum_bytes} bytes"),
        ));
    }
    if !text
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(syn::Error::new(
            value.span(),
            format!("{kind} may contain only ASCII letters, digits, `-`, `_`, and `.`"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::parse_quote;

    #[test]
    fn timeout_parser_enforces_the_runtime_execution_bound() {
        let maximum: Attribute = parse_quote!(#[timeout(seconds = 300)]);
        assert_eq!(parse_timeout_seconds(&maximum).unwrap(), 300);

        let oversized: Attribute = parse_quote!(#[timeout(seconds = 301)]);
        let error = parse_timeout_seconds(&oversized).unwrap_err();
        assert!(error.to_string().contains("must not exceed 300 seconds"));
    }

    #[test]
    fn route_tokens_are_exact_and_bounded() {
        assert!(
            validate_route_token(
                &LitStr::new("chat.v2", proc_macro2::Span::call_site()),
                "namespace",
                128,
            )
            .is_ok()
        );
        assert!(
            validate_route_token(
                &LitStr::new("/", proc_macro2::Span::call_site()),
                "namespace",
                128,
            )
            .is_err()
        );
        assert!(
            validate_route_token(
                &LitStr::new("chat:send", proc_macro2::Span::call_site()),
                "event",
                256,
            )
            .is_err()
        );
        assert!(
            validate_route_token(
                &LitStr::new(&"a".repeat(129), proc_macro2::Span::call_site()),
                "namespace",
                128,
            )
            .is_err()
        );
        assert!(
            validate_route_token(
                &LitStr::new(&"a".repeat(256), proc_macro2::Span::call_site()),
                "event",
                256,
            )
            .is_ok()
        );
    }

    #[test]
    fn optional_single_type_rejects_duplicate_and_list_authority() {
        let attributes: Vec<Attribute> = vec![parse_quote!(#[payload_codec(JsonCodec)])];
        let parsed =
            parse_optional_single_type(&attributes, "payload_codec", "controller payload codec")
                .expect("one codec type is valid");
        assert!(matches!(parsed, Some(Type::Path(_))));

        let attributes: Vec<Attribute> = vec![
            parse_quote!(#[payload_codec(JsonCodec)]),
            parse_quote!(#[payload_codec(BinaryCodec)]),
        ];
        assert!(
            parse_optional_single_type(&attributes, "payload_codec", "controller payload codec",)
                .is_err()
        );

        let attributes: Vec<Attribute> =
            vec![parse_quote!(#[payload_codec(JsonCodec, BinaryCodec)])];
        assert!(
            parse_optional_single_type(&attributes, "payload_codec", "controller payload codec",)
                .is_err()
        );
    }
}
