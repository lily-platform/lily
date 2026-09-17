use std::collections::HashSet;

use quote::ToTokens;
use syn::punctuated::Punctuated;
use syn::{Attribute, LitStr, Token, Type};

pub(crate) const ROUTE_ATTRIBUTES: &[&str] = &[
    "get", "post", "put", "patch", "delete", "head", "options", "route",
];

pub(crate) fn is_named(attribute: &Attribute, name: &str) -> bool {
    attribute.path().is_ident(name)
}

pub(crate) fn named_attributes<'a>(attributes: &'a [Attribute], name: &str) -> Vec<&'a Attribute> {
    attributes
        .iter()
        .filter(|attribute| is_named(attribute, name))
        .collect()
}

pub(crate) fn parse_string(attribute: &Attribute, label: &str) -> syn::Result<LitStr> {
    attribute.parse_args::<LitStr>().map_err(|error| {
        syn::Error::new(
            error.span(),
            format!("{label} must contain exactly one string literal"),
        )
    })
}

pub(crate) fn parse_type_list(attribute: &Attribute, label: &str) -> syn::Result<Vec<Type>> {
    let types = attribute
        .parse_args_with(Punctuated::<Type, Token![,]>::parse_terminated)
        .map_err(|error| {
            syn::Error::new(
                error.span(),
                format!("{label} must contain only a comma-separated type list"),
            )
        })?;
    if types.is_empty() {
        return Err(syn::Error::new_spanned(
            attribute,
            format!("{label} requires at least one type"),
        ));
    }

    let mut seen = HashSet::new();
    for ty in &types {
        let identity = ty.to_token_stream().to_string();
        if !seen.insert(identity) {
            return Err(syn::Error::new_spanned(
                ty,
                format!("{label} contains the same type more than once"),
            ));
        }
    }
    Ok(types.into_iter().collect())
}

pub(crate) fn parse_single_type(attribute: &Attribute, label: &str) -> syn::Result<Type> {
    let types = parse_type_list(attribute, label)?;
    if types.len() != 1 {
        return Err(syn::Error::new_spanned(
            attribute,
            format!("{label} accepts exactly one policy type"),
        ));
    }
    Ok(types.into_iter().next().expect("one type was validated"))
}

pub(crate) fn reject_duplicate_authority(
    attributes: &[&Attribute],
    label: &str,
) -> syn::Result<()> {
    if let Some(duplicate) = attributes.get(1) {
        return Err(syn::Error::new_spanned(
            duplicate,
            format!("duplicate {label} authority"),
        ));
    }
    Ok(())
}

pub(crate) fn validate_base_path(path: &LitStr) -> syn::Result<()> {
    let value = path.value();
    let normalized = if value == "/" {
        value.as_str()
    } else {
        value.trim_end_matches('/')
    };
    validate_path_value(normalized, path, "controller base path")
}

pub(crate) fn validate_action_path(path: &LitStr) -> syn::Result<()> {
    validate_path_value(&path.value(), path, "controller action path")
}

fn validate_path_value(value: &str, literal: &LitStr, label: &str) -> syn::Result<()> {
    if value.len() > 4 * 1024 {
        return Err(syn::Error::new(
            literal.span(),
            format!("{label} is too long"),
        ));
    }
    if !value.starts_with('/') {
        return Err(syn::Error::new(
            literal.span(),
            format!("{label} must start with '/'"),
        ));
    }
    if value.contains(['?', '#']) {
        return Err(syn::Error::new(
            literal.span(),
            format!("{label} cannot contain a query string or fragment"),
        ));
    }
    if value != "/" && value.split('/').skip(1).any(str::is_empty) {
        return Err(syn::Error::new(
            literal.span(),
            format!("{label} cannot contain an empty segment"),
        ));
    }

    let segments = value.split('/').skip(1).collect::<Vec<_>>();
    let mut parameters = HashSet::new();
    for (index, segment) in segments.iter().enumerate() {
        let parameter = if let Some(parameter) = segment.strip_prefix(':') {
            Some(parameter)
        } else if let Some(parameter) = segment.strip_prefix('*') {
            if index + 1 != segments.len() {
                return Err(syn::Error::new(
                    literal.span(),
                    format!("{label} catch-all parameter must be the final segment"),
                ));
            }
            Some(parameter)
        } else {
            None
        };
        let Some(parameter) = parameter else {
            continue;
        };
        if !is_parameter_name(parameter) {
            return Err(syn::Error::new(
                literal.span(),
                format!("{label} contains an invalid parameter name"),
            ));
        }
        if !parameters.insert(parameter) {
            return Err(syn::Error::new(
                literal.span(),
                format!("{label} contains a duplicate parameter name"),
            ));
        }
    }
    Ok(())
}

fn is_parameter_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    matches!(bytes.next(), Some(b'a'..=b'z' | b'A'..=b'Z' | b'_'))
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

pub(crate) fn validate_http_method(method: &LitStr) -> syn::Result<String> {
    let span = method.span();
    let method = method.value();
    if method.is_empty() || method.len() > 32 || !method.bytes().all(is_http_token_byte) {
        return Err(syn::Error::new(
            span,
            "route method must be a non-empty HTTP token of at most 32 bytes",
        ));
    }
    Ok(method.to_ascii_uppercase())
}

fn is_http_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}
