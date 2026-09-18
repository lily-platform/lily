#[cfg(feature = "asyncapi")]
use proc_macro2::TokenStream;
#[cfg(feature = "asyncapi")]
use quote::quote;
#[cfg(feature = "asyncapi")]
use syn::{Attribute, LitBool, LitStr, Type};

#[cfg(feature = "asyncapi")]
const MAX_TAGS_PER_LEVEL: usize = 16;
#[cfg(feature = "asyncapi")]
const MAX_SECURITY_PER_LEVEL: usize = 16;
#[cfg(feature = "asyncapi")]
const MAX_EXAMPLES: usize = 8;
#[cfg(feature = "asyncapi")]
const MAX_EXAMPLE_BYTES: usize = 16 * 1024;
#[cfg(feature = "asyncapi")]
const MAX_EXAMPLE_TOTAL_BYTES: usize = 64 * 1024;
#[cfg(feature = "asyncapi")]
const MAX_CONTENT_TYPE_BYTES: usize = 256;
#[cfg(feature = "asyncapi")]
const MAX_SUMMARY_BYTES: usize = 256;
#[cfg(feature = "asyncapi")]
const MAX_DESCRIPTION_BYTES: usize = 8 * 1024;
#[cfg(feature = "asyncapi")]
const MAX_OPERATION_ID_BYTES: usize = 128;
#[cfg(feature = "asyncapi")]
const MAX_TAG_OR_SECURITY_BYTES: usize = 64;

#[cfg(feature = "asyncapi")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Status {
    Documented,
    Skipped,
}

#[cfg(feature = "asyncapi")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Scope {
    Service,
    Handler,
}

#[cfg(feature = "asyncapi")]
#[derive(Clone, Debug)]
pub(crate) struct AsyncApiArgs {
    pub(crate) status: Status,
    pub(crate) tags: Vec<LitStr>,
    pub(crate) summary: Option<LitStr>,
    pub(crate) description: Option<LitStr>,
    pub(crate) operation_id: Option<LitStr>,
    pub(crate) deprecated: Option<bool>,
    pub(crate) security: Vec<LitStr>,
    pub(crate) examples: Vec<LitStr>,
    pub(crate) schema: Option<Type>,
    pub(crate) opaque: bool,
    pub(crate) content_type: Option<LitStr>,
}

#[cfg(feature = "asyncapi")]
impl AsyncApiArgs {
    fn documented() -> Self {
        Self {
            status: Status::Documented,
            tags: Vec::new(),
            summary: None,
            description: None,
            operation_id: None,
            deprecated: None,
            security: Vec::new(),
            examples: Vec::new(),
            schema: None,
            opaque: false,
            content_type: None,
        }
    }

    fn has_documentation_fields(&self) -> bool {
        !self.tags.is_empty()
            || self.summary.is_some()
            || self.description.is_some()
            || self.operation_id.is_some()
            || self.deprecated.is_some()
            || !self.security.is_empty()
            || !self.examples.is_empty()
            || self.schema.is_some()
            || self.opaque
            || self.content_type.is_some()
    }
}

#[cfg(feature = "asyncapi")]
#[derive(Clone, Debug)]
pub(crate) enum EffectiveAsyncApi {
    Unspecified,
    Skipped,
    Documented(Box<AsyncApiArgs>),
}

#[cfg(feature = "asyncapi")]
pub(crate) fn parse(attribute: &Attribute, scope: Scope) -> syn::Result<AsyncApiArgs> {
    let mut arguments = AsyncApiArgs::documented();
    let mut explicit_documented = false;
    let mut explicit_skip = false;

    if matches!(attribute.meta, syn::Meta::Path(_)) {
        return Ok(arguments);
    }

    attribute.parse_nested_meta(|meta| {
        if meta.path.is_ident("documented") {
            reject_marker_value(&meta, "documented")?;
            if explicit_documented {
                return Err(meta.error("duplicate `documented` marker"));
            }
            explicit_documented = true;
            return Ok(());
        }
        if meta.path.is_ident("skip") {
            reject_marker_value(&meta, "skip")?;
            if explicit_skip {
                return Err(meta.error("duplicate `skip` marker"));
            }
            explicit_skip = true;
            arguments.status = Status::Skipped;
            return Ok(());
        }
        if meta.path.is_ident("tag") {
            let value: LitStr = meta.value()?.parse()?;
            validate_required_literal("tag", &value, MAX_TAG_OR_SECURITY_BYTES)?;
            push_unique(&mut arguments.tags, value, "tag")?;
            return Ok(());
        }
        if meta.path.is_ident("security") {
            let value: LitStr = meta.value()?.parse()?;
            validate_required_literal("security", &value, MAX_TAG_OR_SECURITY_BYTES)?;
            push_unique(&mut arguments.security, value, "security")?;
            return Ok(());
        }
        if meta.path.is_ident("summary") {
            set_string(
                &mut arguments.summary,
                &meta,
                "summary",
                MAX_SUMMARY_BYTES,
                false,
            )?;
            return Ok(());
        }
        if meta.path.is_ident("description") {
            set_string(
                &mut arguments.description,
                &meta,
                "description",
                MAX_DESCRIPTION_BYTES,
                true,
            )?;
            return Ok(());
        }
        if meta.path.is_ident("operation_id") {
            if scope == Scope::Service {
                return Err(meta.error("`operation_id` is valid only on a queue handler"));
            }
            set_string(
                &mut arguments.operation_id,
                &meta,
                "operation_id",
                MAX_OPERATION_ID_BYTES,
                false,
            )?;
            if let Some(value) = arguments.operation_id.as_ref() {
                if !identifier_is_valid(&value.value()) {
                    return Err(syn::Error::new_spanned(
                        value,
                        "operation_id must match [A-Za-z0-9._-]+ and contain at most 128 bytes",
                    ));
                }
            }
            return Ok(());
        }
        if meta.path.is_ident("deprecated") {
            if scope == Scope::Service {
                return Err(meta.error("`deprecated` is valid only on a queue handler"));
            }
            if arguments.deprecated.is_some() {
                return Err(meta.error("duplicate `deprecated` option"));
            }
            arguments.deprecated = if meta.input.peek(syn::Token![=]) {
                Some(meta.value()?.parse::<LitBool>()?.value)
            } else {
                Some(true)
            };
            return Ok(());
        }
        if meta.path.is_ident("example") {
            if scope == Scope::Service {
                return Err(meta.error("`example` is valid only on a queue handler"));
            }
            let example: LitStr = meta.value()?.parse()?;
            validate_example(&example)?;
            arguments.examples.push(example);
            return Ok(());
        }
        if meta.path.is_ident("schema") {
            if scope == Scope::Service {
                return Err(meta.error("`schema` is valid only on a queue handler"));
            }
            if arguments.schema.is_some() {
                return Err(meta.error("duplicate `schema` option"));
            }
            arguments.schema = Some(meta.value()?.parse()?);
            return Ok(());
        }
        if meta.path.is_ident("opaque") {
            if scope == Scope::Service {
                return Err(meta.error("`opaque` is valid only on a queue handler"));
            }
            reject_marker_value(&meta, "opaque")?;
            if arguments.opaque {
                return Err(meta.error("duplicate `opaque` marker"));
            }
            arguments.opaque = true;
            return Ok(());
        }
        if meta.path.is_ident("content_type") {
            if scope == Scope::Service {
                return Err(meta.error("`content_type` is valid only on a queue handler"));
            }
            set_string(
                &mut arguments.content_type,
                &meta,
                "content_type",
                MAX_CONTENT_TYPE_BYTES,
                false,
            )?;
            return Ok(());
        }
        Err(meta.error(
            "unsupported AsyncAPI option; expected `documented`, `skip`, `tag`, `summary`, `description`, `operation_id`, `deprecated`, `security`, `example`, `schema`, `opaque`, or `content_type`",
        ))
    })?;

    if explicit_documented && explicit_skip {
        return Err(syn::Error::new_spanned(
            attribute,
            "AsyncAPI metadata cannot be both `documented` and `skip`",
        ));
    }
    if explicit_skip && arguments.has_documentation_fields() {
        return Err(syn::Error::new_spanned(
            attribute,
            "`asyncapi(skip)` cannot be combined with documentation metadata",
        ));
    }
    if arguments.tags.len() > MAX_TAGS_PER_LEVEL {
        return Err(syn::Error::new_spanned(
            attribute,
            "AsyncAPI metadata accepts at most 16 tags per level",
        ));
    }
    if arguments.security.len() > MAX_SECURITY_PER_LEVEL {
        return Err(syn::Error::new_spanned(
            attribute,
            "AsyncAPI metadata accepts at most 16 security references per level",
        ));
    }
    if arguments.examples.len() > MAX_EXAMPLES {
        return Err(syn::Error::new_spanned(
            attribute,
            "AsyncAPI message metadata accepts at most 8 examples",
        ));
    }
    if arguments.schema.is_some() && arguments.opaque {
        return Err(syn::Error::new_spanned(
            attribute,
            "`schema` and `opaque` are mutually exclusive payload authorities",
        ));
    }
    let has_explicit_payload = arguments.schema.is_some() || arguments.opaque;
    if has_explicit_payload != arguments.content_type.is_some() {
        return Err(syn::Error::new_spanned(
            attribute,
            "explicit `schema` or `opaque` payload metadata must be paired with `content_type`",
        ));
    }

    validate_example_total(&arguments)?;

    Ok(arguments)
}

#[cfg(feature = "asyncapi")]
pub(crate) fn inherit(
    service: Option<&AsyncApiArgs>,
    handler: Option<&AsyncApiArgs>,
) -> syn::Result<EffectiveAsyncApi> {
    if service.is_some_and(|value| value.status == Status::Skipped) {
        if handler.is_some_and(|value| value.status == Status::Documented) {
            return Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                "a documented queue handler cannot reopen an AsyncAPI-skipped queue service",
            ));
        }
        return Ok(EffectiveAsyncApi::Skipped);
    }

    if handler.is_some_and(|value| value.status == Status::Skipped) {
        return Ok(EffectiveAsyncApi::Skipped);
    }

    match (service, handler) {
        (None, None) => Ok(EffectiveAsyncApi::Unspecified),
        (Some(parent), None) => Ok(EffectiveAsyncApi::Documented(Box::new(parent.clone()))),
        (None, Some(child)) => Ok(EffectiveAsyncApi::Documented(Box::new(child.clone()))),
        (Some(parent), Some(child)) => {
            let mut effective = child.clone();
            if effective.summary.is_none() {
                effective.summary.clone_from(&parent.summary);
            }
            if effective.description.is_none() {
                effective.description.clone_from(&parent.description);
            }

            let mut tags = parent.tags.clone();
            for child_tag in &child.tags {
                if !tags.iter().any(|tag| tag.value() == child_tag.value()) {
                    tags.push(child_tag.clone());
                }
            }
            effective.tags = tags;
            if child.security.is_empty() {
                effective.security.clone_from(&parent.security);
            }
            Ok(EffectiveAsyncApi::Documented(Box::new(effective)))
        }
    }
}

#[cfg(feature = "asyncapi")]
pub(crate) fn generate(
    runtime: &TokenStream,
    effective: &EffectiveAsyncApi,
    argument_types: &[Type],
) -> TokenStream {
    match effective {
        EffectiveAsyncApi::Unspecified => {
            quote!(#runtime::__private::QueueAsyncApiRegistration::unspecified())
        }
        EffectiveAsyncApi::Skipped => {
            quote!(#runtime::__private::QueueAsyncApiRegistration::skipped())
        }
        EffectiveAsyncApi::Documented(metadata) => {
            let summary = option_literal(&metadata.summary);
            let description = option_literal(&metadata.description);
            let operation_id = option_literal(&metadata.operation_id);
            let tags = &metadata.tags;
            let security = &metadata.security;
            let deprecated = metadata.deprecated.unwrap_or(false);
            let examples = &metadata.examples;
            let payload = if let Some(schema) = metadata.schema.as_ref() {
                let content_type = metadata
                    .content_type
                    .as_ref()
                    .expect("explicit schema was validated with a content type");
                quote! {
                    #runtime::__private::QueueAsyncApiPayload::explicit_generated(
                        #runtime::__private::SchemaFactory::inbound::<#schema>(),
                        #content_type,
                    )
                }
            } else if metadata.opaque {
                let content_type = metadata
                    .content_type
                    .as_ref()
                    .expect("opaque payload was validated with a content type");
                quote!(#runtime::__private::QueueAsyncApiPayload::opaque(#content_type))
            } else {
                let arguments = tuple_type(argument_types);
                quote!(#runtime::__private::delivery_asyncapi_payload::<#arguments, _>())
            };

            quote! {
                #runtime::__private::QueueAsyncApiRegistration {
                    status: #runtime::__private::QueueAsyncApiStatus::Documented,
                    summary: #summary,
                    description: #description,
                    operation_id: #operation_id,
                    tags: ::std::vec![#(#tags),*],
                    security: ::std::vec![#(#security),*],
                    deprecated: #deprecated,
                    examples: ::std::vec![#(#examples),*],
                    payload: #payload,
                }
            }
        }
    }
}

#[cfg(feature = "asyncapi")]
fn tuple_type(arguments: &[Type]) -> TokenStream {
    if arguments.is_empty() {
        quote!(())
    } else {
        quote!((#(#arguments,)*))
    }
}

#[cfg(feature = "asyncapi")]
fn option_literal(value: &Option<LitStr>) -> TokenStream {
    value.as_ref().map_or_else(
        || quote!(::std::option::Option::None),
        |value| quote!(::std::option::Option::Some(#value)),
    )
}

#[cfg(feature = "asyncapi")]
fn reject_marker_value(meta: &syn::meta::ParseNestedMeta<'_>, name: &str) -> syn::Result<()> {
    if meta.input.peek(syn::Token![=]) || meta.input.peek(syn::token::Paren) {
        return Err(meta.error(format!("`{name}` is a marker and accepts no value")));
    }
    Ok(())
}

#[cfg(feature = "asyncapi")]
fn set_string(
    slot: &mut Option<LitStr>,
    meta: &syn::meta::ParseNestedMeta<'_>,
    name: &str,
    maximum: usize,
    description: bool,
) -> syn::Result<()> {
    if slot.is_some() {
        return Err(meta.error(format!("duplicate `{name}` option")));
    }
    let value: LitStr = meta.value()?.parse()?;
    if description {
        validate_description_literal(name, &value, maximum)?;
    } else {
        validate_required_literal(name, &value, maximum)?;
    }
    *slot = Some(value);
    Ok(())
}

#[cfg(feature = "asyncapi")]
fn push_unique(target: &mut Vec<LitStr>, value: LitStr, name: &str) -> syn::Result<()> {
    if target
        .iter()
        .any(|existing| existing.value() == value.value())
    {
        return Err(syn::Error::new_spanned(
            value,
            format!("duplicate `{name}` value at the same metadata level"),
        ));
    }
    target.push(value);
    Ok(())
}

#[cfg(feature = "asyncapi")]
fn validate_required_literal(name: &str, literal: &LitStr, maximum: usize) -> syn::Result<()> {
    let value = literal.value();
    if value.is_empty()
        || value.len() > maximum
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(syn::Error::new_spanned(
            literal,
            format!("{name} must contain 1..={maximum} trimmed, control-free UTF-8 bytes"),
        ));
    }
    Ok(())
}

#[cfg(feature = "asyncapi")]
fn validate_description_literal(name: &str, literal: &LitStr, maximum: usize) -> syn::Result<()> {
    let value = literal.value();
    if value.is_empty()
        || value.len() > maximum
        || value.trim() != value
        || value
            .chars()
            .any(|character| character.is_control() && !matches!(character, '\t' | '\r' | '\n'))
    {
        return Err(syn::Error::new_spanned(
            literal,
            format!(
                "{name} must contain 1..={maximum} trimmed UTF-8 bytes and no forbidden control characters"
            ),
        ));
    }
    Ok(())
}

#[cfg(feature = "asyncapi")]
fn identifier_is_valid(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

#[cfg(feature = "asyncapi")]
fn validate_example(literal: &LitStr) -> syn::Result<()> {
    let value: serde_json::Value = serde_json::from_str(&literal.value()).map_err(|error| {
        syn::Error::new_spanned(
            literal,
            format!("AsyncAPI example is not valid JSON: {error}"),
        )
    })?;
    let canonical = serde_json::to_vec(&value).map_err(|error| {
        syn::Error::new_spanned(
            literal,
            format!("AsyncAPI example could not be serialized: {error}"),
        )
    })?;
    if canonical.len() > MAX_EXAMPLE_BYTES {
        return Err(syn::Error::new_spanned(
            literal,
            "one canonical AsyncAPI example exceeds 16 KiB",
        ));
    }
    Ok(())
}

#[cfg(feature = "asyncapi")]
pub(crate) fn validate_example_total(arguments: &AsyncApiArgs) -> syn::Result<()> {
    let mut total = 0usize;
    for example in &arguments.examples {
        let value: serde_json::Value = serde_json::from_str(&example.value()).map_err(|error| {
            syn::Error::new_spanned(
                example,
                format!("AsyncAPI example is not valid JSON: {error}"),
            )
        })?;
        let bytes = serde_json::to_vec(&value).map_err(|error| {
            syn::Error::new_spanned(
                example,
                format!("AsyncAPI example serialization failed: {error}"),
            )
        })?;
        total = total.checked_add(bytes.len()).ok_or_else(|| {
            syn::Error::new_spanned(example, "AsyncAPI example byte total overflowed")
        })?;
    }
    if total > MAX_EXAMPLE_TOTAL_BYTES {
        return Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            "canonical AsyncAPI examples exceed the 64 KiB per-message total",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "asyncapi")]
    use super::*;
    #[cfg(feature = "asyncapi")]
    use syn::parse_quote;

    #[cfg(feature = "asyncapi")]
    #[test]
    fn inheritance_uses_tag_union_and_security_override() {
        let service = parse(
            &parse_quote!(#[asyncapi(documented, tag = "orders", security = "service")]),
            Scope::Service,
        )
        .unwrap();
        let handler = parse(
            &parse_quote!(#[asyncapi(documented, tag = "created", security = "handler")]),
            Scope::Handler,
        )
        .unwrap();

        let EffectiveAsyncApi::Documented(effective) =
            inherit(Some(&service), Some(&handler)).unwrap()
        else {
            panic!("documented metadata");
        };
        assert_eq!(
            effective.tags.iter().map(LitStr::value).collect::<Vec<_>>(),
            ["orders", "created"]
        );
        assert_eq!(
            effective
                .security
                .iter()
                .map(LitStr::value)
                .collect::<Vec<_>>(),
            ["handler"]
        );
    }

    #[cfg(feature = "asyncapi")]
    #[test]
    fn skipped_service_cannot_be_reopened() {
        let service = parse(&parse_quote!(#[asyncapi(skip)]), Scope::Service).unwrap();
        let handler = parse(&parse_quote!(#[asyncapi(documented)]), Scope::Handler).unwrap();
        assert!(inherit(Some(&service), Some(&handler)).is_err());
    }

    #[cfg(feature = "asyncapi")]
    #[test]
    fn payload_authorities_are_mutually_exclusive_and_bounded() {
        assert!(
            parse(
                &parse_quote!(#[asyncapi(
                    documented,
                    schema = Event,
                    opaque,
                    content_type = "application/json"
                )]),
                Scope::Handler,
            )
            .is_err()
        );
        assert!(
            parse(
                &parse_quote!(#[asyncapi(documented, opaque)]),
                Scope::Handler,
            )
            .is_err()
        );
    }

    #[cfg(feature = "asyncapi")]
    #[test]
    fn content_type_accepts_exact_common_limit_and_rejects_plus_one() {
        let exact = LitStr::new(
            &format!("application/{}", "x".repeat(MAX_CONTENT_TYPE_BYTES - 12)),
            proc_macro2::Span::call_site(),
        );
        let plus_one = LitStr::new(
            &format!("application/{}", "x".repeat(MAX_CONTENT_TYPE_BYTES - 11)),
            proc_macro2::Span::call_site(),
        );

        assert_eq!(exact.value().len(), MAX_CONTENT_TYPE_BYTES);
        assert!(
            parse(
                &parse_quote!(#[asyncapi(documented, opaque, content_type = #exact)]),
                Scope::Handler,
            )
            .is_ok()
        );
        assert_eq!(plus_one.value().len(), MAX_CONTENT_TYPE_BYTES + 1);
        assert!(
            parse(
                &parse_quote!(#[asyncapi(documented, opaque, content_type = #plus_one)]),
                Scope::Handler,
            )
            .is_err()
        );
    }

    #[cfg(feature = "asyncapi")]
    #[test]
    fn text_metadata_bounds_accept_exact_maximum_and_reject_plus_one() {
        let literal = |length| LitStr::new(&"a".repeat(length), proc_macro2::Span::call_site());

        let summary = literal(MAX_SUMMARY_BYTES);
        assert!(
            parse(
                &parse_quote!(#[asyncapi(summary = #summary)]),
                Scope::Handler
            )
            .is_ok()
        );
        let summary = literal(MAX_SUMMARY_BYTES + 1);
        assert!(
            parse(
                &parse_quote!(#[asyncapi(summary = #summary)]),
                Scope::Handler
            )
            .is_err()
        );

        let description = literal(MAX_DESCRIPTION_BYTES);
        assert!(
            parse(
                &parse_quote!(#[asyncapi(description = #description)]),
                Scope::Handler,
            )
            .is_ok()
        );
        let description = literal(MAX_DESCRIPTION_BYTES + 1);
        assert!(
            parse(
                &parse_quote!(#[asyncapi(description = #description)]),
                Scope::Handler,
            )
            .is_err()
        );

        let operation_id = literal(MAX_OPERATION_ID_BYTES);
        assert!(
            parse(
                &parse_quote!(#[asyncapi(operation_id = #operation_id)]),
                Scope::Handler,
            )
            .is_ok()
        );
        let operation_id = literal(MAX_OPERATION_ID_BYTES + 1);
        assert!(
            parse(
                &parse_quote!(#[asyncapi(operation_id = #operation_id)]),
                Scope::Handler,
            )
            .is_err()
        );

        let tag = literal(MAX_TAG_OR_SECURITY_BYTES);
        assert!(parse(&parse_quote!(#[asyncapi(tag = #tag)]), Scope::Handler).is_ok());
        let tag = literal(MAX_TAG_OR_SECURITY_BYTES + 1);
        assert!(parse(&parse_quote!(#[asyncapi(tag = #tag)]), Scope::Handler).is_err());

        let security = literal(MAX_TAG_OR_SECURITY_BYTES);
        assert!(
            parse(
                &parse_quote!(#[asyncapi(security = #security)]),
                Scope::Handler
            )
            .is_ok()
        );
        let security = literal(MAX_TAG_OR_SECURITY_BYTES + 1);
        assert!(
            parse(
                &parse_quote!(#[asyncapi(security = #security)]),
                Scope::Handler,
            )
            .is_err()
        );
    }

    #[cfg(feature = "asyncapi")]
    #[test]
    fn metadata_collection_bounds_accept_exact_maximum_and_reject_plus_one() {
        let tags = (0..MAX_TAGS_PER_LEVEL)
            .map(|index| LitStr::new(&format!("tag-{index}"), proc_macro2::Span::call_site()))
            .collect::<Vec<_>>();
        assert!(parse(&parse_quote!(#[asyncapi(#(tag = #tags),*)]), Scope::Handler,).is_ok());
        let mut too_many_tags = tags;
        too_many_tags.push(LitStr::new("tag-overflow", proc_macro2::Span::call_site()));
        assert!(
            parse(
                &parse_quote!(#[asyncapi(#(tag = #too_many_tags),*)]),
                Scope::Handler,
            )
            .is_err()
        );

        let security = (0..MAX_SECURITY_PER_LEVEL)
            .map(|index| LitStr::new(&format!("security-{index}"), proc_macro2::Span::call_site()))
            .collect::<Vec<_>>();
        assert!(
            parse(
                &parse_quote!(#[asyncapi(#(security = #security),*)]),
                Scope::Handler,
            )
            .is_ok()
        );
        let mut too_many_security = security;
        too_many_security.push(LitStr::new(
            "security-overflow",
            proc_macro2::Span::call_site(),
        ));
        assert!(
            parse(
                &parse_quote!(#[asyncapi(#(security = #too_many_security),*)]),
                Scope::Handler,
            )
            .is_err()
        );

        let examples = (0..MAX_EXAMPLES)
            .map(|index| LitStr::new(&index.to_string(), proc_macro2::Span::call_site()))
            .collect::<Vec<_>>();
        assert!(
            parse(
                &parse_quote!(#[asyncapi(#(example = #examples),*)]),
                Scope::Handler,
            )
            .is_ok()
        );
        let mut too_many_examples = examples;
        too_many_examples.push(LitStr::new("9", proc_macro2::Span::call_site()));
        assert!(
            parse(
                &parse_quote!(#[asyncapi(#(example = #too_many_examples),*)]),
                Scope::Handler,
            )
            .is_err()
        );
    }

    #[cfg(feature = "asyncapi")]
    #[test]
    fn example_byte_bounds_accept_exact_maximum_and_reject_plus_one() {
        let exact_json = format!("\"{}\"", "a".repeat(MAX_EXAMPLE_BYTES - 2));
        let exact = LitStr::new(&exact_json, proc_macro2::Span::call_site());
        assert!(parse(&parse_quote!(#[asyncapi(example = #exact)]), Scope::Handler).is_ok());

        let plus_one_json = format!("\"{}\"", "a".repeat(MAX_EXAMPLE_BYTES - 1));
        let plus_one = LitStr::new(&plus_one_json, proc_macro2::Span::call_site());
        assert!(
            parse(
                &parse_quote!(#[asyncapi(example = #plus_one)]),
                Scope::Handler
            )
            .is_err()
        );

        let exact_total = (0..4).map(|_| exact.clone()).collect::<Vec<_>>();
        assert!(
            parse(
                &parse_quote!(#[asyncapi(#(example = #exact_total),*)]),
                Scope::Handler,
            )
            .is_ok()
        );
        let one_byte = LitStr::new("0", proc_macro2::Span::call_site());
        assert!(
            parse(
                &parse_quote!(#[asyncapi(#(example = #exact_total),*, example = #one_byte)]),
                Scope::Handler,
            )
            .is_err()
        );
    }

    #[cfg(feature = "asyncapi")]
    #[test]
    fn duplicate_same_level_semantic_sets_are_rejected() {
        assert!(
            parse(
                &parse_quote!(#[asyncapi(tag = "orders", tag = "orders")]),
                Scope::Handler,
            )
            .is_err()
        );
        assert!(
            parse(
                &parse_quote!(#[asyncapi(security = "oauth", security = "oauth")]),
                Scope::Handler,
            )
            .is_err()
        );
    }

    #[cfg(feature = "asyncapi")]
    #[test]
    fn one_summary_is_accepted_and_a_duplicate_is_rejected() {
        let metadata = parse(
            &parse_quote!(#[asyncapi(summary = "Consumes an order event")]),
            Scope::Handler,
        )
        .unwrap();

        assert_eq!(
            metadata.summary.as_ref().map(LitStr::value).as_deref(),
            Some("Consumes an order event")
        );
        assert!(
            parse(
                &parse_quote!(#[asyncapi(summary = "First", summary = "Second")]),
                Scope::Handler,
            )
            .is_err()
        );
    }
}
