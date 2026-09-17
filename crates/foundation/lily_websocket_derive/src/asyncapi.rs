use proc_macro2::TokenStream;
use quote::quote;
use syn::{Attribute, LitBool, LitStr};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Status {
    Documented,
    Skipped,
}

pub(crate) struct AsyncApiArgs {
    status: Status,
    tags: Vec<LitStr>,
    summary: Option<LitStr>,
    description: Option<LitStr>,
    operation_id: Option<LitStr>,
    deprecated: Option<bool>,
    security: Vec<LitStr>,
}

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
        }
    }
}

pub(crate) fn parse(attribute: &Attribute) -> syn::Result<AsyncApiArgs> {
    let mut arguments = AsyncApiArgs::documented();
    let mut explicit_documented = false;
    let mut explicit_skip = false;

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
            arguments.tags.push(meta.value()?.parse()?);
            return Ok(());
        }
        if meta.path.is_ident("security") {
            arguments.security.push(meta.value()?.parse()?);
            return Ok(());
        }
        if meta.path.is_ident("summary") {
            set_string(&mut arguments.summary, &meta, "summary")?;
            return Ok(());
        }
        if meta.path.is_ident("description") {
            set_string(&mut arguments.description, &meta, "description")?;
            return Ok(());
        }
        if meta.path.is_ident("operation_id") {
            set_string(&mut arguments.operation_id, &meta, "operation_id")?;
            return Ok(());
        }
        if meta.path.is_ident("deprecated") {
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
        Err(meta.error(
            "unsupported AsyncAPI option; expected `documented`, `skip`, `tag`, `summary`, `description`, `operation_id`, `deprecated`, or `security`",
        ))
    })?;

    if explicit_documented && explicit_skip {
        return Err(syn::Error::new_spanned(
            attribute,
            "AsyncAPI metadata cannot be both `documented` and `skip`",
        ));
    }
    if explicit_skip
        && (!arguments.tags.is_empty()
            || arguments.summary.is_some()
            || arguments.description.is_some()
            || arguments.operation_id.is_some()
            || arguments.deprecated.is_some()
            || !arguments.security.is_empty())
    {
        return Err(syn::Error::new_spanned(
            attribute,
            "`asyncapi(skip)` cannot be combined with documentation metadata",
        ));
    }

    Ok(arguments)
}

fn reject_marker_value(meta: &syn::meta::ParseNestedMeta<'_>, name: &str) -> syn::Result<()> {
    if meta.input.peek(syn::Token![=]) || meta.input.peek(syn::token::Paren) {
        return Err(meta.error(format!("`{name}` is a marker and accepts no value")));
    }
    Ok(())
}

fn set_string(
    slot: &mut Option<LitStr>,
    meta: &syn::meta::ParseNestedMeta<'_>,
    name: &str,
) -> syn::Result<()> {
    if slot.is_some() {
        return Err(meta.error(format!("duplicate `{name}` option")));
    }
    *slot = Some(meta.value()?.parse()?);
    Ok(())
}

pub(crate) fn generate(runtime: &TokenStream, arguments: Option<&AsyncApiArgs>) -> TokenStream {
    let Some(arguments) = arguments else {
        return quote!(#runtime::__private::WebSocketAsyncApiRegistration::unspecified());
    };
    if arguments.status == Status::Skipped {
        return quote!(#runtime::__private::WebSocketAsyncApiRegistration::skipped());
    }

    let tags = arguments.tags.iter().map(|tag| quote!(.with_tag(#tag)));
    let summary = arguments
        .summary
        .as_ref()
        .map(|summary| quote!(.with_summary(#summary)));
    let description = arguments
        .description
        .as_ref()
        .map(|description| quote!(.with_description(#description)));
    let operation_id = arguments
        .operation_id
        .as_ref()
        .map(|operation_id| quote!(.with_operation_id(#operation_id)));
    let deprecated = arguments
        .deprecated
        .map(|deprecated| quote!(.with_deprecated(#deprecated)));
    let security = arguments
        .security
        .iter()
        .map(|security| quote!(.with_security(#security)));

    quote! {
        #runtime::__private::WebSocketAsyncApiRegistration::documented()
            #(#tags)*
            #summary
            #description
            #operation_id
            #deprecated
            #(#security)*
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::parse_quote;

    #[test]
    fn skip_rejects_documentation_fields() {
        let attribute: Attribute = parse_quote!(#[asyncapi(skip, summary = "hidden")]);
        assert!(parse(&attribute).is_err());
    }

    #[test]
    fn repeatable_tags_and_security_are_accepted() {
        let attribute: Attribute = parse_quote!(
            #[asyncapi(
                documented,
                tag = "chat",
                tag = "public",
                security = "bearer",
                deprecated = false
            )]
        );
        assert!(parse(&attribute).is_ok());
    }
}
