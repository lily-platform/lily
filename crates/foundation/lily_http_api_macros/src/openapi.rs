use std::collections::HashSet;

use syn::ext::IdentExt;
use syn::parenthesized;
use syn::parse::{Parse, ParseStream};
use syn::{Attribute, Expr, Ident, LitBool, LitInt, LitStr, Token, Type};

#[derive(Clone, Default)]
pub(crate) struct ControllerOpenApiArgs {
    pub(crate) tag: Option<LitStr>,
    pub(crate) description: Option<LitStr>,
    pub(crate) responses: Vec<ResponseArg>,
    pub(crate) into_responses: Option<Type>,
    pub(crate) security: Option<Vec<SecurityRequirementArg>>,
}

#[derive(Clone, Default)]
pub(crate) struct ActionOpenApiArgs {
    pub(crate) operation_id: Option<LitStr>,
    pub(crate) summary: Option<LitStr>,
    pub(crate) description: Option<LitStr>,
    pub(crate) deprecated: bool,
    pub(crate) parameters: Vec<ParameterArg>,
    pub(crate) request_body: Option<RequestBodyArg>,
    pub(crate) responses: Vec<ResponseArg>,
    pub(crate) into_responses: Option<Type>,
    pub(crate) security: Option<Vec<SecurityRequirementArg>>,
}

pub(crate) enum ActionOpenApiDirective {
    Documented(Box<ActionOpenApiArgs>),
    Skip,
}

#[derive(Clone)]
pub(crate) struct SecurityRequirementArg {
    pub(crate) name: LitStr,
    pub(crate) scopes: Vec<LitStr>,
}

#[derive(Clone)]
pub(crate) struct ResponseArg {
    pub(crate) status: LitStr,
    pub(crate) description: Option<LitStr>,
    pub(crate) schema: Option<Type>,
    pub(crate) reusable: Option<Type>,
    pub(crate) content_type: Option<LitStr>,
    pub(crate) example: Option<Expr>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ParameterLocation {
    Query,
    Path,
    Header,
    Cookie,
}

impl ParameterLocation {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Query => "query",
            Self::Path => "path",
            Self::Header => "header",
            Self::Cookie => "cookie",
        }
    }
}

#[derive(Clone)]
pub(crate) struct ParameterArg {
    pub(crate) name: LitStr,
    pub(crate) location: ParameterLocation,
    pub(crate) schema: Type,
    pub(crate) description: Option<LitStr>,
    pub(crate) required: bool,
    pub(crate) example: Option<Expr>,
    pub(crate) format: Option<LitStr>,
}

#[derive(Clone)]
pub(crate) struct RequestBodyArg {
    pub(crate) content_type: LitStr,
    pub(crate) schema: Type,
    pub(crate) description: Option<LitStr>,
    pub(crate) required: bool,
    pub(crate) example: Option<Expr>,
}

pub(crate) fn parse_controller_openapi(
    attribute: &Attribute,
) -> syn::Result<ControllerOpenApiArgs> {
    if matches!(attribute.meta, syn::Meta::Path(_)) {
        return Ok(ControllerOpenApiArgs::default());
    }
    attribute.parse_args::<ControllerOpenApiArgs>()
}

pub(crate) fn parse_action_openapi(attribute: &Attribute) -> syn::Result<ActionOpenApiDirective> {
    if matches!(attribute.meta, syn::Meta::Path(_)) {
        return Ok(ActionOpenApiDirective::Documented(Box::default()));
    }
    attribute.parse_args::<ActionOpenApiDirective>()
}

impl Parse for ControllerOpenApiArgs {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let mut output = Self::default();
        let mut seen = HashSet::new();

        while !input.is_empty() {
            let name = Ident::parse_any(input)?;
            let key = name.to_string();
            if !seen.insert(key.clone()) {
                return Err(syn::Error::new(
                    name.span(),
                    format!("duplicate `{key}` OpenAPI option"),
                ));
            }

            match key.as_str() {
                "tag" => output.tag = Some(parse_string_assignment(input, &name)?),
                "description" => output.description = Some(parse_string_assignment(input, &name)?),
                "responses" => {
                    let (responses, into_responses) = parse_responses(input, &name)?;
                    output.responses = responses;
                    output.into_responses = into_responses;
                }
                "security" => output.security = Some(parse_security(input, &name)?),
                _ => {
                    return Err(syn::Error::new(
                        name.span(),
                        "controller #[openapi(...)] accepts only `tag`, `description`, `responses(...)` and `security(...)`",
                    ));
                }
            }
            parse_optional_comma(input)?;
        }

        Ok(output)
    }
}

impl Parse for ActionOpenApiDirective {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let mut output = ActionOpenApiArgs::default();
        let mut seen = HashSet::new();
        let mut skipped = false;

        while !input.is_empty() {
            let name = Ident::parse_any(input)?;
            let key = name.to_string();
            if !seen.insert(key.clone()) {
                return Err(syn::Error::new(
                    name.span(),
                    format!("duplicate `{key}` OpenAPI option"),
                ));
            }

            match key.as_str() {
                "skip" => skipped = true,
                "operation_id" => {
                    output.operation_id = Some(parse_string_assignment(input, &name)?)
                }
                "summary" => output.summary = Some(parse_string_assignment(input, &name)?),
                "description" => output.description = Some(parse_string_assignment(input, &name)?),
                "deprecated" => output.deprecated = true,
                "parameters" => output.parameters = parse_parameters(input, &name)?,
                "request_body" => output.request_body = Some(parse_request_body(input, &name)?),
                "responses" => {
                    let (responses, into_responses) = parse_responses(input, &name)?;
                    output.responses = responses;
                    output.into_responses = into_responses;
                }
                "security" => output.security = Some(parse_security(input, &name)?),
                _ => {
                    return Err(syn::Error::new(
                        name.span(),
                        "action #[openapi(...)] accepts only `skip`, `operation_id`, `summary`, `description`, `deprecated`, `parameters(...)`, `request_body(...)`, `responses(...)` and `security(...)`",
                    ));
                }
            }
            parse_optional_comma(input)?;
        }

        if skipped {
            if seen.len() != 1 {
                return Err(syn::Error::new(
                    input.span(),
                    "`skip` must be the only action OpenAPI option",
                ));
            }
            Ok(Self::Skip)
        } else {
            Ok(Self::Documented(Box::new(output)))
        }
    }
}

fn parse_string_assignment(input: ParseStream<'_>, name: &Ident) -> syn::Result<LitStr> {
    input
        .parse::<Token![=]>()
        .map_err(|_| syn::Error::new(name.span(), format!("`{name}` requires `= \"...\"`")))?;
    input.parse::<LitStr>()
}

fn parse_responses(
    input: ParseStream<'_>,
    name: &Ident,
) -> syn::Result<(Vec<ResponseArg>, Option<Type>)> {
    let content;
    parenthesized!(content in input);
    if !content.peek(syn::token::Paren) {
        let responses_type = content.parse::<Type>()?;
        if !content.is_empty() {
            return Err(content.error("responses(Type) accepts exactly one IntoResponses type"));
        }
        return Ok((Vec::new(), Some(responses_type)));
    }
    let mut responses = Vec::new();
    let mut statuses = HashSet::new();

    while !content.is_empty() {
        let response_content;
        parenthesized!(response_content in content);
        let response = parse_response(&response_content)?;
        if !statuses.insert(response.status.value()) {
            return Err(syn::Error::new(
                response.status.span(),
                "duplicate OpenAPI response status",
            ));
        }
        responses.push(response);
        parse_optional_comma(&content)?;
    }

    if responses.is_empty() {
        return Err(syn::Error::new(
            name.span(),
            "`responses(...)` requires at least one response",
        ));
    }
    Ok((responses, None))
}

fn parse_response(input: ParseStream<'_>) -> syn::Result<ResponseArg> {
    let mut status = None;
    let mut description = None;
    let mut schema = None;
    let mut reusable = None;
    let mut content_type = None;
    let mut example = None;
    let mut seen = HashSet::new();

    while !input.is_empty() {
        let name = Ident::parse_any(input)?;
        let key = name.to_string();
        if !seen.insert(key.clone()) {
            return Err(syn::Error::new(
                name.span(),
                format!("duplicate response `{key}` option"),
            ));
        }
        input.parse::<Token![=]>()?;
        match key.as_str() {
            "status" => status = Some(parse_status(input)?),
            "description" => description = Some(input.parse::<LitStr>()?),
            "schema" => schema = Some(input.parse::<Type>()?),
            "response" => reusable = Some(input.parse::<Type>()?),
            "content_type" => content_type = Some(input.parse::<LitStr>()?),
            "example" => example = Some(input.parse::<Expr>()?),
            _ => {
                return Err(syn::Error::new(
                    name.span(),
                    "response accepts only `status`, `description`, `schema`, `response`, `content_type` and `example`",
                ));
            }
        }
        parse_optional_comma(input)?;
    }

    let status =
        status.ok_or_else(|| syn::Error::new(input.span(), "response requires `status`"))?;
    if reusable.is_some()
        && (description.is_some()
            || schema.is_some()
            || content_type.is_some()
            || example.is_some())
    {
        return Err(syn::Error::new(
            input.span(),
            "reusable response cannot be combined with description, schema, content_type or example",
        ));
    }
    if reusable.is_none() && description.is_none() {
        return Err(syn::Error::new(
            input.span(),
            "inline response requires `description`",
        ));
    }
    if schema.is_none() && (content_type.is_some() || example.is_some()) {
        return Err(syn::Error::new(
            input.span(),
            "response `content_type` and `example` require `schema`",
        ));
    }
    if schema.is_some() && content_type.is_none() {
        content_type = Some(LitStr::new("application/json", status.span()));
    }

    Ok(ResponseArg {
        status,
        description,
        schema,
        reusable,
        content_type,
        example,
    })
}

fn parse_status(input: ParseStream<'_>) -> syn::Result<LitStr> {
    if input.peek(LitStr) {
        let literal = input.parse::<LitStr>()?;
        let value = literal.value();
        if value == "default" || valid_status(&value) {
            return Ok(literal);
        }
        return Err(syn::Error::new(
            literal.span(),
            "response status must be 100..=599 or \"default\"",
        ));
    }

    let literal = input.parse::<LitInt>()?;
    let value = literal.base10_parse::<u16>()?;
    if !(100..=599).contains(&value) {
        return Err(syn::Error::new(
            literal.span(),
            "response status must be between 100 and 599",
        ));
    }
    Ok(LitStr::new(&value.to_string(), literal.span()))
}

fn valid_status(value: &str) -> bool {
    value.len() == 3
        && value.bytes().all(|byte| byte.is_ascii_digit())
        && value
            .parse::<u16>()
            .is_ok_and(|status| (100..=599).contains(&status))
}

fn parse_security(
    input: ParseStream<'_>,
    _name: &Ident,
) -> syn::Result<Vec<SecurityRequirementArg>> {
    let content;
    parenthesized!(content in input);
    let mut requirements = Vec::new();
    let mut names = HashSet::new();

    while !content.is_empty() {
        let requirement_content;
        parenthesized!(requirement_content in content);
        let name = requirement_content.parse::<LitStr>()?;
        requirement_content.parse::<Token![=]>()?;
        let scopes_content;
        syn::bracketed!(scopes_content in requirement_content);
        let mut scopes = Vec::new();
        while !scopes_content.is_empty() {
            scopes.push(scopes_content.parse::<LitStr>()?);
            parse_optional_comma(&scopes_content)?;
        }
        if !requirement_content.is_empty() {
            return Err(requirement_content.error("unexpected security requirement input"));
        }
        if !names.insert(name.value()) {
            return Err(syn::Error::new(
                name.span(),
                "duplicate OpenAPI security scheme reference",
            ));
        }
        requirements.push(SecurityRequirementArg { name, scopes });
        parse_optional_comma(&content)?;
    }

    Ok(requirements)
}

fn parse_parameters(input: ParseStream<'_>, name: &Ident) -> syn::Result<Vec<ParameterArg>> {
    let content;
    parenthesized!(content in input);
    let mut parameters = Vec::new();
    let mut identities = HashSet::new();

    while !content.is_empty() {
        let parameter_content;
        parenthesized!(parameter_content in content);
        let parameter = parse_parameter(&parameter_content)?;
        let identity = (parameter.location.label(), parameter.name.value());
        if !identities.insert(identity) {
            return Err(syn::Error::new(
                parameter.name.span(),
                "duplicate explicit OpenAPI parameter",
            ));
        }
        parameters.push(parameter);
        parse_optional_comma(&content)?;
    }

    if parameters.is_empty() {
        return Err(syn::Error::new(
            name.span(),
            "`parameters(...)` requires at least one parameter",
        ));
    }
    Ok(parameters)
}

fn parse_parameter(input: ParseStream<'_>) -> syn::Result<ParameterArg> {
    let mut name = None;
    let mut location = None;
    let mut schema = None;
    let mut description = None;
    let mut required = None;
    let mut example = None;
    let mut format = None;
    let mut seen = HashSet::new();

    while !input.is_empty() {
        let option = Ident::parse_any(input)?;
        let key = option.to_string();
        if !seen.insert(key.clone()) {
            return Err(syn::Error::new(
                option.span(),
                format!("duplicate parameter `{key}` option"),
            ));
        }
        input.parse::<Token![=]>()?;
        match key.as_str() {
            "name" => name = Some(input.parse::<LitStr>()?),
            "in" => location = Some(parse_parameter_location(input)?),
            "schema" => schema = Some(input.parse::<Type>()?),
            "description" => description = Some(input.parse::<LitStr>()?),
            "required" => required = Some(input.parse::<LitBool>()?.value),
            "example" => example = Some(input.parse::<Expr>()?),
            "format" => format = Some(input.parse::<LitStr>()?),
            _ => {
                return Err(syn::Error::new(
                    option.span(),
                    "parameter accepts only `name`, `in`, `schema`, `description`, `required`, `example` and `format`",
                ));
            }
        }
        parse_optional_comma(input)?;
    }

    let name = name.ok_or_else(|| syn::Error::new(input.span(), "parameter requires `name`"))?;
    let location =
        location.ok_or_else(|| syn::Error::new(input.span(), "parameter requires `in`"))?;
    let schema =
        schema.ok_or_else(|| syn::Error::new(input.span(), "parameter requires `schema`"))?;
    let required = required.unwrap_or(location == ParameterLocation::Path);
    if location == ParameterLocation::Path && !required {
        return Err(syn::Error::new(
            name.span(),
            "OpenAPI path parameters must be required",
        ));
    }

    Ok(ParameterArg {
        name,
        location,
        schema,
        description,
        required,
        example,
        format,
    })
}

fn parse_parameter_location(input: ParseStream<'_>) -> syn::Result<ParameterLocation> {
    let literal = input.parse::<LitStr>()?;
    match literal.value().as_str() {
        "query" => Ok(ParameterLocation::Query),
        "path" => Ok(ParameterLocation::Path),
        "header" => Ok(ParameterLocation::Header),
        "cookie" => Ok(ParameterLocation::Cookie),
        _ => Err(syn::Error::new(
            literal.span(),
            "parameter `in` must be \"query\", \"path\", \"header\" or \"cookie\"",
        )),
    }
}

fn parse_request_body(input: ParseStream<'_>, _name: &Ident) -> syn::Result<RequestBodyArg> {
    let content;
    parenthesized!(content in input);
    let mut content_type = None;
    let mut schema = None;
    let mut description = None;
    let mut required = None;
    let mut example = None;
    let mut seen = HashSet::new();

    while !content.is_empty() {
        let option = Ident::parse_any(&content)?;
        let key = option.to_string();
        if !seen.insert(key.clone()) {
            return Err(syn::Error::new(
                option.span(),
                format!("duplicate request body `{key}` option"),
            ));
        }
        content.parse::<Token![=]>()?;
        match key.as_str() {
            "content_type" => content_type = Some(content.parse::<LitStr>()?),
            "schema" => schema = Some(content.parse::<Type>()?),
            "description" => description = Some(content.parse::<LitStr>()?),
            "required" => required = Some(content.parse::<LitBool>()?.value),
            "example" => example = Some(content.parse::<Expr>()?),
            _ => {
                return Err(syn::Error::new(
                    option.span(),
                    "request_body accepts only `content_type`, `schema`, `description`, `required` and `example`",
                ));
            }
        }
        parse_optional_comma(&content)?;
    }

    Ok(RequestBodyArg {
        content_type: content_type.ok_or_else(|| {
            syn::Error::new(content.span(), "request_body requires `content_type`")
        })?,
        schema: schema
            .ok_or_else(|| syn::Error::new(content.span(), "request_body requires `schema`"))?,
        description,
        required: required.unwrap_or(true),
        example,
    })
}

fn parse_optional_comma(input: ParseStream<'_>) -> syn::Result<()> {
    if input.is_empty() {
        return Ok(());
    }
    input.parse::<Token![,]>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        parse_action_openapi, parse_controller_openapi, ActionOpenApiDirective, ParameterLocation,
    };
    use syn::{parse_quote, Attribute};

    #[test]
    fn parses_controller_defaults_and_nested_metadata() {
        let attribute: Attribute = parse_quote!(
            #[openapi(
                tag = "Users",
                description = "User operations",
                responses((status = 401, description = "Unauthorized", schema = ErrorBody)),
                security(("bearer" = ["users:read"]))
            )]
        );
        let plan = parse_controller_openapi(&attribute).expect("controller metadata parses");

        assert_eq!(plan.tag.expect("tag").value(), "Users");
        assert_eq!(plan.responses[0].status.value(), "401");
        assert_eq!(plan.security.expect("security")[0].scopes.len(), 1);
    }

    #[test]
    fn parses_complete_action_metadata() {
        let attribute: Attribute = parse_quote!(
            #[openapi(
                operation_id = "users.create",
                summary = "Create user",
                deprecated,
                parameters((
                    name = "tenant",
                    in = "header",
                    schema = String,
                    required = true,
                    format = "tenant-id"
                )),
                request_body(content_type = "application/octet-stream", schema = Payload),
                responses((status = 202, description = "Accepted")),
                security()
            )]
        );
        let ActionOpenApiDirective::Documented(plan) =
            parse_action_openapi(&attribute).expect("action metadata parses")
        else {
            panic!("action is documented");
        };

        assert_eq!(
            plan.operation_id.expect("operation id").value(),
            "users.create"
        );
        assert!(plan.deprecated);
        assert_eq!(plan.parameters[0].location, ParameterLocation::Header);
        assert_eq!(
            plan.parameters[0].format.as_ref().expect("format").value(),
            "tenant-id"
        );
        assert_eq!(plan.responses[0].status.value(), "202");
        assert!(plan.security.expect("empty override").is_empty());
    }

    #[test]
    fn rejects_duplicate_and_mixed_skip_options() {
        let duplicate: Attribute = parse_quote!(#[openapi(summary = "one", summary = "two")]);
        assert!(parse_action_openapi(&duplicate)
            .err()
            .expect("duplicate must fail")
            .to_string()
            .contains("duplicate `summary`"));

        let mixed: Attribute = parse_quote!(#[openapi(skip, deprecated)]);
        assert!(parse_action_openapi(&mixed)
            .err()
            .expect("mixed skip must fail")
            .to_string()
            .contains("must be the only"));
    }
}
