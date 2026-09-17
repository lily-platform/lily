use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::{
    parse_macro_input, Attribute, Expr, GenericArgument, Ident, ImplItem, ImplItemFn, ItemImpl,
    Lit, LitStr, Meta, PathArguments, Token, Type, TypePath,
};

use crate::action_signature::{
    plan_action_signature, ActionArgumentKind, ActionResponseMode, ActionSignaturePlan,
};
use crate::openapi::{
    parse_action_openapi, ActionOpenApiArgs, ActionOpenApiDirective, ParameterArg,
    ParameterLocation, RequestBodyArg, ResponseArg,
};
use crate::runtime_path;
use crate::syntax::{
    is_named, named_attributes, parse_single_type, parse_string, parse_type_list,
    reject_duplicate_authority, validate_action_path, validate_http_method, ROUTE_ATTRIBUTES,
};

struct Action {
    method_name: Ident,
    http_method: String,
    path: LitStr,
    guards: Vec<Type>,
    middlewares: Vec<Type>,
    cors: Option<Type>,
    conditional_attributes: Vec<Attribute>,
    signature_plan: ActionSignaturePlan,
    openapi: ActionOpenApiPlan,
}

enum ActionOpenApiPlan {
    Unspecified(syn::Result<OpenApiOperationPlan>),
    Documented(OpenApiOperationPlan),
    Skipped,
}

struct OpenApiActionPlan {
    ty: Type,
    optional: bool,
}

struct OpenApiOperationPlan {
    path_parameters: Vec<OpenApiActionPlan>,
    query_parameters: Vec<OpenApiActionPlan>,
    typed_headers: Vec<OpenApiActionPlan>,
    body: Option<OpenApiBodyPlan>,
    success: Option<OpenApiSuccessPlan>,
    metadata: ActionOpenApiArgs,
    doc_summary: Option<LitStr>,
    doc_description: Option<LitStr>,
}

struct OpenApiBodyPlan {
    schema: Type,
    multipart: bool,
    content_type: LitStr,
    description: Option<LitStr>,
    required: bool,
    example: Option<Expr>,
}

enum OpenApiSuccessPlan {
    Typed {
        schema: Box<Type>,
        content_type: &'static str,
    },
    Binary,
    NoContent,
    Location {
        schema: Option<Box<Type>>,
        status: &'static str,
        description: &'static str,
        location_description: &'static str,
    },
}

#[derive(Default)]
struct GeneralRouteArguments {
    method: Option<LitStr>,
    path: Option<LitStr>,
}

impl Parse for GeneralRouteArguments {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let mut arguments = Self::default();
        while !input.is_empty() {
            let name: Ident = input.parse()?;
            input.parse::<Token![=]>()?;
            let value: LitStr = input.parse()?;
            match name.to_string().as_str() {
                "method" if arguments.method.is_none() => arguments.method = Some(value),
                "path" if arguments.path.is_none() => arguments.path = Some(value),
                "method" | "path" => {
                    return Err(syn::Error::new(name.span(), "duplicate route argument"));
                }
                _ => {
                    return Err(syn::Error::new(
                        name.span(),
                        "route accepts only `method` and `path` arguments",
                    ));
                }
            }
            if input.is_empty() {
                break;
            }
            input.parse::<Token![,]>()?;
            if input.is_empty() {
                break;
            }
        }
        Ok(arguments)
    }
}

pub(crate) fn expand(args: TokenStream, input: TokenStream) -> TokenStream {
    if !args.is_empty() {
        return syn::Error::new(
            proc_macro2::Span::call_site(),
            "#[controller] does not accept arguments",
        )
        .into_compile_error()
        .into();
    }

    let input = parse_macro_input!(input as ItemImpl);
    match expand_impl(input) {
        Ok(expansion) => expansion.into(),
        Err(error) => error.into_compile_error().into(),
    }
}

fn expand_impl(mut input: ItemImpl) -> syn::Result<TokenStream2> {
    let runtime = runtime_path::lily_http_api()?;
    validate_impl(&input)?;
    let controller = input.self_ty.as_ref().clone();
    let controller_identifier = controller_identifier(&controller)?;

    let mut actions = Vec::new();
    for item in &mut input.items {
        let ImplItem::Fn(method) = item else {
            return Err(syn::Error::new_spanned(
                item,
                "#[controller] impl blocks may contain only HTTP action methods",
            ));
        };
        actions.push(parse_action(method)?);
    }
    if actions.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.self_ty,
            "#[controller] requires at least one HTTP action method",
        ));
    }

    let has_unspecified = actions
        .iter()
        .any(|action| matches!(action.openapi, ActionOpenApiPlan::Unspecified(_)));

    if has_unspecified {
        let selector = controller_openapi_selector(&controller, &controller_identifier)?;
        let documented_actions = actions.iter().map(|action| {
            generate_action(&runtime, &controller, &controller_identifier, action, true)
        });
        let undocumented_actions = actions.iter().map(|action| {
            generate_action(&runtime, &controller, &controller_identifier, action, false)
        });

        return Ok(quote! {
            #input
            #selector! {
                documented { #(#documented_actions)* }
                undocumented { #(#undocumented_actions)* }
            }
        });
    }

    let generated_actions = actions.iter().map(|action| {
        generate_action(&runtime, &controller, &controller_identifier, action, false)
    });

    Ok(quote! {
        #input
        #(#generated_actions)*
    })
}

fn validate_impl(input: &ItemImpl) -> syn::Result<()> {
    if input.trait_.is_some() {
        return Err(syn::Error::new_spanned(
            input,
            "#[controller] must be applied to an inherent impl",
        ));
    }
    if !input.generics.params.is_empty() || input.generics.where_clause.is_some() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "generic controller impl blocks are not supported",
        ));
    }
    controller_identifier(&input.self_ty)?;
    Ok(())
}

fn controller_identifier(controller: &Type) -> syn::Result<Ident> {
    let Type::Path(controller) = controller else {
        return Err(syn::Error::new_spanned(
            controller,
            "controller self type must be a concrete path",
        ));
    };
    if controller.qself.is_some()
        || controller
            .path
            .segments
            .iter()
            .any(|segment| !matches!(segment.arguments, PathArguments::None))
    {
        return Err(syn::Error::new_spanned(
            controller,
            "generic or qualified controller self types are not supported",
        ));
    }
    Ok(controller
        .path
        .segments
        .last()
        .expect("a type path contains one segment")
        .ident
        .clone())
}

fn controller_openapi_selector(
    controller: &Type,
    controller_identifier: &Ident,
) -> syn::Result<syn::Path> {
    let Type::Path(controller) = controller else {
        return Err(syn::Error::new_spanned(
            controller,
            "controller self type must be a concrete path",
        ));
    };
    let mut selector = controller.path.clone();
    selector
        .segments
        .pop()
        .expect("a controller type path contains one segment");
    selector.segments.push(syn::PathSegment::from(format_ident!(
        "__lily_controller_openapi_select_{controller_identifier}"
    )));
    Ok(selector)
}

fn parse_action(method: &mut ImplItemFn) -> syn::Result<Action> {
    let signature_plan = plan_action_signature(method)?;

    let (doc_summary, doc_description) = parse_doc_comments(&method.attrs)?;

    let openapi_attributes = named_attributes(&method.attrs, "openapi");
    reject_duplicate_authority(&openapi_attributes, "action OpenAPI")?;
    let openapi = match openapi_attributes.first() {
        Some(attribute) => match parse_action_openapi(attribute)? {
            ActionOpenApiDirective::Documented(metadata) => {
                ActionOpenApiPlan::Documented(plan_openapi_action(
                    &signature_plan,
                    *metadata,
                    doc_summary.clone(),
                    doc_description.clone(),
                )?)
            }
            ActionOpenApiDirective::Skip => ActionOpenApiPlan::Skipped,
        },
        None => ActionOpenApiPlan::Unspecified(plan_openapi_action(
            &signature_plan,
            ActionOpenApiArgs::default(),
            doc_summary,
            doc_description,
        )),
    };

    let route_attributes = method
        .attrs
        .iter()
        .filter(|attribute| {
            ROUTE_ATTRIBUTES
                .iter()
                .any(|name| is_named(attribute, name))
        })
        .collect::<Vec<_>>();
    if route_attributes.is_empty() {
        return Err(syn::Error::new_spanned(
            &method.sig.ident,
            "controller action requires exactly one HTTP method attribute",
        ));
    }
    if let Some(duplicate) = route_attributes.get(1) {
        return Err(syn::Error::new_spanned(
            duplicate,
            "controller action has more than one HTTP method attribute",
        ));
    }
    let route_attribute = route_attributes[0];
    let (http_method, path) = parse_route_attribute(route_attribute)?;
    validate_action_path(&path)?;

    let guard_attributes = named_attributes(&method.attrs, "guard");
    reject_duplicate_authority(&guard_attributes, "action guard")?;
    let guards = guard_attributes
        .first()
        .map(|attribute| parse_type_list(attribute, "action guard"))
        .transpose()?
        .unwrap_or_default();

    let middleware_attributes = named_attributes(&method.attrs, "middleware");
    reject_duplicate_authority(&middleware_attributes, "action middleware")?;
    let middlewares = middleware_attributes
        .first()
        .map(|attribute| parse_type_list(attribute, "action middleware"))
        .transpose()?
        .unwrap_or_default();

    let cors_attributes = named_attributes(&method.attrs, "cors");
    reject_duplicate_authority(&cors_attributes, "action CORS")?;
    let cors = cors_attributes
        .first()
        .map(|attribute| parse_single_type(attribute, "action CORS"))
        .transpose()?;

    let conditional_attributes = method
        .attrs
        .iter()
        .filter(|attribute| is_named(attribute, "cfg") || is_named(attribute, "cfg_attr"))
        .cloned()
        .collect();
    method.attrs.retain(|attribute| {
        !ROUTE_ATTRIBUTES
            .iter()
            .any(|name| is_named(attribute, name))
            && !is_named(attribute, "guard")
            && !is_named(attribute, "middleware")
            && !is_named(attribute, "cors")
            && !is_named(attribute, "openapi")
    });

    Ok(Action {
        method_name: method.sig.ident.clone(),
        http_method,
        path,
        guards,
        middlewares,
        cors,
        conditional_attributes,
        signature_plan,
        openapi,
    })
}

fn plan_openapi_action(
    signature: &ActionSignaturePlan,
    metadata: ActionOpenApiArgs,
    doc_summary: Option<LitStr>,
    doc_description: Option<LitStr>,
) -> syn::Result<OpenApiOperationPlan> {
    let mut path_parameters = Vec::new();
    let mut query_parameters = Vec::new();
    let mut typed_headers = Vec::new();
    let mut inferred_body = None;
    let mut request_cookies = false;
    let mut raw_body = false;
    let mut raw_request = false;
    let mut unknown_parameters = 0_usize;

    for argument in &signature.arguments {
        if matches!(argument.kind, ActionArgumentKind::RawRequest) {
            raw_request = true;
            continue;
        }
        if matches!(
            argument.kind,
            ActionArgumentKind::RawResponse | ActionArgumentKind::PassthroughResponse
        ) {
            continue;
        }

        let (extractor, optional) = unwrap_optional_extractor(&argument.ty);
        let name = plain_type_name(extractor).unwrap_or_default();
        match name.as_str() {
            "Path" => {
                if optional {
                    return Err(syn::Error::new_spanned(
                        &argument.ty,
                        "OpenAPI path extractors cannot be optional",
                    ));
                }
                path_parameters.push(OpenApiActionPlan {
                    ty: extractor_inner_type(extractor, "Path")?,
                    optional,
                });
            }
            "Query" => query_parameters.push(OpenApiActionPlan {
                ty: extractor_inner_type(extractor, "Query")?,
                optional,
            }),
            "TypedHeader" => typed_headers.push(OpenApiActionPlan {
                ty: extractor_inner_type(extractor, "TypedHeader")?,
                optional,
            }),
            "RequestCookies" => request_cookies = true,
            "Json" | "Form" | "MultipartForm" => {
                if optional {
                    return Err(syn::Error::new_spanned(
                        &argument.ty,
                        "request body extractors cannot be optional",
                    ));
                }
                let content_type = match name.as_str() {
                    "Json" => "application/json",
                    "Form" => "application/x-www-form-urlencoded",
                    "MultipartForm" => "multipart/form-data",
                    _ => unreachable!(),
                };
                inferred_body = Some(OpenApiBodyPlan {
                    schema: extractor_inner_type(extractor, &name)?,
                    multipart: name == "MultipartForm",
                    content_type: LitStr::new(content_type, proc_macro2::Span::call_site()),
                    description: None,
                    required: true,
                    example: None,
                });
            }
            "RawBody" | "BodyStream" => raw_body = true,
            "Service" | "Principal" | "Local" | "ClientIp" | "ExecutionCancellation" => {}
            _ => unknown_parameters += 1,
        }
    }

    validate_explicit_parameter_authority(
        &metadata.parameters,
        !path_parameters.is_empty(),
        !query_parameters.is_empty(),
        !typed_headers.is_empty(),
    )?;
    let explicit_cookie_count = metadata
        .parameters
        .iter()
        .filter(|parameter| parameter.location == ParameterLocation::Cookie)
        .count();
    if request_cookies && explicit_cookie_count == 0 {
        return Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            "RequestCookies requires at least one explicit cookie parameter or #[openapi(skip)]",
        ));
    }
    let custom_authority_count = metadata.parameters.len()
        + usize::from(metadata.request_body.is_some() && inferred_body.is_none() && !raw_body);
    if unknown_parameters > custom_authority_count {
        return Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            "custom request extractor requires explicit parameters(...), request_body(...) metadata or #[openapi(skip)]",
        ));
    }
    if raw_request && metadata.parameters.is_empty() && metadata.request_body.is_none() {
        return Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            "&mut Request requires an explicit parameter/request_body contract or #[openapi(skip)]",
        ));
    }

    let body = match (&inferred_body, &metadata.request_body) {
        (Some(_), Some(explicit)) => {
            return Err(syn::Error::new_spanned(
                &explicit.content_type,
                "typed body extractor and explicit request_body metadata are competing authorities",
            ));
        }
        (Some(inferred), None) => Some(OpenApiBodyPlan {
            schema: inferred.schema.clone(),
            multipart: inferred.multipart,
            content_type: inferred.content_type.clone(),
            description: inferred.description.clone(),
            required: inferred.required,
            example: inferred.example.clone(),
        }),
        (None, Some(explicit)) => Some(explicit_body_plan(explicit)),
        (None, None) if raw_body => {
            return Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                "RawBody and BodyStream require explicit request_body(content_type = ..., schema = ...) metadata or #[openapi(skip)]",
            ));
        }
        (None, None) => None,
    };

    let success = plan_openapi_success(
        signature,
        &metadata.responses,
        metadata.into_responses.is_some(),
    )?;

    Ok(OpenApiOperationPlan {
        path_parameters,
        query_parameters,
        typed_headers,
        body,
        success,
        metadata,
        doc_summary,
        doc_description,
    })
}

fn unwrap_optional_extractor(ty: &Type) -> (&Type, bool) {
    if plain_type_name(ty).as_deref() != Some("Option") {
        return (ty, false);
    }
    extractor_inner_type_ref(ty).map_or((ty, false), |inner| (inner, true))
}

fn extractor_inner_type_ref(ty: &Type) -> Option<&Type> {
    let Type::Path(TypePath { qself: None, path }) = ty else {
        return None;
    };
    let segment = path.segments.last()?;
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return None;
    };
    let mut types = arguments.args.iter().filter_map(|argument| match argument {
        GenericArgument::Type(ty) => Some(ty),
        _ => None,
    });
    let inner = types.next()?;
    types.next().is_none().then_some(inner)
}

fn explicit_body_plan(body: &RequestBodyArg) -> OpenApiBodyPlan {
    OpenApiBodyPlan {
        schema: body.schema.clone(),
        multipart: false,
        content_type: body.content_type.clone(),
        description: body.description.clone(),
        required: body.required,
        example: body.example.clone(),
    }
}

fn validate_explicit_parameter_authority(
    parameters: &[ParameterArg],
    has_path: bool,
    has_query: bool,
    has_header: bool,
) -> syn::Result<()> {
    for parameter in parameters {
        let conflict = match parameter.location {
            ParameterLocation::Path => has_path,
            ParameterLocation::Query => has_query,
            ParameterLocation::Header => has_header,
            ParameterLocation::Cookie => false,
        };
        if conflict {
            return Err(syn::Error::new_spanned(
                &parameter.name,
                format!(
                    "explicit {} parameter conflicts with the typed extractor authority; annotate the DTO/header type instead",
                    parameter.location.label(),
                ),
            ));
        }
    }
    Ok(())
}

fn plan_openapi_success(
    signature: &ActionSignaturePlan,
    responses: &[ResponseArg],
    has_into_responses: bool,
) -> syn::Result<Option<OpenApiSuccessPlan>> {
    let explicit_statuses = responses
        .iter()
        .map(|response| response.status.value())
        .collect::<std::collections::HashSet<_>>();

    if signature.response_mode != ActionResponseMode::FrameworkManaged {
        if responses.is_empty() && !has_into_responses {
            return Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                "manual and passthrough response actions require explicit responses(...) metadata or #[openapi(skip)]",
            ));
        }
        return Ok(None);
    }

    let Some(success_type) = signature.success_type.clone() else {
        if responses.is_empty() && !has_into_responses {
            return Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                "unit/preserved response actions require explicit responses(...) metadata or #[openapi(skip)]",
            ));
        }
        return Ok(None);
    };
    let success_name = plain_type_name(&success_type).unwrap_or_default();
    if matches!(success_name.as_str(), "Created" | "Accepted") {
        let (status, description, location_description) = if success_name == "Created" {
            (
                "201",
                "Created",
                "Optional URI of the created resource; relative or absolute.",
            )
        } else {
            (
                "202",
                "Accepted",
                "Optional URI for monitoring the accepted operation; relative or absolute.",
            )
        };
        if explicit_statuses.contains(status) {
            return Err(syn::Error::new_spanned(
                &success_type,
                format!(
                    "explicit {status} response conflicts with inferred {success_name} response"
                ),
            ));
        }
        let schema = extractor_inner_type_ref(&success_type)
            .filter(|inner| plain_type_name(inner).as_deref() != Some("EmptyBody"))
            .cloned()
            .map(Box::new);
        return Ok(Some(OpenApiSuccessPlan::Location {
            schema,
            status,
            description,
            location_description,
        }));
    }
    if success_name == "NoContent" {
        if explicit_statuses.contains("204") {
            return Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                "explicit 204 response conflicts with inferred NoContent response",
            ));
        }
        return Ok(Some(OpenApiSuccessPlan::NoContent));
    }
    if matches!(
        success_name.as_str(),
        "StreamingResponse" | "SseResponse" | "StaticFileResponse"
    ) {
        if responses.is_empty() && !has_into_responses {
            return Err(syn::Error::new_spanned(
                success_type,
                "streaming/SSE/static-file responses require explicit responses(...) metadata or #[openapi(skip)]",
            ));
        }
        return Ok(None);
    }
    if success_name == "ResponseBuilder" {
        if responses.is_empty() && !has_into_responses {
            return Err(syn::Error::new_spanned(
                success_type,
                "ResponseBuilder responses require explicit responses(...) metadata or #[openapi(skip)]",
            ));
        }
        return Ok(None);
    }
    if !signature.success_is_result
        && success_name != "String"
        && success_name != "bool"
        && success_name != "Json"
        && success_name != "PlainText"
        && success_name != "BinaryData"
        && !is_str_reference(&success_type)
        && !is_vec_u8(&success_type)
    {
        if responses.is_empty() && !has_into_responses {
            return Err(syn::Error::new_spanned(
                success_type,
                "direct custom response types require explicit responses(...) metadata or #[openapi(skip)]; return Result<T, E> for Lily's inferred JSON contract",
            ));
        }
        return Ok(None);
    }
    if explicit_statuses.contains("200") {
        return Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            "explicit 200 response conflicts with the inferred typed success response",
        ));
    }

    if success_name == "Json" {
        return Ok(Some(OpenApiSuccessPlan::Typed {
            schema: Box::new(extractor_inner_type(&success_type, "Json")?),
            content_type: "application/json",
        }));
    }
    if success_name == "PlainText" {
        return Ok(Some(OpenApiSuccessPlan::Typed {
            schema: Box::new(syn::parse_quote!(String)),
            content_type: "text/plain; charset=utf-8",
        }));
    }
    if success_name == "BinaryData" {
        return Ok(Some(OpenApiSuccessPlan::Binary));
    }

    if !signature.success_is_result {
        if success_name == "String" || is_str_reference(&success_type) {
            return Ok(Some(OpenApiSuccessPlan::Typed {
                schema: Box::new(syn::parse_quote!(String)),
                content_type: "text/plain; charset=utf-8",
            }));
        }
        if is_vec_u8(&success_type) {
            return Ok(Some(OpenApiSuccessPlan::Binary));
        }
    }

    let schema = if is_str_reference(&success_type) {
        syn::parse_quote!(String)
    } else {
        success_type
    };
    Ok(Some(OpenApiSuccessPlan::Typed {
        schema: Box::new(schema),
        content_type: "application/json",
    }))
}

fn is_str_reference(ty: &Type) -> bool {
    matches!(
        ty,
        Type::Reference(reference)
            if plain_type_name(reference.elem.as_ref()).as_deref() == Some("str")
    )
}

fn is_vec_u8(ty: &Type) -> bool {
    let Type::Path(TypePath { qself: None, path }) = ty else {
        return false;
    };
    let Some(segment) = path
        .segments
        .last()
        .filter(|segment| segment.ident == "Vec")
    else {
        return false;
    };
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return false;
    };
    let mut types = arguments.args.iter().filter_map(|argument| match argument {
        GenericArgument::Type(ty) => Some(ty),
        _ => None,
    });
    let Some(element) = types.next() else {
        return false;
    };
    types.next().is_none() && plain_type_name(element).as_deref() == Some("u8")
}

fn parse_doc_comments(attributes: &[Attribute]) -> syn::Result<(Option<LitStr>, Option<LitStr>)> {
    let mut lines = Vec::new();
    let mut span = proc_macro2::Span::call_site();
    for attribute in attributes
        .iter()
        .filter(|attribute| is_named(attribute, "doc"))
    {
        let Meta::NameValue(name_value) = &attribute.meta else {
            return Err(syn::Error::new_spanned(
                attribute,
                "controller action doc comments must be string literals",
            ));
        };
        let Expr::Lit(expression) = &name_value.value else {
            return Err(syn::Error::new_spanned(
                &name_value.value,
                "controller action doc comments must be string literals",
            ));
        };
        let Lit::Str(line) = &expression.lit else {
            return Err(syn::Error::new_spanned(
                &expression.lit,
                "controller action doc comments must be string literals",
            ));
        };
        span = line.span();
        lines.push(line.value().trim().to_owned());
    }
    while lines.first().is_some_and(String::is_empty) {
        lines.remove(0);
    }
    while lines.last().is_some_and(String::is_empty) {
        lines.pop();
    }
    if lines.is_empty() {
        return Ok((None, None));
    }

    let summary = LitStr::new(&lines[0], span);
    let description = LitStr::new(&lines.join("\n"), span);
    Ok((Some(summary), Some(description)))
}

fn plain_type_name(ty: &Type) -> Option<String> {
    let Type::Path(TypePath { qself: None, path }) = ty else {
        return None;
    };
    path.segments
        .last()
        .map(|segment| segment.ident.to_string())
}

fn extractor_inner_type(ty: &Type, extractor: &str) -> syn::Result<Type> {
    let Type::Path(TypePath { qself: None, path }) = ty else {
        return Err(syn::Error::new_spanned(
            ty,
            format!("{extractor} must be a concrete generic extractor type"),
        ));
    };
    let segment = path
        .segments
        .last()
        .expect("a type path contains one segment");
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return Err(syn::Error::new_spanned(
            ty,
            format!("{extractor} requires exactly one generic DTO type"),
        ));
    };
    let mut types = arguments.args.iter().filter_map(|argument| match argument {
        GenericArgument::Type(ty) => Some(ty),
        _ => None,
    });
    let Some(inner) = types.next() else {
        return Err(syn::Error::new_spanned(
            ty,
            format!("{extractor} requires exactly one generic DTO type"),
        ));
    };
    if types.next().is_some() {
        return Err(syn::Error::new_spanned(
            ty,
            format!("{extractor} requires exactly one generic DTO type"),
        ));
    }
    Ok(inner.clone())
}

fn parse_route_attribute(attribute: &Attribute) -> syn::Result<(String, LitStr)> {
    let name = attribute
        .path()
        .get_ident()
        .expect("route attributes have one identifier")
        .to_string();
    if name != "route" {
        let path = parse_string(attribute, &format!("{name} action attribute"))?;
        return Ok((name.to_ascii_uppercase(), path));
    }

    let arguments = attribute.parse_args::<GeneralRouteArguments>()?;
    let method = arguments
        .method
        .ok_or_else(|| syn::Error::new_spanned(attribute, "route requires `method = \"...\"`"))?;
    let path = arguments
        .path
        .ok_or_else(|| syn::Error::new_spanned(attribute, "route requires `path = \"/...\"`"))?;
    Ok((validate_http_method(&method)?, path))
}

fn generate_action(
    runtime: &TokenStream2,
    controller: &Type,
    controller_identifier: &Ident,
    action: &Action,
    controller_documented: bool,
) -> TokenStream2 {
    let Action {
        method_name,
        http_method,
        path,
        guards,
        middlewares,
        cors,
        conditional_attributes,
        signature_plan,
        openapi,
    } = action;
    let explicitly_skipped = matches!(openapi, ActionOpenApiPlan::Skipped);
    let openapi = match openapi {
        ActionOpenApiPlan::Documented(plan) => Some(plan),
        ActionOpenApiPlan::Skipped => None,
        ActionOpenApiPlan::Unspecified(Ok(plan)) if controller_documented => Some(plan),
        ActionOpenApiPlan::Unspecified(Err(error)) if controller_documented => {
            return error.to_compile_error();
        }
        ActionOpenApiPlan::Unspecified(_) => None,
    };
    let action_adapter = format_ident!(
        "__LilyControllerAction_{}_{}",
        controller_identifier,
        method_name
    );
    let binder_function = format_ident!(
        "__lily_controller_bind_{}_{}",
        controller_identifier,
        method_name
    );
    let route_function = format_ident!(
        "__lily_pending_route_{}_{}",
        controller_identifier,
        method_name
    );
    let route_static = format_ident!(
        "__LILY_PENDING_ROUTE_{}_{}",
        controller_identifier,
        method_name
    );
    let openapi_factory = format_ident!(
        "__lily_openapi_operation_{}_{}",
        controller_identifier,
        method_name
    );

    let openapi_factory_definition = openapi.as_ref().map_or_else(
        || quote! {},
        |plan| {
            generate_openapi_factory(
                runtime,
                controller,
                &openapi_factory,
                conditional_attributes,
                plan,
            )
        },
    );
    let openapi_route_result = if openapi.is_some() {
        quote!(route.with_openapi_operation_factory(#openapi_factory))
    } else if explicitly_skipped {
        quote!(route.with_openapi_skipped())
    } else {
        quote!(route)
    };

    let cors_registration = cors.as_ref().map_or_else(
        || {
            quote!(
                <#controller as #runtime::__private::StructControllerDefinition>::cors_policy_registration()
            )
        },
        |cors| {
            quote!(#runtime::__private::CorsRoutePolicyRegistration::provider::<#cors>())
        },
    );

    let mut extraction_statements = Vec::new();
    let mut action_arguments = Vec::new();
    let passthrough_state = if signature_plan.response_mode == ActionResponseMode::Passthrough {
        quote! {
            let mut __lily_passthrough_response_state =
                #runtime::__private::PassthroughResponseState::new();
        }
    } else {
        quote! {}
    };
    for (index, argument) in signature_plan.arguments.iter().enumerate() {
        let ty = &argument.ty;
        let local = format_ident!("__lily_action_argument_{index}");
        match argument.kind {
            ActionArgumentKind::Parts => {
                extraction_statements.push(quote! {
                    let #local: #ty = match #runtime::__private::extract_request_parts::<#ty>(
                        request,
                        _extensions.as_ref(),
                    ).await {
                        ::std::result::Result::Ok(value) => value,
                        ::std::result::Result::Err(rejection) => {
                            return #runtime::__private::write_error_response(
                                rejection, response, request,
                            ).await;
                        }
                    };
                });
                action_arguments.push(quote!(#local));
            }
            ActionArgumentKind::Service => {
                extraction_statements.push(quote! {
                    let #local: #ty = match #runtime::__private::extract_service::<#ty>(
                        _extensions.as_ref(),
                    ).await {
                        ::std::result::Result::Ok(value) => value,
                        ::std::result::Result::Err(rejection) => {
                            return #runtime::__private::write_error_response(
                                rejection, response, request,
                            ).await;
                        }
                    };
                });
                action_arguments.push(quote!(#local));
            }
            ActionArgumentKind::Terminal => {
                extraction_statements.push(quote! {
                    let #local: #ty = match #runtime::__private::extract_terminal_request::<#ty, _>(
                        request,
                        _extensions.as_ref(),
                    ).await {
                        ::std::result::Result::Ok(value) => value,
                        ::std::result::Result::Err(rejection) => {
                            return #runtime::__private::write_error_response(
                                rejection, response, request,
                            ).await;
                        }
                    };
                });
                action_arguments.push(quote!(#local));
            }
            ActionArgumentKind::Body { .. } => {
                extraction_statements.push(quote! {
                    let #local: #ty = match #runtime::__private::extract_request::<#ty>(
                        request,
                        _extensions.as_ref(),
                    ).await {
                        ::std::result::Result::Ok(value) => value,
                        ::std::result::Result::Err(rejection) => {
                            return #runtime::__private::write_error_response(
                                rejection, response, request,
                            ).await;
                        }
                    };
                });
                action_arguments.push(quote!(#local));
            }
            ActionArgumentKind::RawRequest => action_arguments.push(quote!(request)),
            ActionArgumentKind::RawResponse => action_arguments.push(quote!(response)),
            ActionArgumentKind::PassthroughResponse => action_arguments.push(quote! {
                #runtime::PassthroughResponseContext::new(
                    &mut __lily_passthrough_response_state,
                )
            }),
        }
    }
    let response_commit = match signature_plan.response_mode {
        ActionResponseMode::FrameworkManaged => quote! {
            result.write_to_response(response, request).await
        },
        ActionResponseMode::Manual => quote! {
            let __lily_response_outcome = result.write_to_response(response, request).await?;
            if __lily_response_outcome == #runtime::ResponseWriteOutcome::Preserved
                || __lily_response_outcome.is_error_response()
            {
                ::std::result::Result::Ok(__lily_response_outcome)
            } else {
                ::std::result::Result::Err(#runtime::ResponseWriteError::InvalidOutcome)
            }
        },
        ActionResponseMode::Passthrough => quote! {
            #runtime::__private::write_passthrough_response(
                result,
                __lily_passthrough_response_state,
                response,
                request,
            ).await
        },
    };

    quote! {
        #(#conditional_attributes)*
        #[doc(hidden)]
        #[allow(non_camel_case_types)]
        struct #action_adapter {
            controller: ::std::sync::Arc<#controller>,
        }

        #(#conditional_attributes)*
        impl #runtime::__private::HttpAction for #action_adapter {
            fn call<'a>(
                &'a self,
                _extensions: ::std::sync::Arc<#runtime::__private::Extensions>,
                request: &'a mut #runtime::Request,
                response: &'a mut #runtime::Response,
            ) -> #runtime::__private::HttpActionFuture<'a> {
                ::std::boxed::Box::pin(async move {
                    use #runtime::IntoResponse as _;
                    #passthrough_state
                    #(#extraction_statements)*
                    let result = self.controller.#method_name(#(#action_arguments),*).await;
                    #response_commit
                })
            }
        }

        #(#conditional_attributes)*
        #[doc(hidden)]
        #[allow(non_snake_case)]
        fn #binder_function(
            controller: #runtime::__private::ErasedController,
        ) -> Result<#runtime::__private::Handler, #runtime::__private::ControllerBindingError> {
            let controller = #runtime::__private::downcast_controller::<#controller>(controller)?;
            Ok(#runtime::__private::Handler::from_action(
                #action_adapter { controller },
                false,
            ))
        }

        #(#conditional_attributes)*
        #[doc(hidden)]
        #[allow(non_snake_case)]
        fn #route_function() -> #runtime::__private::PendingControllerRoute {
            #(
                #runtime::__private::register_guard_metadata(#runtime::__private::GuardMetadata {
                    type_id: ::std::any::TypeId::of::<#guards>(),
                    type_name: ::std::any::type_name::<#guards>(),
                    factory_fn: |extensions| ::std::boxed::Box::pin(async move {
                        let guard = <#guards as #runtime::GuardTrait>::new(extensions).await?;
                        Ok(::std::boxed::Box::new(guard)
                            as ::std::boxed::Box<dyn #runtime::GuardTrait + Send + Sync>)
                    }),
                });
            )*

            let guard_type_ids: ::std::vec::Vec<::std::any::TypeId> = ::std::vec![
                #(::std::any::TypeId::of::<#guards>(),)*
            ];
            let mut middleware_registrations =
                <#controller as #runtime::__private::StructControllerDefinition>::middleware_registrations();
            #(
                middleware_registrations.push(
                    #runtime::__private::HttpMiddlewareRegistration::of::<#middlewares>()
                );
            )*
            let cors_policy_registration = #cors_registration;
            let path = #runtime::__private::join_controller_action_path(
                <#controller as #runtime::__private::StructControllerDefinition>::base_path(),
                #path,
            );

            let route = #runtime::__private::PendingControllerRoute::new(
                #http_method,
                path,
                concat!(
                    module_path!(),
                    "::",
                    stringify!(#controller),
                    "::",
                    stringify!(#method_name),
                ),
                guard_type_ids,
                middleware_registrations,
                cors_policy_registration,
                #runtime::__private::ControllerActionRegistration::of::<#controller>(
                    #binder_function,
                ),
            );
            #openapi_route_result
        }

        #(#conditional_attributes)*
        #[doc(hidden)]
        #[allow(non_upper_case_globals)]
        #[#runtime::__private::linkme::distributed_slice(
            #runtime::__private::PENDING_CONTROLLER_ROUTE_REGISTRATIONS
        )]
        #[linkme(crate = #runtime::__private::linkme)]
        static #route_static: #runtime::__private::PendingControllerRouteRegistrationFn =
            #route_function;

        #openapi_factory_definition
    }
}

fn generate_openapi_factory(
    runtime: &TokenStream2,
    controller: &Type,
    factory: &Ident,
    conditional_attributes: &[Attribute],
    plan: &OpenApiOperationPlan,
) -> TokenStream2 {
    let path_parameters = plan.path_parameters.iter().map(|parameter| {
        let ty = &parameter.ty;
        quote! {
            parameters.extend(
                <#ty as #runtime::__private::utoipa::IntoParams>::into_params(
                    || ::std::option::Option::Some(
                        #runtime::__private::utoipa::openapi::path::ParameterIn::Path,
                    ),
                ),
            );
        }
    });
    let query_parameters = plan.query_parameters.iter().map(|parameter| {
        let ty = &parameter.ty;
        let optional = parameter.optional;
        let optional_adjustment = optional.then(|| {
            quote! {
                for parameter in &mut extracted {
                    parameter.required = #runtime::__private::utoipa::openapi::Required::False;
                }
            }
        });
        quote! {
            let mut extracted = <#ty as #runtime::__private::utoipa::IntoParams>::into_params(
                || ::std::option::Option::Some(
                    #runtime::__private::utoipa::openapi::path::ParameterIn::Query,
                ),
            );
            #optional_adjustment
            parameters.extend(extracted);
        }
    });
    let typed_headers = plan.typed_headers.iter().map(|parameter| {
        let ty = &parameter.ty;
        let required = if parameter.optional {
            quote!(#runtime::__private::utoipa::openapi::Required::False)
        } else {
            quote!(#runtime::__private::utoipa::openapi::Required::True)
        };
        quote! {
            parameters.push(
                #runtime::__private::utoipa::openapi::path::ParameterBuilder::new()
                    .name(<#ty as #runtime::headers::Header>::name().as_str())
                    .parameter_in(#runtime::__private::utoipa::openapi::path::ParameterIn::Header)
                    .required(#required)
                    .schema(::std::option::Option::Some(
                        #runtime::__private::utoipa::openapi::ObjectBuilder::new()
                            .schema_type(#runtime::__private::utoipa::openapi::Type::String)
                            .build(),
                    ))
                    .build(),
            );
        }
    });
    let explicit_parameters = plan
        .metadata
        .parameters
        .iter()
        .map(|parameter| generate_explicit_parameter(runtime, parameter));

    let body_schema_collection = plan.body.as_ref().map(|body| {
        let schema = &body.schema;
        if body.multipart {
            quote! {
                metadata.collect_multipart_schema::<#schema>();
            }
        } else {
            quote! {
                metadata.collect_schema::<#schema>();
            }
        }
    });
    let request_body = plan.body.as_ref().map_or_else(
        || quote!(::std::option::Option::None),
        |body| {
            let schema = &body.schema;
            let content_type = &body.content_type;
            let description = body
                .description
                .as_ref()
                .map(|description| quote!(.description(::std::option::Option::Some(#description))));
            let required = if body.required {
                quote!(#runtime::__private::utoipa::openapi::Required::True)
            } else {
                quote!(#runtime::__private::utoipa::openapi::Required::False)
            };
            let example = body.example.as_ref().map(|example| {
                quote! {
                    .example(::std::option::Option::Some(
                        #runtime::__private::serde_json::json!(#example),
                    ))
                }
            });
            let schema_ref = if body.multipart {
                quote!(#runtime::__private::openapi_multipart_schema_ref::<#schema>())
            } else {
                quote!(#runtime::__private::openapi_schema_ref::<#schema>())
            };
            quote! {
                ::std::option::Option::Some(
                    #runtime::__private::utoipa::openapi::request_body::RequestBodyBuilder::new()
                        .required(::std::option::Option::Some(
                            #required,
                        ))
                        #description
                        .content(
                            #content_type,
                            #runtime::__private::utoipa::openapi::ContentBuilder::new()
                                .schema(::std::option::Option::Some(
                                    #schema_ref,
                                ))
                                #example
                                .build(),
                        )
                        .build(),
                )
            }
        },
    );

    let inferred_success_schema = match &plan.success {
        Some(
            OpenApiSuccessPlan::Typed { schema, .. }
            | OpenApiSuccessPlan::Location {
                schema: Some(schema),
                ..
            },
        ) => Some(quote! {
            metadata.collect_schema::<#schema>();
        }),
        Some(
            OpenApiSuccessPlan::Binary
            | OpenApiSuccessPlan::NoContent
            | OpenApiSuccessPlan::Location { schema: None, .. },
        )
        | None => None,
    };
    let inferred_response = match &plan.success {
        Some(OpenApiSuccessPlan::Typed {
            schema,
            content_type,
        }) => Some(quote! {
            responses = responses.response(
                "200",
                #runtime::__private::utoipa::openapi::response::ResponseBuilder::new()
                    .description("Success")
                    .content(
                        #content_type,
                        #runtime::__private::utoipa::openapi::Content::new(::std::option::Option::Some(
                            #runtime::__private::openapi_schema_ref::<#schema>(),
                        )),
                    ),
            );
        }),
        Some(OpenApiSuccessPlan::Binary) => Some(quote! {
            responses = responses.response(
                "200",
                #runtime::__private::utoipa::openapi::response::ResponseBuilder::new()
                    .description("Success")
                    .content(
                        "application/octet-stream",
                        #runtime::__private::utoipa::openapi::Content::new(::std::option::Option::Some(
                            #runtime::__private::utoipa::openapi::ObjectBuilder::new()
                                .schema_type(#runtime::__private::utoipa::openapi::Type::String)
                                .format(::std::option::Option::Some(
                                    #runtime::__private::utoipa::openapi::schema::SchemaFormat::KnownFormat(
                                        #runtime::__private::utoipa::openapi::schema::KnownFormat::Binary,
                                    ),
                                ))
                                .build(),
                        )),
                    ),
            );
        }),
        Some(OpenApiSuccessPlan::NoContent) => Some(quote! {
            responses = responses.response(
                "204",
                #runtime::__private::utoipa::openapi::response::ResponseBuilder::new()
                    .description("No Content"),
            );
        }),
        Some(OpenApiSuccessPlan::Location {
            schema,
            status,
            description,
            location_description,
        }) => {
            let content = schema.as_ref().map(|schema| quote! {
                .content(
                    "application/json",
                    #runtime::__private::utoipa::openapi::Content::new(::std::option::Option::Some(
                        #runtime::__private::openapi_schema_ref::<#schema>(),
                    )),
                )
            });
            Some(quote! {
                responses = responses.response(
                    #status,
                    #runtime::__private::utoipa::openapi::response::ResponseBuilder::new()
                        .description(#description)
                        .header(
                            "Location",
                            #runtime::__private::utoipa::openapi::header::HeaderBuilder::new()
                                .schema(#runtime::__private::utoipa::openapi::ObjectBuilder::new()
                                    .schema_type(#runtime::__private::utoipa::openapi::Type::String)
                                    .build())
                                .description(::std::option::Option::Some(#location_description))
                                .build(),
                        )
                        #content,
                );
            })
        }
        None => None,
    };
    let explicit_response_schema_collection =
        plan.metadata.responses.iter().filter_map(|response| {
            response.schema.as_ref().map(|schema| {
                quote! {
                    metadata.collect_schema::<#schema>();
                }
            })
        });
    let explicit_parameter_schema_collection = plan.metadata.parameters.iter().map(|parameter| {
        let schema = &parameter.schema;
        quote! { metadata.collect_schema::<#schema>(); }
    });
    let explicit_responses = plan
        .metadata
        .responses
        .iter()
        .filter(|response| response.reusable.is_none())
        .map(|response| generate_explicit_response(runtime, response));
    let reusable_responses = plan.metadata.responses.iter().filter_map(|response| {
        response.reusable.as_ref().map(|reusable| {
            let status = &response.status;
            quote! { metadata.register_reusable_response::<#reusable>(#status); }
        })
    });
    let into_responses = plan.metadata.into_responses.as_ref().map(|responses| {
        quote! { metadata.extend_responses::<#responses>(); }
    });

    let operation_id = plan
        .metadata
        .operation_id
        .as_ref()
        .map(|operation_id| quote!(.operation_id(::std::option::Option::Some(#operation_id))));
    let summary = plan
        .metadata
        .summary
        .as_ref()
        .or(plan.doc_summary.as_ref())
        .map(|summary| quote!(.summary(::std::option::Option::Some(#summary))));
    let description = plan
        .metadata
        .description
        .as_ref()
        .or(plan.doc_description.as_ref())
        .map(|description| quote!(.description(::std::option::Option::Some(#description))));
    let deprecated = plan.metadata.deprecated.then(|| {
        quote!(.deprecated(::std::option::Option::Some(
            #runtime::__private::utoipa::openapi::Deprecated::True,
        )))
    });
    let security = plan.metadata.security.as_ref().map(|requirements| {
        if requirements.is_empty() {
            return quote!(.securities(::std::option::Option::Some(
                ::std::vec::Vec::<
                    #runtime::__private::utoipa::openapi::security::SecurityRequirement,
                >::new(),
            )));
        }
        let requirements = requirements.iter().map(|requirement| {
            let name = &requirement.name;
            let scopes = &requirement.scopes;
            if scopes.is_empty() {
                quote! {
                    #runtime::__private::utoipa::openapi::security::SecurityRequirement::new(
                        #name,
                        ::std::vec::Vec::<::std::string::String>::new(),
                    )
                }
            } else {
                quote! {
                    #runtime::__private::utoipa::openapi::security::SecurityRequirement::new(
                        #name,
                        [#(#scopes),*],
                    )
                }
            }
        });
        quote!(.securities(::std::option::Option::Some(
            ::std::vec![#(#requirements),*],
        )))
    });

    quote! {
        #(#conditional_attributes)*
        #[doc(hidden)]
        #[allow(non_snake_case)]
        fn #factory() -> #runtime::__private::OpenApiOperationMetadata {
            let mut parameters: ::std::vec::Vec<
                #runtime::__private::utoipa::openapi::path::Parameter,
            > = ::std::vec::Vec::new();
            #(#path_parameters)*
            #(#query_parameters)*
            #(#typed_headers)*

            #(#explicit_parameters)*

            let mut responses = #runtime::__private::utoipa::openapi::response::ResponsesBuilder::new();
            #inferred_response
            #(#explicit_responses)*
            let responses = responses.build();
            let operation = #runtime::__private::utoipa::openapi::path::OperationBuilder::new()
                #operation_id
                #summary
                #description
                #deprecated
                .parameters((!parameters.is_empty()).then_some(parameters))
                .request_body(#request_body)
                .responses(responses)
                #security
                .build();
            let mut metadata = #runtime::__private::OpenApiOperationMetadata::new(
                operation,
                #runtime::__private::utoipa::openapi::schema::Components::new(),
            );
            #body_schema_collection
            #inferred_success_schema
            #(#explicit_response_schema_collection)*
            #(#explicit_parameter_schema_collection)*
            #(#reusable_responses)*
            #into_responses
            <#controller as #runtime::__private::StructControllerDefinition>::apply_openapi_defaults(
                &mut metadata,
            );
            metadata
        }
    }
}

fn generate_explicit_parameter(runtime: &TokenStream2, parameter: &ParameterArg) -> TokenStream2 {
    let name = &parameter.name;
    let schema = &parameter.schema;
    let location = match parameter.location {
        ParameterLocation::Query => {
            quote!(#runtime::__private::utoipa::openapi::path::ParameterIn::Query)
        }
        ParameterLocation::Path => {
            quote!(#runtime::__private::utoipa::openapi::path::ParameterIn::Path)
        }
        ParameterLocation::Header => {
            quote!(#runtime::__private::utoipa::openapi::path::ParameterIn::Header)
        }
        ParameterLocation::Cookie => {
            quote!(#runtime::__private::utoipa::openapi::path::ParameterIn::Cookie)
        }
    };
    let required = if parameter.required {
        quote!(#runtime::__private::utoipa::openapi::Required::True)
    } else {
        quote!(#runtime::__private::utoipa::openapi::Required::False)
    };
    let description = parameter
        .description
        .as_ref()
        .map(|description| quote!(.description(::std::option::Option::Some(#description))));
    let example = parameter.example.as_ref().map(|example| {
        quote! {
            .example(::std::option::Option::Some(
                #runtime::__private::serde_json::json!(#example),
            ))
        }
    });
    let schema_value = parameter.format.as_ref().map_or_else(
        || quote!(#runtime::__private::openapi_schema_ref::<#schema>()),
        |format| quote!(#runtime::__private::openapi_schema_with_format::<#schema>(#format)),
    );

    quote! {
        parameters.push(
            #runtime::__private::utoipa::openapi::path::ParameterBuilder::new()
                .name(#name)
                .parameter_in(#location)
                .required(#required)
                #description
                .schema(::std::option::Option::Some(
                    #schema_value,
                ))
                #example
                .build(),
        );
    }
}

fn generate_explicit_response(runtime: &TokenStream2, response: &ResponseArg) -> TokenStream2 {
    let status = &response.status;
    let description = response
        .description
        .as_ref()
        .expect("inline responses require descriptions");
    let content = response.schema.as_ref().map(|schema| {
        let content_type = response
            .content_type
            .as_ref()
            .expect("response parser supplies content type for a schema");
        let example = response.example.as_ref().map(|example| {
            quote! {
                .example(::std::option::Option::Some(
                    #runtime::__private::serde_json::json!(#example),
                ))
            }
        });
        quote! {
            .content(
                #content_type,
                #runtime::__private::utoipa::openapi::ContentBuilder::new()
                    .schema(::std::option::Option::Some(
                        #runtime::__private::openapi_schema_ref::<#schema>(),
                    ))
                    #example
                    .build(),
            )
        }
    });

    quote! {
        responses = responses.response(
            #status,
            #runtime::__private::utoipa::openapi::response::ResponseBuilder::new()
                .description(#description)
                #content,
        );
    }
}

#[cfg(test)]
mod response_inference_tests {
    use super::{plan_openapi_success, ActionSignaturePlan, OpenApiSuccessPlan};
    use crate::action_signature::plan_action_signature;
    use quote::quote;
    use syn::ImplItemFn;

    fn signature(return_type: &str) -> ActionSignaturePlan {
        let source = format!("async fn action(&self) -> {return_type} {{ loop {{}} }}");
        let method: ImplItemFn = syn::parse_str(&source).expect("fixture method parses");
        plan_action_signature(&method).expect("fixture signature is valid")
    }

    fn inferred(return_type: &str) -> OpenApiSuccessPlan {
        plan_openapi_success(&signature(return_type), &[], false)
            .expect("response metadata can be inferred")
            .expect("response has inferred success metadata")
    }

    fn assert_typed(return_type: &str, schema: &str, content_type: &str) {
        let OpenApiSuccessPlan::Typed {
            schema: inferred_schema,
            content_type: inferred_content_type,
        } = inferred(return_type)
        else {
            panic!("{return_type} must infer a typed response");
        };
        assert_eq!(quote!(#inferred_schema).to_string(), schema);
        assert_eq!(inferred_content_type, content_type);
    }

    #[test]
    fn direct_primitives_follow_their_runtime_representations() {
        assert_typed("String", "String", "text/plain; charset=utf-8");
        assert_typed("&'static str", "String", "text/plain; charset=utf-8");
        assert_typed("bool", "bool", "application/json");
        assert!(matches!(inferred("Vec<u8>"), OpenApiSuccessPlan::Binary));
    }

    #[test]
    fn result_primitives_follow_the_blanket_json_contract() {
        assert_typed("Result<String, HttpApiError>", "String", "application/json");
        assert_typed(
            "Result<&'static str, HttpApiError>",
            "String",
            "application/json",
        );
        assert_typed("Result<bool, HttpApiError>", "bool", "application/json");
        assert_typed(
            "Result<Vec<u8>, HttpApiError>",
            "Vec < u8 >",
            "application/json",
        );
    }

    #[test]
    fn explicit_response_wrappers_infer_their_inner_or_wire_schema() {
        for return_type in ["Json<View>", "Result<Json<View>, HttpApiError>"] {
            assert_typed(return_type, "View", "application/json");
        }
        for return_type in ["PlainText", "Result<PlainText, HttpApiError>"] {
            assert_typed(return_type, "String", "text/plain; charset=utf-8");
        }
        for return_type in ["BinaryData", "Result<BinaryData, HttpApiError>"] {
            assert!(matches!(inferred(return_type), OpenApiSuccessPlan::Binary));
        }
        for return_type in ["NoContent", "Result<NoContent, HttpApiError>"] {
            assert!(matches!(
                inferred(return_type),
                OpenApiSuccessPlan::NoContent
            ));
        }
    }

    #[test]
    fn location_results_infer_status_and_distinguish_empty_from_json_bodies() {
        for (name, expected_status) in [("Created", "201"), ("Accepted", "202")] {
            for (suffix, expected_schema) in [
                ("", None),
                ("<EmptyBody>", None),
                ("<lily_http_api::EmptyBody>", None),
                ("<View>", Some("View")),
                ("<()>", Some("()")),
                ("<Option<View>>", Some("Option < View >")),
            ] {
                for return_type in [
                    format!("{name}{suffix}"),
                    format!("Result<lily_http_api::{name}{suffix}, ApplicationError>"),
                ] {
                    let OpenApiSuccessPlan::Location { status, schema, .. } =
                        inferred(&return_type)
                    else {
                        panic!("{return_type} must infer a location response");
                    };
                    assert_eq!(status, expected_status);
                    assert_eq!(
                        schema.map(|schema| quote!(#schema).to_string()).as_deref(),
                        expected_schema
                    );
                }
            }
        }
    }

    #[test]
    fn location_results_reject_duplicate_success_status_metadata() {
        for (name, status) in [("Created", "201"), ("Accepted", "202")] {
            let response = crate::openapi::ResponseArg {
                status: syn::LitStr::new(status, proc_macro2::Span::call_site()),
                description: None,
                schema: None,
                reusable: None,
                content_type: None,
                example: None,
            };
            for return_type in [
                name.to_owned(),
                format!("Result<{name}<View>, ApplicationError>"),
            ] {
                let error = plan_openapi_success(
                    &signature(&return_type),
                    std::slice::from_ref(&response),
                    false,
                )
                .err()
                .expect("duplicate inferred success metadata is rejected");
                assert!(error.to_string().contains(&format!(
                    "explicit {status} response conflicts with inferred {name}"
                )));
            }
        }
    }

    #[test]
    fn ordinary_result_dto_is_json_but_direct_custom_responses_are_explicit() {
        assert_typed(
            "Result<ApplicationView, HttpApiError>",
            "ApplicationView",
            "application/json",
        );

        let error = plan_openapi_success(&signature("ApplicationView"), &[], false)
            .err()
            .expect("a direct custom response has no inferable wire contract");
        assert!(error
            .to_string()
            .contains("direct custom response types require explicit responses"));
        assert!(
            plan_openapi_success(&signature("ApplicationView"), &[], true)
                .expect("explicit IntoResponses metadata supplies the contract")
                .is_none()
        );
    }

    #[test]
    fn dynamic_and_streaming_response_types_require_explicit_metadata() {
        for return_type in [
            "ResponseBuilder",
            "Result<ResponseBuilder, HttpApiError>",
            "StreamingResponse",
            "Result<StreamingResponse, HttpApiError>",
            "SseResponse",
            "Result<SseResponse, HttpApiError>",
            "StaticFileResponse",
            "Result<StaticFileResponse, HttpApiError>",
        ] {
            let error = plan_openapi_success(&signature(return_type), &[], false)
                .err()
                .expect("dynamic response metadata must be explicit");
            assert!(error.to_string().contains("explicit responses"), "{error}");
            assert!(plan_openapi_success(&signature(return_type), &[], true)
                .expect("explicit IntoResponses metadata supplies the contract")
                .is_none());
        }
    }
}
