use syn::{FnArg, GenericArgument, ImplItemFn, PathArguments, ReturnType, Type, TypePath};

const BODY_EXTRACTOR_NAMES: &[&str] = &["Json", "Form", "MultipartForm", "RawBody", "BodyStream"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ActionArgumentKind {
    Parts,
    Service,
    Terminal,
    Body { streaming: bool },
    RawRequest,
    RawResponse,
    PassthroughResponse,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ActionResponseMode {
    FrameworkManaged,
    Manual,
    Passthrough,
}

pub(crate) struct ActionArgument {
    pub(crate) ty: Type,
    pub(crate) kind: ActionArgumentKind,
}

pub(crate) struct ActionSignaturePlan {
    pub(crate) arguments: Vec<ActionArgument>,
    pub(crate) response_mode: ActionResponseMode,
    pub(crate) success_type: Option<Type>,
    /// Whether `success_type` was extracted from a syntactic `Result<T, E>`.
    ///
    /// This distinction is observable at the response boundary: Lily's
    /// blanket `Result<T, E>` implementation serializes `T` as
    /// JSON, while several direct primitive return types select a different
    /// representation.
    pub(crate) success_is_result: bool,
}

pub(crate) fn plan_action_signature(method: &ImplItemFn) -> syn::Result<ActionSignaturePlan> {
    validate_function_shape(method)?;

    let mut inputs = method.sig.inputs.iter();
    let Some(FnArg::Receiver(receiver)) = inputs.next() else {
        return Err(syn::Error::new_spanned(
            &method.sig.inputs,
            "controller action first parameter must be &self",
        ));
    };
    if receiver.reference.is_none()
        || receiver.mutability.is_some()
        || receiver.colon_token.is_some()
    {
        return Err(syn::Error::new_spanned(
            receiver,
            "controller action receiver must be immutable &self",
        ));
    }

    let mut arguments = Vec::with_capacity(method.sig.inputs.len().saturating_sub(1));
    let mut body_seen = false;
    let mut body_stream_seen = false;
    let mut raw_request_count = 0_usize;
    let mut raw_response_count = 0_usize;
    let mut passthrough_response_count = 0_usize;

    for input in inputs {
        let FnArg::Typed(argument) = input else {
            return Err(syn::Error::new_spanned(
                input,
                "controller action receiver must appear exactly once and be the first parameter",
            ));
        };
        let kind = classify_argument(&argument.ty)?;
        match kind {
            ActionArgumentKind::Parts | ActionArgumentKind::Service if body_seen => {
                return Err(syn::Error::new_spanned(
                    argument,
                    "request-parts extractors must appear before the action body extractor",
                ));
            }
            ActionArgumentKind::Body { streaming } => {
                if body_seen {
                    return Err(syn::Error::new_spanned(
                        argument,
                        "controller actions accept at most one request-body extractor",
                    ));
                }
                body_seen = true;
                body_stream_seen = streaming;
            }
            ActionArgumentKind::RawRequest => {
                raw_request_count += 1;
                if raw_request_count > 1 {
                    return Err(syn::Error::new_spanned(
                        argument,
                        "controller actions accept at most one &mut Request binding",
                    ));
                }
            }
            ActionArgumentKind::RawResponse => {
                raw_response_count += 1;
                if raw_response_count > 1 {
                    return Err(syn::Error::new_spanned(
                        argument,
                        "controller actions accept at most one &mut Response binding",
                    ));
                }
            }
            ActionArgumentKind::PassthroughResponse => {
                passthrough_response_count += 1;
                if passthrough_response_count > 1 {
                    return Err(syn::Error::new_spanned(
                        argument,
                        "controller actions accept at most one PassthroughResponseContext binding",
                    ));
                }
            }
            ActionArgumentKind::Parts => {}
            ActionArgumentKind::Service => {}
            ActionArgumentKind::Terminal => {
                unreachable!("terminal classification is assigned after validation")
            }
        }
        arguments.push(ActionArgument {
            ty: argument.ty.as_ref().clone(),
            kind,
        });
    }

    if body_stream_seen && raw_request_count != 0 {
        return Err(syn::Error::new_spanned(
            &method.sig.inputs,
            "BodyStream and &mut Request cannot be used by the same controller action",
        ));
    }

    if raw_response_count != 0 && passthrough_response_count != 0 {
        return Err(syn::Error::new_spanned(
            &method.sig.inputs,
            "&mut Response and PassthroughResponseContext cannot be used by the same controller action",
        ));
    }

    // Rust trait resolution chooses FromRequestParts or FromRequest for an
    // otherwise unknown last typed argument. Keep raw Request combinations
    // syntactic and conservative so a BodyStream alias cannot bypass the
    // no-shared-body-authority rule.
    if !body_seen && raw_request_count == 0 {
        if let Some(argument) = arguments
            .iter_mut()
            .rev()
            .find(|argument| matches!(argument.kind, ActionArgumentKind::Parts))
        {
            argument.kind = ActionArgumentKind::Terminal;
        }
    }

    let response_mode = if raw_response_count != 0 {
        ActionResponseMode::Manual
    } else if passthrough_response_count != 0 {
        ActionResponseMode::Passthrough
    } else {
        ActionResponseMode::FrameworkManaged
    };
    // A proc macro cannot resolve application error types or aliases.
    // Validate the manual mode's unit-success shape here; the
    // generated `IntoResponse` call then enforces that the complete return
    // type is `()` or `Result<(), E>` with `E: IntoResponse + Send`.
    if response_mode == ActionResponseMode::Manual && !has_unit_success_return(&method.sig.output) {
        return Err(syn::Error::new_spanned(
            &method.sig.output,
            "an action using &mut Response must return () or Result<(), E> where E: IntoResponse + Send",
        ));
    }
    if response_mode == ActionResponseMode::Passthrough
        && has_unit_success_return(&method.sig.output)
    {
        return Err(syn::Error::new_spanned(
            &method.sig.output,
            "an action using PassthroughResponseContext must return a typed response or NoContent",
        ));
    }

    let (success_type, success_is_result) = success_return_type(&method.sig.output);

    Ok(ActionSignaturePlan {
        arguments,
        response_mode,
        success_type: success_type.cloned(),
        success_is_result,
    })
}

fn validate_function_shape(method: &ImplItemFn) -> syn::Result<()> {
    let signature = &method.sig;
    if signature.asyncness.is_none() {
        return Err(syn::Error::new_spanned(
            signature.fn_token,
            "controller actions must be async",
        ));
    }
    if signature.constness.is_some()
        || signature.unsafety.is_some()
        || signature.abi.is_some()
        || signature.variadic.is_some()
    {
        return Err(syn::Error::new_spanned(
            signature,
            "controller actions must be safe, non-const Rust async methods",
        ));
    }
    if !signature.generics.params.is_empty() || signature.generics.where_clause.is_some() {
        return Err(syn::Error::new_spanned(
            &signature.generics,
            "generic controller actions are not supported",
        ));
    }
    Ok(())
}

fn classify_argument(ty: &Type) -> syn::Result<ActionArgumentKind> {
    if let Type::Reference(reference) = ty {
        let Some(name) = plain_type_name(reference.elem.as_ref()) else {
            return Err(syn::Error::new_spanned(
                ty,
                "controller action references support only &mut Request or &mut Response",
            ));
        };
        if reference.mutability.is_none() {
            return Err(syn::Error::new_spanned(
                ty,
                format!("raw {name} binding must use &mut {name}"),
            ));
        }
        return match name.as_str() {
            "Request" => Ok(ActionArgumentKind::RawRequest),
            "Response" => Ok(ActionArgumentKind::RawResponse),
            _ => Err(syn::Error::new_spanned(
                ty,
                "controller action mutable references support only &mut Request or &mut Response",
            )),
        };
    }

    let Some(name) = plain_type_name(ty) else {
        return Err(syn::Error::new_spanned(
            ty,
            "typed controller action parameters must be concrete extractor type paths",
        ));
    };
    if name == "Request" || name == "Response" {
        return Err(syn::Error::new_spanned(
            ty,
            format!("raw {name} binding must use &mut {name}"),
        ));
    }
    if name == "PassthroughResponseContext" {
        return Ok(ActionArgumentKind::PassthroughResponse);
    }
    if BODY_EXTRACTOR_NAMES.contains(&name.as_str()) {
        return Ok(ActionArgumentKind::Body {
            streaming: name == "BodyStream",
        });
    }
    if name == "Service" {
        return Ok(ActionArgumentKind::Service);
    }
    Ok(ActionArgumentKind::Parts)
}

fn plain_type_name(ty: &Type) -> Option<String> {
    let Type::Path(TypePath { qself: None, path }) = ty else {
        return None;
    };
    path.segments
        .last()
        .map(|segment| segment.ident.to_string())
}

fn has_unit_success_return(output: &ReturnType) -> bool {
    match output {
        ReturnType::Default => true,
        ReturnType::Type(_, ty) if is_unit(ty) => true,
        ReturnType::Type(_, ty) => result_success_type(ty).is_some_and(is_unit),
    }
}

fn result_success_type(ty: &Type) -> Option<&Type> {
    let Type::Path(TypePath { qself: None, path }) = ty else {
        return None;
    };
    let segment = path.segments.last()?;
    if segment.ident != "Result" {
        return None;
    }
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return None;
    };
    arguments.args.iter().find_map(|argument| match argument {
        GenericArgument::Type(ty) => Some(ty),
        _ => None,
    })
}

fn success_return_type(output: &ReturnType) -> (Option<&Type>, bool) {
    let ReturnType::Type(_, ty) = output else {
        return (None, false);
    };
    if let Some(success) = result_success_type(ty) {
        return ((!is_unit(success)).then_some(success), true);
    }
    ((!is_unit(ty)).then_some(ty), false)
}

fn is_unit(ty: &Type) -> bool {
    matches!(ty, Type::Tuple(tuple) if tuple.elems.is_empty())
}

#[cfg(test)]
mod tests {
    use super::{plan_action_signature, ActionArgumentKind, ActionResponseMode};
    use syn::ImplItemFn;

    fn method(source: &str) -> ImplItemFn {
        syn::parse_str(source).expect("fixture method parses")
    }

    #[test]
    fn plans_parts_body_and_raw_request_in_declaration_order() {
        let plan = plan_action_signature(&method(
            "async fn action(&self, parts: Probe, body: RawBody, request: &mut Request) -> Result<String, Error> { loop {} }",
        ))
        .expect("the typed signature is valid");
        assert!(matches!(plan.arguments[0].kind, ActionArgumentKind::Parts));
        assert!(matches!(
            plan.arguments[1].kind,
            ActionArgumentKind::Body { streaming: false }
        ));
        assert!(matches!(
            plan.arguments[2].kind,
            ActionArgumentKind::RawRequest
        ));
    }

    #[test]
    fn classifies_framework_and_manual_response_modes_without_legacy_exceptions() {
        let framework =
            plan_action_signature(&method("async fn action(&self) -> String { loop {} }"))
                .expect("typed success is framework managed");
        assert_eq!(
            framework.response_mode,
            ActionResponseMode::FrameworkManaged
        );
        let success_type = framework
            .success_type
            .as_ref()
            .expect("typed success metadata is retained");
        assert_eq!(quote::quote!(#success_type).to_string(), "String");
        assert!(!framework.success_is_result);

        let manual = plan_action_signature(&method(
            "async fn action(&self, response: &mut Response) -> Result<(), HttpApiError> { loop {} }",
        ))
        .expect("canonical unit success with raw response is manual");
        assert_eq!(manual.response_mode, ActionResponseMode::Manual);
        assert!(manual.success_type.is_none());
        assert!(manual.success_is_result);

        for source in [
            "async fn action(&self, response: &mut Response) -> Result<(), lily_http_api::HttpApiError> { loop {} }",
            "async fn action(&self, response: &mut Response) -> Result<(), ApplicationHttpError> { loop {} }",
        ] {
            let aliased = plan_action_signature(&method(source))
                .expect("qualified HttpApiError and aliases retain unit-success validation");
            assert_eq!(aliased.response_mode, ActionResponseMode::Manual);
            assert!(aliased.success_type.is_none());
        }

        for source in [
            "async fn action(&self, response: &mut Response) -> String { loop {} }",
            "async fn action(&self, request: &mut Request, response: &mut Response) -> Result<String, Error> { loop {} }",
        ] {
            let error = plan_action_signature(&method(source))
                .err()
                .expect("raw response always rejects typed success");
            assert!(
                error
                    .to_string()
                    .contains("must return () or Result<(), E> where E: IntoResponse + Send")
            );
        }
    }

    #[test]
    fn preserves_the_result_success_type_for_compile_time_metadata() {
        let plan = plan_action_signature(&method(
            "async fn action(&self, path: Path<UserPath>, body: Json<CreateUser>) -> Result<UserView, Error> { loop {} }",
        ))
        .expect("the typed signature is valid");

        let success_type = plan
            .success_type
            .as_ref()
            .expect("typed success metadata is retained");
        assert_eq!(quote::quote!(#success_type).to_string(), "UserView");
        assert!(plan.success_is_result);
    }

    #[test]
    fn leaves_the_last_unknown_typed_argument_for_trait_based_selection() {
        let plan = plan_action_signature(&method(
            "async fn action(&self, parts: Probe, payload: ApplicationPayload) -> Result<(), Error> { loop {} }",
        ))
        .expect("a custom terminal extractor can be selected by its trait implementation");
        assert!(matches!(plan.arguments[0].kind, ActionArgumentKind::Parts));
        assert!(matches!(
            plan.arguments[1].kind,
            ActionArgumentKind::Terminal
        ));

        let conservative = plan_action_signature(&method(
            "async fn action(&self, payload: ApplicationPayload, request: &mut Request) -> Result<(), Error> { loop {} }",
        ))
        .expect("raw request signatures remain syntactically classified");
        assert!(matches!(
            conservative.arguments[0].kind,
            ActionArgumentKind::Parts
        ));
    }

    #[test]
    fn keeps_service_as_a_request_parts_extractor_when_it_is_last() {
        let plan = plan_action_signature(&method(
            "async fn action(&self, service: Service<dyn ApplicationService>) -> Result<(), Error> { loop {} }",
        ))
        .expect("Service has a fixed request-parts contract");
        assert!(matches!(
            plan.arguments[0].kind,
            ActionArgumentKind::Service
        ));
    }

    #[test]
    fn plans_passthrough_as_a_distinct_response_mode() {
        let plan = plan_action_signature(&method(
            "async fn action(&self, body: Json<Input>, response: PassthroughResponseContext<'_>) -> Result<Output, Error> { loop {} }",
        ))
        .expect("typed success can compose with passthrough metadata");
        assert_eq!(plan.response_mode, ActionResponseMode::Passthrough);
        assert!(matches!(
            plan.arguments[1].kind,
            ActionArgumentKind::PassthroughResponse
        ));
    }

    #[test]
    fn rejects_passthrough_with_manual_response_duplicate_or_unit_success() {
        for (source, message) in [
            (
                "async fn action(&self, raw: &mut Response, staged: PassthroughResponseContext<'_>) -> Result<(), Error> { loop {} }",
                "cannot be used by the same controller action",
            ),
            (
                "async fn action(&self, first: PassthroughResponseContext<'_>, second: PassthroughResponseContext<'_>) -> String { loop {} }",
                "at most one PassthroughResponseContext",
            ),
            (
                "async fn action(&self, staged: PassthroughResponseContext<'_>) -> Result<(), Error> { loop {} }",
                "must return a typed response or NoContent",
            ),
        ] {
            let error = plan_action_signature(&method(source))
                .err()
                .expect("invalid passthrough authority must be rejected");
            assert!(error.to_string().contains(message), "{error}");
        }
    }
}
