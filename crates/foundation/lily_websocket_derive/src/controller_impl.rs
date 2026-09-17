use std::collections::BTreeSet;

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{
    Attribute, FnArg, GenericArgument, ImplItem, ImplItemFn, ItemImpl, PathArguments, ReturnType,
    Type, parse_macro_input,
};

use crate::asyncapi;
use crate::runtime_path;
use crate::syntax::{
    is_named, named_attributes, parse_optional_single_type, parse_string, parse_timeout_seconds,
    parse_type_list, reject_duplicate_authority, validate_route_token,
};

#[derive(Clone, Copy, PartialEq, Eq)]
enum OperationKind {
    Message,
    Connected,
    Disconnected,
}

struct Operation {
    kind: OperationKind,
    method_name: syn::Ident,
    event: Option<syn::LitStr>,
    argument_types: Vec<Type>,
    message_middlewares: Vec<Type>,
    guards: Vec<Type>,
    payload_codec: Option<Type>,
    timeout_seconds: Option<u64>,
    asyncapi: Option<asyncapi::AsyncApiArgs>,
    conditional_attributes: Vec<Attribute>,
}

pub(crate) fn expand(args: TokenStream, input: TokenStream) -> TokenStream {
    if !args.is_empty() {
        return syn::Error::new(
            proc_macro2::Span::call_site(),
            "#[websocket_controller] does not accept arguments",
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
    validate_impl(&input)?;
    let controller = input.self_ty.as_ref().clone();
    let controller_identifier = controller_identifier(&controller)?;

    let mut operations = Vec::new();
    let mut events = BTreeSet::new();
    let mut connected = None;
    let mut disconnected = None;

    for item in &mut input.items {
        let ImplItem::Fn(method) = item else {
            return Err(syn::Error::new_spanned(
                item,
                "#[websocket_controller] impl blocks may contain only WebSocket operation methods",
            ));
        };
        let operation = parse_operation(method)?;
        match operation.kind {
            OperationKind::Message => {
                let event = operation.event.as_ref().expect("messages carry events");
                if !events.insert(event.value()) {
                    return Err(syn::Error::new(
                        event.span(),
                        format!("duplicate WebSocket message event `{}`", event.value()),
                    ));
                }
            }
            OperationKind::Connected => {
                if let Some(first) = connected.as_ref() {
                    return Err(syn::Error::new_spanned(
                        &operation.method_name,
                        format!("controller already declares #[connected] on `{first}`"),
                    ));
                }
                connected = Some(operation.method_name.clone());
            }
            OperationKind::Disconnected => {
                if let Some(first) = disconnected.as_ref() {
                    return Err(syn::Error::new_spanned(
                        &operation.method_name,
                        format!("controller already declares #[disconnected] on `{first}`"),
                    ));
                }
                disconnected = Some(operation.method_name.clone());
            }
        }
        operations.push(operation);
    }

    if operations.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.self_ty,
            "#[websocket_controller] requires at least one message or lifecycle operation",
        ));
    }

    let runtime = runtime_path::lily_websocket()?;
    let runtime_path: syn::Path =
        syn::parse2(runtime.clone()).expect("resolved Lily WebSocket runtime must be a Rust path");
    let runtime_crate = &runtime_path
        .segments
        .last()
        .expect("resolved Lily WebSocket runtime path must contain its crate name")
        .ident;
    validate_lifecycle_arguments(&operations, runtime_crate)?;
    let generated = operations.iter().map(|operation| {
        generate_operation(&runtime, &controller, &controller_identifier, operation)
    });

    Ok(quote! {
        #input
        #(#generated)*
    })
}

fn validate_impl(input: &ItemImpl) -> syn::Result<()> {
    if input.trait_.is_some() {
        return Err(syn::Error::new_spanned(
            input,
            "#[websocket_controller] must be applied to an inherent impl",
        ));
    }
    if !input.generics.params.is_empty() || input.generics.where_clause.is_some() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "generic WebSocket controller impl blocks are not supported",
        ));
    }
    controller_identifier(&input.self_ty)?;
    Ok(())
}

fn controller_identifier(controller: &Type) -> syn::Result<syn::Ident> {
    let Type::Path(controller) = controller else {
        return Err(syn::Error::new_spanned(
            controller,
            "WebSocket controller self type must be a concrete path",
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
            "generic or qualified WebSocket controller self types are not supported",
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

fn parse_operation(method: &mut ImplItemFn) -> syn::Result<Operation> {
    let operation_attributes = method
        .attrs
        .iter()
        .filter(|attribute| {
            is_named(attribute, "message")
                || is_named(attribute, "connected")
                || is_named(attribute, "disconnected")
        })
        .collect::<Vec<_>>();
    let operation_attribute = operation_attributes.first().ok_or_else(|| {
        syn::Error::new_spanned(
            &method.sig.ident,
            "WebSocket operation requires exactly one of #[message(\"...\")], #[connected], or #[disconnected]",
        )
    })?;
    if let Some(duplicate) = operation_attributes.get(1) {
        return Err(syn::Error::new_spanned(
            duplicate,
            "WebSocket operation has more than one operation attribute",
        ));
    }

    let (kind, event) = if is_named(operation_attribute, "message") {
        let event = parse_string(operation_attribute, "message event")?;
        validate_route_token(&event, "message event", 256)?;
        (OperationKind::Message, Some(event))
    } else if is_named(operation_attribute, "connected") {
        require_path_attribute(operation_attribute, "connected")?;
        (OperationKind::Connected, None)
    } else {
        require_path_attribute(operation_attribute, "disconnected")?;
        (OperationKind::Disconnected, None)
    };

    let argument_types = validate_signature(method, kind)?;

    let message_middlewares = parse_types(
        &method.attrs,
        "message_middleware",
        "action message middleware",
    )?;
    let guards = parse_types(&method.attrs, "guard", "action guard")?;
    if kind != OperationKind::Message && (!message_middlewares.is_empty() || !guards.is_empty()) {
        return Err(syn::Error::new_spanned(
            &method.sig.ident,
            "lifecycle operations cannot declare message middleware or guards",
        ));
    }
    for (name, label) in [
        ("handshake_middleware", "handshake middleware"),
        ("connection_middleware", "connection middleware"),
    ] {
        if let Some(attribute) = named_attributes(&method.attrs, name).first() {
            return Err(syn::Error::new_spanned(
                attribute,
                format!(
                    "#[{name}(...)] is controller-level {label} metadata and cannot be declared on an operation"
                ),
            ));
        }
    }

    if let Some(attribute) = named_attributes(&method.attrs, "frame_codec").first() {
        return Err(syn::Error::new_spanned(
            attribute,
            "#[frame_codec(Type)] is controller-level metadata and cannot be declared on an operation",
        ));
    }
    let payload_codec_attributes = named_attributes(&method.attrs, "payload_codec");
    let payload_codec =
        parse_optional_single_type(&method.attrs, "payload_codec", "action payload codec")?;
    if kind != OperationKind::Message
        && let Some(attribute) = payload_codec_attributes.first()
    {
        return Err(syn::Error::new_spanned(
            attribute,
            "lifecycle operations cannot declare a payload codec",
        ));
    }

    let timeout_attributes = named_attributes(&method.attrs, "timeout");
    reject_duplicate_authority(&timeout_attributes, "action timeout")?;
    let timeout_seconds = timeout_attributes
        .first()
        .map(|attribute| parse_timeout_seconds(attribute))
        .transpose()?;

    let asyncapi_attributes = named_attributes(&method.attrs, "asyncapi");
    reject_duplicate_authority(&asyncapi_attributes, "action AsyncAPI")?;
    let asyncapi = asyncapi_attributes
        .first()
        .map(|attribute| asyncapi::parse(attribute))
        .transpose()?;

    let conditional_attributes = method
        .attrs
        .iter()
        .filter(|attribute| is_named(attribute, "cfg") || is_named(attribute, "cfg_attr"))
        .cloned()
        .collect();
    method.attrs.retain(|attribute| {
        !is_named(attribute, "message")
            && !is_named(attribute, "connected")
            && !is_named(attribute, "disconnected")
            && !is_named(attribute, "message_middleware")
            && !is_named(attribute, "guard")
            && !is_named(attribute, "frame_codec")
            && !is_named(attribute, "payload_codec")
            && !is_named(attribute, "timeout")
            && !is_named(attribute, "asyncapi")
    });

    Ok(Operation {
        kind,
        method_name: method.sig.ident.clone(),
        event,
        argument_types,
        message_middlewares,
        guards,
        payload_codec,
        timeout_seconds,
        asyncapi,
        conditional_attributes,
    })
}

fn require_path_attribute(attribute: &Attribute, name: &str) -> syn::Result<()> {
    if !matches!(attribute.meta, syn::Meta::Path(_)) {
        return Err(syn::Error::new_spanned(
            attribute,
            format!("#[{name}] accepts no arguments"),
        ));
    }
    Ok(())
}

fn parse_types(attributes: &[Attribute], name: &str, context: &str) -> syn::Result<Vec<Type>> {
    let attributes = named_attributes(attributes, name);
    reject_duplicate_authority(&attributes, context)?;
    attributes
        .first()
        .map(|attribute| parse_type_list(attribute, context))
        .transpose()
        .map(Option::unwrap_or_default)
}

const MAX_OPERATION_ARGUMENTS: usize = 16;

fn validate_signature(method: &ImplItemFn, kind: OperationKind) -> syn::Result<Vec<Type>> {
    if method.sig.asyncness.is_none() {
        return Err(syn::Error::new_spanned(
            method.sig.fn_token,
            "WebSocket controller operations must be async",
        ));
    }
    if method.sig.constness.is_some()
        || method.sig.unsafety.is_some()
        || method.sig.abi.is_some()
        || method.sig.variadic.is_some()
        || !method.sig.generics.params.is_empty()
        || method.sig.generics.where_clause.is_some()
    {
        return Err(syn::Error::new_spanned(
            &method.sig,
            "WebSocket controller operations cannot be const, unsafe, extern, variadic, or generic",
        ));
    }

    let Some(FnArg::Receiver(receiver)) = method.sig.inputs.first() else {
        return Err(syn::Error::new_spanned(
            &method.sig.inputs,
            "WebSocket controller operations require immutable `&self` first",
        ));
    };
    if receiver.reference.is_none()
        || receiver.mutability.is_some()
        || receiver.colon_token.is_some()
    {
        return Err(syn::Error::new_spanned(
            receiver,
            "WebSocket controller operations require immutable `&self` first",
        ));
    }

    let arguments = method.sig.inputs.iter().skip(1).collect::<Vec<_>>();
    if arguments.len() > MAX_OPERATION_ARGUMENTS {
        return Err(syn::Error::new_spanned(
            &method.sig.inputs,
            format!(
                "WebSocket controller operations accept at most {MAX_OPERATION_ARGUMENTS} typed extractor arguments"
            ),
        ));
    }
    let argument_types = arguments
        .into_iter()
        .map(operation_argument_type)
        .collect::<syn::Result<Vec<_>>>()?;

    match kind {
        OperationKind::Message => validate_message_result(&method.sig.output)?,
        OperationKind::Connected | OperationKind::Disconnected => {
            validate_lifecycle_result(&method.sig.output)?;
        }
    }
    Ok(argument_types)
}

fn operation_argument_type(argument: &FnArg) -> syn::Result<Type> {
    let FnArg::Typed(argument) = argument else {
        return Err(syn::Error::new_spanned(
            argument,
            "WebSocket controller receiver must appear exactly once and be the first parameter",
        ));
    };

    if contains_reference(&argument.ty) {
        return Err(syn::Error::new_spanned(
            &argument.ty,
            "WebSocket controller extractor arguments must be owned values; references and mutable raw inputs are not supported",
        ));
    }
    if contains_raw_input(&argument.ty) {
        return Err(syn::Error::new_spanned(
            &argument.ty,
            "raw WebSocket invocation/request inputs are not action extractors; use WebSocketMessageContext or a typed extractor",
        ));
    }

    Ok(argument.ty.as_ref().clone())
}

fn validate_lifecycle_arguments(
    operations: &[Operation],
    runtime_crate: &syn::Ident,
) -> syn::Result<()> {
    for operation in operations {
        for argument_type in &operation.argument_types {
            validate_lifecycle_argument(argument_type, operation.kind, runtime_crate)?;
        }
    }
    Ok(())
}

fn validate_lifecycle_argument(
    argument_type: &Type,
    kind: OperationKind,
    runtime_crate: &syn::Ident,
) -> syn::Result<()> {
    let lifecycle_attribute = match kind {
        OperationKind::Message => return Ok(()),
        OperationKind::Connected => "connected",
        OperationKind::Disconnected => "disconnected",
    };
    let Some(extractor) = message_only_extractor(argument_type, runtime_crate) else {
        return Ok(());
    };

    Err(syn::Error::new_spanned(
        argument_type,
        format!(
            "WebSocket message-only extractor `{extractor}` cannot be used by a `#[{lifecycle_attribute}]` lifecycle operation because lifecycle operations do not receive message payloads"
        ),
    ))
}

fn message_only_extractor<'a>(ty: &'a Type, runtime_crate: &syn::Ident) -> Option<&'a syn::Ident> {
    let path = match ty {
        Type::Group(group) => return message_only_extractor(&group.elem, runtime_crate),
        Type::Paren(paren) => return message_only_extractor(&paren.elem, runtime_crate),
        Type::Path(path) => path,
        _ => return None,
    };
    if path.qself.is_some() || path.path.segments.len() != 2 {
        return None;
    }
    let mut segments = path.path.segments.iter();
    let crate_segment = segments.next()?;
    if crate_segment.ident != *runtime_crate
        || !matches!(crate_segment.arguments, PathArguments::None)
    {
        return None;
    }
    let identifier = &segments.next()?.ident;
    match identifier.to_string().as_str() {
        "Payload" | "TextPayload" | "BinaryPayload" | "RawPayload" | "RawEnvelope" => {
            Some(identifier)
        }
        _ => None,
    }
}

fn contains_reference(ty: &Type) -> bool {
    match ty {
        Type::Reference(_) => true,
        Type::Array(array) => contains_reference(&array.elem),
        Type::BareFn(function) => {
            function
                .inputs
                .iter()
                .any(|argument| contains_reference(&argument.ty))
                || match &function.output {
                    ReturnType::Default => false,
                    ReturnType::Type(_, ty) => contains_reference(ty),
                }
        }
        Type::Group(group) => contains_reference(&group.elem),
        Type::Paren(paren) => contains_reference(&paren.elem),
        Type::Path(path) => path
            .path
            .segments
            .iter()
            .any(|segment| path_arguments_contain(&segment.arguments, contains_reference)),
        Type::Ptr(pointer) => contains_reference(&pointer.elem),
        Type::Slice(slice) => contains_reference(&slice.elem),
        Type::Tuple(tuple) => tuple.elems.iter().any(contains_reference),
        _ => false,
    }
}

fn contains_raw_input(ty: &Type) -> bool {
    const RAW_INPUT_NAMES: &[&str] = &[
        "WsRequest",
        "WebSocketMessageInvocation",
        "WebSocketLifecycleInvocation",
    ];

    match ty {
        Type::Array(array) => contains_raw_input(&array.elem),
        Type::BareFn(function) => {
            function
                .inputs
                .iter()
                .any(|argument| contains_raw_input(&argument.ty))
                || match &function.output {
                    ReturnType::Default => false,
                    ReturnType::Type(_, ty) => contains_raw_input(ty),
                }
        }
        Type::Group(group) => contains_raw_input(&group.elem),
        Type::Paren(paren) => contains_raw_input(&paren.elem),
        Type::Path(path) => path.path.segments.iter().any(|segment| {
            RAW_INPUT_NAMES.contains(&segment.ident.to_string().as_str())
                || path_arguments_contain(&segment.arguments, contains_raw_input)
        }),
        Type::Ptr(pointer) => contains_raw_input(&pointer.elem),
        Type::Reference(reference) => contains_raw_input(&reference.elem),
        Type::Slice(slice) => contains_raw_input(&slice.elem),
        Type::Tuple(tuple) => tuple.elems.iter().any(contains_raw_input),
        _ => false,
    }
}

fn path_arguments_contain(arguments: &PathArguments, predicate: fn(&Type) -> bool) -> bool {
    match arguments {
        PathArguments::AngleBracketed(arguments) => arguments
            .args
            .iter()
            .any(|argument| matches!(argument, GenericArgument::Type(ty) if predicate(ty))),
        PathArguments::Parenthesized(arguments) => {
            arguments.inputs.iter().any(predicate)
                || match &arguments.output {
                    ReturnType::Default => false,
                    ReturnType::Type(_, ty) => predicate(ty),
                }
        }
        PathArguments::None => false,
    }
}

fn validate_message_result(output: &ReturnType) -> syn::Result<()> {
    let (success, error) = result_types(output, "WebSocket message operations")?;
    if is_unit(success) || !type_name_is(error, "WebSocketActionError") {
        return Err(syn::Error::new_spanned(
            output,
            "WebSocket message operations must return Result<O, WebSocketActionError> where O is a typed action outcome; unit success is not supported",
        ));
    }
    Ok(())
}

fn validate_lifecycle_result(output: &ReturnType) -> syn::Result<()> {
    let (success, error) = result_types(output, "WebSocket lifecycle operations")?;
    if !is_unit(success) || !type_name_is(error, "WebSocketLifecycleError") {
        return Err(syn::Error::new_spanned(
            output,
            "WebSocket lifecycle operations must return Result<(), WebSocketLifecycleError>",
        ));
    }
    Ok(())
}

fn result_types<'a>(output: &'a ReturnType, operation: &str) -> syn::Result<(&'a Type, &'a Type)> {
    let ReturnType::Type(_, ty) = output else {
        return Err(syn::Error::new_spanned(
            output,
            format!("{operation} must return an explicit Result<Success, Error>"),
        ));
    };
    let Type::Path(result) = ty.as_ref() else {
        return Err(syn::Error::new_spanned(
            ty,
            format!("{operation} must return an explicit Result<Success, Error>"),
        ));
    };
    let Some(segment) = result
        .path
        .segments
        .last()
        .filter(|segment| segment.ident == "Result")
    else {
        return Err(syn::Error::new_spanned(
            ty,
            format!("{operation} must return an explicit Result<Success, Error>"),
        ));
    };
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return Err(syn::Error::new_spanned(
            ty,
            format!("{operation} must return an explicit Result<Success, Error>"),
        ));
    };
    let mut arguments = arguments.args.iter();
    let (Some(GenericArgument::Type(success)), Some(GenericArgument::Type(error))) =
        (arguments.next(), arguments.next())
    else {
        return Err(syn::Error::new_spanned(
            ty,
            format!("{operation} must return an explicit Result<Success, Error>"),
        ));
    };
    if arguments.next().is_some() {
        return Err(syn::Error::new_spanned(
            ty,
            format!("{operation} must return an explicit Result<Success, Error>"),
        ));
    }
    Ok((success, error))
}

fn is_unit(ty: &Type) -> bool {
    matches!(ty, Type::Tuple(tuple) if tuple.elems.is_empty())
}

fn type_name_is(ty: &Type, expected: &str) -> bool {
    matches!(ty, Type::Path(path) if path.qself.is_none() && path.path.segments.last().is_some_and(|segment| segment.ident == expected))
}

fn generate_operation(
    runtime: &TokenStream2,
    controller: &Type,
    controller_identifier: &syn::Ident,
    operation: &Operation,
) -> TokenStream2 {
    let Operation {
        kind,
        method_name,
        event,
        argument_types,
        message_middlewares,
        guards,
        payload_codec,
        timeout_seconds,
        asyncapi,
        conditional_attributes,
    } = operation;
    let adapter = format_ident!(
        "__LilyWebSocketOperation_{}_{}",
        controller_identifier,
        method_name
    );
    let binder = format_ident!(
        "__lily_websocket_bind_{}_{}",
        controller_identifier,
        method_name
    );
    let pending_function = format_ident!(
        "__lily_pending_websocket_operation_{}_{}",
        controller_identifier,
        method_name
    );
    let pending_static = format_ident!(
        "__LILY_PENDING_WEBSOCKET_OPERATION_{}_{}",
        controller_identifier,
        method_name
    );

    let timeout = timeout_seconds.map_or_else(
        || quote!(::std::option::Option::None),
        |seconds| quote!(::std::option::Option::Some(::std::time::Duration::from_secs(#seconds))),
    );
    let asyncapi = asyncapi::generate(runtime, asyncapi.as_ref());
    let payload_codec = payload_codec.as_ref().map_or_else(
        || quote!(::std::option::Option::None),
        |codec| {
            quote!(::std::option::Option::Some(
                #runtime::__private::WebSocketPayloadCodecRegistration::of::<#codec>()
            ))
        },
    );
    let handler_name = quote! {
        concat!(
            module_path!(),
            "::",
            stringify!(#controller),
            "::",
            stringify!(#method_name),
        )
    };
    let (argument_tuple, argument_pattern, action_arguments) =
        generate_argument_tuple(argument_types);

    let (kind_token, event_token, adapter_impl, bound_token) = match kind {
        OperationKind::Message => {
            let event = event.as_ref().expect("messages carry events");
            (
                quote!(#runtime::__private::WebSocketOperationKind::Message),
                quote!(::std::option::Option::Some(#event)),
                quote! {
                    impl #runtime::__private::WebSocketMessageAction for #adapter {
                        fn call(
                            &self,
                            invocation: #runtime::__private::WebSocketMessageInvocation,
                        ) -> #runtime::__private::WebSocketActionFuture {
                            let controller = ::std::sync::Arc::clone(&self.controller);
                            ::std::boxed::Box::pin(async move {
                                let mut invocation = invocation;
                                let #argument_pattern: #argument_tuple =
                                    #runtime::__private::extract_message_arguments::<
                                        #argument_tuple,
                                        _,
                                    >(&mut invocation).await?;
                                let output = controller
                                    .#method_name(#(#action_arguments),*)
                                    .await?;
                                #runtime::__private::into_websocket_action_outcome(output)
                            })
                        }
                    }
                },
                quote! {
                    let handler: #runtime::__private::WebSocketActionHandler =
                        ::std::sync::Arc::new(#adapter { controller });
                    #runtime::__private::BoundWebSocketOperation::Message(handler)
                },
            )
        }
        OperationKind::Connected | OperationKind::Disconnected => {
            let is_connected = *kind == OperationKind::Connected;
            let lifecycle_kind = if is_connected {
                quote!(#runtime::__private::WebSocketOperationKind::Connected)
            } else {
                quote!(#runtime::__private::WebSocketOperationKind::Disconnected)
            };
            let bound = if is_connected {
                quote!(#runtime::__private::BoundWebSocketOperation::Connected(handler))
            } else {
                quote!(#runtime::__private::BoundWebSocketOperation::Disconnected(handler))
            };
            let extract_arguments = if is_connected {
                quote! {
                    #runtime::__private::extract_connected_arguments::<#argument_tuple>(
                        &mut invocation,
                    ).await?
                }
            } else {
                quote! {
                    #runtime::__private::extract_disconnected_arguments::<#argument_tuple>(
                        &mut invocation,
                    ).await?
                }
            };
            (
                lifecycle_kind,
                quote!(::std::option::Option::None),
                quote! {
                    impl #runtime::__private::WebSocketLifecycleAction for #adapter {
                        fn call(
                            &self,
                            invocation: #runtime::__private::WebSocketLifecycleInvocation,
                        ) -> #runtime::__private::WebSocketLifecycleFuture {
                            let controller = ::std::sync::Arc::clone(&self.controller);
                            ::std::boxed::Box::pin(async move {
                                let mut invocation = invocation;
                                let #argument_pattern: #argument_tuple =
                                    #extract_arguments;
                                controller
                                    .#method_name(#(#action_arguments),*)
                                    .await
                            })
                        }
                    }
                },
                quote! {
                    let handler: #runtime::__private::WebSocketLifecycleHandler =
                        ::std::sync::Arc::new(#adapter { controller });
                    #bound
                },
            )
        }
    };

    quote! {
        #(#conditional_attributes)*
        #[doc(hidden)]
        #[allow(non_camel_case_types)]
        struct #adapter {
            controller: ::std::sync::Arc<#controller>,
        }

        #(#conditional_attributes)*
        #adapter_impl

        #(#conditional_attributes)*
        #[doc(hidden)]
        #[allow(non_snake_case)]
        fn #binder(
            controller: #runtime::__private::ErasedWebSocketController,
        ) -> ::std::result::Result<
            #runtime::__private::BoundWebSocketOperation,
            #runtime::__private::WebSocketControllerBindingError,
        > {
            let controller =
                #runtime::__private::downcast_websocket_controller::<#controller>(controller)?;
            ::std::result::Result::Ok({ #bound_token })
        }

        #(#conditional_attributes)*
        #[doc(hidden)]
        #[allow(non_snake_case)]
        fn #pending_function() -> #runtime::__private::PendingWebSocketOperation {
            let metadata = #runtime::__private::WebSocketOperationMetadata::new(
                ::std::vec![
                    #(#runtime::__private::WebSocketMessageMiddlewareRegistration::of::<#message_middlewares>(),)*
                ],
                ::std::vec![
                    #(#runtime::__private::WebSocketGuardRegistration::of::<#guards>(),)*
                ],
                #payload_codec,
                #timeout,
                #asyncapi,
            );
            #runtime::__private::PendingWebSocketOperation::new(
                #kind_token,
                #event_token,
                #handler_name,
                #runtime::__private::WebSocketActionRegistration::of::<#controller>(#binder),
                metadata,
            )
        }

        #(#conditional_attributes)*
        #[doc(hidden)]
        #[allow(non_upper_case_globals)]
        #[#runtime::__private::linkme::distributed_slice(
            #runtime::__private::PENDING_WEBSOCKET_OPERATION_REGISTRATIONS
        )]
        #[linkme(crate = #runtime::__private::linkme)]
        static #pending_static:
            #runtime::__private::PendingWebSocketOperationRegistrationFn = #pending_function;
    }
}

fn generate_argument_tuple(
    argument_types: &[Type],
) -> (TokenStream2, TokenStream2, Vec<syn::Ident>) {
    let arguments = (0..argument_types.len())
        .map(|index| format_ident!("__lily_websocket_argument_{index}"))
        .collect::<Vec<_>>();
    if argument_types.is_empty() {
        return (quote!(()), quote!(()), arguments);
    }

    (
        quote!((#(#argument_types,)*)),
        quote!((#(#arguments,)*)),
        arguments,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::parse_quote;

    #[test]
    fn duplicate_lifecycle_hook_is_rejected() {
        let input: ItemImpl = parse_quote! {
            impl ChatController {
                #[connected]
                async fn first(&self, context: WebSocketContext)
                    -> Result<(), WebSocketLifecycleError> { Ok(()) }

                #[connected]
                async fn second(&self, context: WebSocketContext)
                    -> Result<(), WebSocketLifecycleError> { Ok(()) }
            }
        };
        let error = expand_impl(input).unwrap_err();
        assert!(error.to_string().contains("already declares #[connected]"));
    }

    #[test]
    fn lifecycle_guard_is_rejected() {
        let mut method: ImplItemFn = parse_quote! {
            #[connected]
            #[guard(AuthenticationGuard)]
            async fn connected(&self, context: WebSocketContext)
                -> Result<(), WebSocketLifecycleError> { Ok(()) }
        };
        let error = match parse_operation(&mut method) {
            Ok(_) => panic!("lifecycle guard must be rejected"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("lifecycle operations cannot"));
    }

    #[test]
    fn duplicate_message_event_is_rejected() {
        let input: ItemImpl = parse_quote! {
            impl ChatController {
                #[message("send")]
                async fn first(&self, context: WebSocketContext, payload: Payload<Input>)
                    -> Result<NoReply, WebSocketActionError> { Ok(NoReply) }

                #[message("send")]
                async fn second(&self, context: WebSocketContext, payload: Payload<Input>)
                    -> Result<NoReply, WebSocketActionError> { Ok(NoReply) }
            }
        };
        let error = expand_impl(input).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("duplicate WebSocket message event")
        );
    }

    #[test]
    fn payload_before_service_is_preserved_for_runtime_tuple_resolution() {
        let mut method: ImplItemFn = parse_quote! {
            #[message("send")]
            async fn send(
                &self,
                context: WebSocketContext,
                payload: Payload<SendMessage>,
                audit: Service<dyn AuditService>,
            ) -> Result<Ack<Receipt>, WebSocketActionError> {
                loop {}
            }
        };
        let operation = parse_operation(&mut method).expect("typed signature must be accepted");
        assert_eq!(operation.argument_types.len(), 3);
        assert!(matches!(operation.kind, OperationKind::Message));
    }

    #[test]
    fn message_unit_success_is_rejected() {
        let mut method: ImplItemFn = parse_quote! {
            #[message("send")]
            async fn send(&self, context: WebSocketContext)
                -> Result<(), WebSocketActionError> { Ok(()) }
        };
        let error = match parse_operation(&mut method) {
            Ok(_) => panic!("unit success must be rejected"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("unit success is not supported"));
    }

    #[test]
    fn referenced_and_raw_inputs_are_rejected() {
        let mut referenced: ImplItemFn = parse_quote! {
            #[message("send")]
            async fn send(&self, context: &WebSocketContext)
                -> Result<NoReply, WebSocketActionError> { loop {} }
        };
        let error = match parse_operation(&mut referenced) {
            Ok(_) => panic!("references must be rejected"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("must be owned values"));

        let mut raw: ImplItemFn = parse_quote! {
            #[message("send")]
            async fn send(&self, request: Arc<WsRequest>)
                -> Result<NoReply, WebSocketActionError> { loop {} }
        };
        let error = match parse_operation(&mut raw) {
            Ok(_) => panic!("raw request must be rejected"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("raw WebSocket invocation/request")
        );
    }

    #[test]
    fn lifecycle_rejects_runtime_qualified_message_only_extractors() {
        for (runtime_crate, argument_type, extractor) in [
            ("ws", "ws::Payload<Input>", "Payload"),
            ("ws", "::ws::TextPayload", "TextPayload"),
            (
                "lily_websocket",
                "lily_websocket::BinaryPayload",
                "BinaryPayload",
            ),
            ("ws", "ws::RawPayload", "RawPayload"),
            ("ws", "ws::RawEnvelope", "RawEnvelope"),
        ] {
            let source = format!(
                r#"
                #[connected]
                async fn connected(&self, input: {argument_type})
                    -> Result<(), WebSocketLifecycleError> {{ Ok(()) }}
                "#
            );
            let mut method: ImplItemFn = syn::parse_str(&source).expect("fixture method parses");
            let operation = parse_operation(&mut method).expect("operation syntax is valid");
            let runtime_crate: syn::Ident =
                syn::parse_str(runtime_crate).expect("runtime crate alias is an identifier");
            let error = match validate_lifecycle_arguments(
                std::slice::from_ref(&operation),
                &runtime_crate,
            ) {
                Ok(()) => panic!("{extractor} must be rejected on a lifecycle operation"),
                Err(error) => error,
            };

            assert_eq!(
                error.to_string(),
                format!(
                    "WebSocket message-only extractor `{extractor}` cannot be used by a `#[connected]` lifecycle operation because lifecycle operations do not receive message payloads"
                )
            );
        }
    }

    #[test]
    fn lifecycle_custom_extractors_and_aliases_keep_trait_bound_fallback() {
        let runtime_crate: syn::Ident = parse_quote!(ws);
        for argument_type in [
            "Payload<Input>",
            "custom::Payload<Input>",
            "custom::BinaryPayload",
            "PayloadAlias<Input>",
            "ws::aliases::RawPayload",
            "<Codec as Types>::RawEnvelope",
            "custom::LifecycleExtractor",
        ] {
            let source = format!(
                r#"
                #[disconnected]
                async fn disconnected(&self, input: {argument_type})
                    -> Result<(), WebSocketLifecycleError> {{ Ok(()) }}
                "#
            );
            let mut method: ImplItemFn = syn::parse_str(&source).expect("fixture method parses");
            let operation = parse_operation(&mut method)
                .unwrap_or_else(|error| panic!("{argument_type} syntax must be valid: {error}"));
            validate_lifecycle_arguments(std::slice::from_ref(&operation), &runtime_crate)
                .unwrap_or_else(|error| {
                    panic!("{argument_type} must reach trait fallback: {error}")
                });
        }

        let mut message: ImplItemFn = parse_quote! {
            #[message("send")]
            async fn send(&self, json: Payload<Input>, binary: BinaryPayload)
                -> Result<NoReply, WebSocketActionError> { loop {} }
        };
        parse_operation(&mut message)
            .expect("message operations retain runtime payload-consumer validation");
    }

    #[test]
    fn operation_argument_count_is_bounded() {
        let arguments = (0..17)
            .map(|index| format!("argument_{index}: Extractor{index}"))
            .collect::<Vec<_>>()
            .join(", ");
        let source = format!(
            "#[message(\"send\")] async fn send(&self, {arguments}) -> Result<NoReply, WebSocketActionError> {{ loop {{}} }}"
        );
        let mut method: ImplItemFn = syn::parse_str(&source).expect("fixture method parses");
        let error = match parse_operation(&mut method) {
            Ok(_) => panic!("argument limit must be enforced"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("at most 16"));
    }

    #[test]
    fn operation_codec_authority_is_exact_and_message_only() {
        let mut message: ImplItemFn = parse_quote! {
            #[message("send")]
            #[payload_codec(JsonPayloadCodec)]
            async fn send(&self, payload: Payload<Input>)
                -> Result<NoReply, WebSocketActionError> { loop {} }
        };
        let operation = parse_operation(&mut message).expect("message payload codec is valid");
        assert!(matches!(operation.payload_codec, Some(Type::Path(_))));

        let mut duplicate: ImplItemFn = parse_quote! {
            #[message("send")]
            #[payload_codec(FirstCodec)]
            #[payload_codec(SecondCodec)]
            async fn send(&self, payload: Payload<Input>)
                -> Result<NoReply, WebSocketActionError> { loop {} }
        };
        let error = match parse_operation(&mut duplicate) {
            Ok(_) => panic!("duplicate payload codec must be rejected"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("duplicate action payload codec"));

        let mut action_frame: ImplItemFn = parse_quote! {
            #[message("send")]
            #[frame_codec(FrameCodec)]
            async fn send(&self, payload: Payload<Input>)
                -> Result<NoReply, WebSocketActionError> { loop {} }
        };
        let error = match parse_operation(&mut action_frame) {
            Ok(_) => panic!("action frame codec must be rejected"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("controller-level metadata"));

        let mut lifecycle: ImplItemFn = parse_quote! {
            #[connected]
            #[payload_codec(JsonPayloadCodec)]
            async fn connected(&self, context: WebSocketContext)
                -> Result<(), WebSocketLifecycleError> { Ok(()) }
        };
        let error = match parse_operation(&mut lifecycle) {
            Ok(_) => panic!("lifecycle payload codec must be rejected"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("cannot declare a payload codec"));
    }

    #[test]
    fn tuple_generation_handles_zero_one_and_multiple_arguments() {
        let (empty_type, empty_pattern, empty_arguments) = generate_argument_tuple(&[]);
        assert_eq!(empty_type.to_string(), "()");
        assert_eq!(empty_pattern.to_string(), "()");
        assert!(empty_arguments.is_empty());

        let one = vec![parse_quote!(Payload<Input>)];
        let (one_type, one_pattern, one_arguments) = generate_argument_tuple(&one);
        assert_eq!(one_type.to_string(), "(Payload < Input > ,)");
        assert_eq!(one_pattern.to_string(), "(__lily_websocket_argument_0 ,)");
        assert_eq!(one_arguments.len(), 1);

        let multiple = vec![parse_quote!(WebSocketContext), parse_quote!(Service<Audit>)];
        let (multiple_type, multiple_pattern, multiple_arguments) =
            generate_argument_tuple(&multiple);
        assert_eq!(
            multiple_type.to_string(),
            "(WebSocketContext , Service < Audit > ,)"
        );
        assert_eq!(multiple_arguments.len(), 2);
        assert!(multiple_pattern.to_string().contains("argument_1"));
    }

    #[test]
    fn generated_adapters_use_invocation_tuple_and_typed_outcome_abi() {
        let mut message: ImplItemFn = parse_quote! {
            #[message("send")]
            #[message_middleware(MessageTracing)]
            #[payload_codec(JsonPayloadCodec)]
            #[timeout(seconds = 1)]
            async fn send(
                &self,
                payload: Payload<Input>,
                service: Service<Audit>,
            ) -> Result<Ack<Receipt>, WebSocketActionError> {
                loop {}
            }
        };
        let message = parse_operation(&mut message).expect("message operation parses");
        let controller: Type = parse_quote!(ChatController);
        let controller_identifier = parse_quote!(ChatController);
        let generated = generate_operation(
            &quote!(::runtime),
            &controller,
            &controller_identifier,
            &message,
        )
        .to_string();
        assert!(generated.contains("WebSocketMessageInvocation"));
        assert!(generated.contains("extract_message_arguments"));
        assert!(generated.contains("into_websocket_action_outcome (output)"));
        assert!(!generated.contains("into_websocket_action_outcome (output ,"));
        assert!(generated.contains("WebSocketPayloadCodecRegistration"));
        assert!(generated.contains("WebSocketMessageMiddlewareRegistration"));
        assert!(generated.contains("of :: < MessageTracing >"));
        assert!(generated.contains("Payload < Input > , Service < Audit >"));
        assert!(!generated.contains("WsRequest"));

        let payload_codec = generated
            .find("WebSocketPayloadCodecRegistration")
            .expect("metadata contains the payload codec registration");
        let timeout = generated[payload_codec..]
            .find("Duration")
            .map(|offset| payload_codec + offset)
            .expect("metadata contains the timeout after the payload codec");
        assert!(payload_codec < timeout);

        let mut lifecycle: ImplItemFn = parse_quote! {
            #[disconnected]
            async fn disconnected(
                &self,
                context: WebSocketContext,
                reason: DisconnectReason,
            ) -> Result<(), WebSocketLifecycleError> {
                Ok(())
            }
        };
        let lifecycle = parse_operation(&mut lifecycle).expect("lifecycle operation parses");
        let generated = generate_operation(
            &quote!(::runtime),
            &controller,
            &controller_identifier,
            &lifecycle,
        )
        .to_string();
        assert!(generated.contains("WebSocketLifecycleInvocation"));
        assert!(generated.contains("extract_disconnected_arguments"));
        assert!(!generated.contains("extract_connected_arguments"));
        assert!(!generated.contains("extract_message_arguments"));

        let mut lifecycle: ImplItemFn = parse_quote! {
            #[connected]
            async fn connected(
                &self,
                context: WebSocketContext,
            ) -> Result<(), WebSocketLifecycleError> {
                Ok(())
            }
        };
        let lifecycle = parse_operation(&mut lifecycle).expect("lifecycle operation parses");
        let generated = generate_operation(
            &quote!(::runtime),
            &controller,
            &controller_identifier,
            &lifecycle,
        )
        .to_string();
        assert!(generated.contains("extract_connected_arguments"));
        assert!(!generated.contains("extract_disconnected_arguments"));
    }

    #[test]
    fn duplicate_action_message_middleware_authority_is_rejected() {
        let mut method: ImplItemFn = parse_quote! {
            #[message("send")]
            #[message_middleware(MessageTracing)]
            #[message_middleware(MessageQuota)]
            async fn send(&self) -> Result<NoReply, WebSocketActionError> {
                Ok(NoReply)
            }
        };
        let error = match parse_operation(&mut method) {
            Ok(_) => panic!("duplicate action middleware authority must be rejected"),
            Err(error) => error,
        };

        assert!(
            error
                .to_string()
                .contains("duplicate action message middleware")
        );
    }

    #[test]
    fn duplicate_action_guard_authority_is_rejected() {
        let mut method: ImplItemFn = parse_quote! {
            #[message("send")]
            #[guard(Authenticated)]
            #[guard(CanSend)]
            async fn send(&self) -> Result<NoReply, WebSocketActionError> {
                Ok(NoReply)
            }
        };
        let error = match parse_operation(&mut method) {
            Ok(_) => panic!("duplicate action guard authority must be rejected"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("duplicate action guard"));
    }

    #[test]
    fn connection_and_handshake_middleware_are_rejected_on_actions() {
        for (attribute, expected) in [
            ("#[connection_middleware(ConnectionAudit)]", "connection"),
            ("#[handshake_middleware(RequestAdmission)]", "handshake"),
        ] {
            let source = format!(
                r#"
                #[message("send")]
                {attribute}
                async fn send(&self) -> Result<NoReply, WebSocketActionError> {{
                    Ok(NoReply)
                }}
                "#
            );
            let mut method: ImplItemFn = syn::parse_str(&source).expect("fixture method parses");
            let error = match parse_operation(&mut method) {
                Ok(_) => panic!("{expected} middleware must be controller-only"),
                Err(error) => error,
            };

            assert!(error.to_string().contains(expected));
            assert!(error.to_string().contains("controller-level"));
        }
    }
}
