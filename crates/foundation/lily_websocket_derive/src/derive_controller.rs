use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, parse_macro_input};

use crate::asyncapi;
use crate::runtime_path;
use crate::syntax::{
    named_attributes, parse_optional_single_type, parse_timeout_seconds, parse_type_list,
    reject_duplicate_authority, validate_route_token,
};

pub(crate) fn derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand(input) {
        Ok(expansion) => expansion.into(),
        Err(error) => error.into_compile_error().into(),
    }
}

fn expand(input: DeriveInput) -> syn::Result<TokenStream2> {
    expand_with_runtime(input, runtime_path::lily_websocket)
}

fn expand_with_runtime(
    input: DeriveInput,
    resolve_runtime: impl FnOnce() -> syn::Result<TokenStream2>,
) -> syn::Result<TokenStream2> {
    let controller = &input.ident;

    if !matches!(input.data, Data::Struct(_)) {
        return Err(syn::Error::new_spanned(
            controller,
            "WebSocketController can only be derived for a struct",
        ));
    }
    if !input.generics.params.is_empty() || input.generics.where_clause.is_some() {
        return Err(syn::Error::new_spanned(
            input.generics,
            "generic WebSocket controllers are not supported",
        ));
    }

    let namespace_attributes = named_attributes(&input.attrs, "namespace");
    reject_duplicate_authority(&namespace_attributes, "controller namespace")?;
    let namespace_attribute = namespace_attributes.first().ok_or_else(|| {
        syn::Error::new_spanned(
            controller,
            "WebSocket controllers require #[namespace(\"...\")]",
        )
    })?;
    let namespace = crate::syntax::parse_string(namespace_attribute, "controller namespace")?;
    validate_route_token(&namespace, "controller namespace", 128)?;

    let handshake_middlewares = parse_types(
        &input.attrs,
        "handshake_middleware",
        "controller handshake middleware",
    )?;
    let connection_middlewares = parse_types(
        &input.attrs,
        "connection_middleware",
        "controller connection middleware",
    )?;
    let message_middlewares = parse_types(
        &input.attrs,
        "message_middleware",
        "controller message middleware",
    )?;
    let guards = parse_types(&input.attrs, "guard", "controller guard")?;
    let frame_codec =
        parse_optional_single_type(&input.attrs, "frame_codec", "controller frame codec")?;
    let payload_codec =
        parse_optional_single_type(&input.attrs, "payload_codec", "controller payload codec")?;

    let timeout_attributes = named_attributes(&input.attrs, "timeout");
    reject_duplicate_authority(&timeout_attributes, "controller timeout")?;
    let timeout = timeout_attributes
        .first()
        .map(|attribute| parse_timeout_seconds(attribute))
        .transpose()?;
    let timeout = timeout.map_or_else(
        || quote!(::std::option::Option::None),
        |seconds| quote!(::std::option::Option::Some(::std::time::Duration::from_secs(#seconds))),
    );

    let asyncapi_attributes = named_attributes(&input.attrs, "asyncapi");
    reject_duplicate_authority(&asyncapi_attributes, "controller AsyncAPI")?;
    let asyncapi = asyncapi_attributes
        .first()
        .map(|attribute| asyncapi::parse(attribute))
        .transpose()?;
    let runtime = resolve_runtime()?;
    let asyncapi = asyncapi::generate(&runtime, asyncapi.as_ref());
    let frame_codec = frame_codec.map_or_else(
        || quote!(::std::option::Option::None),
        |codec| {
            quote!(::std::option::Option::Some(
                #runtime::__private::WebSocketFrameCodecRegistration::of::<#codec>()
            ))
        },
    );
    let payload_codec = payload_codec.map_or_else(
        || quote!(::std::option::Option::None),
        |codec| {
            quote!(::std::option::Option::Some(
                #runtime::__private::WebSocketPayloadCodecRegistration::of::<#codec>()
            ))
        },
    );

    let registration_function =
        format_ident!("__lily_websocket_controller_registration_for_{controller}");
    let registration_static =
        format_ident!("__LILY_WEBSOCKET_CONTROLLER_REGISTRATION_FOR_{controller}");

    Ok(quote! {
        impl #runtime::__private::WebSocketControllerDefinition for #controller {
            fn namespace() -> &'static str {
                #namespace
            }

            fn handshake_middleware_registrations(
            ) -> ::std::vec::Vec<#runtime::__private::WebSocketHandshakeMiddlewareRegistration> {
                ::std::vec![
                    #(#runtime::__private::WebSocketHandshakeMiddlewareRegistration::of::<#handshake_middlewares>(),)*
                ]
            }

            fn connection_middleware_registrations(
            ) -> ::std::vec::Vec<#runtime::__private::WebSocketConnectionMiddlewareRegistration> {
                ::std::vec![
                    #(#runtime::__private::WebSocketConnectionMiddlewareRegistration::of::<#connection_middlewares>(),)*
                ]
            }

            fn message_middleware_registrations(
            ) -> ::std::vec::Vec<
                #runtime::__private::WebSocketMessageMiddlewareRegistration
            > {
                ::std::vec![
                    #(#runtime::__private::WebSocketMessageMiddlewareRegistration::of::<#message_middlewares>(),)*
                ]
            }

            fn guard_registrations(
            ) -> ::std::vec::Vec<#runtime::__private::WebSocketGuardRegistration> {
                ::std::vec![
                    #(#runtime::__private::WebSocketGuardRegistration::of::<#guards>(),)*
                ]
            }

            fn frame_codec_registration(
            ) -> ::std::option::Option<
                #runtime::__private::WebSocketFrameCodecRegistration
            > {
                #frame_codec
            }

            fn payload_codec_registration(
            ) -> ::std::option::Option<
                #runtime::__private::WebSocketPayloadCodecRegistration
            > {
                #payload_codec
            }

            fn timeout() -> ::std::option::Option<::std::time::Duration> {
                #timeout
            }

            fn asyncapi_registration(
            ) -> #runtime::__private::WebSocketAsyncApiRegistration {
                #asyncapi
            }
        }

        #[doc(hidden)]
        #[allow(non_snake_case)]
        fn #registration_function() -> #runtime::__private::WebSocketControllerRegistration {
            #runtime::__private::WebSocketControllerRegistration::of::<#controller>()
        }

        #[doc(hidden)]
        #[allow(non_upper_case_globals)]
        #[#runtime::__private::linkme::distributed_slice(
            #runtime::__private::WEBSOCKET_CONTROLLER_REGISTRATIONS
        )]
        #[linkme(crate = #runtime::__private::linkme)]
        static #registration_static:
            #runtime::__private::WebSocketControllerRegistrationFn = #registration_function;
    })
}

fn parse_types(
    attributes: &[syn::Attribute],
    name: &str,
    context: &str,
) -> syn::Result<Vec<syn::Type>> {
    let attributes = named_attributes(attributes, name);
    reject_duplicate_authority(&attributes, context)?;
    attributes
        .first()
        .map(|attribute| parse_type_list(attribute, context))
        .transpose()
        .map(Option::unwrap_or_default)
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::parse_quote;

    #[test]
    fn missing_namespace_is_rejected_before_runtime_generation() {
        let input: DeriveInput = parse_quote!(
            struct ChatController;
        );
        let error = expand(input).unwrap_err();
        assert!(error.to_string().contains("require #[namespace"));
    }

    #[test]
    fn generic_controller_is_rejected() {
        let input: DeriveInput = parse_quote!(
            #[namespace("chat")]
            struct ChatController<T>(T);
        );
        let error = expand(input).unwrap_err();
        assert!(error.to_string().contains("generic WebSocket controllers"));
    }

    #[test]
    fn duplicate_controller_codec_authority_is_rejected() {
        let input: DeriveInput = parse_quote! {
            #[namespace("chat")]
            #[frame_codec(FirstCodec)]
            #[frame_codec(SecondCodec)]
            struct ChatController;
        };
        let error = expand(input).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("duplicate controller frame codec")
        );
    }

    #[test]
    fn controller_message_middleware_uses_typed_registration() {
        let input: DeriveInput = parse_quote! {
            #[namespace("chat")]
            #[message_middleware(MessageTracing, TenantContext)]
            struct ChatController;
        };
        let generated = expand_with_runtime(input, || Ok(quote!(::lily_websocket)))
            .expect("controller message middleware metadata must expand")
            .to_string();

        assert!(generated.contains("WebSocketMessageMiddlewareRegistration"));
        assert!(generated.contains("of :: < MessageTracing >"));
        assert!(generated.contains("of :: < TenantContext >"));
    }

    #[test]
    fn duplicate_controller_message_middleware_authority_is_rejected() {
        let input: DeriveInput = parse_quote! {
            #[namespace("chat")]
            #[message_middleware(MessageTracing)]
            #[message_middleware(TenantContext)]
            struct ChatController;
        };
        let error = expand(input).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("duplicate controller message middleware")
        );
    }

    #[test]
    fn duplicate_controller_guard_authority_is_rejected() {
        let input: DeriveInput = parse_quote! {
            #[namespace("chat")]
            #[guard(Authenticated)]
            #[guard(CanChat)]
            struct ChatController;
        };
        let error = expand(input).unwrap_err();

        assert!(error.to_string().contains("duplicate controller guard"));
    }
}
