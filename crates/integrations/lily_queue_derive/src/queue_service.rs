//! Queue service macro implementation
//!
//! This module implements the #[queue_service] attribute macro that processes
//! impl blocks and registers queue handler methods.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{parse_macro_input, ImplItem, ItemImpl, PathArguments, Type};

#[cfg(feature = "asyncapi")]
use crate::asyncapi::{self, Scope};
use crate::runtime_path;
use crate::utils::{
    contains_asyncapi_attributes, extract_pipeline_types, extract_queue_methods,
    generate_handler_wrapper, generate_metadata_registration, strip_queue_attributes,
};

/// Implementation of the #[queue_service] attribute macro
///
/// This macro:
/// 1. Parses the impl block
/// 2. Extracts service type
/// 3. Finds all methods with #[queue(...)] attribute
/// 4. Generates handler wrapper functions
/// 5. Generates metadata registration code
/// 6. Returns original impl + generated code
pub(crate) fn queue_service_impl(args: TokenStream, input: TokenStream) -> TokenStream {
    if !args.is_empty() {
        return syn::Error::new(
            proc_macro2::Span::call_site(),
            "#[queue_service] does not accept arguments",
        )
        .to_compile_error()
        .into();
    }
    let input_impl = parse_macro_input!(input as ItemImpl);
    let runtime = match runtime_path::lily_queue() {
        Ok(runtime) => runtime,
        Err(error) => return error.to_compile_error().into(),
    };

    expand_queue_service(
        input_impl,
        &runtime.tokens,
        runtime.attribute_prefix.as_str(),
    )
    .into()
}

/// Expand a parsed queue service implementation.
///
/// Keeping the expansion itself on `proc_macro2::TokenStream` makes the logic
/// independently testable without invoking Rust's `proc_macro` bridge outside
/// a compiler expansion.
fn expand_queue_service(
    mut input_impl: ItemImpl,
    runtime_path: &TokenStream2,
    runtime_attribute_prefix: &str,
) -> TokenStream2 {
    if input_impl.trait_.is_some() || !input_impl.generics.params.is_empty() {
        return syn::Error::new_spanned(
            &input_impl,
            "#[queue_service] supports only non-generic inherent impl blocks",
        )
        .to_compile_error();
    }
    let service_type = match validate_service_type(&input_impl.self_ty) {
        Ok(service_type) => service_type,
        Err(error) => return error.to_compile_error(),
    };

    #[cfg(not(feature = "asyncapi"))]
    if contains_asyncapi_attributes(&input_impl, runtime_attribute_prefix) {
        return syn::Error::new_spanned(
            &input_impl,
            "queue AsyncAPI metadata requires enabling the `asyncapi` feature on `lily_queue`",
        )
        .to_compile_error();
    }

    #[cfg(feature = "asyncapi")]
    let service_asyncapi = {
        let attributes = input_impl
            .attrs
            .iter()
            .filter(|attribute| {
                let segments = &attribute.path().segments;
                match segments.len() {
                    1 => segments[0].ident == "asyncapi",
                    2 => {
                        segments[0].ident == runtime_attribute_prefix
                            && segments[1].ident == "asyncapi"
                    }
                    _ => false,
                }
            })
            .collect::<Vec<_>>();
        if attributes.len() > 1 {
            return syn::Error::new_spanned(
                &input_impl,
                "a queue service must declare at most one #[asyncapi(...)] attribute",
            )
            .to_compile_error();
        }
        match attributes
            .first()
            .map(|attribute| asyncapi::parse(attribute, Scope::Service))
            .transpose()
        {
            Ok(metadata) => metadata,
            Err(error) => return error.to_compile_error(),
        }
    };

    let service_pipeline = match extract_pipeline_types(
        &input_impl.attrs,
        runtime_attribute_prefix,
        "queue service",
    ) {
        Ok(pipeline) => pipeline,
        Err(error) => return error.to_compile_error(),
    };

    let uses_unqualified_marker = |name: &str| {
        input_impl.attrs.iter().any(|attribute| {
            attribute.path().segments.len() == 1 && attribute.path().is_ident(name)
        }) || input_impl.items.iter().any(|item| {
            let ImplItem::Fn(method) = item else {
                return false;
            };
            method.attrs.iter().any(|attribute| {
                attribute.path().segments.len() == 1 && attribute.path().is_ident(name)
            })
        })
    };
    let uses_unqualified_queue = uses_unqualified_marker("queue");
    let uses_unqualified_middleware = uses_unqualified_marker("middleware");
    let uses_unqualified_guard = uses_unqualified_marker("guard");
    let uses_unqualified_asyncapi = uses_unqualified_marker("asyncapi");
    let has_asyncapi = contains_asyncapi_attributes(&input_impl, runtime_attribute_prefix);

    // Find all methods with #[queue(...)] attribute
    let queue_methods = match extract_queue_methods(
        &input_impl,
        runtime_attribute_prefix,
        #[cfg(feature = "asyncapi")]
        service_asyncapi.as_ref(),
    ) {
        Ok(methods) => methods,
        Err(err) => return err.to_compile_error(),
    };
    strip_queue_attributes(&mut input_impl, runtime_attribute_prefix);

    // If no queue methods found, just return original impl
    if queue_methods.is_empty() {
        if !service_pipeline.is_empty() || has_asyncapi {
            let detail = if has_asyncapi {
                "queue service AsyncAPI metadata requires at least one #[queue(...)] handler"
            } else {
                "queue service middleware and guards require at least one #[queue(...)] handler"
            };
            return syn::Error::new_spanned(&input_impl, detail).to_compile_error();
        }
        return quote! { #input_impl };
    }

    // Generate handler wrapper functions for each queue method
    let handler_wrappers: Vec<_> = queue_methods
        .iter()
        .map(|method_info| generate_handler_wrapper(runtime_path, &service_type, method_info))
        .collect();

    // Generate metadata registration code for each queue method
    let metadata_registrations: Vec<_> = queue_methods
        .iter()
        .map(|method_info| {
            generate_metadata_registration(
                runtime_path,
                &service_type,
                &service_pipeline,
                method_info,
            )
        })
        .collect();

    let marker_import = uses_unqualified_queue.then(|| {
        quote! {
            const _: () = {
                #[allow(unused_imports)]
                use queue as _;
            };
        }
    });
    let middleware_marker_import = uses_unqualified_middleware.then(|| {
        quote! {
            const _: () = {
                #[allow(unused_imports)]
                use middleware as _;
            };
        }
    });
    let guard_marker_import = uses_unqualified_guard.then(|| {
        quote! {
            const _: () = {
                #[allow(unused_imports)]
                use guard as _;
            };
        }
    });
    let asyncapi_marker_import = uses_unqualified_asyncapi.then(|| {
        quote! {
            const _: () = {
                #[allow(unused_imports)]
                use asyncapi as _;
            };
        }
    });

    // Combine everything
    let expanded = quote! {
        // Original impl block (unchanged)
        #input_impl

        // Keep an unqualified marker import observably used after consuming
        // the nested attributes in this outer macro.
        #marker_import
        #middleware_marker_import
        #guard_marker_import
        #asyncapi_marker_import

        // Generated handler wrapper functions
        #(#handler_wrappers)*

        // Generated metadata registration code
        #(#metadata_registrations)*
    };

    expanded
}

fn validate_service_type(service_type: &Type) -> syn::Result<Type> {
    let Type::Path(path) = service_type else {
        return Err(syn::Error::new_spanned(
            service_type,
            "#[queue_service] requires a concrete, non-generic service path",
        ));
    };
    if path.qself.is_some()
        || path.path.segments.iter().any(|segment| {
            !matches!(segment.arguments, PathArguments::None)
                || segment.ident.to_string().starts_with("r#")
        })
    {
        return Err(syn::Error::new_spanned(
            service_type,
            "#[queue_service] requires a concrete, non-generic service path",
        ));
    }
    Ok(service_type.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;

    #[test]
    fn test_queue_service_basic() {
        let input = quote! {
            impl UserService {
                #[queue("user.created", version = 1, content = "json")]
                async fn handle_user_created(&self, msg: Json<UserCreated>) -> Result<(), QueueHandlerError> {
                    Ok(())
                }
            }
        };

        let output = expand_queue_service(
            syn::parse2(input).unwrap(),
            &quote!(::lily_queue),
            "lily_queue",
        );
        let output_str = output.to_string();

        // Verify original impl is preserved
        assert!(output_str.contains("impl UserService"));
        assert!(output_str.contains("handle_user_created"));

        // Verify wrapper function is generated
        assert!(output_str.contains("__queue_handler_UserService_handle_user_created"));

        // Verify metadata registration is generated
        assert!(output_str.contains("__QUEUE_METADATA_USERSERVICE_HANDLE_USER_CREATED"));
    }

    #[test]
    fn test_queue_service_multiple_handlers() {
        let input = quote! {
            impl UserService {
                #[queue("user.created", version = 1, content = "json")]
                async fn handle_user_created(&self, msg: Json<UserCreated>) -> Result<(), QueueHandlerError> {
                    Ok(())
                }

                #[queue("user.deleted", version = 1, content = "json")]
                async fn handle_user_deleted(&self, msg: Json<UserDeleted>) -> Result<(), QueueHandlerError> {
                    Ok(())
                }
            }
        };

        let output = expand_queue_service(
            syn::parse2(input).unwrap(),
            &quote!(::lily_queue),
            "lily_queue",
        );
        let output_str = output.to_string();

        // Verify both handlers are generated
        assert!(output_str.contains("handle_user_created"));
        assert!(output_str.contains("handle_user_deleted"));
        assert!(output_str.contains("__queue_handler_UserService_handle_user_created"));
        assert!(output_str.contains("__queue_handler_UserService_handle_user_deleted"));
    }

    #[test]
    fn test_queue_service_no_handlers() {
        let input = quote! {
            impl UserService {
                fn regular_method(&self) {
                    // No #[queue] attribute
                }
            }
        };

        let output = expand_queue_service(
            syn::parse2(input).unwrap(),
            &quote!(::lily_queue),
            "lily_queue",
        );
        let output_str = output.to_string();

        // Should just return original impl
        assert!(output_str.contains("impl UserService"));
        assert!(output_str.contains("regular_method"));
        assert!(!output_str.contains("__queue_handler"));
    }

    #[test]
    fn service_and_handler_pipeline_metadata_are_generated_in_source_order() {
        let input = quote! {
            #[middleware(ServiceFirst)]
            #[middleware(ServiceSecond)]
            #[guard(ServiceGuard)]
            impl UserService {
                #[queue("user.created", version = 1, content = "json")]
                #[middleware(HandlerFirst)]
                #[middleware(HandlerSecond)]
                #[guard(HandlerGuard)]
                async fn handle_user_created(&self) -> Result<(), QueueHandlerError> {
                    Ok(())
                }
            }
        };

        let output = expand_queue_service(
            syn::parse2(input).unwrap(),
            &quote!(::lily_queue),
            "lily_queue",
        );
        let output = output.to_string();

        for field in [
            "service_middlewares",
            "service_guards",
            "handler_middlewares",
            "handler_guards",
        ] {
            assert!(output.contains(field), "missing generated field {field}");
        }
        assert!(output.find("ServiceFirst").unwrap() < output.find("ServiceSecond").unwrap());
        assert!(output.find("HandlerFirst").unwrap() < output.find("HandlerSecond").unwrap());
        assert!(!output.contains("# [middleware"));
        assert!(!output.contains("# [guard"));
    }

    #[test]
    fn service_pipeline_without_a_queue_handler_is_rejected() {
        let input = quote! {
            #[middleware(ServiceAudit)]
            impl UserService {
                async fn helper(&self) {}
            }
        };

        let output = expand_queue_service(
            syn::parse2(input).unwrap(),
            &quote!(::lily_queue),
            "lily_queue",
        )
        .to_string();

        assert!(output.contains("require at least one"));
    }

    #[cfg(not(feature = "asyncapi"))]
    #[test]
    fn asyncapi_metadata_without_the_feature_is_rejected_at_the_outer_macro() {
        let input = quote! {
            #[asyncapi(documented)]
            impl UserService {
                #[queue("user.created", version = 1, content = "json")]
                async fn handle_user_created(&self, msg: Json<UserCreated>) -> Result<(), QueueHandlerError> {
                    Ok(())
                }
            }
        };

        let output = expand_queue_service(
            syn::parse2(input).unwrap(),
            &quote!(::lily_queue),
            "lily_queue",
        )
        .to_string();

        assert!(output.contains("requires enabling the `asyncapi` feature"));
        assert!(!output.contains("__QUEUE_METADATA"));
    }

    #[cfg(feature = "asyncapi")]
    #[test]
    fn asyncapi_metadata_is_inherited_into_the_canonical_handler_record() {
        let input = quote! {
            #[asyncapi(
                documented,
                tag = "orders",
                summary = "Order messages",
                security = "service-token"
            )]
            impl OrderService {
                #[queue("orders.created", version = 2, content = "json")]
                #[asyncapi(
                    tag = "created",
                    description = "Consumes schema version two",
                    operation_id = "orders.created.v2",
                    security = "handler-token",
                    deprecated,
                    example = r#"{"order_id":42}"#
                )]
                async fn created(&self, event: Json<OrderCreated>) -> Result<(), QueueHandlerError> {
                    Ok(())
                }
            }
        };

        let output = expand_queue_service(
            syn::parse2(input).unwrap(),
            &quote!(::lily_queue),
            "lily_queue",
        )
        .to_string();

        assert_eq!(output.matches("static __QUEUE_METADATA_").count(), 1);
        assert!(output.contains("QueueAsyncApiStatus :: Documented"));
        assert!(output.contains("Order messages"));
        assert!(output.contains("Consumes schema version two"));
        assert!(output.contains("orders.created.v2"));
        let tags = output
            .split_once("tags :")
            .and_then(|(_, rest)| rest.split_once("security :"))
            .map(|(tags, _)| tags)
            .expect("generated AsyncAPI tags field");
        assert!(tags.find("orders").unwrap() < tags.find("created").unwrap());
        assert!(output.contains("handler-token"));
        assert!(!output.contains("service-token"));
        assert!(output.contains("delivery_asyncapi_payload"));
        assert!(!output.contains("# [asyncapi"));
    }

    #[cfg(feature = "asyncapi")]
    #[test]
    fn skipped_service_emits_skipped_metadata_for_each_handler() {
        let input = quote! {
            #[asyncapi(skip)]
            impl InternalService {
                #[queue("internal.events", version = 1, content = "binary")]
                async fn handle(&self, event: BinaryPayload) -> Result<(), QueueHandlerError> {
                    Ok(())
                }
            }
        };

        let output = expand_queue_service(
            syn::parse2(input).unwrap(),
            &quote!(::lily_queue),
            "lily_queue",
        )
        .to_string();

        assert!(output.contains("QueueAsyncApiRegistration :: skipped"));
        assert!(!output.contains("delivery_asyncapi_payload"));
    }

    #[cfg(feature = "asyncapi")]
    #[test]
    fn explicit_schema_uses_a_distinct_authority_from_inferred_json() {
        let input = quote! {
            impl ExternalService {
                #[queue("external.events", version = 1, content = "custom")]
                #[asyncapi(
                    documented,
                    schema = ExternalEvent,
                    content_type = "application/x-external+json"
                )]
                async fn handle(&self, event: RawDelivery) -> Result<(), QueueHandlerError> {
                    Ok(())
                }
            }
        };

        let output = expand_queue_service(
            syn::parse2(input).unwrap(),
            &quote!(::lily_queue),
            "lily_queue",
        )
        .to_string();

        assert!(output.contains("QueueAsyncApiPayload :: explicit_generated"));
        assert!(!output.contains("delivery_asyncapi_payload"));
    }
}
