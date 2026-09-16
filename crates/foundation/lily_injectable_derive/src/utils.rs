//! Utility functions for Injectable derive macro
//!
//! Helper functions for code generation and type analysis.

use proc_macro2::{Ident, Span, TokenStream};
use quote::{quote, ToTokens};
use syn::Type;

/// Generate a unique identifier for registration functions
pub(crate) fn generate_registration_fn_name(prefix: &str, type_name: &str) -> Ident {
    let sanitized_name = type_name
        .replace("::", "_")
        .replace("<", "_")
        .replace(">", "_")
        .replace(" ", "")
        .to_uppercase();

    Ident::new(
        &format!(
            "__{}_REGISTRATION_{}",
            prefix.to_uppercase(),
            sanitized_name
        ),
        Span::call_site(),
    )
}

/// Generate TypeId expression for a given type
pub(crate) fn generate_type_id(ty: &Type) -> TokenStream {
    quote! {
        std::any::TypeId::of::<#ty>()
    }
}

/// Generate type name expression for a given type
pub(crate) fn generate_type_name(ty: &Type) -> TokenStream {
    let type_str = ty.to_token_stream().to_string();
    quote! {
        #type_str
    }
}

/// Generate compile-time trait validation
pub(crate) fn generate_trait_check(
    runtime: &TokenStream,
    ty: &Type,
    trait_name: &str,
) -> TokenStream {
    let _trait_ident = Ident::new(trait_name, Span::call_site());
    quote! {
        const _: fn() = || {
            fn assert_impl<T: #runtime::ServiceTrait + Send + Sync + 'static>() {}
            assert_impl::<#ty>();
        };
    }
}

/// Generate the concrete route projector used by the unified resolver.
pub(crate) fn generate_concrete_projection(
    runtime: &TokenStream,
    projector_fn_name: &Ident,
    struct_name: &Ident,
) -> TokenStream {
    quote! {
        fn #projector_fn_name(
            instance: std::sync::Arc<dyn std::any::Any + Send + Sync>
        ) -> Result<Box<dyn std::any::Any + Send + Sync>, #runtime::InjectionError> {
            let concrete = instance.downcast::<#struct_name>().map_err(|_| {
                #runtime::InjectionError::ServiceResolutionFailed(format!(
                    "Failed to project service '{}' as its concrete type",
                    std::any::type_name::<#struct_name>()
                ))
            })?;
            Ok(Box::new(concrete))
        }
    }
}

/// Generate a safe `Arc<Concrete> -> Arc<dyn Interface>` projector.
///
/// The coercion provides the compile-time `Concrete: Interface` check and also
/// rejects interfaces that are not dyn-compatible, `Send` and `Sync`.
pub(crate) fn generate_interface_projection(
    runtime: &TokenStream,
    projector_fn_name: &Ident,
    struct_name: &Ident,
    interface: &Type,
) -> TokenStream {
    quote! {
        fn #projector_fn_name(
            instance: std::sync::Arc<dyn std::any::Any + Send + Sync>
        ) -> Result<Box<dyn std::any::Any + Send + Sync>, #runtime::InjectionError> {
            let concrete = instance.downcast::<#struct_name>().map_err(|_| {
                #runtime::InjectionError::ServiceResolutionFailed(format!(
                    "Failed to project service '{}' as interface '{}'",
                    std::any::type_name::<#struct_name>(),
                    std::any::type_name::<#interface>()
                ))
            })?;
            let interface: std::sync::Arc<#interface> = concrete;
            Ok(Box::new(interface))
        }
    }
}

/// Generate factory function for service instantiation
pub(crate) fn generate_factory_function(
    runtime: &TokenStream,
    factory_fn_name: &Ident,
    disposer_fn_name: &Ident,
    struct_name: &Ident,
    dependency_resolution: &TokenStream,
) -> TokenStream {
    quote! {
        fn #factory_fn_name(
            extensions: std::sync::Arc<dyn std::any::Any + Send + Sync>
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Box<dyn std::any::Any + Send + Sync>, #runtime::InjectionError>> + Send + 'static>> {
            Box::pin(async move {
                // Downcast Arc<dyn Any> back to Arc<Extensions>
                let extensions = extensions
                    .downcast::<#runtime::Extensions>()
                    .map_err(|_| #runtime::InjectionError::ServiceNotFound("Failed to downcast to Extensions".to_string()))?;

                // Construction and dependency resolution do not run lifecycle
                // hooks. The lifecycle factory invoked by the owning container
                // is the single start boundary for this instance.
                let service = #dependency_resolution;
                let mut initialization_guard = #runtime::__private::InitializationGuard::new(
                    service,
                    std::sync::Arc::clone(&extensions),
                    std::any::type_name::<#struct_name>(),
                    #disposer_fn_name,
                );
                use #runtime::__private_futures::FutureExt as _;
                let initialization = std::panic::AssertUnwindSafe(
                    #runtime::ServiceTrait::initialize(initialization_guard.service_mut())
                )
                .catch_unwind()
                .await;
                let initialization = match initialization {
                    Ok(Ok(())) => None,
                    Ok(Err(error)) => Some(error),
                    Err(payload) => {
                        let message = if let Some(message) = payload.downcast_ref::<&str>() {
                            (*message).to_string()
                        } else if let Some(message) = payload.downcast_ref::<String>() {
                            message.clone()
                        } else {
                            "non-string panic payload".to_string()
                        };
                        Some(#runtime::InjectionError::InitError(format!(
                            "service initialization panicked: {message}"
                        )))
                    }
                };
                if let Some(initialization) = initialization {
                    let cleanup = std::panic::AssertUnwindSafe(
                        #runtime::ServiceTrait::dispose(initialization_guard.service())
                    )
                    .catch_unwind()
                    .await;
                    let cleanup = match cleanup {
                        Ok(result) => result,
                        Err(_) => Err(#runtime::InjectionError::DisposeError(
                            format!(
                                "Service '{}' cleanup panicked after initialization failure",
                                std::any::type_name::<#struct_name>()
                            )
                        )),
                    };
                    let source = match cleanup {
                        Ok(()) => initialization,
                        Err(cleanup) => #runtime::InjectionError::InitializationCleanupFailed {
                            service: std::any::type_name::<#struct_name>().to_string(),
                            initialization: Box::new(initialization),
                            cleanup: Box::new(cleanup),
                        },
                    };
                    // Direct failure cleanup completed (or reported its own
                    // error), so disarm the cancellation path before return.
                    let _ = initialization_guard.into_service();
                    return Err(#runtime::InjectionError::ServiceInitializationFailed {
                        service: std::any::type_name::<#struct_name>().to_string(),
                        source: Box::new(source),
                    });
                }
                let service = initialization_guard.into_service();
                Ok(Box::new(service) as Box<dyn std::any::Any + Send + Sync>)
            })
        }
    }
}

/// Generate the type-erased disposal callback stored alongside DI metadata.
pub(crate) fn generate_disposer_function(
    runtime: &TokenStream,
    disposer_fn_name: &Ident,
    struct_name: &Ident,
) -> TokenStream {
    quote! {
        fn #disposer_fn_name(
            instance: std::sync::Arc<dyn std::any::Any + Send + Sync>
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), #runtime::InjectionError>> + Send + 'static>> {
            Box::pin(async move {
                let instance = instance
                    .downcast::<#struct_name>()
                    .map_err(|_| #runtime::InjectionError::DisposeError(
                        format!("Failed to downcast service '{}' during disposal", std::any::type_name::<#struct_name>())
                    ))?;
                #runtime::ServiceTrait::dispose(instance.as_ref())
                    .await
                    .map_err(|source| #runtime::InjectionError::DisposeError(
                        format!("Service '{}' disposal failed: {source}", std::any::type_name::<#struct_name>())
                    ))
            })
        }
    }
}
#[allow(clippy::too_many_arguments)]
/// Generate metadata getter function (using OnceLock pattern)
pub(crate) fn generate_metadata_getter(
    runtime: &TokenStream,
    static_var_name: &Ident,
    _struct_name: &Ident,
    type_id: &TokenStream,
    type_name: &str,
    trait_type_id: &TokenStream,
    trait_name: &TokenStream,
    lifetime: &TokenStream,
    factory_fn_name: &Ident,
    dependencies: &TokenStream,
) -> TokenStream {
    let getter_fn_name = Ident::new(
        &format!("get_{}", static_var_name.to_string().to_lowercase()),
        Span::call_site(),
    );

    quote! {
        fn #getter_fn_name() -> &'static #runtime::__private::registry::ServiceMetadata {
            #static_var_name.get_or_init(|| #runtime::__private::registry::ServiceMetadata {
                type_id: #type_id,
                type_name: #type_name,
                trait_type_id: #trait_type_id,
                trait_name: #trait_name,
                lifetime: #lifetime,
                factory_fn: #factory_fn_name,
                dependencies: #dependencies,
            })
        }
    }
}

/// Extract struct fields for dependency injection analysis (only fields with #[inject] attribute)
pub(crate) fn extract_injectable_fields(fields: &syn::Fields) -> Vec<&syn::Field> {
    match fields {
        syn::Fields::Named(fields_named) => fields_named
            .named
            .iter()
            .filter(|field| has_inject_attribute(field))
            .collect(),
        syn::Fields::Unnamed(_) => {
            // Tuple structs not supported for DI
            Vec::new()
        }
        syn::Fields::Unit => {
            // Unit structs have no fields
            Vec::new()
        }
    }
}

/// Check if a field has #[inject] attribute
pub(crate) fn has_inject_attribute(field: &syn::Field) -> bool {
    field
        .attrs
        .iter()
        .any(|attr| attr.path().is_ident("inject"))
}

/// Extract injectable fields from syn::Data
pub(crate) fn extract_injectable_fields_from_data(data: &syn::Data) -> Vec<syn::Field> {
    match data {
        syn::Data::Struct(data_struct) => extract_injectable_fields(&data_struct.fields)
            .into_iter()
            .cloned()
            .collect(),
        _ => vec![], // Enums and unions don't have injectable fields
    }
}

/// Generate dependencies vector from injectable fields
pub(crate) fn generate_dependencies_vector(fields: &[&syn::Field]) -> TokenStream {
    let dependency_type_ids = fields.iter().filter_map(|field| {
        if has_inject_attribute(field) {
            // Extract inner type from Arc<T> if present
            let field_type = &field.ty;
            let inner_type = extract_arc_inner_type(field_type).unwrap_or(field_type);
            Some(quote! { std::any::TypeId::of::<#inner_type>() })
        } else {
            None
        }
    });

    quote! {
        vec![#(#dependency_type_ids),*]
    }
}

/// Extract inner type from Arc<T> wrapper
pub(crate) fn extract_arc_inner_type(ty: &syn::Type) -> Option<&syn::Type> {
    if let syn::Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.last() {
            if segment.ident == "Arc" {
                if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                    if let Some(syn::GenericArgument::Type(inner_type)) = args.args.first() {
                        return Some(inner_type);
                    }
                }
            }
        }
    }
    None
}

/// Generate dependency resolution code for constructor injection
pub(crate) fn generate_dependency_resolution(
    runtime: &TokenStream,
    struct_name: &Ident,
    data: &syn::Data,
    injectable_fields: &[&syn::Field],
) -> syn::Result<TokenStream> {
    // Note: injectable_fields contains only fields with #[inject] attribute.
    // A dependency-only struct can be constructed directly, which means an
    // `Arc<dyn Trait>` field does not force the service to invent a dummy
    // `Default` value. Services that also own runtime state keep the framework's
    // existing contract: their `Default` implementation creates that state and
    // the resolved dependencies are assigned afterwards.

    for field in injectable_fields {
        if extract_arc_inner_type(&field.ty).is_none() {
            return Err(syn::Error::new_spanned(
                &field.ty,
                "`#[inject]` fields must use `Arc<T>`; supported examples are `Arc<MyService>` and `Arc<dyn MyService>`",
            ));
        }
    }

    let dependency_resolutions = injectable_fields.iter().map(|field| {
        let field_name = field.ident.as_ref().unwrap();
        let field_type = &field.ty;

        // For injected Arc<T> fields, resolve the inner type
        if let Some(inner_type) = extract_arc_inner_type(field_type) {
            quote! {
                let #field_name = extensions.get_service::<#inner_type>(None).await
                    .map_err(|source| #runtime::InjectionError::DependencyResolutionFailed {
                        service: std::any::type_name::<#struct_name>().to_string(),
                        dependency: std::any::type_name::<#inner_type>().to_string(),
                        source: Box::new(source),
                    })?;
            }
        } else {
            // For non-Arc injected fields, resolve directly
            quote! {
                let #field_name = extensions.get_service::<#field_type>(None).await
                    .map_err(|source| #runtime::InjectionError::DependencyResolutionFailed {
                        service: std::any::type_name::<#struct_name>().to_string(),
                        dependency: std::any::type_name::<#field_type>().to_string(),
                        source: Box::new(source),
                    })?;
            }
        }
    });

    let construction = match data {
        syn::Data::Struct(data) => match &data.fields {
            syn::Fields::Named(fields) => {
                if fields.named.iter().all(has_inject_attribute) {
                    let initializers = fields.named.iter().map(|field| {
                        let field_name = field
                            .ident
                            .as_ref()
                            .expect("named fields always have an identifier");
                        quote! { #field_name: #field_name }
                    });
                    quote! { #struct_name { #(#initializers),* } }
                } else {
                    let assignments = injectable_fields.iter().map(|field| {
                        let field_name = field
                            .ident
                            .as_ref()
                            .expect("named fields always have an identifier");
                        quote! { service.#field_name = #field_name; }
                    });
                    quote! {
                        {
                            let mut service: #struct_name = std::default::Default::default();
                            #(#assignments)*
                            service
                        }
                    }
                }
            }
            syn::Fields::Unit => quote! { #struct_name },
            syn::Fields::Unnamed(_) => {
                return Err(syn::Error::new_spanned(
                    struct_name,
                    "Injectable derive macro only supports named fields",
                ));
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                struct_name,
                "Injectable derive macro can only be applied to structs",
            ));
        }
    };

    Ok(quote! {
        {
            // Resolve injected dependencies
            #(#dependency_resolutions)*

            // Dependency-only services need no `Default`. A service that owns
            // additional state uses its own `Default` as the state constructor.
            #construction
        }
    })
}
