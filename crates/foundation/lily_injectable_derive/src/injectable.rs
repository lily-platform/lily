//! Injectable derive macro implementation
//!
//! Core implementation of the #[derive(Injectable)] macro for automatic
//! dependency injection service registration.

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::{parse_macro_input, Attribute, DeriveInput, Ident};

use crate::service_args::{parse_lifetime, ServiceArgs};
use crate::utils::{
    extract_injectable_fields_from_data, generate_concrete_projection,
    generate_dependencies_vector, generate_dependency_resolution, generate_disposer_function,
    generate_factory_function, generate_interface_projection, generate_metadata_getter,
    generate_registration_fn_name, generate_trait_check, generate_type_id, generate_type_name,
};

/// Main implementation of the Injectable derive macro
pub(crate) fn derive_impl(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    match derive_injectable_impl(&input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// Internal implementation with proper error handling
fn derive_injectable_impl(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let struct_name = &input.ident;
    let _struct_vis = &input.vis; // May be used for visibility in future features

    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "Injectable services cannot declare generic type or lifetime parameters; register a concrete 'static wrapper type instead",
        ));
    }

    // Parse #[service(...)] attribute
    let service_args = parse_service_attribute(&input.attrs)?;
    if let Some(interface) = &service_args.interface {
        if !matches!(interface, syn::Type::TraitObject(_)) {
            return Err(syn::Error::new_spanned(
                interface,
                "service interfaces must use trait-object syntax, for example `interface = dyn PaymentService`",
            ));
        }
    }

    let runtime = crate::runtime_path::lily_injection()?;

    // Generate lifetime token stream
    let lifetime = if let Some(lifetime_str) = &service_args.lifetime {
        parse_lifetime(lifetime_str, &runtime)?
    } else {
        // Default to Transient if not specified
        quote! { #runtime::ServiceLifetime::Transient }
    };

    // Generate type information
    let type_id = generate_type_id(&syn::parse_quote!(#struct_name));
    let _type_name = generate_type_name(&syn::parse_quote!(#struct_name));

    // Handle trait interface if specified
    let (trait_type_id, trait_name) = if let Some(interface) = &service_args.interface {
        let interface_type_id = generate_type_id(interface);
        let interface_type_name = generate_type_name(interface);
        (
            quote! { Some(#interface_type_id) },
            quote! { Some(#interface_type_name) },
        )
    } else {
        (quote! { None }, quote! { None })
    };

    // Generate compile-time trait validation
    let trait_check =
        generate_trait_check(&runtime, &syn::parse_quote!(#struct_name), "ServiceTrait");

    // Generate dependencies vector from injectable fields
    let injectable_fields = extract_injectable_fields_from_data(&input.data);
    let field_refs: Vec<&syn::Field> = injectable_fields.iter().collect();
    let dependencies = generate_dependencies_vector(&field_refs);

    // Generate dependency resolution code
    let dependency_resolution =
        generate_dependency_resolution(&runtime, struct_name, &input.data, &field_refs)?;

    // Generate factory function name
    let factory_fn_name = Ident::new(
        &format!("create_{}", struct_name.to_string().to_lowercase()),
        Span::call_site(),
    );

    let disposer_fn_name = Ident::new(
        &format!("dispose_{}", struct_name.to_string().to_lowercase()),
        Span::call_site(),
    );
    // Generate factory function. It receives the disposer symbol so a
    // cancellation guard can publish partial initialization to the owning
    // lifecycle ledger from Drop.
    let factory_function = generate_factory_function(
        &runtime,
        &factory_fn_name,
        &disposer_fn_name,
        struct_name,
        &dependency_resolution,
    );
    let disposer_function = generate_disposer_function(&runtime, &disposer_fn_name, struct_name);
    let concrete_projector_fn_name = Ident::new(
        &format!(
            "__lily_project_concrete_{}",
            struct_name.to_string().to_lowercase()
        ),
        Span::call_site(),
    );
    let concrete_projection =
        generate_concrete_projection(&runtime, &concrete_projector_fn_name, struct_name);
    let interface_projector_fn_name = Ident::new(
        &format!(
            "__lily_project_interface_{}",
            struct_name.to_string().to_lowercase()
        ),
        Span::call_site(),
    );
    let interface_projection = service_args.interface.as_ref().map(|interface| {
        generate_interface_projection(
            &runtime,
            &interface_projector_fn_name,
            struct_name,
            interface,
        )
    });

    // Generate unique names for static variables and functions
    let metadata_static_name = Ident::new(
        &format!(
            "__SERVICE_METADATA_{}",
            struct_name.to_string().to_uppercase()
        ),
        Span::call_site(),
    );

    let registration_fn_name = generate_registration_fn_name("SERVICE", &struct_name.to_string());
    let disposer_registration_fn_name =
        generate_registration_fn_name("SERVICE_DISPOSER", &struct_name.to_string());
    let metadata_getter_fn_name = Ident::new(
        &format!("get_{}", metadata_static_name.to_string().to_lowercase()),
        Span::call_site(),
    );

    let metadata_getter = generate_metadata_getter(
        &runtime,
        &metadata_static_name,
        struct_name,
        &type_id,
        &struct_name.to_string(),
        &trait_type_id,
        &trait_name,
        &lifetime,
        &factory_fn_name,
        &dependencies,
    );
    let disposer_metadata_static_name = Ident::new(
        &format!(
            "__SERVICE_DISPOSER_METADATA_{}",
            struct_name.to_string().to_uppercase()
        ),
        Span::call_site(),
    );
    let disposer_metadata_getter_fn_name = Ident::new(
        &format!(
            "get_{}",
            disposer_metadata_static_name.to_string().to_lowercase()
        ),
        Span::call_site(),
    );

    let concrete_route_static_name = Ident::new(
        &format!(
            "__SERVICE_CONCRETE_ROUTE_METADATA_{}",
            struct_name.to_string().to_uppercase()
        ),
        Span::call_site(),
    );
    let concrete_route_getter_fn_name = Ident::new(
        &format!(
            "get_{}",
            concrete_route_static_name.to_string().to_lowercase()
        ),
        Span::call_site(),
    );
    let concrete_route_registration_fn_name =
        generate_registration_fn_name("SERVICE_CONCRETE_ROUTE", &struct_name.to_string());

    let interface_route_static_name = Ident::new(
        &format!(
            "__SERVICE_INTERFACE_ROUTE_METADATA_{}",
            struct_name.to_string().to_uppercase()
        ),
        Span::call_site(),
    );
    let interface_route_getter_fn_name = Ident::new(
        &format!(
            "get_{}",
            interface_route_static_name.to_string().to_lowercase()
        ),
        Span::call_site(),
    );
    let interface_route_registration_fn_name =
        generate_registration_fn_name("SERVICE_INTERFACE_ROUTE", &struct_name.to_string());

    let interface_route_registration = service_args.interface.as_ref().map(|interface| {
        quote! {
            static #interface_route_static_name: std::sync::OnceLock<#runtime::__private::registry::ServiceRouteMetadata> =
                std::sync::OnceLock::new();

            fn #interface_route_getter_fn_name() -> &'static #runtime::__private::registry::ServiceRouteMetadata {
                #interface_route_static_name.get_or_init(|| #runtime::__private::registry::ServiceRouteMetadata {
                    requested_type_id: std::any::TypeId::of::<#interface>(),
                    requested_type_name: std::any::type_name::<#interface>(),
                    implementation_type_id: std::any::TypeId::of::<#struct_name>(),
                    implementation_type_name: std::any::type_name::<#struct_name>(),
                    kind: #runtime::__private::registry::ServiceRouteKind::Interface,
                    project_fn: #interface_projector_fn_name,
                })
            }

            #[#runtime::__private::linkme::distributed_slice(#runtime::__private::registry::SERVICE_ROUTE_GETTERS)]
            #[linkme(crate = #runtime::__private::linkme)]
            static #interface_route_registration_fn_name: fn() -> &'static #runtime::__private::registry::ServiceRouteMetadata =
                #interface_route_getter_fn_name;
        }
    });

    let active_registration = if service_args.enabled {
        quote! {
            static #metadata_static_name: std::sync::OnceLock<#runtime::__private::registry::ServiceMetadata> =
                std::sync::OnceLock::new();

            #metadata_getter

            #[#runtime::__private::linkme::distributed_slice(#runtime::__private::registry::SERVICE_METADATA_GETTERS)]
            #[linkme(crate = #runtime::__private::linkme)]
            static #registration_fn_name: fn() -> &'static #runtime::__private::registry::ServiceMetadata = #metadata_getter_fn_name;

            static #disposer_metadata_static_name: std::sync::OnceLock<#runtime::__private::registry::ServiceDisposerMetadata> =
                std::sync::OnceLock::new();

            fn #disposer_metadata_getter_fn_name() -> &'static #runtime::__private::registry::ServiceDisposerMetadata {
                #disposer_metadata_static_name.get_or_init(|| #runtime::__private::registry::ServiceDisposerMetadata {
                    type_id: std::any::TypeId::of::<#struct_name>(),
                    dispose_fn: #disposer_fn_name,
                })
            }

            #[#runtime::__private::linkme::distributed_slice(#runtime::__private::registry::SERVICE_DISPOSER_GETTERS)]
            #[linkme(crate = #runtime::__private::linkme)]
            static #disposer_registration_fn_name: fn() -> &'static #runtime::__private::registry::ServiceDisposerMetadata =
                #disposer_metadata_getter_fn_name;

            static #concrete_route_static_name: std::sync::OnceLock<#runtime::__private::registry::ServiceRouteMetadata> =
                std::sync::OnceLock::new();

            fn #concrete_route_getter_fn_name() -> &'static #runtime::__private::registry::ServiceRouteMetadata {
                #concrete_route_static_name.get_or_init(|| #runtime::__private::registry::ServiceRouteMetadata {
                    requested_type_id: std::any::TypeId::of::<#struct_name>(),
                    requested_type_name: std::any::type_name::<#struct_name>(),
                    implementation_type_id: std::any::TypeId::of::<#struct_name>(),
                    implementation_type_name: std::any::type_name::<#struct_name>(),
                    kind: #runtime::__private::registry::ServiceRouteKind::Concrete,
                    project_fn: #concrete_projector_fn_name,
                })
            }

            #[#runtime::__private::linkme::distributed_slice(#runtime::__private::registry::SERVICE_ROUTE_GETTERS)]
            #[linkme(crate = #runtime::__private::linkme)]
            static #concrete_route_registration_fn_name: fn() -> &'static #runtime::__private::registry::ServiceRouteMetadata =
                #concrete_route_getter_fn_name;

            #interface_route_registration
        }
    } else {
        quote! {}
    };

    // Complete macro with all components
    let expanded = quote! {
        #trait_check

        // Factory function for type-erased creation
        #factory_function
        #disposer_function
        #concrete_projection
        #interface_projection

        #active_registration
    };

    Ok(expanded)
}

/// Parse #[service(...)] attribute from struct attributes
fn parse_service_attribute(attrs: &[Attribute]) -> syn::Result<ServiceArgs> {
    for attr in attrs {
        if attr.path().is_ident("service") {
            return attr.parse_args::<ServiceArgs>();
        }
    }

    // Return default if no #[service] attribute found
    Ok(ServiceArgs::default())
}
