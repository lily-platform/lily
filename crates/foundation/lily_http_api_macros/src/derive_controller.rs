use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{parse_macro_input, Data, DeriveInput};

use crate::openapi::{parse_controller_openapi, ControllerOpenApiArgs, ResponseArg};
use crate::runtime_path;
use crate::syntax::{
    named_attributes, parse_single_type, parse_string, parse_type_list, reject_duplicate_authority,
    validate_base_path,
};

pub(crate) fn derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand(input) {
        Ok(expansion) => expansion.into(),
        Err(error) => error.into_compile_error().into(),
    }
}

fn expand(input: DeriveInput) -> syn::Result<TokenStream2> {
    let runtime = runtime_path::lily_http_api()?;
    let controller = &input.ident;

    if !matches!(input.data, Data::Struct(_)) {
        return Err(syn::Error::new_spanned(
            controller,
            "Controller can only be derived for a struct",
        ));
    }
    if !input.generics.params.is_empty() || input.generics.where_clause.is_some() {
        return Err(syn::Error::new_spanned(
            input.generics,
            "generic controllers are not supported",
        ));
    }

    let base_paths = named_attributes(&input.attrs, "base_path");
    reject_duplicate_authority(&base_paths, "controller base_path")?;
    let base_path_attribute = base_paths.first().ok_or_else(|| {
        syn::Error::new_spanned(
            controller,
            "struct controllers require #[base_path(\"/...\")]",
        )
    })?;
    let base_path = parse_string(base_path_attribute, "base_path")?;
    validate_base_path(&base_path)?;

    let middleware_attributes = named_attributes(&input.attrs, "middleware");
    reject_duplicate_authority(&middleware_attributes, "controller middleware")?;
    let middlewares = middleware_attributes
        .first()
        .map(|attribute| parse_type_list(attribute, "controller middleware"))
        .transpose()?
        .unwrap_or_default();

    let cors_attributes = named_attributes(&input.attrs, "cors");
    reject_duplicate_authority(&cors_attributes, "controller CORS")?;
    let cors = cors_attributes
        .first()
        .map(|attribute| parse_single_type(attribute, "controller CORS"))
        .transpose()?;

    let openapi_attributes = named_attributes(&input.attrs, "openapi");
    reject_duplicate_authority(&openapi_attributes, "controller OpenAPI")?;
    let openapi = openapi_attributes
        .first()
        .map(|attribute| parse_controller_openapi(attribute))
        .transpose()?;

    let registration_function = format_ident!("__lily_controller_registration_for_{controller}");
    let registration_static = format_ident!("__LILY_CONTROLLER_REGISTRATION_FOR_{controller}");
    let openapi_selector = format_ident!("__lily_controller_openapi_select_{controller}");
    let cors_registration = cors.map_or_else(
        || quote!(#runtime::__private::CorsRoutePolicyRegistration::inherit()),
        |cors| quote!(#runtime::__private::CorsRoutePolicyRegistration::provider::<#cors>()),
    );
    let openapi_documented_by_default = openapi.is_some();
    let openapi_defaults = openapi
        .as_ref()
        .map(|openapi| generate_openapi_defaults(&runtime, openapi))
        .unwrap_or_default();
    let openapi_selector_body = if openapi_documented_by_default {
        quote! { $($documented)* }
    } else {
        quote! { $($undocumented)* }
    };

    Ok(quote! {
        impl #runtime::__private::StructControllerDefinition for #controller {
            fn base_path() -> &'static str {
                #base_path
            }

            fn middleware_registrations() -> ::std::vec::Vec<#runtime::__private::HttpMiddlewareRegistration> {
                ::std::vec![
                    #(#runtime::__private::HttpMiddlewareRegistration::of::<#middlewares>(),)*
                ]
            }

            fn cors_policy_registration() -> #runtime::__private::CorsRoutePolicyRegistration {
                #cors_registration
            }

            fn openapi_documented_by_default() -> bool {
                #openapi_documented_by_default
            }

            fn apply_openapi_defaults(
                metadata: &mut #runtime::__private::OpenApiOperationMetadata,
            ) {
                #openapi_defaults
            }
        }

        #[doc(hidden)]
        #[allow(unused_macros)]
        macro_rules! #openapi_selector {
            (
                documented { $($documented:item)* }
                undocumented { $($undocumented:item)* }
            ) => {
                #openapi_selector_body
            };
        }

        #[doc(hidden)]
        #[allow(unused_imports)]
        pub(crate) use #openapi_selector;

        #[doc(hidden)]
        #[allow(non_snake_case)]
        fn #registration_function() -> #runtime::__private::ControllerRegistration {
            #runtime::__private::ControllerRegistration::of::<#controller>()
        }

        #[doc(hidden)]
        #[allow(non_upper_case_globals)]
        #[#runtime::__private::linkme::distributed_slice(#runtime::__private::STRUCT_CONTROLLER_REGISTRATIONS)]
        #[linkme(crate = #runtime::__private::linkme)]
        static #registration_static: #runtime::__private::StructControllerRegistrationFn =
            #registration_function;
    })
}

fn generate_openapi_defaults(
    runtime: &TokenStream2,
    openapi: &ControllerOpenApiArgs,
) -> TokenStream2 {
    let tag = openapi.tag.as_ref().map(|tag| {
        quote! {
            let tags = metadata.operation_mut().tags.get_or_insert_with(::std::vec::Vec::new);
            if !tags.iter().any(|existing| existing == #tag) {
                tags.push(#tag.to_owned());
            }
        }
    });
    let description = openapi.description.as_ref().map(|description| {
        quote! {
            if metadata.operation().description.is_none() {
                metadata.operation_mut().description = ::std::option::Option::Some(
                    #description.to_owned(),
                );
            }
        }
    });
    let security = openapi.security.as_ref().map(|requirements| {
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
        quote! {
            if metadata.operation().security.is_none() {
                metadata.operation_mut().security = ::std::option::Option::Some(
                    ::std::vec![#(#requirements),*],
                );
            }
        }
    });
    let responses = openapi
        .responses
        .iter()
        .map(|response| generate_controller_response(runtime, response));
    let into_responses = openapi.into_responses.as_ref().map(|responses| {
        quote! { metadata.extend_default_responses::<#responses>(); }
    });

    quote! {
        #tag
        #description
        #security
        #(#responses)*
        #into_responses
    }
}

fn generate_controller_response(runtime: &TokenStream2, response: &ResponseArg) -> TokenStream2 {
    let status = &response.status;
    if let Some(reusable) = response.reusable.as_ref() {
        return quote! {
            metadata.register_default_reusable_response::<#reusable>(#status);
        };
    }
    let description = response
        .description
        .as_ref()
        .expect("inline responses require descriptions");
    let schema_collection = response.schema.as_ref().map(|schema| {
        quote! {
            metadata.collect_schema::<#schema>();
        }
    });
    let content = response.schema.as_ref().map(|schema| {
        let content_type = response
            .content_type
            .as_ref()
            .expect("response parser supplies a content type for schemas");
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
        if !metadata.operation().responses.responses.contains_key(#status) {
            #schema_collection
            let response = #runtime::__private::utoipa::openapi::response::ResponseBuilder::new()
                .description(#description)
                #content
                .build();
            metadata
                .operation_mut()
                .responses
                .responses
                .insert(#status.to_owned(), response.into());
        }
    }
}
