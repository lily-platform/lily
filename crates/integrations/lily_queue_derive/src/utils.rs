//! Utility functions for queue macro code generation
//!
//! Helper functions for parsing, validation, and code generation.

use proc_macro2::{Ident, Span, TokenStream};
use quote::{format_ident, quote, ToTokens};
use syn::{
    ext::IdentExt, FnArg, GenericArgument, ImplItem, ItemImpl, PathArguments, ReturnType,
    Signature, Type,
};

#[cfg(feature = "asyncapi")]
use crate::asyncapi::{self, AsyncApiArgs, EffectiveAsyncApi, Scope};
use crate::queue_args::QueueArgs;

/// Ordered middleware and guard declarations at one source level.
#[derive(Debug, Clone, Default)]
pub(crate) struct QueuePipelineTypes {
    /// Middleware types in source declaration order.
    pub(crate) middlewares: Vec<Type>,

    /// Guard types in source declaration order.
    pub(crate) guards: Vec<Type>,
}

impl QueuePipelineTypes {
    pub(crate) fn is_empty(&self) -> bool {
        self.middlewares.is_empty() && self.guards.is_empty()
    }
}

/// Information about a queue handler method
#[derive(Debug, Clone)]
pub(crate) struct QueueMethodInfo {
    /// Method name (e.g., handle_user_created)
    pub(crate) method_name: Ident,

    /// Queue configuration from #[queue(...)] attribute
    pub(crate) queue_args: QueueArgs,

    /// Ordered owned extractor argument types.
    pub(crate) argument_types: Vec<Type>,

    /// Handler-local middleware and guard types.
    pub(crate) pipeline: QueuePipelineTypes,

    /// Effective service/handler AsyncAPI metadata baked into this handler.
    #[cfg(feature = "asyncapi")]
    pub(crate) asyncapi: EffectiveAsyncApi,
}

pub(crate) fn is_runtime_attribute(
    attribute: &syn::Attribute,
    runtime_attribute_prefix: &[&str],
    name: &str,
) -> bool {
    let segments = &attribute.path().segments;
    if segments.len() == 1 {
        return attribute.path().is_ident(name);
    }
    if !segments.last().is_some_and(|segment| segment.ident == name) {
        return false;
    }
    runtime_attribute_prefix.iter().any(|prefix| {
        let prefix: Vec<_> = prefix.split("::").collect();
        segments.len() == prefix.len() + 1
            && segments.iter().zip(prefix).all(|(segment, name)| {
                segment.ident == name && matches!(segment.arguments, PathArguments::None)
            })
    })
}

fn is_queue_attribute(attribute: &syn::Attribute, runtime_attribute_prefix: &[&str]) -> bool {
    is_runtime_attribute(attribute, runtime_attribute_prefix, "queue")
}

fn is_middleware_attribute(attribute: &syn::Attribute, runtime_attribute_prefix: &[&str]) -> bool {
    is_runtime_attribute(attribute, runtime_attribute_prefix, "middleware")
}

fn is_guard_attribute(attribute: &syn::Attribute, runtime_attribute_prefix: &[&str]) -> bool {
    is_runtime_attribute(attribute, runtime_attribute_prefix, "guard")
}

fn is_asyncapi_attribute(attribute: &syn::Attribute, runtime_attribute_prefix: &[&str]) -> bool {
    is_runtime_attribute(attribute, runtime_attribute_prefix, "asyncapi")
}

/// Whether an impl contains facade-owned AsyncAPI metadata at either level.
pub(crate) fn contains_asyncapi_attributes(
    impl_block: &ItemImpl,
    runtime_attribute_prefix: &[&str],
) -> bool {
    impl_block
        .attrs
        .iter()
        .any(|attribute| is_asyncapi_attribute(attribute, runtime_attribute_prefix))
        || impl_block.items.iter().any(|item| {
            let ImplItem::Fn(method) = item else {
                return false;
            };
            method
                .attrs
                .iter()
                .any(|attribute| is_asyncapi_attribute(attribute, runtime_attribute_prefix))
        })
}

/// Parse repeatable, single-type sibling pipeline markers in source order.
pub(crate) fn extract_pipeline_types(
    attributes: &[syn::Attribute],
    runtime_attribute_prefix: &[&str],
    owner: &str,
) -> syn::Result<QueuePipelineTypes> {
    let mut pipeline = QueuePipelineTypes::default();
    for attribute in attributes {
        let (target, kind) = if is_middleware_attribute(attribute, runtime_attribute_prefix) {
            (&mut pipeline.middlewares, "middleware")
        } else if is_guard_attribute(attribute, runtime_attribute_prefix) {
            (&mut pipeline.guards, "guard")
        } else {
            continue;
        };
        let ty = attribute.parse_args::<Type>().map_err(|_| {
            syn::Error::new_spanned(
                attribute,
                format!("{owner} #[{kind}] must contain exactly one concrete type"),
            )
        })?;
        target.push(ty);
    }
    Ok(pipeline)
}

/// Extract all methods with #[queue(...)] attribute from impl block
pub(crate) fn extract_queue_methods(
    impl_block: &ItemImpl,
    runtime_attribute_prefix: &[&str],
    #[cfg(feature = "asyncapi")] service_asyncapi: Option<&AsyncApiArgs>,
) -> syn::Result<Vec<QueueMethodInfo>> {
    let mut queue_methods = Vec::new();

    for item in &impl_block.items {
        if let ImplItem::Fn(method) = item {
            let pipeline =
                extract_pipeline_types(&method.attrs, runtime_attribute_prefix, "queue handler")?;
            let asyncapi_attributes = method
                .attrs
                .iter()
                .filter(|attribute| is_asyncapi_attribute(attribute, runtime_attribute_prefix))
                .collect::<Vec<_>>();
            if asyncapi_attributes.len() > 1 {
                return Err(syn::Error::new_spanned(
                    &method.sig,
                    "a queue handler must declare at most one #[asyncapi(...)] attribute",
                ));
            }
            // Look for #[queue(...)] attribute
            let queue_attributes = method
                .attrs
                .iter()
                .filter(|attribute| is_queue_attribute(attribute, runtime_attribute_prefix))
                .collect::<Vec<_>>();
            if queue_attributes.len() > 1 {
                return Err(syn::Error::new_spanned(
                    &method.sig,
                    "a queue handler must declare exactly one #[queue(...)] attribute",
                ));
            }
            if let Some(attr) = queue_attributes.into_iter().next() {
                // Parse queue arguments
                let queue_args: QueueArgs = attr.parse_args()?;

                // Validate method signature
                let argument_types = validate_handler_signature(&method.sig)?;
                #[cfg(feature = "asyncapi")]
                let effective_asyncapi = {
                    let handler_asyncapi = asyncapi_attributes
                        .first()
                        .map(|attribute| asyncapi::parse(attribute, Scope::Handler))
                        .transpose()?;
                    asyncapi::inherit(service_asyncapi, handler_asyncapi.as_ref())?
                };
                // Extractor roles are Rust trait contracts, not identifier
                // spellings. The generated tuple bound enforces parts-before-
                // payload structure, while the canonical consumer plan checks
                // the resolved payload kind against the declared content kind.

                queue_methods.push(QueueMethodInfo {
                    method_name: method.sig.ident.clone(),
                    queue_args,
                    argument_types,
                    pipeline,
                    #[cfg(feature = "asyncapi")]
                    asyncapi: effective_asyncapi,
                });
            } else if !asyncapi_attributes.is_empty() {
                return Err(syn::Error::new_spanned(
                    &method.sig,
                    "queue handler AsyncAPI metadata requires #[queue(...)] on the same method",
                ));
            } else if !pipeline.is_empty() {
                return Err(syn::Error::new_spanned(
                    &method.sig,
                    "queue handler middleware and guards require #[queue(...)] on the same method",
                ));
            }
        }
    }

    Ok(queue_methods)
}

/// Validate handler method signature
pub(crate) fn validate_handler_signature(signature: &Signature) -> syn::Result<Vec<Type>> {
    // Check if method is async
    if signature.asyncness.is_none() {
        return Err(syn::Error::new_spanned(
            signature,
            "Queue handler must be async. Add 'async' keyword before 'fn'",
        ));
    }

    let receiver = signature.receiver().ok_or_else(|| {
        syn::Error::new_spanned(
            signature,
            "Queue handler must take immutable &self as its first parameter",
        )
    })?;
    if receiver.reference.is_none()
        || receiver.mutability.is_some()
        || receiver.colon_token.is_some()
    {
        return Err(syn::Error::new_spanned(
            receiver,
            "Queue handler receiver must be &self",
        ));
    }
    if !signature.generics.params.is_empty()
        || signature.generics.where_clause.is_some()
        || signature.constness.is_some()
        || signature.unsafety.is_some()
        || signature.abi.is_some()
        || signature.variadic.is_some()
    {
        return Err(syn::Error::new_spanned(
            signature,
            "Queue handlers cannot be generic, const, unsafe, extern or variadic",
        ));
    }

    let arguments = signature
        .inputs
        .iter()
        .skip(1)
        .map(|input| {
            let FnArg::Typed(argument) = input else {
                return Err(syn::Error::new_spanned(
                    input,
                    "Queue handler receiver must appear exactly once and be first",
                ));
            };
            if contains_reference(&argument.ty) {
                return Err(syn::Error::new_spanned(
                    &argument.ty,
                    "Queue handler extractors must be owned values; references are not supported",
                ));
            }
            Ok(argument.ty.as_ref().clone())
        })
        .collect::<syn::Result<Vec<_>>>()?;
    if arguments.len() > 16 {
        return Err(syn::Error::new_spanned(
            &signature.inputs,
            "Queue handlers accept at most 16 typed extractor arguments",
        ));
    }

    // Check return type
    match &signature.output {
        ReturnType::Default => {
            return Err(syn::Error::new_spanned(
                signature,
                "Queue handler must return Result<(), Error>",
            ));
        }
        ReturnType::Type(_, _) => {}
    }

    Ok(arguments)
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

/// Remove marker attributes after the outer macro has consumed them.
pub(crate) fn strip_queue_attributes(impl_block: &mut ItemImpl, runtime_attribute_prefix: &[&str]) {
    impl_block.attrs.retain(|attribute| {
        !is_middleware_attribute(attribute, runtime_attribute_prefix)
            && !is_guard_attribute(attribute, runtime_attribute_prefix)
            && !is_asyncapi_attribute(attribute, runtime_attribute_prefix)
    });
    for item in &mut impl_block.items {
        if let ImplItem::Fn(method) = item {
            method.attrs.retain(|attribute| {
                !is_queue_attribute(attribute, runtime_attribute_prefix)
                    && !is_middleware_attribute(attribute, runtime_attribute_prefix)
                    && !is_guard_attribute(attribute, runtime_attribute_prefix)
                    && !is_asyncapi_attribute(attribute, runtime_attribute_prefix)
            });
        }
    }
}

fn identifier_fragment(identifier: &Ident) -> String {
    identifier.unraw().to_string()
}

/// Generate unique identifier for handler wrapper function
pub(crate) fn generate_handler_wrapper_name(service_type: &Type, method_name: &Ident) -> Ident {
    let service_name = service_type
        .to_token_stream()
        .to_string()
        .replace("::", "_")
        .replace("<", "_")
        .replace(">", "_")
        .replace(" ", "");

    Ident::new(
        &format!(
            "__queue_handler_{}_{}",
            service_name,
            identifier_fragment(method_name)
        ),
        Span::call_site(),
    )
}

/// Generate unique identifier for metadata static variable
pub(crate) fn generate_metadata_static_name(service_type: &Type, method_name: &Ident) -> Ident {
    let service_name = service_type
        .to_token_stream()
        .to_string()
        .replace("::", "_")
        .replace("<", "_")
        .replace(">", "_")
        .replace(" ", "");

    Ident::new(
        &format!(
            "__QUEUE_METADATA_{}_{}",
            service_name.to_uppercase(),
            identifier_fragment(method_name).to_uppercase()
        ),
        Span::call_site(),
    )
}

/// Generate unique identifier for metadata getter function
pub(crate) fn generate_metadata_getter_name(service_type: &Type, method_name: &Ident) -> Ident {
    let metadata_static = generate_metadata_static_name(service_type, method_name);

    Ident::new(
        &format!("get_{}", metadata_static.to_string().to_lowercase()),
        Span::call_site(),
    )
}

/// Generate unique identifier for registration function
pub(crate) fn generate_registration_name(service_type: &Type, method_name: &Ident) -> Ident {
    let service_name = service_type
        .to_token_stream()
        .to_string()
        .replace("::", "_")
        .replace("<", "_")
        .replace(">", "_")
        .replace(" ", "");

    Ident::new(
        &format!(
            "__QUEUE_REGISTRATION_{}_{}",
            service_name.to_uppercase(),
            identifier_fragment(method_name).to_uppercase()
        ),
        Span::call_site(),
    )
}

/// Generate handler wrapper function
pub(crate) fn generate_handler_wrapper(
    runtime_path: &TokenStream,
    service_type: &Type,
    method_info: &QueueMethodInfo,
) -> TokenStream {
    let wrapper_name = generate_handler_wrapper_name(service_type, &method_info.method_name);
    let method_name = &method_info.method_name;
    let argument_types = &method_info.argument_types;
    let argument_names = (0..argument_types.len())
        .map(|index| format_ident!("__lily_queue_argument_{index}"))
        .collect::<Vec<_>>();
    let argument_tuple = tuple_type(argument_types);
    let argument_pattern = tuple_pattern(&argument_names);

    quote! {
        fn #wrapper_name<'a>(
            service: ::std::sync::Arc<dyn ::std::any::Any + Send + Sync>,
            invocation: &'a mut (dyn ::std::any::Any + Send),
        ) -> ::std::pin::Pin<::std::boxed::Box<dyn ::std::future::Future<Output = ::core::result::Result<(), #runtime_path::__private::QueueHandlerError>> + Send + 'a>> {
            ::std::boxed::Box::pin(async move {
                let service = service
                    .downcast::<#service_type>()
                    .map_err(|_| #runtime_path::__private::QueueHandlerError::retryable(
                        "QUEUE_HANDLER_SERVICE_TYPE_MISMATCH"
                    ))?;
                let invocation = invocation
                    .downcast_mut::<#runtime_path::__private::DeliveryInvocation>()
                    .ok_or_else(|| #runtime_path::__private::QueueHandlerError::retryable(
                        "QUEUE_HANDLER_INVOCATION_TYPE_MISMATCH"
                    ))?;
                let #argument_pattern: #argument_tuple =
                    #runtime_path::__private::extract_delivery_arguments::<#argument_tuple, _>(
                        invocation,
                    )
                    .await?;

                let result = service.#method_name(#(#argument_names),*).await;
                let (): () = result.map_err(|error| {
                    let error: #runtime_path::__private::QueueHandlerError =
                        ::core::convert::Into::into(error);
                    error
                })?;
                ::core::result::Result::Ok(())
            })
        }
    }
}

fn tuple_type(arguments: &[Type]) -> TokenStream {
    if arguments.is_empty() {
        quote!(())
    } else {
        quote!((#(#arguments,)*))
    }
}

fn tuple_pattern(arguments: &[Ident]) -> TokenStream {
    if arguments.is_empty() {
        quote!(())
    } else {
        quote!((#(#arguments,)*))
    }
}

/// Generate metadata registration code
pub(crate) fn generate_metadata_registration(
    runtime_path: &TokenStream,
    service_type: &Type,
    service_pipeline: &QueuePipelineTypes,
    method_info: &QueueMethodInfo,
) -> TokenStream {
    let wrapper_name = generate_handler_wrapper_name(service_type, &method_info.method_name);
    let metadata_static = generate_metadata_static_name(service_type, &method_info.method_name);
    let getter_name = generate_metadata_getter_name(service_type, &method_info.method_name);
    let registration_name = generate_registration_name(service_type, &method_info.method_name);

    let queue_name = &method_info.queue_args.name;
    let schema_version = method_info.queue_args.version;
    let content_kind = &method_info.queue_args.content;
    let delivery_guarantee = match method_info.queue_args.delivery_guarantee {
        crate::queue_args::DeliveryGuaranteeArg::AtLeastOnce => {
            quote!(#runtime_path::__private::DeliveryGuarantee::AtLeastOnce)
        }
        crate::queue_args::DeliveryGuaranteeArg::TransactionalInbox => {
            quote!(#runtime_path::__private::DeliveryGuarantee::TransactionalInbox)
        }
    };
    let method_name = &method_info.method_name;
    let argument_tuple = tuple_type(&method_info.argument_types);
    let service_middlewares = &service_pipeline.middlewares;
    let service_guards = &service_pipeline.guards;
    let handler_middlewares = &method_info.pipeline.middlewares;
    let handler_guards = &method_info.pipeline.guards;
    #[cfg(feature = "asyncapi")]
    let asyncapi = asyncapi::generate(
        runtime_path,
        &method_info.asyncapi,
        &method_info.argument_types,
    );
    #[cfg(feature = "asyncapi")]
    let asyncapi_field = quote!(asyncapi: #asyncapi,);
    #[cfg(not(feature = "asyncapi"))]
    let asyncapi_field = TokenStream::new();

    quote! {
        // Static metadata storage (lazy initialization)
        static #metadata_static: ::std::sync::OnceLock<#runtime_path::__private::QueueHandlerMetadata> =
            ::std::sync::OnceLock::new();

        // Metadata getter function
        fn #getter_name() -> &'static #runtime_path::__private::QueueHandlerMetadata {
            #metadata_static.get_or_init(|| #runtime_path::__private::QueueHandlerMetadata {
                service_type_id: ::std::any::TypeId::of::<#service_type>(),
                service_type_name: ::core::stringify!(#service_type),
                component_kind: None,
                queue_name: #queue_name,
                method_name: ::core::stringify!(#method_name),
                handler_name: ::core::concat!(
                    ::core::module_path!(),
                    "::",
                    ::core::stringify!(#service_type),
                    "::",
                    ::core::stringify!(#method_name),
                ),
                schema_version: #schema_version,
                content_kind: #content_kind,
                delivery_guarantee: #delivery_guarantee,
                input_contract: #runtime_path::__private::delivery_input_contract::<#argument_tuple, _>(),
                #asyncapi_field
                service_middlewares: ::std::vec![
                    #(#runtime_path::__private::queue_middleware_registration::<#service_middlewares>(),)*
                ],
                service_guards: ::std::vec![
                    #(#runtime_path::__private::queue_guard_registration::<#service_guards>(),)*
                ],
                handler_middlewares: ::std::vec![
                    #(#runtime_path::__private::queue_middleware_registration::<#handler_middlewares>(),)*
                ],
                handler_guards: ::std::vec![
                    #(#runtime_path::__private::queue_guard_registration::<#handler_guards>(),)*
                ],
                handler_fn: #wrapper_name,
            })
        }

        // Register in distributed slice
        #[#runtime_path::__private::linkme::distributed_slice(#runtime_path::__private::QUEUE_HANDLER_GETTERS)]
        #[linkme(crate = #runtime_path::__private::linkme)]
        static #registration_name: fn() -> &'static #runtime_path::__private::QueueHandlerMetadata = #getter_name;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::parse_quote;

    #[test]
    fn handler_arguments_preserve_order() {
        let sig: Signature = parse_quote! {
            async fn handle_user_created(
                &self,
                context: DeliveryContext,
                msg: Json<UserCreated>,
            ) -> Result<(), QueueHandlerError>
        };

        let arguments = validate_handler_signature(&sig).unwrap();
        assert_eq!(arguments.len(), 2);
        assert_eq!(
            arguments[0].to_token_stream().to_string(),
            "DeliveryContext"
        );
        assert_eq!(
            arguments[1].to_token_stream().to_string(),
            "Json < UserCreated >"
        );
    }

    #[test]
    fn test_validate_async_signature() {
        let sig: Signature = parse_quote! {
            async fn handler(&self, msg: Message) -> Result<(), Error>
        };

        assert!(validate_handler_signature(&sig).is_ok());
    }

    #[test]
    fn test_validate_non_async_fails() {
        let sig: Signature = parse_quote! {
            fn handler(&self, msg: Message) -> Result<(), Error>
        };

        assert!(validate_handler_signature(&sig).is_err());
    }

    #[test]
    fn zero_arguments_are_supported_but_references_are_rejected() {
        let empty: Signature = parse_quote! {
            async fn handler(&self) -> Result<(), Error>
        };
        let borrowed: Signature = parse_quote! {
            async fn handler(&self, msg: Json<&str>) -> Result<(), Error>
        };

        assert!(validate_handler_signature(&empty).is_ok());
        assert!(validate_handler_signature(&borrowed).is_err());
    }

    #[test]
    fn test_generate_wrapper_name() {
        let service_type: Type = parse_quote! { UserService };
        let method_name = Ident::new("handle_created", Span::call_site());

        let wrapper = generate_handler_wrapper_name(&service_type, &method_name);
        assert_eq!(
            wrapper.to_string(),
            "__queue_handler_UserService_handle_created"
        );
    }

    #[test]
    fn queue_attribute_matching_is_limited_to_the_runtime_facade() {
        let unqualified: syn::Attribute =
            parse_quote!(#[queue("orders", version = 1, content = "json")]);
        let qualified: syn::Attribute =
            parse_quote!(#[queue_runtime::queue("orders", version = 1, content = "json")]);
        let unrelated: syn::Attribute =
            parse_quote!(#[another_framework::queue("orders", version = 1, content = "json")]);

        assert!(is_queue_attribute(&unqualified, &["queue_runtime"]));
        assert!(is_queue_attribute(&qualified, &["queue_runtime"]));
        assert!(!is_queue_attribute(&unrelated, &["queue_runtime"]));
    }

    #[test]
    fn nested_and_direct_facade_markers_can_be_mixed_without_consuming_foreign_paths() {
        let prefixes = &["queue_runtime", "platform::queue"];
        for attribute in [
            parse_quote!(#[queue("events", version = 1, content = "json")]),
            parse_quote!(#[queue_runtime::queue("events", version = 1, content = "json")]),
            parse_quote!(#[::platform::queue::queue("events", version = 1, content = "json")]),
        ] {
            assert!(is_queue_attribute(&attribute, prefixes));
        }
        for attribute in [
            parse_quote!(#[other::queue::queue("events", version = 1, content = "json")]),
            parse_quote!(#[platform::other::queue("events", version = 1, content = "json")]),
            parse_quote!(#[platform::queue::nested::queue("events", version = 1, content = "json")]),
        ] {
            assert!(!is_queue_attribute(&attribute, prefixes));
        }
        let block: ItemImpl = parse_quote! {
            impl Handler {
                #[queue_runtime::queue("events", version = 1, content = "json")]
                #[platform::queue::queue("events", version = 1, content = "json")]
                async fn handle(&self) -> Result<(), Error> { Ok(()) }
            }
        };
        let error = extract_queue_methods(
            &block,
            prefixes,
            #[cfg(feature = "asyncapi")]
            None,
        )
        .unwrap_err();
        assert!(error.to_string().contains("exactly one #[queue"));
    }

    #[test]
    fn sibling_pipeline_markers_preserve_source_order_and_facade_qualification() {
        let attributes: Vec<syn::Attribute> = vec![
            parse_quote!(#[middleware(ServiceAudit)]),
            parse_quote!(#[queue_runtime::middleware(TenantContext)]),
            parse_quote!(#[guard(ServiceAdmission)]),
            parse_quote!(#[queue_runtime::guard(CanProcess)]),
        ];

        let pipeline =
            extract_pipeline_types(&attributes, &["queue_runtime"], "queue service").unwrap();
        assert_eq!(
            pipeline
                .middlewares
                .iter()
                .map(ToTokens::to_token_stream)
                .map(|tokens| tokens.to_string())
                .collect::<Vec<_>>(),
            ["ServiceAudit", "TenantContext"]
        );
        assert_eq!(
            pipeline
                .guards
                .iter()
                .map(ToTokens::to_token_stream)
                .map(|tokens| tokens.to_string())
                .collect::<Vec<_>>(),
            ["ServiceAdmission", "CanProcess"]
        );
    }

    #[test]
    fn pipeline_marker_requires_exactly_one_type() {
        for attribute in [
            parse_quote!(#[middleware()]),
            parse_quote!(#[middleware(First, Second)]),
            parse_quote!(#[guard("not a type")]),
        ] {
            assert!(
                extract_pipeline_types(&[attribute], &["queue_runtime"], "queue handler").is_err()
            );
        }
    }

    #[test]
    fn unrelated_qualified_pipeline_attributes_are_not_consumed() {
        let attributes: Vec<syn::Attribute> = vec![
            parse_quote!(#[another_framework::middleware(ForeignMiddleware)]),
            parse_quote!(#[another_framework::guard(ForeignGuard)]),
        ];

        assert!(
            extract_pipeline_types(&attributes, &["queue_runtime"], "queue handler")
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn stripping_removes_only_queue_runtime_markers() {
        let mut block: ItemImpl = parse_quote! {
            #[middleware(ServiceAudit)]
            #[another_framework::middleware(ForeignServiceAudit)]
            impl AuditService {
                #[queue("audit.created", version = 1, content = "json")]
                #[guard(CanProcess)]
                #[another_framework::guard(ForeignGuard)]
                async fn handle(&self) -> Result<(), QueueHandlerError> {
                    Ok(())
                }
            }
        };

        strip_queue_attributes(&mut block, &["queue_runtime"]);
        assert_eq!(block.attrs.len(), 1);
        assert_eq!(block.attrs[0].path().segments[0].ident, "another_framework");
        let ImplItem::Fn(method) = &block.items[0] else {
            panic!("fixture method")
        };
        assert_eq!(method.attrs.len(), 1);
        assert_eq!(
            method.attrs[0].path().segments[0].ident,
            "another_framework"
        );
    }

    #[test]
    fn handler_pipeline_requires_a_queue_marker() {
        let block: ItemImpl = parse_quote! {
            impl AuditService {
                #[middleware(Audit)]
                async fn helper(&self) -> Result<(), QueueHandlerError> {
                    Ok(())
                }
            }
        };

        let error = extract_queue_methods(
            &block,
            &["queue_runtime"],
            #[cfg(feature = "asyncapi")]
            None,
        )
        .unwrap_err();
        assert!(error.to_string().contains("require #[queue(...)]"));
    }

    #[test]
    fn raw_method_names_generate_valid_internal_identifiers() {
        let service_type: Type = parse_quote! { AuditService };
        let method_name: Ident = parse_quote! { r#match };

        let wrapper = generate_handler_wrapper_name(&service_type, &method_name);
        assert_eq!(wrapper.to_string(), "__queue_handler_AuditService_match");
    }
}
