use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, Expr, ExprLit, Fields, Lit};

fn validate_gateway_url(value: &str) -> Result<url::Url, String> {
    let invalid = || {
        format!(
            "Invalid WebSocket URL: '{value}'. WebSocket URLs must start with 'ws://' or 'wss://'. Example: #[gateway(url = \"ws://127.0.0.1:43000\", namespace = \"appmanager\")]"
        )
    };
    let parsed = url::Url::parse(value).map_err(|_| invalid())?;
    if !matches!(parsed.scheme(), "ws" | "wss") || parsed.host_str().is_none() {
        return Err(invalid());
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(
            "BaseGateway URL credentials are not supported; configure an AuthHeaderProvider".into(),
        );
    }
    if parsed.fragment().is_some() {
        return Err("BaseGateway URL fragments are not part of the handshake".into());
    }
    let mut namespace_count = 0usize;
    for (name, _) in parsed.query_pairs() {
        if name == "namespace" {
            namespace_count += 1;
        }
        if matches!(
            name.to_ascii_lowercase().as_str(),
            "access_token"
                | "api_key"
                | "apikey"
                | "authorization"
                | "password"
                | "secret"
                | "sig"
                | "signature"
                | "token"
        ) {
            return Err(format!(
                "BaseGateway URL cannot contain sensitive query parameter `{name}`; configure an AuthHeaderProvider"
            ));
        }
    }
    if namespace_count > 1 {
        return Err("BaseGateway URL cannot contain duplicate namespace parameters".into());
    }
    Ok(parsed)
}

fn validate_namespace(value: &str) -> Result<(), String> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(
            "BaseGateway namespace must be a non-empty, at most 128-byte ASCII route token".into(),
        );
    }
    Ok(())
}

/// Main implementation of the BaseGateway derive macro.
pub(crate) fn derive_impl(input: TokenStream) -> TokenStream {
    let ast = syn::parse_macro_input!(input as DeriveInput);

    // Extract struct name
    let struct_name = &ast.ident;

    // Parse gateway attributes
    let mut url: Option<String> = None;
    let mut namespace: Option<String> = None;
    let mut dto_type: Option<syn::Type> = None;

    for attr in ast.attrs.iter() {
        if attr.path().is_ident("gateway") {
            let _ = attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("url") {
                    let value = meta.value()?;
                    let lit: Expr = value.parse()?;
                    if let Expr::Lit(ExprLit {
                        lit: Lit::Str(lit_str),
                        ..
                    }) = lit
                    {
                        url = Some(lit_str.value());
                    }
                } else if meta.path.is_ident("namespace") {
                    let value = meta.value()?;
                    let lit: Expr = value.parse()?;
                    if let Expr::Lit(ExprLit {
                        lit: Lit::Str(lit_str),
                        ..
                    }) = lit
                    {
                        namespace = Some(lit_str.value());
                    }
                }
                Ok(())
            });
        } else if attr.path().is_ident("dto_type")
            && let Ok(type_path) = attr.parse_args::<syn::Type>()
        {
            dto_type = Some(type_path);
        }
    }

    // Validate that url is specified (required)
    let url = match url {
        Some(u) => {
            if let Err(reason) = validate_gateway_url(&u) {
                return TokenStream::from(
                    syn::Error::new_spanned(struct_name, reason).to_compile_error(),
                );
            }
            u
        }
        None => {
            return TokenStream::from(
                syn::Error::new_spanned(
                    struct_name,
                    "BaseGateway requires #[gateway(url = \"ws://...\", namespace = \"...\")] attribute. Example: #[gateway(url = \"ws://localhost:8080\", namespace = \"application\")]"
                ).to_compile_error()
            );
        }
    };

    // Find gateway_client field
    let client_field_name = match validate_gateway_fields(&ast) {
        Ok(field_name) => field_name,
        Err(error) => {
            return TokenStream::from(error.to_compile_error());
        }
    };

    // Validate that dto_type is specified (required)
    let dto_type = match dto_type {
        Some(ref t) => t,
        None => {
            return TokenStream::from(
                syn::Error::new_spanned(
                    struct_name,
                    "BaseGateway requires #[dto_type(YourDtoType)] attribute. Example: #[dto_type(ApplicationDto)]"
                ).to_compile_error()
            );
        }
    };

    // Extract entity name from struct name (e.g., ApplicationGateway -> application)
    let entity_name = struct_name
        .to_string()
        .trim_end_matches("Gateway")
        .to_lowercase();

    // Generate namespace string
    let namespace_str = namespace.unwrap_or_else(|| entity_name.clone());
    if let Err(reason) = validate_namespace(&namespace_str) {
        return TokenStream::from(syn::Error::new_spanned(struct_name, reason).to_compile_error());
    }
    let parsed_url = validate_gateway_url(&url).expect("URL was validated above");
    if let Some(url_namespace) = parsed_url
        .query_pairs()
        .find_map(|(name, value)| (name == "namespace").then_some(value.into_owned()))
        && url_namespace != namespace_str
    {
        return TokenStream::from(
            syn::Error::new_spanned(
                struct_name,
                "BaseGateway URL namespace and #[gateway(namespace = ...)] must match",
            )
            .to_compile_error(),
        );
    }

    // Generate implementation
    let expanded = quote! {
        impl #struct_name {
            /// Create new gateway instance (for manual usage)
            pub fn new() -> Self {
                let client = lily_websocket_client::TokioWsClient::from_compile_time_config(
                    #url,
                    #namespace_str,
                );

                Self {
                    #client_field_name: client,
                    // Event prefix and connection namespace must be identical;
                    // otherwise the server routes to one controller and broadcasts in
                    // another namespace.
                    entity_name: #namespace_str.to_string(),
                }
            }

            /// Notify entity created
            pub async fn notify_created(
                &self,
                item: &#dto_type,
            ) -> Result<(), lily_websocket_client::WebSocketError>
            where
                #dto_type: serde::Serialize,
            {
                let event = format!("{}:created", self.entity_name);
                lily_websocket_client::WsClient::send(&self.#client_field_name, &event, item).await
            }

            /// Notify multiple entities created
            pub async fn notify_created_many(
                &self,
                items: &[#dto_type],
            ) -> Result<(), lily_websocket_client::WebSocketError>
            where
                #dto_type: serde::Serialize,
            {
                let event = format!("{}:createdMany", self.entity_name);
                lily_websocket_client::WsClient::send(&self.#client_field_name, &event, items).await
            }

            /// Notify entity updated
            pub async fn notify_updated(
                &self,
                item: &#dto_type,
            ) -> Result<(), lily_websocket_client::WebSocketError>
            where
                #dto_type: serde::Serialize,
            {
                let event = format!("{}:updated", self.entity_name);
                lily_websocket_client::WsClient::send(&self.#client_field_name, &event, item).await
            }

            /// Notify entity deleted
            pub async fn notify_deleted(
                &self,
                item: &#dto_type,
            ) -> Result<(), lily_websocket_client::WebSocketError>
            where
                #dto_type: serde::Serialize,
            {
                let event = format!("{}:deleted", self.entity_name);
                lily_websocket_client::WsClient::send(&self.#client_field_name, &event, item).await
            }

            /// Notify deletion using the stable CRUD wire contract: the
            /// payload is the deleted entity ID.
            pub async fn notify_deleted_id(
                &self,
                id: &str,
            ) -> Result<(), lily_websocket_client::WebSocketError> {
                let event = format!("{}:deleted", self.entity_name);
                lily_websocket_client::WsClient::send(&self.#client_field_name, &event, id).await
            }

            /// Notify multiple entities deleted
            pub async fn notify_deleted_many(
                &self,
                items: &[#dto_type],
            ) -> Result<(), lily_websocket_client::WebSocketError>
            where
                #dto_type: serde::Serialize,
            {
                let event = format!("{}:deletedMany", self.entity_name);
                lily_websocket_client::WsClient::send(&self.#client_field_name, &event, items).await
            }

            /// Get gateway client for custom operations
            pub fn client(&self) -> &lily_websocket_client::TokioWsClient {
                &self.#client_field_name
            }

            /// Check if gateway is connected
            pub fn is_connected(&self) -> bool {
                lily_websocket_client::WsClient::is_connected(&self.#client_field_name)
            }

            /// Disconnect gateway
            pub async fn disconnect(
                &self,
            ) -> Result<(), lily_websocket_client::WebSocketError> {
                lily_websocket_client::WsClient::disconnect(&self.#client_field_name).await
            }
        }

        impl Default for #struct_name {
            fn default() -> Self {
                Self::new()
            }
        }

        /// ServiceTrait implementation for DI lifecycle integration
        #[async_trait::async_trait]
        impl lily_injection::service::ServiceTrait for #struct_name {
            /// Initialize gateway connection when service is initialized by DI container
            async fn initialize(&mut self) -> Result<(), lily_error::injection::InjectionError> {
                lily_websocket_client::WsClient::connect(
                    &self.#client_field_name,
                    tokio_util::sync::CancellationToken::new(),
                ).await
                    .map_err(|e| lily_error::injection::InjectionError::InitError(
                        format!("Gateway connection failed: {}", e)
                    ))?;

                Ok(())
            }

            /// Disconnect gateway when service is disposed by DI container
            async fn dispose(&self) -> Result<(), lily_error::injection::InjectionError> {
                lily_websocket_client::WsClient::disconnect(&self.#client_field_name).await
                    .map_err(|e| lily_error::injection::InjectionError::DisposeError(
                        format!("Gateway disconnection failed: {}", e)
                    ))?;

                Ok(())
            }
        }
    };

    TokenStream::from(expanded)
}

/// Validate the exact two-field layout initialized by the generated `new`.
fn validate_gateway_fields(ast: &DeriveInput) -> syn::Result<syn::Ident> {
    let Fields::Named(fields) = (match &ast.data {
        syn::Data::Struct(data) => &data.fields,
        _ => {
            return Err(syn::Error::new_spanned(
                &ast.ident,
                "BaseGateway can only be derived for a struct with named fields",
            ));
        }
    }) else {
        return Err(syn::Error::new_spanned(
            &ast.ident,
            "BaseGateway requires a struct with named fields",
        ));
    };

    let client_fields = fields
        .named
        .iter()
        .filter(|field| {
            field
                .attrs
                .iter()
                .any(|attribute| attribute.path().is_ident("gateway_client"))
        })
        .collect::<Vec<_>>();
    if client_fields.is_empty() {
        return Err(syn::Error::new_spanned(
            &ast.ident,
            "BaseGateway requires a field marked with #[gateway_client] of type TokioWsClient. Example: #[gateway_client] pub client: lily_websocket_client::TokioWsClient",
        ));
    }
    if client_fields.len() != 1 {
        return Err(syn::Error::new_spanned(
            &ast.ident,
            "BaseGateway requires exactly one #[gateway_client] field",
        ));
    }

    let has_entity_name = fields.named.iter().any(|field| {
        field
            .ident
            .as_ref()
            .is_some_and(|ident| ident == "entity_name")
    });
    if !has_entity_name {
        return Err(syn::Error::new_spanned(
            &ast.ident,
            "BaseGateway requires an `entity_name: String` field",
        ));
    }
    if fields.named.len() != 2 {
        return Err(syn::Error::new_spanned(
            &ast.ident,
            "BaseGateway currently supports exactly two fields: one #[gateway_client] field and `entity_name: String`",
        ));
    }

    client_fields[0]
        .ident
        .clone()
        .ok_or_else(|| syn::Error::new_spanned(&ast.ident, "BaseGateway requires named fields"))
}

#[cfg(test)]
mod tests {
    use super::{validate_gateway_fields, validate_gateway_url, validate_namespace};

    #[test]
    fn gateway_url_validation_rejects_credential_and_routing_ambiguity() {
        assert!(validate_gateway_url("https://example.test").is_err());
        assert!(validate_gateway_url("wss://user:secret@example.test/socket").is_err());
        assert!(validate_gateway_url("wss://example.test/socket#fragment").is_err());
        assert!(validate_gateway_url("wss://example.test/socket?token=secret").is_err());
        assert!(
            validate_gateway_url("wss://example.test/socket?namespace=one&namespace=two").is_err()
        );
        assert!(validate_gateway_url("wss://example.test/socket?namespace=chat").is_ok());
    }

    #[test]
    fn namespace_validation_matches_the_server_route_token_contract() {
        assert!(validate_namespace("audit.events").is_ok());
        assert!(validate_namespace("").is_err());
        assert!(validate_namespace("audit:events").is_err());
        assert!(validate_namespace("audit/events").is_err());
        assert!(validate_namespace(&"a".repeat(129)).is_err());
    }

    #[test]
    fn generated_constructor_requires_its_exact_named_field_contract() {
        let valid: syn::DeriveInput = syn::parse_quote! {
            struct AuditGateway {
                #[gateway_client]
                client: TokioWsClient,
                entity_name: String,
            }
        };
        assert_eq!(
            validate_gateway_fields(&valid).unwrap().to_string(),
            "client"
        );

        let missing_entity: syn::DeriveInput = syn::parse_quote! {
            struct AuditGateway {
                #[gateway_client]
                client: TokioWsClient,
            }
        };
        assert!(validate_gateway_fields(&missing_entity).is_err());

        let duplicate_client: syn::DeriveInput = syn::parse_quote! {
            struct AuditGateway {
                #[gateway_client]
                client: TokioWsClient,
                #[gateway_client]
                secondary: TokioWsClient,
                entity_name: String,
            }
        };
        assert!(validate_gateway_fields(&duplicate_client).is_err());
    }
}
