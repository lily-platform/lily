use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, Fields};

/// Generates the DTO-facing `BaseService` implementation backed by a
/// MongoDB-specific repository.
pub(crate) fn derive_impl(input: TokenStream) -> TokenStream {
    let ast = syn::parse_macro_input!(input as DeriveInput);
    match expand(&ast) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

fn expand(ast: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let runtime = crate::runtime_path::lily_mongodb();
    let struct_name = &ast.ident;
    let entity_type = required_type_attribute(ast, "entity_type", "YourEntityType")?;
    let dto_type = required_type_attribute(ast, "dto_type", "YourDtoType")?;
    let repository_type = required_type_attribute(ast, "repository_type", "YourRepositoryType")?;
    let repository_field = find_named_field(ast, "repository").ok_or_else(|| {
        syn::Error::new_spanned(
            struct_name,
            format!(
                "CrudService requires a named repository field of type Arc<{}>",
                quote!(#repository_type)
            ),
        )
    })?;

    let gateway_type = optional_type_attribute(ast, "gateway")?;
    let gateway_field = gateway_type
        .as_ref()
        .map(|gateway_type| {
            find_named_field(ast, "gateway").ok_or_else(|| {
                syn::Error::new_spanned(
                    struct_name,
                    format!(
                        "#[gateway({})] requires a named gateway field of type Arc<{}>",
                        quote!(#gateway_type),
                        quote!(#gateway_type)
                    ),
                )
            })
        })
        .transpose()?;

    let notify_created = gateway_field.as_ref().map_or_else(
        || quote! {},
        |field| {
            quote! {
                if let Err(error) = self.#field.notify_created(&result_dto).await {
                    #runtime::lily_trace::prelude::warn!(
                        "failed to send CrudService create notification: {}",
                        error
                    );
                }
            }
        },
    );
    let notify_created_many = gateway_field.as_ref().map_or_else(
        || quote! {},
        |field| {
            quote! {
                if let Err(error) = self.#field.notify_created_many(&result_dtos).await {
                    #runtime::lily_trace::prelude::warn!(
                        "failed to send CrudService create_many notification: {}",
                        error
                    );
                }
            }
        },
    );
    let notify_updated = gateway_field.as_ref().map_or_else(
        || quote! {},
        |field| {
            quote! {
                if let Err(error) = self.#field.notify_updated(&result_dto).await {
                    #runtime::lily_trace::prelude::warn!(
                        "failed to send CrudService update notification: {}",
                        error
                    );
                }
            }
        },
    );
    let notify_deleted = gateway_field.as_ref().map_or_else(
        || quote! {},
        |field| {
            quote! {
                if deleted {
                    if let Err(error) = self.#field.notify_deleted_id(&deleted_id).await {
                        #runtime::lily_trace::prelude::warn!(
                            "failed to send CrudService delete notification: {}",
                            error
                        );
                    }
                }
            }
        },
    );
    let prepare_deleted_id = gateway_field.as_ref().map_or_else(
        || quote! {},
        |_| {
            quote! {
                let deleted_id = id.to_string();
            }
        },
    );
    let prepare_deleted_id_from_str = gateway_field.as_ref().map_or_else(
        || quote! {},
        |_| {
            quote! {
                let deleted_id = id.to_string();
            }
        },
    );

    Ok(quote! {
        // Preserve trait-method lookup without colliding across multiple derives.
        use #runtime::BaseService as _;
        #[#runtime::__private::async_trait]
        impl #runtime::BaseService<#dto_type, #entity_type> for #struct_name {
            async fn create(
                &self,
                dto: #dto_type,
            ) -> Result<#dto_type, #runtime::BaseServiceError> {
                let repository: &std::sync::Arc<#repository_type> = &self.#repository_field;
                let operation = #runtime::MongoOperationContext::detached();
                let entity: #entity_type = dto.into();
                let result = #runtime::MongoRepository::<#entity_type>::create(
                    repository.as_ref(),
                    entity,
                    &operation,
                ).await.map_err(|source| #runtime::BaseServiceError::operation_failed(
                    stringify!(#entity_type),
                    #runtime::BaseServiceOperation::Create,
                    source,
                ))?;
                let result_dto: #dto_type = result.into();
                #notify_created
                Ok(result_dto)
            }

            async fn create_many(
                &self,
                dtos: Vec<#dto_type>,
            ) -> Result<Vec<#dto_type>, #runtime::BaseServiceError> {
                let repository: &std::sync::Arc<#repository_type> = &self.#repository_field;
                let entities = dtos
                    .into_iter()
                    .map(<#entity_type as From<#dto_type>>::from)
                    .collect::<Vec<_>>();
                let batch = #runtime::MongoWriteBatch::new(entities).map_err(|source| {
                    #runtime::BaseServiceError::operation_failed(
                        stringify!(#entity_type),
                        #runtime::BaseServiceOperation::CreateMany,
                        source,
                    )
                })?;
                let operation = #runtime::MongoOperationContext::detached();
                let results = #runtime::MongoRepository::<#entity_type>::create_many(
                    repository.as_ref(),
                    batch,
                    &operation,
                ).await.map_err(|source| #runtime::BaseServiceError::operation_failed(
                    stringify!(#entity_type),
                    #runtime::BaseServiceOperation::CreateMany,
                    source,
                ))?;
                let result_dtos = results
                    .into_iter()
                    .map(<#dto_type as From<#entity_type>>::from)
                    .collect::<Vec<_>>();
                #notify_created_many
                Ok(result_dtos)
            }

            async fn update(
                &self,
                dto: #dto_type,
            ) -> Result<#dto_type, #runtime::BaseServiceError> {
                let repository: &std::sync::Arc<#repository_type> = &self.#repository_field;
                let operation = #runtime::MongoOperationContext::detached();
                let entity: #entity_type = dto.into();
                let result = #runtime::MongoRepository::<#entity_type>::update(
                    repository.as_ref(),
                    entity,
                    &operation,
                ).await.map_err(|source| #runtime::BaseServiceError::operation_failed(
                    stringify!(#entity_type),
                    #runtime::BaseServiceOperation::Update,
                    source,
                ))?;
                let result_dto: #dto_type = result.into();
                #notify_updated
                Ok(result_dto)
            }

            async fn delete(
                &self,
                dto: #dto_type,
            ) -> Result<bool, #runtime::BaseServiceError> {
                let repository: &std::sync::Arc<#repository_type> = &self.#repository_field;
                let entity: #entity_type = dto.into();
                let id = #runtime::MongoDocumentId::from_entity(&entity).map_err(|source| {
                    #runtime::BaseServiceError::operation_failed(
                        stringify!(#entity_type),
                        #runtime::BaseServiceOperation::Delete,
                        source,
                    )
                })?;
                #prepare_deleted_id
                let operation = #runtime::MongoOperationContext::detached();
                let deleted = #runtime::MongoRepository::<#entity_type>::delete_by_id(
                    repository.as_ref(),
                    id,
                    &operation,
                ).await.map_err(|source| #runtime::BaseServiceError::operation_failed(
                    stringify!(#entity_type),
                    #runtime::BaseServiceOperation::Delete,
                    source,
                ))?;
                #notify_deleted
                Ok(deleted)
            }

            async fn delete_many(
                &self,
                dtos: Vec<#dto_type>,
            ) -> Result<u64, #runtime::BaseServiceError> {
                let repository: &std::sync::Arc<#repository_type> = &self.#repository_field;
                let ids = dtos
                    .into_iter()
                    .map(<#entity_type as From<#dto_type>>::from)
                    .map(|entity| #runtime::MongoDocumentId::from_entity(&entity))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|source| #runtime::BaseServiceError::operation_failed(
                        stringify!(#entity_type),
                        #runtime::BaseServiceOperation::DeleteMany,
                        source,
                    ))?;
                let ids = #runtime::MongoIdBatch::new(ids).map_err(|source| {
                    #runtime::BaseServiceError::operation_failed(
                        stringify!(#entity_type),
                        #runtime::BaseServiceOperation::DeleteMany,
                        source,
                    )
                })?;
                let filter = #runtime::MongoFilter::by_ids(&ids).map_err(|source| {
                    #runtime::BaseServiceError::operation_failed(
                        stringify!(#entity_type),
                        #runtime::BaseServiceOperation::DeleteMany,
                        source,
                    )
                })?;
                let operation = #runtime::MongoOperationContext::detached();
                #runtime::MongoRepository::<#entity_type>::delete_many(
                    repository.as_ref(),
                    filter,
                    &operation,
                ).await.map_err(|source| #runtime::BaseServiceError::operation_failed(
                    stringify!(#entity_type),
                    #runtime::BaseServiceOperation::DeleteMany,
                    source,
                ))
            }

            async fn find_by_id(
                &self,
                id: &str,
            ) -> Result<#dto_type, #runtime::BaseServiceError> {
                let repository: &std::sync::Arc<#repository_type> = &self.#repository_field;
                let requested_id = id.to_string();
                let id = #runtime::MongoDocumentId::parse(id).map_err(|source| {
                    #runtime::BaseServiceError::operation_failed(
                        stringify!(#entity_type),
                        #runtime::BaseServiceOperation::FindById,
                        source,
                    )
                })?;
                let operation = #runtime::MongoOperationContext::detached();
                let result = #runtime::MongoRepository::<#entity_type>::find_by_id(
                    repository.as_ref(),
                    id,
                    &operation,
                ).await.map_err(|source| #runtime::BaseServiceError::operation_failed(
                    stringify!(#entity_type),
                    #runtime::BaseServiceOperation::FindById,
                    source,
                ))?;
                result
                    .map(<#dto_type as From<#entity_type>>::from)
                    .ok_or_else(|| #runtime::BaseServiceError::not_found(
                        stringify!(#entity_type),
                        requested_id,
                    ))
            }

            async fn delete_by_id(
                &self,
                id: &str,
            ) -> Result<bool, #runtime::BaseServiceError> {
                let repository: &std::sync::Arc<#repository_type> = &self.#repository_field;
                let parsed_id = #runtime::MongoDocumentId::parse(id).map_err(|source| {
                    #runtime::BaseServiceError::operation_failed(
                        stringify!(#entity_type),
                        #runtime::BaseServiceOperation::DeleteById,
                        source,
                    )
                })?;
                let operation = #runtime::MongoOperationContext::detached();
                let deleted = #runtime::MongoRepository::<#entity_type>::delete_by_id(
                    repository.as_ref(),
                    parsed_id,
                    &operation,
                ).await.map_err(|source| #runtime::BaseServiceError::operation_failed(
                    stringify!(#entity_type),
                    #runtime::BaseServiceOperation::DeleteById,
                    source,
                ))?;
                #prepare_deleted_id_from_str
                #notify_deleted
                Ok(deleted)
            }

            async fn find_by_ids(
                &self,
                ids: Vec<String>,
            ) -> Result<Vec<#dto_type>, #runtime::BaseServiceError> {
                let repository: &std::sync::Arc<#repository_type> = &self.#repository_field;
                let ids = ids
                    .iter()
                    .map(|id| #runtime::MongoDocumentId::parse(id))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|source| #runtime::BaseServiceError::operation_failed(
                        stringify!(#entity_type),
                        #runtime::BaseServiceOperation::FindByIds,
                        source,
                    ))?;
                let ids = #runtime::MongoIdBatch::new(ids).map_err(|source| {
                    #runtime::BaseServiceError::operation_failed(
                        stringify!(#entity_type),
                        #runtime::BaseServiceOperation::FindByIds,
                        source,
                    )
                })?;
                let operation = #runtime::MongoOperationContext::detached();
                let results = #runtime::MongoRepository::<#entity_type>::find_by_ids(
                    repository.as_ref(),
                    ids,
                    &operation,
                ).await.map_err(|source| #runtime::BaseServiceError::operation_failed(
                    stringify!(#entity_type),
                    #runtime::BaseServiceOperation::FindByIds,
                    source,
                ))?;
                Ok(results
                    .into_iter()
                    .map(<#dto_type as From<#entity_type>>::from)
                    .collect())
            }
        }
    })
}

fn required_type_attribute(
    ast: &DeriveInput,
    attribute_name: &str,
    example_type: &str,
) -> syn::Result<syn::Type> {
    optional_type_attribute(ast, attribute_name)?.ok_or_else(|| {
        syn::Error::new_spanned(
            &ast.ident,
            format!("CrudService requires #[{attribute_name}({example_type})] attribute"),
        )
    })
}

fn optional_type_attribute(
    ast: &DeriveInput,
    attribute_name: &str,
) -> syn::Result<Option<syn::Type>> {
    let mut parsed = None;
    for attribute in &ast.attrs {
        if attribute.path().is_ident(attribute_name) {
            if parsed.is_some() {
                return Err(syn::Error::new_spanned(
                    attribute,
                    format!("duplicate #[{attribute_name}(...)] attribute"),
                ));
            }
            parsed = Some(attribute.parse_args::<syn::Type>()?);
        }
    }
    Ok(parsed)
}

fn find_named_field(ast: &DeriveInput, name_fragment: &str) -> Option<syn::Ident> {
    let syn::Data::Struct(data_struct) = &ast.data else {
        return None;
    };
    let Fields::Named(fields) = &data_struct.fields else {
        return None;
    };
    fields
        .named
        .iter()
        .filter_map(|field| field.ident.as_ref())
        .find(|ident| ident.to_string().contains(name_fragment))
        .cloned()
}
