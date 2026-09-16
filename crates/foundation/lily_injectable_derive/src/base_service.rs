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
                    lily_trace::prelude::warn!(
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
                    lily_trace::prelude::warn!(
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
                    lily_trace::prelude::warn!(
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
                        lily_trace::prelude::warn!(
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
        use lily_mongo_service::BaseService;
        #[async_trait::async_trait]
        impl BaseService<#dto_type, #entity_type> for #struct_name {
            async fn create(
                &self,
                dto: #dto_type,
            ) -> Result<#dto_type, lily_mongo_service::BaseServiceError> {
                let repository: &std::sync::Arc<#repository_type> = &self.#repository_field;
                let operation = lily_mongo_repository::MongoOperationContext::detached();
                let entity: #entity_type = dto.into();
                let result = lily_mongo_repository::MongoRepository::<#entity_type>::create(
                    repository.as_ref(),
                    entity,
                    &operation,
                ).await.map_err(|source| lily_mongo_service::BaseServiceError::operation_failed(
                    stringify!(#entity_type),
                    lily_mongo_service::BaseServiceOperation::Create,
                    source,
                ))?;
                let result_dto: #dto_type = result.into();
                #notify_created
                Ok(result_dto)
            }

            async fn create_many(
                &self,
                dtos: Vec<#dto_type>,
            ) -> Result<Vec<#dto_type>, lily_mongo_service::BaseServiceError> {
                let repository: &std::sync::Arc<#repository_type> = &self.#repository_field;
                let entities = dtos
                    .into_iter()
                    .map(<#entity_type as From<#dto_type>>::from)
                    .collect::<Vec<_>>();
                let batch = lily_mongo_repository::MongoWriteBatch::new(entities).map_err(|source| {
                    lily_mongo_service::BaseServiceError::operation_failed(
                        stringify!(#entity_type),
                        lily_mongo_service::BaseServiceOperation::CreateMany,
                        source,
                    )
                })?;
                let operation = lily_mongo_repository::MongoOperationContext::detached();
                let results = lily_mongo_repository::MongoRepository::<#entity_type>::create_many(
                    repository.as_ref(),
                    batch,
                    &operation,
                ).await.map_err(|source| lily_mongo_service::BaseServiceError::operation_failed(
                    stringify!(#entity_type),
                    lily_mongo_service::BaseServiceOperation::CreateMany,
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
            ) -> Result<#dto_type, lily_mongo_service::BaseServiceError> {
                let repository: &std::sync::Arc<#repository_type> = &self.#repository_field;
                let operation = lily_mongo_repository::MongoOperationContext::detached();
                let entity: #entity_type = dto.into();
                let result = lily_mongo_repository::MongoRepository::<#entity_type>::update(
                    repository.as_ref(),
                    entity,
                    &operation,
                ).await.map_err(|source| lily_mongo_service::BaseServiceError::operation_failed(
                    stringify!(#entity_type),
                    lily_mongo_service::BaseServiceOperation::Update,
                    source,
                ))?;
                let result_dto: #dto_type = result.into();
                #notify_updated
                Ok(result_dto)
            }

            async fn delete(
                &self,
                dto: #dto_type,
            ) -> Result<bool, lily_mongo_service::BaseServiceError> {
                let repository: &std::sync::Arc<#repository_type> = &self.#repository_field;
                let entity: #entity_type = dto.into();
                let id = lily_mongo_repository::MongoDocumentId::from_entity(&entity).map_err(|source| {
                    lily_mongo_service::BaseServiceError::operation_failed(
                        stringify!(#entity_type),
                        lily_mongo_service::BaseServiceOperation::Delete,
                        source,
                    )
                })?;
                #prepare_deleted_id
                let operation = lily_mongo_repository::MongoOperationContext::detached();
                let deleted = lily_mongo_repository::MongoRepository::<#entity_type>::delete_by_id(
                    repository.as_ref(),
                    id,
                    &operation,
                ).await.map_err(|source| lily_mongo_service::BaseServiceError::operation_failed(
                    stringify!(#entity_type),
                    lily_mongo_service::BaseServiceOperation::Delete,
                    source,
                ))?;
                #notify_deleted
                Ok(deleted)
            }

            async fn delete_many(
                &self,
                dtos: Vec<#dto_type>,
            ) -> Result<u64, lily_mongo_service::BaseServiceError> {
                let repository: &std::sync::Arc<#repository_type> = &self.#repository_field;
                let ids = dtos
                    .into_iter()
                    .map(<#entity_type as From<#dto_type>>::from)
                    .map(|entity| lily_mongo_repository::MongoDocumentId::from_entity(&entity))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|source| lily_mongo_service::BaseServiceError::operation_failed(
                        stringify!(#entity_type),
                        lily_mongo_service::BaseServiceOperation::DeleteMany,
                        source,
                    ))?;
                let ids = lily_mongo_repository::MongoIdBatch::new(ids).map_err(|source| {
                    lily_mongo_service::BaseServiceError::operation_failed(
                        stringify!(#entity_type),
                        lily_mongo_service::BaseServiceOperation::DeleteMany,
                        source,
                    )
                })?;
                let filter = lily_mongo_repository::MongoFilter::by_ids(&ids).map_err(|source| {
                    lily_mongo_service::BaseServiceError::operation_failed(
                        stringify!(#entity_type),
                        lily_mongo_service::BaseServiceOperation::DeleteMany,
                        source,
                    )
                })?;
                let operation = lily_mongo_repository::MongoOperationContext::detached();
                lily_mongo_repository::MongoRepository::<#entity_type>::delete_many(
                    repository.as_ref(),
                    filter,
                    &operation,
                ).await.map_err(|source| lily_mongo_service::BaseServiceError::operation_failed(
                    stringify!(#entity_type),
                    lily_mongo_service::BaseServiceOperation::DeleteMany,
                    source,
                ))
            }

            async fn find_by_id(
                &self,
                id: &str,
            ) -> Result<#dto_type, lily_mongo_service::BaseServiceError> {
                let repository: &std::sync::Arc<#repository_type> = &self.#repository_field;
                let requested_id = id.to_string();
                let id = lily_mongo_repository::MongoDocumentId::parse(id).map_err(|source| {
                    lily_mongo_service::BaseServiceError::operation_failed(
                        stringify!(#entity_type),
                        lily_mongo_service::BaseServiceOperation::FindById,
                        source,
                    )
                })?;
                let operation = lily_mongo_repository::MongoOperationContext::detached();
                let result = lily_mongo_repository::MongoRepository::<#entity_type>::find_by_id(
                    repository.as_ref(),
                    id,
                    &operation,
                ).await.map_err(|source| lily_mongo_service::BaseServiceError::operation_failed(
                    stringify!(#entity_type),
                    lily_mongo_service::BaseServiceOperation::FindById,
                    source,
                ))?;
                result
                    .map(<#dto_type as From<#entity_type>>::from)
                    .ok_or_else(|| lily_mongo_service::BaseServiceError::not_found(
                        stringify!(#entity_type),
                        requested_id,
                    ))
            }

            async fn delete_by_id(
                &self,
                id: &str,
            ) -> Result<bool, lily_mongo_service::BaseServiceError> {
                let repository: &std::sync::Arc<#repository_type> = &self.#repository_field;
                let parsed_id = lily_mongo_repository::MongoDocumentId::parse(id).map_err(|source| {
                    lily_mongo_service::BaseServiceError::operation_failed(
                        stringify!(#entity_type),
                        lily_mongo_service::BaseServiceOperation::DeleteById,
                        source,
                    )
                })?;
                let operation = lily_mongo_repository::MongoOperationContext::detached();
                let deleted = lily_mongo_repository::MongoRepository::<#entity_type>::delete_by_id(
                    repository.as_ref(),
                    parsed_id,
                    &operation,
                ).await.map_err(|source| lily_mongo_service::BaseServiceError::operation_failed(
                    stringify!(#entity_type),
                    lily_mongo_service::BaseServiceOperation::DeleteById,
                    source,
                ))?;
                #prepare_deleted_id_from_str
                #notify_deleted
                Ok(deleted)
            }

            async fn find_by_ids(
                &self,
                ids: Vec<String>,
            ) -> Result<Vec<#dto_type>, lily_mongo_service::BaseServiceError> {
                let repository: &std::sync::Arc<#repository_type> = &self.#repository_field;
                let ids = ids
                    .iter()
                    .map(|id| lily_mongo_repository::MongoDocumentId::parse(id))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|source| lily_mongo_service::BaseServiceError::operation_failed(
                        stringify!(#entity_type),
                        lily_mongo_service::BaseServiceOperation::FindByIds,
                        source,
                    ))?;
                let ids = lily_mongo_repository::MongoIdBatch::new(ids).map_err(|source| {
                    lily_mongo_service::BaseServiceError::operation_failed(
                        stringify!(#entity_type),
                        lily_mongo_service::BaseServiceOperation::FindByIds,
                        source,
                    )
                })?;
                let operation = lily_mongo_repository::MongoOperationContext::detached();
                let results = lily_mongo_repository::MongoRepository::<#entity_type>::find_by_ids(
                    repository.as_ref(),
                    ids,
                    &operation,
                ).await.map_err(|source| lily_mongo_service::BaseServiceError::operation_failed(
                    stringify!(#entity_type),
                    lily_mongo_service::BaseServiceOperation::FindByIds,
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
