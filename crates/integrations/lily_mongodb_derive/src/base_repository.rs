use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, Fields};

/// Generates the MongoDB-specific, bounded repository contract.
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
    let collection_type = required_type_attribute(ast, "collection_type", "YourCollectionType")?;
    let collection_type_name = quote!(#collection_type).to_string();
    let collection_field = find_collection_field(ast).ok_or_else(|| {
        syn::Error::new_spanned(
            struct_name,
            format!(
                "Repository requires a field whose name contains `collection` and whose type is Arc<{collection_type_name}>"
            ),
        )
    })?;

    Ok(quote! {
        #[async_trait::async_trait]
        impl lily_mongo_repository::MongoRepository<#entity_type> for #struct_name {
            async fn create(
                &self,
                document: #entity_type,
                operation: &lily_mongo_repository::MongoOperationContext<'_>,
            ) -> Result<#entity_type, lily_mongo_repository::MongoRepositoryError> {
                let result = self.#collection_field
                    .insert_one(document.clone(), operation)
                    .await?;
                let mut persisted = mongodb::bson::to_document(&document)
                    .map_err(lily_mongo_repository::MongoRepositoryError::from)?;
                persisted.insert("_id", result.inserted_id);
                mongodb::bson::from_document(persisted)
                    .map_err(lily_mongo_repository::MongoRepositoryError::from)
            }

            async fn create_many(
                &self,
                documents: lily_mongo_repository::MongoWriteBatch<#entity_type>,
                operation: &lily_mongo_repository::MongoOperationContext<'_>,
            ) -> Result<Vec<#entity_type>, lily_mongo_repository::MongoRepositoryError> {
                let mut returned = documents.as_slice().to_vec();
                let result = self.#collection_field.insert_many(documents, operation).await?;
                for (index, id) in result.inserted_ids {
                    if let Some(document) = returned.get_mut(index) {
                        let mut persisted = mongodb::bson::to_document(&*document)
                            .map_err(lily_mongo_repository::MongoRepositoryError::from)?;
                        persisted.insert("_id", id);
                        *document = mongodb::bson::from_document(persisted)
                            .map_err(lily_mongo_repository::MongoRepositoryError::from)?;
                    }
                }
                Ok(returned)
            }

            async fn update(
                &self,
                document: #entity_type,
                operation: &lily_mongo_repository::MongoOperationContext<'_>,
            ) -> Result<#entity_type, lily_mongo_repository::MongoRepositoryError> {
                let mut serialized = mongodb::bson::to_document(&document)
                    .map_err(lily_mongo_repository::MongoRepositoryError::from)?;
                let id = serialized
                    .get("_id")
                    .filter(|id| !matches!(id, mongodb::bson::Bson::Null))
                    .cloned()
                    .ok_or_else(|| {
                        lily_mongo_repository::MongoRepositoryError::InvalidDocumentId(
                            "update requires a non-null BSON value in `_id`".to_string()
                        )
                    })?;
                let filter = operation.apply_concurrency_filter(
                    mongodb::bson::doc! { "_id": id }
                );
                let document = if let Some(revision) = operation.expected_revision() {
                    let next_revision = revision.checked_add(1).ok_or_else(|| {
                        lily_mongo_repository::MongoRepositoryError::InvalidOperationContext(
                            "expected revision cannot be incremented".to_string()
                        )
                    })?;
                    serialized.insert("_revision", next_revision);
                    mongodb::bson::from_document(serialized)
                        .map_err(lily_mongo_repository::MongoRepositoryError::from)?
                } else {
                    document
                };
                let result = self.#collection_field
                    .replace_one(filter, document.clone(), operation)
                    .await?;
                if result.matched_count == 0 {
                    return if operation.expected_revision().is_some() {
                        Err(lily_mongo_repository::MongoRepositoryError::ConcurrencyConflict)
                    } else {
                        Err(lily_mongo_repository::MongoRepositoryError::DocumentNotFound(
                            "update target does not exist".to_string()
                        ))
                    };
                }
                Ok(document)
            }

            async fn delete_by_id(
                &self,
                id: lily_mongo_repository::MongoDocumentId,
                operation: &lily_mongo_repository::MongoOperationContext<'_>,
            ) -> Result<bool, lily_mongo_repository::MongoRepositoryError> {
                let filter = operation.apply_concurrency_filter(
                    mongodb::bson::doc! { "_id": id.into_object_id() }
                );
                let result = self.#collection_field.delete_one(filter, operation).await?;
                if result.deleted_count == 0 && operation.expected_revision().is_some() {
                    return Err(lily_mongo_repository::MongoRepositoryError::ConcurrencyConflict);
                }
                Ok(result.deleted_count > 0)
            }

            async fn delete_one(
                &self,
                filter: lily_mongo_repository::MongoFilter,
                operation: &lily_mongo_repository::MongoOperationContext<'_>,
            ) -> Result<bool, lily_mongo_repository::MongoRepositoryError> {
                let filter = operation.apply_concurrency_filter(filter.into_document());
                let result = self.#collection_field.delete_one(filter, operation).await?;
                if result.deleted_count == 0 && operation.expected_revision().is_some() {
                    return Err(lily_mongo_repository::MongoRepositoryError::ConcurrencyConflict);
                }
                Ok(result.deleted_count > 0)
            }

            async fn delete_many(
                &self,
                filter: lily_mongo_repository::MongoFilter,
                operation: &lily_mongo_repository::MongoOperationContext<'_>,
            ) -> Result<u64, lily_mongo_repository::MongoRepositoryError> {
                let filter = operation.apply_concurrency_filter(filter.into_document());
                let result = self.#collection_field.delete_many(filter, operation).await?;
                if result.deleted_count == 0 && operation.expected_revision().is_some() {
                    return Err(lily_mongo_repository::MongoRepositoryError::ConcurrencyConflict);
                }
                Ok(result.deleted_count)
            }

            async fn find_by_id(
                &self,
                id: lily_mongo_repository::MongoDocumentId,
                operation: &lily_mongo_repository::MongoOperationContext<'_>,
            ) -> Result<Option<#entity_type>, lily_mongo_repository::MongoRepositoryError> {
                self.#collection_field.find_one(
                    lily_mongo_repository::MongoFilter::new(
                        mongodb::bson::doc! { "_id": id.into_object_id() }
                    )?,
                    operation,
                ).await
            }

            async fn find_by_ids(
                &self,
                ids: lily_mongo_repository::MongoIdBatch,
                operation: &lily_mongo_repository::MongoOperationContext<'_>,
            ) -> Result<Vec<#entity_type>, lily_mongo_repository::MongoRepositoryError> {
                let object_ids = ids
                    .as_slice()
                    .iter()
                    .map(|id| *id.as_object_id())
                    .collect::<Vec<_>>();
                let page = lily_mongo_repository::MongoPageRequest::new(
                    0,
                    u32::try_from(object_ids.len()).map_err(|_| {
                        lily_mongo_repository::MongoRepositoryError::InvalidBatch(
                            "id batch length does not fit the driver page limit".to_string()
                        )
                    })?,
                )?;
                Ok(self.#collection_field.find_page(
                    lily_mongo_repository::MongoFilter::new(
                        mongodb::bson::doc! { "_id": { "$in": object_ids } }
                    )?,
                    page,
                    operation,
                ).await?.items)
            }

            async fn find_one(
                &self,
                filter: lily_mongo_repository::MongoFilter,
                operation: &lily_mongo_repository::MongoOperationContext<'_>,
            ) -> Result<Option<#entity_type>, lily_mongo_repository::MongoRepositoryError> {
                self.#collection_field.find_one(filter, operation).await
            }

            async fn find_page(
                &self,
                filter: lily_mongo_repository::MongoFilter,
                page: lily_mongo_repository::MongoPageRequest,
                operation: &lily_mongo_repository::MongoOperationContext<'_>,
            ) -> Result<lily_mongo_repository::MongoPage<#entity_type>, lily_mongo_repository::MongoRepositoryError> {
                self.#collection_field.find_page(filter, page, operation).await
            }

            async fn count(
                &self,
                filter: lily_mongo_repository::MongoFilter,
                operation: &lily_mongo_repository::MongoOperationContext<'_>,
            ) -> Result<u64, lily_mongo_repository::MongoRepositoryError> {
                self.#collection_field.count_documents(filter, operation).await
            }

            async fn exists(
                &self,
                filter: lily_mongo_repository::MongoFilter,
                operation: &lily_mongo_repository::MongoOperationContext<'_>,
            ) -> Result<bool, lily_mongo_repository::MongoRepositoryError> {
                Ok(self.#collection_field.find_one(filter, operation).await?.is_some())
            }
        }
    })
}

fn required_type_attribute(
    ast: &DeriveInput,
    attribute_name: &str,
    example_type: &str,
) -> syn::Result<syn::Type> {
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
    parsed.ok_or_else(|| {
        syn::Error::new_spanned(
            &ast.ident,
            format!("Repository requires #[{attribute_name}({example_type})] attribute"),
        )
    })
}

fn find_collection_field(ast: &DeriveInput) -> Option<syn::Ident> {
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
        .find(|ident| ident.to_string().contains("collection"))
        .cloned()
}
