use proc_macro::TokenStream;
use quote::quote;
use syn::punctuated::Punctuated;
use syn::{DeriveInput, Token};

/// Expands the public `MongoCollection` derive.
pub(crate) fn derive_impl(input: TokenStream) -> TokenStream {
    let ast = syn::parse_macro_input!(input as DeriveInput);

    // Extract struct name
    let struct_name = &ast.ident;

    let mut collection_name = struct_name.to_string().to_lowercase();
    let mut collection_seen = false;
    let mut collection_type: Option<syn::Type> = None;
    let mut cell_name: Option<String> = None;
    let mut indexes = Vec::new();
    let mut unique_fields = Vec::new();
    let mut unique_combinations = Vec::new();

    for attr in ast.attrs.iter() {
        if attr.path().is_ident("collection") {
            if collection_seen {
                return syn::Error::new_spanned(attr, "duplicate #[collection(...)] attribute")
                    .to_compile_error()
                    .into();
            }
            collection_seen = true;
            let lit_str = match attr.parse_args::<syn::LitStr>() {
                Ok(value) => value,
                Err(error) => return error.to_compile_error().into(),
            };
            collection_name = lit_str.value();
        } else if attr.path().is_ident("collection_type") {
            if collection_type.is_some() {
                return syn::Error::new_spanned(
                    attr,
                    "duplicate #[collection_type(...)] attribute",
                )
                .to_compile_error()
                .into();
            }
            collection_type = match attr.parse_args::<syn::Type>() {
                Ok(value) => Some(value),
                Err(error) => return error.to_compile_error().into(),
            };
        } else if attr.path().is_ident("cell_name") {
            if cell_name.is_some() {
                return syn::Error::new_spanned(attr, "duplicate #[cell_name(...)] attribute")
                    .to_compile_error()
                    .into();
            }
            let value = match attr.parse_args::<syn::LitStr>() {
                Ok(value) => value.value(),
                Err(error) => return error.to_compile_error().into(),
            };
            if !valid_cell_name(&value) {
                return syn::Error::new_spanned(attr, "MongoDB cell name is invalid")
                    .to_compile_error()
                    .into();
            }
            cell_name = Some(value);
        } else if attr.path().is_ident("unique_field") {
            let value = match attr.parse_args::<syn::LitStr>() {
                Ok(value) => value.value(),
                Err(error) => return error.to_compile_error().into(),
            };
            if !valid_field_path(&value) {
                return syn::Error::new_spanned(attr, "MongoDB unique field path is invalid")
                    .to_compile_error()
                    .into();
            }
            unique_fields.push(value);
        } else if attr.path().is_ident("index") {
            let mut field = None;
            if let Err(error) = attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("field") {
                    if field.is_some() {
                        return Err(meta.error("duplicate index field"));
                    }
                    let value = meta.value()?;
                    let lit_str: syn::LitStr = value.parse()?;
                    field = Some(lit_str.value());
                    return Ok(());
                }
                Err(meta.error("unsupported index option"))
            }) {
                return error.to_compile_error().into();
            }
            let field = match field {
                Some(field) if valid_field_path(&field) => field,
                Some(_) => {
                    return syn::Error::new_spanned(attr, "MongoDB index field path is invalid")
                        .to_compile_error()
                        .into();
                }
                None => {
                    return syn::Error::new_spanned(attr, "index requires field = \"...\"")
                        .to_compile_error()
                        .into();
                }
            };
            indexes.push(field);
        } else if attr.path().is_ident("unique_combination") {
            let fields = match attr
                .parse_args_with(Punctuated::<syn::LitStr, Token![,]>::parse_terminated)
            {
                Ok(fields) => fields
                    .into_iter()
                    .map(|field| field.value())
                    .collect::<Vec<_>>(),
                Err(error) => return error.to_compile_error().into(),
            };
            if fields.len() < 2
                || fields.len() > 8
                || fields.iter().any(|field| !valid_field_path(field))
            {
                return syn::Error::new_spanned(
                    attr,
                    "unique_combination requires 2 to 8 valid string field paths",
                )
                .to_compile_error()
                .into();
            }
            unique_combinations.push(fields);
        }
    }

    if !valid_collection_name(&collection_name) {
        return syn::Error::new_spanned(struct_name, "MongoDB collection name is invalid")
            .to_compile_error()
            .into();
    }
    if has_duplicates(&indexes)
        || has_duplicates(&unique_fields)
        || unique_combinations
            .iter()
            .any(|fields| has_duplicates(fields))
    {
        return syn::Error::new_spanned(struct_name, "MongoDB index metadata contains duplicates")
            .to_compile_error()
            .into();
    }

    // Validate that collection_type is specified (now required)
    let coll_type = match collection_type {
        Some(ref t) => t,
        None => {
            return TokenStream::from(
                syn::Error::new_spanned(
                    struct_name,
                    "MongoCollection requires #[collection_type(YourType)] attribute. Example: #[collection_type(User)]"
                ).to_compile_error()
            );
        }
    };

    if let syn::Data::Struct(data_struct) = &ast.data
        && let Err(error) =
            validate_required_fields(&data_struct.fields, struct_name, &collection_type)
    {
        return error.to_compile_error().into();
    }

    let index_steps = indexes.iter().map(|field| {
        let name = migration_index_name(&collection_name, std::slice::from_ref(field), false);
        quote! {
            {
                let mut keys = mongodb::bson::Document::new();
                keys.insert(#field, 1);
                steps.push(lily_mongodb::MongoMigrationStep::ensure_index(
                    #collection_name, #name, keys, false,
                )?);
            }
        }
    });
    let unique_steps = unique_fields.iter().map(|field| {
        let name = migration_index_name(&collection_name, std::slice::from_ref(field), true);
        quote! {
            {
                let mut keys = mongodb::bson::Document::new();
                keys.insert(#field, 1);
                steps.push(lily_mongodb::MongoMigrationStep::ensure_index(
                    #collection_name, #name, keys, true,
                )?);
            }
        }
    });
    let combination_steps = unique_combinations.iter().map(|fields| {
        let name = migration_index_name(&collection_name, fields, true);
        let inserts = fields
            .iter()
            .map(|field| quote! { keys.insert(#field, 1); });
        quote! {
            {
                let mut keys = mongodb::bson::Document::new();
                #(#inserts)*
                steps.push(lily_mongodb::MongoMigrationStep::ensure_index(
                    #collection_name, #name, keys, true,
                )?);
            }
        }
    });

    // Generate initialization code based on whether cell_name is specified
    let init_db_code = if let Some(ref cell) = cell_name {
        quote! {
            // Factory mode: Get database from MongoFactory
            let db = self.mongo_factory.get(#cell)
                .ok_or_else(|| lily_error::injection::InjectionError::ServiceNotFound(
                    format!("Database cell '{}' not found in MongoFactory", #cell)
                ))?;
            self.db = db;
        }
    } else {
        quote! {
            // Single mode: db is already injected, no action needed
        }
    };

    let expanded = quote! {
        // Required import for generated cursor collection.
        use futures::TryStreamExt as _;

        impl #struct_name {
            /// Returns the validated MongoDB collection name declared by this
            /// adapter.
            pub fn collection_name() -> &'static str {
                #collection_name
            }

            /// Returns explicit migration steps; calling this function does
            /// not execute DDL. The deployment-owned migration runner decides
            /// the version and applies the returned plan.
            pub fn migration_steps() -> Result<Vec<lily_mongodb::MongoMigrationStep>, lily_mongo_repository::MongoRepositoryError> {
                let mut steps = vec![
                    lily_mongodb::MongoMigrationStep::ensure_collection(#collection_name)?
                ];
                #(#index_steps)*
                #(#unique_steps)*
                #(#combination_steps)*
                Ok(steps)
            }

            /// Insert one document using the explicit Mongo operation context.
            #[lily_mongodb::lily_trace::lily_trace(
                name = "mongodb.collection.insert_one",
                crate_path = "::lily_mongodb::lily_trace"
            )]
            pub async fn insert_one(
                &self,
                document: #coll_type,
                operation: &lily_mongo_repository::MongoOperationContext<'_>,
            ) -> Result<mongodb::results::InsertOneResult, lily_mongo_repository::MongoRepositoryError> {
                let collection = self.collection.as_ref()
                    .ok_or_else(|| lily_error::application::mongodb::MongoDbError::InternalError("Collection not initialized. Call initialize() first.".to_string()))?;
                operation.execute(async {
                    if let Some(transaction) = operation.transaction() {
                        let mut session = transaction.lock_session().await;
                        collection.insert_one(document).session(&mut *session).await
                    } else {
                        collection.insert_one(document).await
                    }
                    .map_err(|error| operation.map_driver_error(error))
                }).await
            }

            /// Insert a bounded document batch.
            #[lily_mongodb::lily_trace::lily_trace(
                name = "mongodb.collection.insert_many",
                crate_path = "::lily_mongodb::lily_trace"
            )]
            pub async fn insert_many(
                &self,
                documents: lily_mongo_repository::MongoWriteBatch<#coll_type>,
                operation: &lily_mongo_repository::MongoOperationContext<'_>,
            ) -> Result<mongodb::results::InsertManyResult, lily_mongo_repository::MongoRepositoryError> {
                let collection = self.collection.as_ref()
                    .ok_or_else(|| lily_error::application::mongodb::MongoDbError::InternalError("Collection not initialized. Call initialize() first.".to_string()))?;
                let documents = documents.into_inner();
                operation.execute(async {
                    if let Some(transaction) = operation.transaction() {
                        let mut session = transaction.lock_session().await;
                        collection.insert_many(documents).session(&mut *session).await
                    } else {
                        collection.insert_many(documents).await
                    }
                    .map_err(|error| operation.map_driver_error(error))
                }).await
            }

            /// Find one document using a validated filter.
            #[lily_mongodb::lily_trace::lily_trace(
                name = "mongodb.collection.find_one",
                crate_path = "::lily_mongodb::lily_trace"
            )]
            pub async fn find_one(
                &self,
                filter: lily_mongo_repository::MongoFilter,
                operation: &lily_mongo_repository::MongoOperationContext<'_>,
            ) -> Result<Option<#coll_type>, lily_mongo_repository::MongoRepositoryError> {
                let collection = self.collection.as_ref()
                    .ok_or_else(|| lily_error::application::mongodb::MongoDbError::InternalError("Collection not initialized. Call initialize() first.".to_string()))?;
                let filter = filter.into_document();
                operation.execute(async {
                    if let Some(transaction) = operation.transaction() {
                        let mut session = transaction.lock_session().await;
                        collection.find_one(filter).session(&mut *session).await
                    } else {
                        collection.find_one(filter).await
                    }
                    .map_err(|error| operation.map_driver_error(error))
                }).await
            }

            /// Find a bounded, deterministically ordered page.
            #[lily_mongodb::lily_trace::lily_trace(
                name = "mongodb.collection.find_page",
                crate_path = "::lily_mongodb::lily_trace"
            )]
            pub async fn find_page(
                &self,
                filter: lily_mongo_repository::MongoFilter,
                page: lily_mongo_repository::MongoPageRequest,
                operation: &lily_mongo_repository::MongoOperationContext<'_>,
            ) -> Result<lily_mongo_repository::MongoPage<#coll_type>, lily_mongo_repository::MongoRepositoryError> {
                let collection = self.collection.as_ref()
                    .ok_or_else(|| lily_error::application::mongodb::MongoDbError::InternalError("Collection not initialized. Call initialize() first.".to_string()))?;
                let filter = filter.into_document();
                let options = mongodb::options::FindOptions::builder()
                    .skip(page.offset())
                    .limit(page.driver_limit())
                    .sort(page.sort().clone())
                    .build();
                let results = operation.execute(async {
                    if let Some(transaction) = operation.transaction() {
                        let mut session = transaction.lock_session().await;
                        let mut cursor = collection
                            .find(filter)
                            .with_options(options)
                            .session(&mut *session)
                            .await
                            .map_err(|error| operation.map_driver_error(error))?;
                        cursor
                            .stream(&mut session)
                            .try_collect::<Vec<#coll_type>>()
                            .await
                            .map_err(|error| operation.map_driver_error(error))
                    } else {
                        let cursor = collection.find(filter).with_options(options).await
                            .map_err(|error| operation.map_driver_error(error))?;
                        cursor.try_collect::<Vec<#coll_type>>().await
                            .map_err(|error| operation.map_driver_error(error))
                    }
                }).await?;
                Ok(lily_mongo_repository::MongoPage::from_driver_window(results, &page))
            }

            /// Replace at most one document matching the supplied MongoDB
            /// filter and return the driver's matched/modified counts.
            ///
            /// This lower-level collection API accepts the filter verbatim; an
            /// empty document may match an arbitrary first document.
            #[lily_mongodb::lily_trace::lily_trace(
                name = "mongodb.collection.replace_one",
                crate_path = "::lily_mongodb::lily_trace"
            )]
            pub async fn replace_one(
                &self,
                filter: mongodb::bson::Document,
                document: #coll_type,
                operation: &lily_mongo_repository::MongoOperationContext<'_>,
            ) -> Result<mongodb::results::UpdateResult, lily_mongo_repository::MongoRepositoryError> {
                let collection = self.collection.as_ref()
                    .ok_or_else(|| lily_error::application::mongodb::MongoDbError::InternalError("Collection not initialized. Call initialize() first.".to_string()))?;
                operation.execute(async {
                    if let Some(transaction) = operation.transaction() {
                        let mut session = transaction.lock_session().await;
                        collection.replace_one(filter, document).session(&mut *session).await
                    } else {
                        collection.replace_one(filter, document).await
                    }
                    .map_err(|error| operation.map_driver_error(error))
                }).await
            }

            /// Delete at most one document matching the supplied MongoDB
            /// filter and return the driver's deleted count.
            ///
            /// This lower-level collection API accepts the filter verbatim; an
            /// empty document may match an arbitrary first document.
            #[lily_mongodb::lily_trace::lily_trace(
                name = "mongodb.collection.delete_one",
                crate_path = "::lily_mongodb::lily_trace"
            )]
            pub async fn delete_one(
                &self,
                filter: mongodb::bson::Document,
                operation: &lily_mongo_repository::MongoOperationContext<'_>,
            ) -> Result<mongodb::results::DeleteResult, lily_mongo_repository::MongoRepositoryError> {
                let collection = self.collection.as_ref()
                    .ok_or_else(|| lily_error::application::mongodb::MongoDbError::InternalError("Collection not initialized. Call initialize() first.".to_string()))?;
                operation.execute(async {
                    if let Some(transaction) = operation.transaction() {
                        let mut session = transaction.lock_session().await;
                        collection.delete_one(filter).session(&mut *session).await
                    } else {
                        collection.delete_one(filter).await
                    }
                    .map_err(|error| operation.map_driver_error(error))
                }).await
            }

            /// Delete every document matching the supplied MongoDB filter.
            ///
            /// An empty filter matches the whole collection. Callers using the
            /// collection adapter directly must make that choice deliberately.
            #[lily_mongodb::lily_trace::lily_trace(
                name = "mongodb.collection.delete_many",
                crate_path = "::lily_mongodb::lily_trace"
            )]
            pub async fn delete_many(
                &self,
                filter: mongodb::bson::Document,
                operation: &lily_mongo_repository::MongoOperationContext<'_>,
            ) -> Result<mongodb::results::DeleteResult, lily_mongo_repository::MongoRepositoryError> {
                let collection = self.collection.as_ref()
                    .ok_or_else(|| lily_error::application::mongodb::MongoDbError::InternalError("Collection not initialized. Call initialize() first.".to_string()))?;
                operation.execute(async {
                    if let Some(transaction) = operation.transaction() {
                        let mut session = transaction.lock_session().await;
                        collection.delete_many(filter).session(&mut *session).await
                    } else {
                        collection.delete_many(filter).await
                    }
                    .map_err(|error| operation.map_driver_error(error))
                }).await
            }

            /// Count documents by validated filter.
            #[lily_mongodb::lily_trace::lily_trace(
                name = "mongodb.collection.count",
                crate_path = "::lily_mongodb::lily_trace"
            )]
            pub async fn count_documents(
                &self,
                filter: lily_mongo_repository::MongoFilter,
                operation: &lily_mongo_repository::MongoOperationContext<'_>,
            ) -> Result<u64, lily_mongo_repository::MongoRepositoryError> {
                let collection = self.collection.as_ref()
                    .ok_or_else(|| lily_error::application::mongodb::MongoDbError::InternalError("Collection not initialized. Call initialize() first.".to_string()))?;
                let filter = filter.into_document();
                operation.execute(async {
                    if let Some(transaction) = operation.transaction() {
                        let mut session = transaction.lock_session().await;
                        collection.count_documents(filter).session(&mut *session).await
                    } else {
                        collection.count_documents(filter).await
                    }
                    .map_err(|error| operation.map_driver_error(error))
                }).await
            }
        }

        // The collection is itself a DI service. Repository derives do not
        // generate this lifecycle implementation.
        #[async_trait::async_trait]
        impl lily_injection::ServiceTrait for #struct_name {
            async fn initialize(&mut self) -> Result<(), lily_error::injection::InjectionError> {
                #init_db_code

                let coll_name = Self::collection_name();

                // Startup only acquires a lightweight collection handle. DDL,
                // collection creation and indexes belong to the explicit
                // versioned migration runner and never execute as a DI side
                // effect.
                self.collection = Some(self.db.collection::<#coll_type>(coll_name)
                    .map_err(|error| lily_error::injection::InjectionError::InitError(error.to_string()))?);
                Ok(())
            }

            async fn dispose(&self) -> Result<(), lily_error::injection::InjectionError> {
                // A MongoDB Collection is a lightweight shared handle; the
                // owning client/factory performs actual resource shutdown.
                // Shared disposal deliberately does not require unique `Arc`
                // ownership merely to clear this handle.
                Ok(())
            }
        }
    };

    TokenStream::from(expanded)
}

fn valid_collection_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 120
        && !name.starts_with("system.")
        && !name.contains('$')
        && !name.contains('\0')
}

fn valid_cell_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn valid_field_path(field: &str) -> bool {
    !field.is_empty()
        && !field.starts_with('.')
        && !field.ends_with('.')
        && field.split('.').all(|part| {
            !part.is_empty()
                && !part.starts_with('$')
                && part
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        })
}

fn has_duplicates(values: &[String]) -> bool {
    let mut unique = std::collections::HashSet::with_capacity(values.len());
    values.iter().any(|value| !unique.insert(value))
}

fn migration_index_name(collection: &str, fields: &[String], unique: bool) -> String {
    format!(
        "lily_{}_{}_{}",
        if unique { "uidx" } else { "idx" },
        collection.replace('.', "_"),
        fields.join("_").replace('.', "_")
    )
}

/// Validates the state fields required by generated collection code.
fn validate_required_fields(
    fields: &syn::Fields,
    struct_name: &syn::Ident,
    collection_type: &Option<syn::Type>,
) -> Result<(), syn::Error> {
    let mut has_db_field = false;
    let mut has_collection_field = false;

    if let syn::Fields::Named(fields_named) = fields {
        for field in &fields_named.named {
            if let Some(ident) = &field.ident {
                match ident.to_string().as_str() {
                    "db" => {
                        // Check if it's Arc<dyn DatabaseTrait> or similar
                        has_db_field = true;
                    }
                    "collection" => {
                        // Accept any collection field type - user can use Collection<Document> or Collection<CustomType>
                        has_collection_field = true;
                    }
                    _ => {}
                }
            }
        }
    }

    if !has_db_field {
        return Err(syn::Error::new_spanned(
            struct_name,
            format!(
                "MongoCollection struct '{struct_name}' must have a 'db' field of type 'Arc<DatabaseService>'. \
                Add: #[inject] pub db: Arc<DatabaseService>,"
            ),
        ));
    }

    if !has_collection_field {
        let collection_type_hint = match collection_type {
            Some(coll_type) => {
                // Extract type name for hint
                if let syn::Type::Path(type_path) = coll_type {
                    if let Some(segment) = type_path.path.segments.last() {
                        format!("Option<Collection<{}>>", segment.ident)
                    } else {
                        "Option<Collection<YourType>>".to_string()
                    }
                } else {
                    "Option<Collection<YourType>>".to_string()
                }
            }
            None => "Option<Collection<Document>>".to_string(),
        };

        return Err(syn::Error::new_spanned(
            struct_name,
            format!(
                "MongoCollection struct '{struct_name}' must have a 'collection' field. \
                Add: pub collection: {collection_type_hint},"
            ),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_bounded_collection_cell_and_field_names() {
        assert!(valid_collection_name("orders.audit"));
        assert!(!valid_collection_name("system.users"));
        assert!(!valid_collection_name("orders$raw"));
        assert!(valid_cell_name("orders-eu_1"));
        assert!(!valid_cell_name("orders/eu"));
        assert!(valid_field_path("customer.address.city"));
        assert!(!valid_field_path("$where"));
        assert!(!valid_field_path("customer..city"));
    }

    #[test]
    fn migration_index_names_are_deterministic_and_collision_inputs_are_rejected() {
        let fields = vec!["customer.id".to_string(), "created_at".to_string()];
        assert_eq!(
            migration_index_name("order.events", &fields, true),
            "lily_uidx_order_events_customer_id_created_at"
        );
        assert!(has_duplicates(&[
            "event_id".to_string(),
            "event_id".to_string()
        ]));
    }
}
