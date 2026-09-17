#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

//! Procedural derive for Lily's optional Diesel PostgreSQL repository adapter.
//!
//! Applications normally import the re-exported macro as
//! `lily_postgresql::PgRepository`; a direct dependency on this proc-macro crate
//! is unnecessary.
//!
//! The derive is optional. A domain service may inject
//! `lily_postgresql::PgDatabaseService` and execute ordinary Diesel queries
//! without defining a repository. When used, this derive:
//!
//! - implements the runtime `lily_postgresql::PgRepository` trait;
//! - generates public entity CRUD methods on the repository struct;
//! - delegates connection acquisition, transaction accounting and shutdown
//!   admission to the selected `PgDatabaseService` for ordinary CRUD calls;
//! - generates matching `*_in` methods that use an explicit `PgExecutor`, so
//!   repositories can share an existing transaction without another acquisition.
//!
//! It does not register the repository with Lily DI, implement
//! `lily_injection::ServiceTrait`, infer a table name, or own schema metadata.
//! The entity remains an ordinary Diesel model and `#[pg(entity = Entity)]` is
//! the only supported macro option.
//!
//! # Single mode
//!
//! The repository must contain exactly one `Arc<PgDatabaseService>` field. The
//! field name is irrelevant. A container-managed repository separately derives
//! `Injectable` and `Default`, marks that field with `#[inject]`, declares its
//! service lifetime, and supplies its own `ServiceTrait` implementation.
//!
//! ```ignore
//! #[derive(Default, Injectable, PgRepository)]
//! #[service(lifetime = "Singleton")]
//! #[pg(entity = Order)]
//! struct OrderRepository {
//!     #[inject]
//!     database: Arc<PgDatabaseService>,
//! }
//!
//! impl ServiceTrait for OrderRepository {}
//! ```
//!
//! # Factory mode
//!
//! The repository must contain exactly one `Arc<PgFactory>` and one
//! `OnceLock<Arc<PgDatabaseService>>`. The factory field is injected; the lock
//! is runtime state and must not have `#[inject]`. The application's
//! `ServiceTrait::initialize` implementation chooses a configured name through
//! `PgFactory::get` and sets the lock. This derive deliberately cannot guess
//! which database cell belongs to the repository.
//!
//! ```ignore
//! #[derive(Default, Injectable, PgRepository)]
//! #[service(lifetime = "Singleton")]
//! #[pg(entity = Order)]
//! struct OrderRepository {
//!     #[inject]
//!     factory: Arc<PgFactory>,
//!     database: OnceLock<Arc<PgDatabaseService>>,
//! }
//!
//! #[async_trait]
//! impl ServiceTrait for OrderRepository {
//!     async fn initialize(&mut self) -> Result<(), InjectionError> {
//!         let database = self.factory.get("orders").map_err(|_| {
//!             InjectionError::InitError("orders PostgreSQL cell is unavailable".into())
//!         })?;
//!         self.database.set(database).map_err(|_| {
//!             InjectionError::InitError("orders repository is already initialized".into())
//!         })
//!     }
//! }
//! ```
//!
//! # Generated methods
//!
//! `create`, `create_many`, `find_by_id`, `find_by_ids`, `update`,
//! `delete_by_id`, `count` and `exists` are intentional public application API.
//! ID types and entity capabilities are checked by Diesel at compile time.
//! Batch vectors are not assigned an application limit by the derive; callers
//! must enforce an appropriate boundary.
//!
//! Each method also has an explicit-executor counterpart: `create_in`,
//! `create_many_in`, `find_by_id_in`, `find_by_ids_in`, `update_in`,
//! `delete_by_id_in`, `count_in`, and `exists_in`. Its first argument after
//! `&self` is a `lily_postgresql::PgExecutor`: a mutable Diesel connection,
//! a borrowed `PgTransaction` handle, or a borrowed `PgDatabaseService`.
//! Ordinary methods acquire a connection; they do not implicitly join an open
//! transaction. The `*_in` methods use the supplied executor's database,
//! independently of the repository's configured factory cell.

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    Attribute, Data, DeriveInput, Fields, GenericArgument, PathArguments, Type, parse_macro_input,
};

/// Generates an optional Diesel entity repository adapter.
///
/// The input must be a non-generic named-field struct with
/// `#[pg(entity = EntityType)]`. In `single` mode it contains one
/// `Arc<PgDatabaseService>`; in `factory` mode it contains one `Arc<PgFactory>`
/// and one `OnceLock<Arc<PgDatabaseService>>`. See the crate documentation for
/// DI ownership and factory initialization responsibilities.
#[proc_macro_derive(PgRepository, attributes(pg))]
pub fn derive_pg_repository(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

fn expand(input: DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "PgRepository does not support generic repository structs",
        ));
    }
    let entity = parse_entity(&input.attrs)?;
    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    &input.ident,
                    "PgRepository requires a struct with named fields",
                ));
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                &input.ident,
                "PgRepository can only be derived for structs",
            ));
        }
    };

    let mut direct_databases = Vec::new();
    let mut factories = Vec::new();
    let mut factory_databases = Vec::new();
    for field in fields {
        let Some(name) = &field.ident else {
            continue;
        };
        if is_arc_of(&field.ty, "PgDatabaseService") {
            direct_databases.push(name.clone());
        }
        if is_arc_of(&field.ty, "PgFactory") {
            factories.push(name.clone());
        }
        if is_once_lock_arc_of(&field.ty, "PgDatabaseService") {
            factory_databases.push(name.clone());
        }
    }

    let database_expression = match (
        direct_databases.as_slice(),
        factories.as_slice(),
        factory_databases.as_slice(),
    ) {
        ([database], [], []) => quote! {
            Ok(::std::sync::Arc::clone(&self.#database))
        },
        ([], [_factory], [database]) => quote! {
            self.#database
                .get()
                .cloned()
                .ok_or(::lily_postgresql::PgError::RepositoryDatabaseNotSet)
        },
        _ => {
            return Err(syn::Error::new_spanned(
                &input.ident,
                "PgRepository requires exactly one `Arc<PgDatabaseService>` field (single mode), or exactly one `Arc<PgFactory>` plus one `OnceLock<Arc<PgDatabaseService>>` field (factory mode)",
            ));
        }
    };

    let repository = &input.ident;
    let find_bounds = find_id_bounds(&entity);
    let pooled_methods = pooled_methods(&entity);

    Ok(quote! {
        impl ::lily_postgresql::PgRepository for #repository {
            type Entity = #entity;

            fn database_service(
                &self,
            ) -> ::lily_postgresql::PgResult<::std::sync::Arc<::lily_postgresql::PgDatabaseService>> {
                #database_expression
            }
        }

        impl #repository {
            #pooled_methods

            /// Inserts one ordinary Diesel entity and returns its persisted
            /// PostgreSQL representation via `RETURNING`.
            ///
            /// Uses the supplied executor, independently of this repository's
            /// configured database. An existing connection or transaction handle
            /// reuses its connection without acquiring from the pool or finalizing
            /// the transaction. A plain connection does not start a transaction.
            #[::lily_postgresql::lily_trace::lily_trace(
                name = "postgresql.repository.create",
                crate_path = "::lily_postgresql::lily_trace"
            )]
            pub async fn create_in(
                &self,
                executor: impl ::lily_postgresql::PgExecutor,
                entity: #entity,
            ) -> ::lily_postgresql::PgResult<#entity> {
                ::lily_postgresql::PgExecutor::with_connection(
                    executor,
                    move |connection| {
                        ::std::boxed::Box::pin(async move {
                            use ::lily_postgresql::diesel::associations::HasTable as _;
                            use ::lily_postgresql::diesel::prelude::SelectableHelper as _;
                            let query = ::lily_postgresql::diesel::insert_into(<#entity as ::lily_postgresql::diesel::associations::HasTable>::table())
                                .values(&entity)
                                .returning(<#entity>::as_returning());
                            ::lily_postgresql::diesel_async::RunQueryDsl::get_result(
                                query,
                                connection,
                            )
                                .await
                                .map_err(::lily_postgresql::PgError::from)
                        })
                    },
                )
                .await
            }

            /// Inserts a caller-owned batch and returns persisted rows via
            /// `RETURNING`.
            ///
            /// Empty batches return immediately without acquiring a database
            /// connection. This method does not impose an application batch
            /// limit; callers must bound request-controlled vectors.
            ///
            /// Uses the supplied executor, independently of this repository's
            /// configured database. An existing connection or transaction handle
            /// reuses its connection without acquiring from the pool or finalizing
            /// the transaction. A plain connection does not start a transaction.
            #[::lily_postgresql::lily_trace::lily_trace(
                name = "postgresql.repository.create_many",
                crate_path = "::lily_postgresql::lily_trace"
            )]
            pub async fn create_many_in(
                &self,
                executor: impl ::lily_postgresql::PgExecutor,
                entities: ::std::vec::Vec<#entity>,
            ) -> ::lily_postgresql::PgResult<::std::vec::Vec<#entity>> {
                if entities.is_empty() {
                    return Ok(::std::vec::Vec::new());
                }
                ::lily_postgresql::PgExecutor::with_connection(
                    executor,
                    move |connection| {
                        ::std::boxed::Box::pin(async move {
                            use ::lily_postgresql::diesel::prelude::SelectableHelper as _;
                            let query = ::lily_postgresql::diesel::insert_into(<#entity as ::lily_postgresql::diesel::associations::HasTable>::table())
                                .values(&entities)
                                .returning(<#entity>::as_returning());
                            ::lily_postgresql::diesel_async::RunQueryDsl::get_results(
                                query,
                                connection,
                            )
                                .await
                                .map_err(::lily_postgresql::PgError::from)
                        })
                    },
                )
                .await
            }

            /// Finds one entity by its Diesel primary-key type.
            ///
            /// Missing rows return `Ok(None)`. Supplying an incompatible ID
            /// type fails at compile time through Diesel's `FindDsl` contract.
            ///
            /// Uses the supplied executor, independently of this repository's
            /// configured database. An existing connection or transaction handle
            /// reuses its connection without acquiring from the pool or finalizing
            /// the transaction. A plain connection does not start a transaction.
            #[::lily_postgresql::lily_trace::lily_trace(
                name = "postgresql.repository.find_by_id",
                crate_path = "::lily_postgresql::lily_trace"
            )]
            pub async fn find_by_id_in<Id>(
                &self,
                executor: impl ::lily_postgresql::PgExecutor,
                id: Id,
            ) -> ::lily_postgresql::PgResult<::std::option::Option<#entity>>
            where
                #find_bounds
            {
                ::lily_postgresql::PgExecutor::with_connection(
                    executor,
                    move |connection| {
                        ::std::boxed::Box::pin(async move {
                            <Self as ::lily_postgresql::PgRepositoryId<Id>>::find_on_connection(
                                connection,
                                id,
                            )
                            .await
                        })
                    },
                )
                .await
            }

            /// Updates an identifiable entity and returns its persisted
            /// representation via `RETURNING`.
            ///
            /// If the entity no longer exists, Diesel's not-found result is
            /// returned as `PgError::Query { kind: NotFound }`.
            ///
            /// Uses the supplied executor, independently of this repository's
            /// configured database. An existing connection or transaction handle
            /// reuses its connection without acquiring from the pool or finalizing
            /// the transaction. A plain connection does not start a transaction.
            #[::lily_postgresql::lily_trace::lily_trace(
                name = "postgresql.repository.update",
                crate_path = "::lily_postgresql::lily_trace"
            )]
            pub async fn update_in(
                &self,
                executor: impl ::lily_postgresql::PgExecutor,
                entity: #entity,
            ) -> ::lily_postgresql::PgResult<#entity> {
                ::lily_postgresql::PgExecutor::with_connection(
                    executor,
                    move |connection| {
                        ::std::boxed::Box::pin(async move {
                            use ::lily_postgresql::diesel::prelude::SelectableHelper as _;
                            let query = ::lily_postgresql::diesel::update(&entity)
                                .set(&entity)
                                .returning(<#entity>::as_returning());
                            ::lily_postgresql::diesel_async::RunQueryDsl::get_result(
                                query,
                                connection,
                            )
                                .await
                                .map_err(::lily_postgresql::PgError::from)
                        })
                    },
                )
                .await
            }

            /// Deletes an entity by its Diesel primary-key type.
            ///
            /// Returns `false` when the row was absent or disappeared before
            /// deletion, and `true` when at least one row was deleted.
            ///
            /// Uses the supplied executor, independently of this repository's
            /// configured database. An existing connection or transaction handle
            /// reuses its connection without acquiring from the pool or finalizing
            /// the transaction. A plain connection does not start a transaction.
            #[::lily_postgresql::lily_trace::lily_trace(
                name = "postgresql.repository.delete_by_id",
                crate_path = "::lily_postgresql::lily_trace"
            )]
            pub async fn delete_by_id_in<Id>(
                &self,
                executor: impl ::lily_postgresql::PgExecutor,
                id: Id,
            ) -> ::lily_postgresql::PgResult<bool>
            where
                #find_bounds
            {
                ::lily_postgresql::PgExecutor::with_connection(
                    executor,
                    move |connection| {
                        ::std::boxed::Box::pin(async move {
                            let entity =
                                <Self as ::lily_postgresql::PgRepositoryId<Id>>::find_on_connection(
                                    connection,
                                    id,
                                )
                                .await?;
                            let Some(entity) = entity else {
                                return Ok(false);
                            };
                            let query = ::lily_postgresql::diesel::delete(&entity);
                            ::lily_postgresql::diesel_async::RunQueryDsl::execute(
                                query,
                                connection,
                            )
                                .await
                                .map(|affected| affected > 0)
                                .map_err(::lily_postgresql::PgError::from)
                        })
                    },
                )
                .await
            }

            /// Returns found entities in input-ID order. Duplicate IDs are
            /// preserved and missing IDs are omitted. Sequential Diesel
            /// `find` calls are used so composite primary keys remain supported
            /// without inventing primary-key metadata.
            ///
            /// This method does not impose an application batch limit; callers
            /// must bound request-controlled vectors.
            ///
            /// Uses the supplied executor, independently of this repository's
            /// configured database. An existing connection or transaction handle
            /// reuses its connection without acquiring from the pool or finalizing
            /// the transaction. A plain connection does not start a transaction.
            #[::lily_postgresql::lily_trace::lily_trace(
                name = "postgresql.repository.find_by_ids",
                crate_path = "::lily_postgresql::lily_trace"
            )]
            pub async fn find_by_ids_in<Id>(
                &self,
                executor: impl ::lily_postgresql::PgExecutor,
                ids: ::std::vec::Vec<Id>,
            ) -> ::lily_postgresql::PgResult<::std::vec::Vec<#entity>>
            where
                #find_bounds
            {
                if ids.is_empty() {
                    return Ok(::std::vec::Vec::new());
                }
                ::lily_postgresql::PgExecutor::with_connection(
                    executor,
                    move |connection| {
                        ::std::boxed::Box::pin(async move {
                            let mut entities = ::std::vec::Vec::with_capacity(ids.len());
                            for id in ids {
                                let entity =
                                    <Self as ::lily_postgresql::PgRepositoryId<Id>>::find_on_connection(
                                        connection,
                                        id,
                                    )
                                    .await?;
                                if let Some(entity) = entity {
                                    entities.push(entity);
                                }
                            }
                            Ok(entities)
                        })
                    },
                )
                .await
            }

            /// Counts every row in the entity's Diesel table.
            ///
            /// Uses the supplied executor, independently of this repository's
            /// configured database. An existing connection or transaction handle
            /// reuses its connection without acquiring from the pool or finalizing
            /// the transaction. A plain connection does not start a transaction.
            #[::lily_postgresql::lily_trace::lily_trace(
                name = "postgresql.repository.count",
                crate_path = "::lily_postgresql::lily_trace"
            )]
            pub async fn count_in(
                &self,
                executor: impl ::lily_postgresql::PgExecutor,
            ) -> ::lily_postgresql::PgResult<i64> {
                ::lily_postgresql::PgExecutor::with_connection(
                    executor,
                    move |connection| {
                        ::std::boxed::Box::pin(async move {
                            let query = ::lily_postgresql::diesel::QueryDsl::count(
                                <#entity as ::lily_postgresql::diesel::associations::HasTable>::table(),
                            );
                            ::lily_postgresql::diesel_async::RunQueryDsl::get_result::<i64>(
                                query,
                                connection,
                            )
                            .await
                            .map_err(::lily_postgresql::PgError::from)
                        })
                    },
                )
                .await
            }

            /// Uses the same Diesel `find` contract as `find_by_id`. This keeps
            /// entity-only composite IDs expressible through public Diesel
            /// traits; Diesel does not expose the internal `ValidSubselect`
            /// bound needed for a fully generic `dsl::exists(find(...))`.
            ///
            /// Uses the supplied executor, independently of this repository's
            /// configured database. An existing connection or transaction handle
            /// reuses its connection without acquiring from the pool or finalizing
            /// the transaction. A plain connection does not start a transaction.
            #[::lily_postgresql::lily_trace::lily_trace(
                name = "postgresql.repository.exists",
                crate_path = "::lily_postgresql::lily_trace"
            )]
            pub async fn exists_in<Id>(
                &self,
                executor: impl ::lily_postgresql::PgExecutor,
                id: Id,
            ) -> ::lily_postgresql::PgResult<bool>
            where
                #find_bounds
            {
                ::lily_postgresql::PgExecutor::with_connection(
                    executor,
                    move |connection| {
                        ::std::boxed::Box::pin(async move {
                            <Self as ::lily_postgresql::PgRepositoryId<Id>>::find_on_connection(
                                connection,
                                id,
                            )
                            .await
                            .map(|entity| entity.is_some())
                        })
                    },
                )
                .await
            }
        }
    })
}

/// Keep pooled entry points as thin wrappers around the same SQL operations
/// used by explicit executors. Empty batches retain their no-acquisition path.
fn pooled_methods(entity: &Type) -> proc_macro2::TokenStream {
    let methods = [
        (
            "create",
            quote!(entity: #entity),
            quote!(entity),
            quote!(#entity),
            false,
            false,
        ),
        (
            "create_many",
            quote!(entities: ::std::vec::Vec<#entity>),
            quote!(entities),
            quote!(::std::vec::Vec<#entity>),
            false,
            true,
        ),
        (
            "find_by_id",
            quote!(id: Id),
            quote!(id),
            quote!(::std::option::Option<#entity>),
            true,
            false,
        ),
        (
            "update",
            quote!(entity: #entity),
            quote!(entity),
            quote!(#entity),
            false,
            false,
        ),
        (
            "delete_by_id",
            quote!(id: Id),
            quote!(id),
            quote!(bool),
            true,
            false,
        ),
        (
            "find_by_ids",
            quote!(ids: ::std::vec::Vec<Id>),
            quote!(ids),
            quote!(::std::vec::Vec<#entity>),
            true,
            true,
        ),
        ("count", quote!(), quote!(), quote!(i64), false, false),
        (
            "exists",
            quote!(id: Id),
            quote!(id),
            quote!(bool),
            true,
            false,
        ),
    ];
    let methods = methods.into_iter().map(|(name, parameter, argument, output, by_id, batch)| {
        let method = format_ident!("{name}");
        let explicit_method = format_ident!("{name}_in");
        let documentation = format!(
            "Runs [`Self::{explicit_method}`] on a connection acquired from the repository's configured database. \
             This method does not join an existing transaction; use `{explicit_method}` to supply its executor."
        );
        let generics = by_id.then(|| quote!(<Id>));
        let bounds = by_id.then(|| {
            let bounds = find_id_bounds(entity);
            quote!(where #bounds)
        });
        let empty_batch = batch.then(|| quote! {
            if #argument.is_empty() {
                return Ok(::std::vec::Vec::new());
            }
        });
        quote! {
            #[doc = #documentation]
            pub async fn #method #generics (
                &self,
                #parameter
            ) -> ::lily_postgresql::PgResult<#output>
            #bounds
            {
                #empty_batch
                let database = <Self as ::lily_postgresql::PgRepository>::database_service(self)?;
                self.#explicit_method(database.as_ref(), #argument).await
            }
        }
    });
    quote!(#(#methods)*)
}

fn parse_entity(attributes: &[Attribute]) -> syn::Result<Type> {
    let mut entity = None;
    let mut pg_attribute_count = 0usize;
    for attribute in attributes {
        if !attribute.path().is_ident("pg") {
            continue;
        }
        pg_attribute_count += 1;
        attribute.parse_nested_meta(|meta| {
            if meta.path.is_ident("entity") {
                if entity.is_some() {
                    return Err(meta.error("duplicate `entity` argument"));
                }
                entity = Some(meta.value()?.parse::<Type>()?);
                Ok(())
            } else {
                Err(meta.error("unsupported PgRepository option; expected `entity = Type`"))
            }
        })?;
    }
    if pg_attribute_count == 0 {
        return Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            "missing `#[pg(entity = EntityType)]`",
        ));
    }
    entity.ok_or_else(|| {
        syn::Error::new(
            proc_macro2::Span::call_site(),
            "missing `entity = EntityType` in `#[pg(...)]`",
        )
    })
}

fn is_arc_of(ty: &Type, inner_name: &str) -> bool {
    generic_inner(ty, "Arc").is_some_and(|inner| type_last_ident(inner, inner_name))
}

fn is_once_lock_arc_of(ty: &Type, inner_name: &str) -> bool {
    generic_inner(ty, "OnceLock")
        .and_then(|inner| generic_inner(inner, "Arc"))
        .is_some_and(|inner| type_last_ident(inner, inner_name))
}

fn generic_inner<'a>(ty: &'a Type, outer_name: &str) -> Option<&'a Type> {
    let Type::Path(path) = ty else {
        return None;
    };
    let segment = path.path.segments.last()?;
    if segment.ident != outer_name {
        return None;
    }
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return None;
    };
    if arguments.args.len() != 1 {
        return None;
    }
    match arguments.args.first()? {
        GenericArgument::Type(inner) => Some(inner),
        _ => None,
    }
}

fn type_last_ident(ty: &Type, expected: &str) -> bool {
    let Type::Path(path) = ty else {
        return false;
    };
    path.path
        .segments
        .last()
        .is_some_and(|segment| segment.ident == expected)
}

fn find_id_bounds(_entity: &Type) -> proc_macro2::TokenStream {
    quote! {
        Id: ::std::marker::Send + 'static,
        Self: ::lily_postgresql::PgRepositoryId<Id>
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_single_database_field_by_type_not_by_name() {
        let input: DeriveInput = syn::parse_quote! {
            #[pg(entity = Order)]
            struct Repository {
                any_name: std::sync::Arc<lily_postgresql::PgDatabaseService>,
            }
        };
        assert!(expand(input).is_ok());
    }

    #[test]
    fn accepts_factory_and_once_lock_database_fields_by_type() {
        let input: DeriveInput = syn::parse_quote! {
            #[pg(entity = Order)]
            struct Repository {
                source: std::sync::Arc<lily_postgresql::PgFactory>,
                selected: std::sync::OnceLock<
                    std::sync::Arc<lily_postgresql::PgDatabaseService>
                >,
            }
        };
        assert!(expand(input).is_ok());
    }

    #[test]
    fn rejects_ambiguous_or_wrong_database_fields() {
        let input: DeriveInput = syn::parse_quote! {
            #[pg(entity = Order)]
            struct Repository {
                first: std::sync::Arc<lily_postgresql::PgDatabaseService>,
                second: std::sync::Arc<lily_postgresql::PgDatabaseService>,
            }
        };
        let error = expand(input).unwrap_err().to_string();
        assert!(error.contains("exactly one `Arc<PgDatabaseService>`"));

        let input: DeriveInput = syn::parse_quote! {
            #[pg(entity = Order)]
            struct Repository {
                database: std::sync::Arc<OtherDatabaseService>,
            }
        };
        let error = expand(input).unwrap_err().to_string();
        assert!(error.contains("exactly one `Arc<PgDatabaseService>`"));
    }

    #[test]
    fn rejects_lily_owned_entity_metadata() {
        let input: DeriveInput = syn::parse_quote! {
            #[pg(entity = Order, table = orders::table)]
            struct Repository {
                database: std::sync::Arc<lily_postgresql::PgDatabaseService>,
            }
        };
        let error = expand(input).unwrap_err().to_string();
        assert!(error.contains("expected `entity = Type`"));
    }
}
