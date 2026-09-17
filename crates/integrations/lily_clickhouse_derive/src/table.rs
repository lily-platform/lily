use proc_macro::TokenStream;
use quote::quote;
use syn::DeriveInput;

use crate::utils::validate_required_fields;

pub(crate) fn derive_impl(input: TokenStream) -> TokenStream {
    let ast = syn::parse_macro_input!(input as DeriveInput);
    let runtime = crate::runtime_path::lily_clickhouse();
    let struct_name = &ast.ident;

    let mut entity_type: Option<syn::Type> = None;
    let mut cell_name: Option<syn::LitStr> = None;
    for attr in &ast.attrs {
        if attr.path().is_ident("entity_type") {
            match attr.parse_args::<syn::Type>() {
                Ok(value) => entity_type = Some(value),
                Err(error) => return error.to_compile_error().into(),
            }
        } else if attr.path().is_ident("cell_name") {
            match attr.parse_args::<syn::LitStr>() {
                Ok(value) => cell_name = Some(value),
                Err(error) => return error.to_compile_error().into(),
            }
        }
    }

    if let syn::Data::Struct(data) = &ast.data
        && let Err(error) = validate_required_fields(&data.fields, struct_name, &entity_type)
    {
        return error.to_compile_error().into();
    }
    let entity_type = match entity_type {
        Some(value) => value,
        None => {
            return syn::Error::new_spanned(
                struct_name,
                "ClickhouseTable requires #[entity_type(YourType)] attribute",
            )
            .to_compile_error()
            .into();
        }
    };

    let initialize_database = cell_name.map_or_else(
        || quote! {},
        |cell| {
            quote! {
                let database = self.clickhouse_factory.get(#cell).ok_or_else(|| {
                    #runtime::__private::InjectionError::ServiceNotFound(
                        format!("ClickHouse cell '{}' not found", #cell)
                    )
                })?;
                self.db = database;
            }
        },
    );

    quote! {
        impl #struct_name {
            /// Returns the validated table name generated from the entity schema.
            pub fn table_name() -> &'static str {
                <#entity_type as #runtime::ClickhouseSchemaProvider>::table_name()
            }

            /// Returns the generated allowlist used by structural query arguments.
            pub fn columns() -> &'static [&'static str] {
                <#entity_type as #runtime::ClickhouseSchemaProvider>::columns()
            }

            /// Returns reviewed migration DDL without changing schema during startup.
            pub fn migration_sql(database: &str) -> Result<String, #runtime::ClickhouseError> {
                <#entity_type as #runtime::ClickhouseSchemaProvider>::create_table_sql(database)
            }

            /// Inserts one entity using the caller's bounded operation context.
            pub async fn insert_one(
                &self,
                entity: &#entity_type,
                operation: &#runtime::ClickhouseOperationContext,
            ) -> Result<(), #runtime::ClickhouseError> {
                self.db.insert_one(Self::table_name(), entity, operation).await
            }

            /// Inserts a bounded entity batch using the caller's operation context.
            pub async fn insert_many(
                &self,
                entities: &[#entity_type],
                operation: &#runtime::ClickhouseOperationContext,
            ) -> Result<(), #runtime::ClickhouseError> {
                self.db.insert_many(Self::table_name(), entities, operation).await
            }

            /// Reads one page by equality on a generated allowlisted column.
            pub async fn find_equal<V: #runtime::__private::serde::Serialize>(
                &self,
                column: &str,
                value: V,
                page: #runtime::ClickhousePageRequest,
                operation: &#runtime::ClickhouseOperationContext,
            ) -> Result<Vec<#entity_type>, #runtime::ClickhouseError> {
                self.db.find_equal(
                    Self::table_name(), Self::columns(), column, value, page, operation
                ).await
            }

            /// Executes a validated, bound and bounded select plan.
            pub async fn select(
                &self,
                plan: &#runtime::ClickhouseSelectPlan,
                operation: &#runtime::ClickhouseOperationContext,
            ) -> Result<Vec<#entity_type>, #runtime::ClickhouseError> {
                self.db.select(
                    Self::table_name(), Self::columns(), plan, operation
                ).await
            }

            /// Creates a deadline-bounded context without caller cancellation.
            pub fn bounded_operation(
                &self,
            ) -> Result<#runtime::ClickhouseOperationContext, #runtime::ClickhouseError> {
                self.db.bounded_operation()
            }

            /// Creates a context carrying caller cancellation and the configured deadline.
            pub fn operation_context(
                &self,
                cancellation: #runtime::CancellationToken,
            ) -> Result<#runtime::ClickhouseOperationContext, #runtime::ClickhouseError> {
                self.db.operation_context(cancellation)
            }

            /// Reads at most one entity by equality on an allowlisted column.
            pub async fn find_one_equal<V: #runtime::__private::serde::Serialize>(
                &self,
                column: &str,
                value: V,
                operation: &#runtime::ClickhouseOperationContext,
            ) -> Result<Option<#entity_type>, #runtime::ClickhouseError> {
                self.db.find_one_equal(
                    Self::table_name(), Self::columns(), column, value, operation
                ).await
            }

            /// Counts all rows in this table.
            pub async fn count(
                &self,
                operation: &#runtime::ClickhouseOperationContext,
            ) -> Result<u64, #runtime::ClickhouseError> {
                self.db.count(Self::table_name(), operation).await
            }

            /// Issues an asynchronous delete mutation on an allowlisted column.
            pub async fn delete_equal<V: #runtime::__private::serde::Serialize>(
                &self,
                column: &str,
                value: V,
                operation: &#runtime::ClickhouseOperationContext,
            ) -> Result<(), #runtime::ClickhouseError> {
                self.db.delete_equal(
                    Self::table_name(), Self::columns(), column, value, operation
                ).await
            }
        }

        #[#runtime::__private::async_trait]
        impl #runtime::__private::ServiceTrait for #struct_name {
            async fn initialize(
                &mut self,
            ) -> Result<(), #runtime::__private::InjectionError> {
                #initialize_database
                Ok(())
            }

            async fn dispose(&self) -> Result<(), #runtime::__private::InjectionError> {
                Ok(())
            }
        }
    }
    .into()
}
