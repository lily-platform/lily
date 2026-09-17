use proc_macro::TokenStream;
use quote::quote;
use syn::DeriveInput;

pub(crate) fn derive_impl(input: TokenStream) -> TokenStream {
    let ast = syn::parse_macro_input!(input as DeriveInput);
    let struct_name = &ast.ident;
    let mut table_type: Option<syn::Type> = None;
    let mut entity_type: Option<syn::Type> = None;
    for attr in &ast.attrs {
        if attr.path().is_ident("table_type") {
            match attr.parse_args::<syn::Type>() {
                Ok(value) => table_type = Some(value),
                Err(error) => return error.to_compile_error().into(),
            }
        } else if attr.path().is_ident("entity_type") {
            match attr.parse_args::<syn::Type>() {
                Ok(value) => entity_type = Some(value),
                Err(error) => return error.to_compile_error().into(),
            }
        }
    }
    let table_type = match table_type {
        Some(value) => value,
        None => {
            return syn::Error::new_spanned(
                struct_name,
                "ClickhouseRepository requires #[table_type(YourTableType)] attribute",
            )
            .to_compile_error()
            .into();
        }
    };
    let entity_type = match entity_type {
        Some(value) => value,
        None => {
            return syn::Error::new_spanned(
                struct_name,
                "ClickhouseRepository requires #[entity_type(YourEntityType)] attribute",
            )
            .to_compile_error()
            .into();
        }
    };
    let has_table = match &ast.data {
        syn::Data::Struct(data) => match &data.fields {
            syn::Fields::Named(fields) => fields
                .named
                .iter()
                .any(|field| field.ident.as_ref().is_some_and(|ident| ident == "table")),
            _ => false,
        },
        _ => false,
    };
    if !has_table {
        return syn::Error::new_spanned(
            struct_name,
            "ClickhouseRepository requires a 'table: Arc<TableType>' field with #[inject] attribute",
        )
        .to_compile_error()
        .into();
    }

    quote! {
        impl #struct_name {
            /// Inserts and returns one owned entity.
            pub async fn create(
                &self,
                entity: #entity_type,
                operation: &lily_clickhouse::ClickhouseOperationContext,
            ) -> Result<#entity_type, lily_clickhouse::ClickhouseError> {
                self.table.insert_one(&entity, operation).await?;
                Ok(entity)
            }

            /// Inserts and returns one bounded owned entity batch.
            pub async fn create_many(
                &self,
                entities: Vec<#entity_type>,
                operation: &lily_clickhouse::ClickhouseOperationContext,
            ) -> Result<Vec<#entity_type>, lily_clickhouse::ClickhouseError> {
                self.table.insert_many(&entities, operation).await?;
                Ok(entities)
            }

            /// Reads one page by equality on an allowlisted column.
            pub async fn find_equal<V: serde::Serialize>(
                &self,
                column: &str,
                value: V,
                page: lily_clickhouse::ClickhousePageRequest,
                operation: &lily_clickhouse::ClickhouseOperationContext,
            ) -> Result<Vec<#entity_type>, lily_clickhouse::ClickhouseError> {
                self.table.find_equal(column, value, page, operation).await
            }

            /// Executes a validated, bound and bounded select plan.
            pub async fn select(
                &self,
                plan: &lily_clickhouse::ClickhouseSelectPlan,
                operation: &lily_clickhouse::ClickhouseOperationContext,
            ) -> Result<Vec<#entity_type>, lily_clickhouse::ClickhouseError> {
                self.table.select(plan, operation).await
            }

            /// Creates a deadline-bounded context without caller cancellation.
            pub fn bounded_operation(
                &self,
            ) -> Result<lily_clickhouse::ClickhouseOperationContext, lily_clickhouse::ClickhouseError> {
                self.table.bounded_operation()
            }

            /// Creates a context carrying caller cancellation and the configured deadline.
            pub fn operation_context(
                &self,
                cancellation: lily_clickhouse::CancellationToken,
            ) -> Result<lily_clickhouse::ClickhouseOperationContext, lily_clickhouse::ClickhouseError> {
                self.table.operation_context(cancellation)
            }

            /// Reads at most one entity by equality on an allowlisted column.
            pub async fn find_one_equal<V: serde::Serialize>(
                &self,
                column: &str,
                value: V,
                operation: &lily_clickhouse::ClickhouseOperationContext,
            ) -> Result<Option<#entity_type>, lily_clickhouse::ClickhouseError> {
                self.table.find_one_equal(column, value, operation).await
            }

            /// Counts all rows in the repository's table.
            pub async fn count(
                &self,
                operation: &lily_clickhouse::ClickhouseOperationContext,
            ) -> Result<u64, lily_clickhouse::ClickhouseError> {
                self.table.count(operation).await
            }

            /// Issues an asynchronous delete mutation on an allowlisted column.
            pub async fn delete_equal<V: serde::Serialize>(
                &self,
                column: &str,
                value: V,
                operation: &lily_clickhouse::ClickhouseOperationContext,
            ) -> Result<(), lily_clickhouse::ClickhouseError> {
                self.table.delete_equal(column, value, operation).await
            }

            /// Returns the validated table name generated from the entity schema.
            pub fn table_name(&self) -> &'static str {
                #table_type::table_name()
            }
        }

        #[async_trait::async_trait]
        impl lily_injection::ServiceTrait for #struct_name {
            async fn initialize(
                &mut self,
            ) -> Result<(), lily_error::injection::InjectionError> {
                Ok(())
            }

            async fn dispose(&self) -> Result<(), lily_error::injection::InjectionError> {
                Ok(())
            }
        }
    }
    .into()
}
