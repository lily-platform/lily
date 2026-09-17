/// Trait for types that can provide ClickHouse schema information
///
/// This trait is automatically implemented by the `ClickhouseSchema` derive macro.
/// It provides compile-time schema generation from Rust struct definitions.
///
/// # Example
///
/// ```rust,ignore
/// #[derive(ClickhouseSchema)]
/// #[clickhouse(table = "users", order_by = "id", engine = "MergeTree()")]
/// pub struct User {
///     pub id: String,
///     pub email: String,
///     pub age: u32,
/// }
///
/// // Auto-generated implementation:
/// // impl ClickhouseSchemaProvider for User {
/// //     fn schema() -> &'static str { "id String, email String, age UInt32" }
/// //     fn table_name() -> &'static str { "users" }
/// //     fn order_by() -> &'static str { "id" }
/// //     fn engine() -> &'static str { "MergeTree()" }
/// // }
/// ```
pub trait ClickhouseSchemaProvider {
    /// Returns the ClickHouse column definitions
    ///
    /// Format: "column1 Type1, column2 Type2, ..."
    /// Example: "id String, created_at UInt64, email String"
    fn schema() -> &'static str;

    /// Returns the compile-time generated column allowlist used by query APIs.
    fn columns() -> &'static [&'static str];

    /// Returns the table name
    ///
    /// If not specified in attributes, defaults to lowercase struct name
    fn table_name() -> &'static str;

    /// Returns the ORDER BY clause fields
    ///
    /// Format: "field1, field2, ..."
    /// Example: "id, created_at"
    fn order_by() -> &'static str;

    /// Returns the ENGINE clause
    ///
    /// Default: "MergeTree()"
    /// Example: "ReplacingMergeTree()", "SummingMergeTree()"
    fn engine() -> &'static str;

    /// Produces source-controlled migration DDL. It does not execute it.
    fn create_table_sql(database: &str) -> Result<String, crate::ClickhouseError> {
        let database = crate::options::quote_identifier(database, "database")?;
        let table = crate::options::quote_identifier(Self::table_name(), "table")?;
        Ok(format!(
            "CREATE TABLE {database}.{table} ({}) ENGINE = {} ORDER BY ({})",
            Self::schema(),
            Self::engine(),
            Self::order_by()
        ))
    }
}
