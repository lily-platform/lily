//! Utility functions for ClickHouse derive macros

use syn::{Fields, Ident, Type};

/// Validate that required fields exist in the struct
pub(crate) fn validate_required_fields(
    fields: &Fields,
    struct_name: &Ident,
    entity_type: &Option<Type>,
) -> Result<(), syn::Error> {
    let mut has_db = false;

    if let Fields::Named(fields_named) = fields {
        for field in &fields_named.named {
            if let Some(ident) = &field.ident
                && ident == "db"
            {
                has_db = true;
            }
        }
    }

    if !has_db {
        return Err(syn::Error::new_spanned(
            struct_name,
            "ClickhouseTable requires a 'db: Arc<DatabaseService>' field with #[inject] attribute",
        ));
    }

    if entity_type.is_none() {
        return Err(syn::Error::new_spanned(
            struct_name,
            "ClickhouseTable requires #[entity_type(YourType)] attribute",
        ));
    }

    Ok(())
}
