use std::collections::HashSet;

use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields};

use crate::type_mapper::TypeMapper;

pub(crate) fn derive_impl(input: TokenStream) -> TokenStream {
    let ast = syn::parse_macro_input!(input as DeriveInput);
    let runtime = crate::runtime_path::lily_clickhouse();
    let struct_name = &ast.ident;
    let mut table_name = struct_name.to_string().to_lowercase();
    let mut order_by = String::new();
    let mut engine = "MergeTree()".to_owned();

    for attr in &ast.attrs {
        if attr.path().is_ident("clickhouse")
            && let Err(error) =
                parse_container_attribute(attr, &mut table_name, &mut order_by, &mut engine)
        {
            return error.to_compile_error().into();
        }
    }
    if !valid_identifier(&table_name, false) {
        return syn::Error::new_spanned(
            struct_name,
            "ClickHouse table name is not a safe identifier",
        )
        .to_compile_error()
        .into();
    }
    if engine != "MergeTree()" {
        return syn::Error::new_spanned(
            struct_name,
            "ClickHouse v1 supports only engine = \"MergeTree()\"; use an explicit reviewed migration for other engines",
        )
        .to_compile_error()
        .into();
    }

    let fields = match &ast.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => {
                return syn::Error::new_spanned(
                    struct_name,
                    "ClickhouseSchema only supports structs with named fields",
                )
                .to_compile_error()
                .into();
            }
        },
        _ => {
            return syn::Error::new_spanned(
                struct_name,
                "ClickhouseSchema can only be derived for structs",
            )
            .to_compile_error()
            .into();
        }
    };

    let mut column_names = Vec::with_capacity(fields.len());
    let mut schema_parts = Vec::with_capacity(fields.len());
    let mut seen = HashSet::with_capacity(fields.len());
    for field in fields {
        let field_ident = field.ident.as_ref().expect("named field");
        let field_name = serde_rename(field).unwrap_or_else(|| field_ident.to_string());
        if !valid_identifier(&field_name, true) || !seen.insert(field_name.clone()) {
            return syn::Error::new_spanned(
                field,
                "ClickHouse column names must be unique safe identifiers (dot-separated nested names are allowed)",
            )
            .to_compile_error()
            .into();
        }
        let clickhouse_type = match field_type_override(field) {
            Ok(Some(value)) => value,
            Ok(None) => match TypeMapper::rust_to_clickhouse(&field.ty) {
                Ok(value) => value,
                Err(error) => {
                    return syn::Error::new_spanned(
                        field,
                        format!("Failed to map field '{field_ident}': {error}"),
                    )
                    .to_compile_error()
                    .into();
                }
            },
            Err(error) => return error.to_compile_error().into(),
        };
        if !valid_type_expression(&clickhouse_type) {
            return syn::Error::new_spanned(
                field,
                "ClickHouse type override contains unsupported tokens or syntax",
            )
            .to_compile_error()
            .into();
        }
        schema_parts.push(format!("`{field_name}` {clickhouse_type}"));
        column_names.push(field_name);
    }

    let allowed = column_names
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    let order_columns = order_by
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    if order_columns.is_empty()
        || order_columns
            .iter()
            .any(|column| !valid_identifier(column, true) || !allowed.contains(column))
    {
        return syn::Error::new_spanned(
            struct_name,
            "ClickHouse order_by must list one or more columns declared by this struct",
        )
        .to_compile_error()
        .into();
    }
    let order_by = order_columns
        .into_iter()
        .map(|column| format!("`{column}`"))
        .collect::<Vec<_>>()
        .join(", ");
    let schema = schema_parts.join(", ");

    quote! {
        impl #runtime::ClickhouseSchemaProvider for #struct_name {
            fn schema() -> &'static str { #schema }
            fn columns() -> &'static [&'static str] { &[#(#column_names),*] }
            fn table_name() -> &'static str { #table_name }
            fn order_by() -> &'static str { #order_by }
            fn engine() -> &'static str { #engine }
        }
    }
    .into()
}

fn parse_container_attribute(
    attr: &syn::Attribute,
    table_name: &mut String,
    order_by: &mut String,
    engine: &mut String,
) -> Result<(), syn::Error> {
    attr.parse_nested_meta(|meta| {
        let value: syn::LitStr = meta.value()?.parse()?;
        if meta.path.is_ident("table") {
            *table_name = value.value();
        } else if meta.path.is_ident("order_by") {
            *order_by = value.value();
        } else if meta.path.is_ident("engine") {
            *engine = value.value();
        } else {
            return Err(meta.error("Unsupported attribute. Use: table, order_by, or engine"));
        }
        Ok(())
    })
}

fn field_type_override(field: &syn::Field) -> Result<Option<String>, syn::Error> {
    let mut result = None;
    for attr in &field.attrs {
        if !attr.path().is_ident("clickhouse") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if !meta.path.is_ident("type") {
                return Err(meta.error("Unsupported field attribute. Use: type"));
            }
            if result.is_some() {
                return Err(meta.error("duplicate ClickHouse type override"));
            }
            let value: syn::LitStr = meta.value()?.parse()?;
            result = Some(value.value());
            Ok(())
        })?;
    }
    Ok(result)
}

fn serde_rename(field: &syn::Field) -> Option<String> {
    let mut result = None;
    for attr in &field.attrs {
        if !attr.path().is_ident("serde") {
            continue;
        }
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename") {
                result = Some(meta.value()?.parse::<syn::LitStr>()?.value());
            }
            Ok(())
        });
    }
    result
}

fn valid_identifier(value: &str, allow_dot: bool) -> bool {
    if value.is_empty() || value.len() > 128 {
        return false;
    }
    value.split('.').all(|segment| {
        !segment.is_empty()
            && (allow_dot || !value.contains('.'))
            && !segment.as_bytes()[0].is_ascii_digit()
            && segment
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    })
}

fn valid_type_expression(value: &str) -> bool {
    const ALLOWED: &[&str] = &[
        "Array",
        "Boolean",
        "DateTime",
        "DateTime64",
        "Float32",
        "Float64",
        "Int8",
        "Int16",
        "Int32",
        "Int64",
        "Int128",
        "LowCardinality",
        "Map",
        "Nullable",
        "String",
        "UInt8",
        "UInt16",
        "UInt32",
        "UInt64",
        "UInt128",
    ];
    if value.is_empty() || value.len() > 256 {
        return false;
    }
    let mut depth = 0_i32;
    let mut token = String::new();
    let flush = |token: &mut String| {
        let valid = token.is_empty()
            || token.bytes().all(|byte| byte.is_ascii_digit())
            || ALLOWED.contains(&token.as_str());
        token.clear();
        valid
    };
    for character in value.chars() {
        if character.is_ascii_alphanumeric() || character == '_' {
            token.push(character);
            continue;
        }
        if !flush(&mut token) {
            return false;
        }
        match character {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth < 0 {
                    return false;
                }
            }
            ',' | ' ' => {}
            _ => return false,
        }
    }
    flush(&mut token) && depth == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_override_grammar_rejects_sql() {
        assert!(valid_type_expression(
            "Array(Map(LowCardinality(String), String))"
        ));
        assert!(valid_type_expression("DateTime64(9)"));
        assert!(!valid_type_expression("String); DROP TABLE users; --"));
        assert!(!valid_type_expression("CustomType(String)"));
    }
}
