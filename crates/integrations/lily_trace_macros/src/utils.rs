use std::collections::HashSet;

use syn::ext::IdentExt;
use syn::parse::{ParseStream, Parser};
use syn::punctuated::Punctuated;
use syn::{Expr, ExprArray, ExprLit, FnArg, Ident, ItemFn, Lit, Meta, Pat, Path, Token};

pub(crate) struct TraceArguments {
    pub(crate) capture_result: bool,
    pub(crate) span_name: Option<String>,
    pub(crate) level: String,
    pub(crate) fields: Vec<String>,
    pub(crate) skip_fields: Vec<String>,
    pub(crate) environments: Vec<String>,
    pub(crate) crate_path: Path,
}

pub(crate) fn parse_trace_arguments(args: &[Meta], input: &ItemFn) -> syn::Result<TraceArguments> {
    let mut capture_result = false;
    let mut span_name = None;
    let mut level = None;
    let mut fields = Vec::new();
    let mut skip_fields = Vec::new();
    let mut environments = Vec::new();
    let mut crate_path = None;
    let mut seen_keys = HashSet::new();

    for arg in args {
        let key = arg
            .path()
            .get_ident()
            .map(Ident::to_string)
            .ok_or_else(|| syn::Error::new_spanned(arg, "trace argument must be an identifier"))?;
        if !matches!(
            key.as_str(),
            "name" | "level" | "fields" | "skip" | "env" | "crate_path" | "result"
        ) {
            return Err(syn::Error::new_spanned(
                arg,
                format!("unknown lily_trace argument `{key}`"),
            ));
        }
        if !seen_keys.insert(key.clone()) {
            return Err(syn::Error::new_spanned(
                arg,
                format!("duplicate lily_trace argument `{key}`"),
            ));
        }

        match (key.as_str(), arg) {
            ("result", Meta::Path(_)) => capture_result = true,
            ("name", Meta::NameValue(value)) => {
                let value = string_literal(&value.value, "name must be a string literal")?;
                if value.trim().is_empty() {
                    return Err(syn::Error::new_spanned(arg, "name cannot be empty"));
                }
                span_name = Some(value);
            }
            ("level", Meta::NameValue(value)) => {
                let value = string_literal(&value.value, "level must be a string literal")?
                    .to_ascii_uppercase();
                if !matches!(
                    value.as_str(),
                    "TRACE" | "DEBUG" | "INFO" | "WARN" | "ERROR"
                ) {
                    return Err(syn::Error::new_spanned(
                        arg,
                        "level must be one of trace, debug, info, warn, or error",
                    ));
                }
                level = Some(value);
            }
            ("fields", Meta::List(list)) => {
                fields = identifier_list(list.tokens.clone(), "fields")?;
            }
            ("skip", Meta::List(list)) => {
                skip_fields = identifier_list(list.tokens.clone(), "skip")?;
            }
            ("env", Meta::NameValue(value)) => {
                environments = environment_values(&value.value)?;
            }
            ("crate_path", Meta::NameValue(value)) => {
                let path = string_literal(
                    &value.value,
                    "crate_path must be a string containing a Rust path",
                )?;
                crate_path = Some(syn::parse_str::<Path>(&path).map_err(|_| {
                    syn::Error::new_spanned(
                        arg,
                        "crate_path must be a valid Rust path such as `::provider::lily_trace`",
                    )
                })?);
            }
            _ => {
                return Err(syn::Error::new_spanned(
                    arg,
                    format!("invalid syntax for lily_trace argument `{key}`"),
                ));
            }
        }
    }

    let function_fields = input
        .sig
        .inputs
        .iter()
        .filter_map(|argument| match argument {
            FnArg::Typed(typed) => match typed.pat.as_ref() {
                Pat::Ident(ident) => Some(ident.ident.to_string()),
                _ => None,
            },
            FnArg::Receiver(_) => Some("self".to_string()),
        })
        .collect::<HashSet<_>>();
    validate_field_names("fields", &fields, &function_fields, input)?;
    validate_field_names("skip", &skip_fields, &function_fields, input)?;

    let skipped = skip_fields.iter().collect::<HashSet<_>>();
    if let Some(field) = fields.iter().find(|field| skipped.contains(field)) {
        return Err(syn::Error::new_spanned(
            input,
            format!("trace field `{field}` cannot be present in both fields and skip"),
        ));
    }

    Ok(TraceArguments {
        capture_result,
        span_name,
        level: level.unwrap_or_else(|| "INFO".to_string()),
        fields,
        skip_fields,
        environments,
        crate_path: crate_path.unwrap_or_else(|| syn::parse_quote!(::lily_trace)),
    })
}

fn identifier_list(tokens: proc_macro2::TokenStream, key: &str) -> syn::Result<Vec<String>> {
    let parser = |input: ParseStream<'_>| {
        Punctuated::<Ident, Token![,]>::parse_terminated_with(input, Ident::parse_any)
    };
    let identifiers = parser.parse2(tokens)?;
    let mut seen = HashSet::new();
    identifiers
        .into_iter()
        .map(|identifier| {
            let value = identifier.to_string();
            if seen.insert(value.clone()) {
                Ok(value)
            } else {
                Err(syn::Error::new(
                    identifier.span(),
                    format!("duplicate `{key}` field `{value}`"),
                ))
            }
        })
        .collect()
}

fn string_literal(expression: &Expr, message: &str) -> syn::Result<String> {
    match expression {
        Expr::Lit(ExprLit {
            lit: Lit::Str(value),
            ..
        }) => Ok(value.value()),
        _ => Err(syn::Error::new_spanned(expression, message)),
    }
}

fn environment_values(expression: &Expr) -> syn::Result<Vec<String>> {
    let values = match expression {
        Expr::Array(ExprArray { elems, .. }) => elems
            .iter()
            .map(|value| string_literal(value, "env array values must be string literals"))
            .collect::<syn::Result<Vec<_>>>()?,
        _ => vec![string_literal(
            expression,
            "env must be a string literal or an array of string literals",
        )?],
    };
    if values.iter().any(|value| value.trim().is_empty()) {
        return Err(syn::Error::new_spanned(
            expression,
            "env values cannot be empty",
        ));
    }
    Ok(values)
}

fn validate_field_names(
    key: &str,
    values: &[String],
    function_fields: &HashSet<String>,
    input: &ItemFn,
) -> syn::Result<()> {
    if let Some(value) = values
        .iter()
        .find(|value| !function_fields.contains(value.as_str()))
    {
        return Err(syn::Error::new_spanned(
            input,
            format!("`{key}` references unknown function argument `{value}`"),
        ));
    }
    Ok(())
}
