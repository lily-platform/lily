use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::ext::IdentExt;
use syn::{
    parse_macro_input, Data, DeriveInput, Field, Fields, GenericArgument, Meta, PathArguments,
    Type, TypePath,
};

use crate::runtime_path;

#[derive(Clone, Copy)]
enum Cardinality {
    Required,
    Optional,
    Repeated,
}

#[derive(Clone, Copy)]
enum FieldKind {
    Text,
    File,
}

struct FieldPlan<'a> {
    identifier: &'a syn::Ident,
    wire_name: String,
    value_type: &'a Type,
    cardinality: Cardinality,
    kind: FieldKind,
    slot: syn::Ident,
}

pub(crate) fn derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand(input) {
        Ok(expansion) => expansion.into(),
        Err(error) => error.into_compile_error().into(),
    }
}

fn expand(input: DeriveInput) -> syn::Result<TokenStream2> {
    let runtime = runtime_path::lily_http_api()?;
    expand_with_runtime(input, runtime)
}

fn expand_with_runtime(input: DeriveInput, runtime: TokenStream2) -> syn::Result<TokenStream2> {
    let dto = &input.ident;
    let openapi_schema_name = multipart_schema_name(&input)?;
    if !input.generics.params.is_empty() || input.generics.where_clause.is_some() {
        return Err(syn::Error::new_spanned(
            input.generics,
            "generic multipart form DTOs are not supported",
        ));
    }

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    dto,
                    "MultipartForm can only be derived for a struct with named fields",
                ));
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                dto,
                "MultipartForm can only be derived for a struct with named fields",
            ));
        }
    };

    let plans = fields
        .iter()
        .enumerate()
        .map(|(index, field)| field_plan(field, index))
        .collect::<syn::Result<Vec<_>>>()?;

    let slot_declarations = plans.iter().map(|plan| {
        let slot = &plan.slot;
        let value_type = plan.value_type;
        match plan.cardinality {
            Cardinality::Required | Cardinality::Optional => {
                quote!(let mut #slot: ::std::option::Option<#value_type> = ::std::option::Option::None;)
            }
            Cardinality::Repeated => {
                quote!(let mut #slot: ::std::vec::Vec<#value_type> = ::std::vec::Vec::new();)
            }
        }
    });

    let field_indices = plans.iter().enumerate().map(|(index, plan)| {
        let wire_name = &plan.wire_name;
        quote!(#wire_name => #index,)
    });

    let field_bindings = plans.iter().enumerate().map(|(index, plan)| {
        let slot = &plan.slot;
        let decode = match plan.kind {
            FieldKind::Text => quote!(#runtime::__private::multipart_text_field(__lily_field)?),
            FieldKind::File => quote!(#runtime::__private::multipart_file_field(__lily_field)),
        };
        match plan.cardinality {
            Cardinality::Required | Cardinality::Optional => quote! {
                #index => {
                    if #slot.is_some() {
                        return ::std::result::Result::Err(
                            #runtime::MultipartFormRejection::DuplicateField
                        );
                    }
                    #slot = ::std::option::Option::Some(#decode);
                }
            },
            Cardinality::Repeated => quote! {
                #index => {
                    #runtime::__private::reserve_multipart_slot(&mut #slot)?;
                    #slot.push(#decode);
                }
            },
        }
    });

    let dto_fields = plans.iter().map(|plan| {
        let identifier = plan.identifier;
        let slot = &plan.slot;
        match plan.cardinality {
            Cardinality::Required => quote! {
                #identifier: #slot.ok_or(#runtime::MultipartFormRejection::MissingField)?
            },
            Cardinality::Optional | Cardinality::Repeated => quote!(#identifier: #slot),
        }
    });

    let openapi_properties = plans.iter().map(|plan| {
        let wire_name = &plan.wire_name;
        let scalar = match plan.kind {
            FieldKind::Text => quote! {
                #runtime::__private::utoipa::openapi::ObjectBuilder::new()
                    .schema_type(#runtime::__private::utoipa::openapi::Type::String)
            },
            FieldKind::File => quote! {
                #runtime::__private::utoipa::openapi::ObjectBuilder::new()
                    .schema_type(#runtime::__private::utoipa::openapi::Type::String)
                    .format(::std::option::Option::Some(
                        #runtime::__private::utoipa::openapi::schema::SchemaFormat::KnownFormat(
                            #runtime::__private::utoipa::openapi::schema::KnownFormat::Binary,
                        ),
                    ))
            },
        };
        let property = if matches!(plan.cardinality, Cardinality::Repeated) {
            quote! {
                #runtime::__private::utoipa::openapi::ArrayBuilder::new().items(#scalar)
            }
        } else {
            scalar
        };
        let required = matches!(plan.cardinality, Cardinality::Required)
            .then(|| quote! { __lily_schema = __lily_schema.required(#wire_name); });
        quote! {
            __lily_schema = __lily_schema.property(#wire_name, #property);
            #required
        }
    });

    Ok(quote! {
        impl #runtime::__private::FromMultipartForm for #dto {
            fn from_multipart_form(
                __lily_fields: ::std::vec::Vec<#runtime::__private::MultipartField>,
            ) -> ::std::result::Result<Self, #runtime::MultipartFormRejection> {
                #(#slot_declarations)*

                for __lily_field in __lily_fields {
                    let __lily_field_index: usize = match __lily_field.name() {
                        #(#field_indices)*
                        _ => {
                            return ::std::result::Result::Err(
                                #runtime::MultipartFormRejection::UnknownField
                            );
                        }
                    };
                    match __lily_field_index {
                        #(#field_bindings)*
                        _ => ::core::unreachable!("generated multipart field index is exhaustive"),
                    }
                }

                ::std::result::Result::Ok(Self {
                    #(#dto_fields,)*
                })
            }
        }

        impl #runtime::__private::MultipartFormOpenApi for #dto {
            fn openapi_schema_name() -> ::std::borrow::Cow<'static, str> {
                ::std::borrow::Cow::Borrowed(#openapi_schema_name)
            }

            fn openapi_schema(
            ) -> #runtime::__private::utoipa::openapi::RefOr<#runtime::__private::utoipa::openapi::schema::Schema> {
                let mut __lily_schema = #runtime::__private::utoipa::openapi::ObjectBuilder::new()
                    .schema_type(#runtime::__private::utoipa::openapi::Type::Object);
                #(#openapi_properties)*
                __lily_schema.build().into()
            }
        }
    })
}

fn multipart_schema_name(input: &DeriveInput) -> syn::Result<String> {
    let mut schema_name = None;
    for attribute in input
        .attrs
        .iter()
        .filter(|attribute| attribute.path().is_ident("schema"))
    {
        if schema_name.is_some() {
            return Err(syn::Error::new_spanned(
                attribute,
                "duplicate #[schema(...)] attribute",
            ));
        }
        let mut parsed_name = None;
        attribute.parse_nested_meta(|meta| {
            if !meta.path.is_ident("as") {
                return Err(meta.error("MultipartForm #[schema(...)] accepts only `as = TypePath`"));
            }
            let path = meta.value()?.parse::<TypePath>()?;
            if parsed_name.is_some() {
                return Err(meta.error("duplicate multipart schema `as` option"));
            }
            parsed_name = Some(
                path.path
                    .segments
                    .iter()
                    .map(|segment| segment.ident.unraw().to_string())
                    .collect::<Vec<_>>()
                    .join("."),
            );
            Ok(())
        })?;
        schema_name = Some(parsed_name.ok_or_else(|| {
            syn::Error::new_spanned(
                attribute,
                "MultipartForm #[schema(...)] requires `as = TypePath`",
            )
        })?);
    }
    Ok(schema_name.unwrap_or_else(|| input.ident.unraw().to_string()))
}

fn field_plan(field: &Field, index: usize) -> syn::Result<FieldPlan<'_>> {
    let identifier = field.ident.as_ref().expect("named fields have identifiers");
    if let Some(attribute) = field
        .attrs
        .iter()
        .find(|attribute| attribute.path().is_ident("schema"))
    {
        return Err(syn::Error::new_spanned(
            attribute,
            "MultipartForm supports #[schema(as = ...)] only on the DTO; field schema is derived from the runtime binding plan",
        ));
    }
    let form_file_attributes = field
        .attrs
        .iter()
        .filter(|attribute| attribute.path().is_ident("form_file"))
        .collect::<Vec<_>>();
    if form_file_attributes.len() > 1 {
        return Err(syn::Error::new_spanned(
            form_file_attributes[1],
            "duplicate #[form_file] attribute",
        ));
    }
    if let Some(attribute) = form_file_attributes.first() {
        if !matches!(attribute.meta, Meta::Path(_)) {
            return Err(syn::Error::new_spanned(
                attribute,
                "#[form_file] does not accept arguments",
            ));
        }
    }

    let (cardinality, value_type) = unwrap_cardinality(&field.ty)?;
    let value_name = terminal_type_name(value_type).ok_or_else(|| {
        syn::Error::new_spanned(
            value_type,
            "multipart fields must use String or FormFile shapes",
        )
    })?;
    let marked_file = !form_file_attributes.is_empty();
    let kind = match (marked_file, value_name.as_str()) {
        (false, "String") => FieldKind::Text,
        (true, "FormFile") => FieldKind::File,
        (false, "FormFile") => {
            return Err(syn::Error::new_spanned(
                field,
                "FormFile fields require #[form_file]",
            ));
        }
        (true, _) => {
            return Err(syn::Error::new_spanned(
                field,
                "#[form_file] fields must use FormFile, Option<FormFile>, or Vec<FormFile>",
            ));
        }
        (false, _) => {
            return Err(syn::Error::new_spanned(
                field,
                "multipart text fields must use String, Option<String>, or Vec<String>",
            ));
        }
    };

    Ok(FieldPlan {
        identifier,
        wire_name: identifier.unraw().to_string(),
        value_type,
        cardinality,
        kind,
        slot: format_ident!("__lily_multipart_field_{index}"),
    })
}

fn unwrap_cardinality(ty: &Type) -> syn::Result<(Cardinality, &Type)> {
    let Some((name, arguments)) = terminal_segment(ty) else {
        return Err(syn::Error::new_spanned(
            ty,
            "multipart fields must use String or FormFile shapes",
        ));
    };
    let cardinality = match name.as_str() {
        "Option" => Cardinality::Optional,
        "Vec" => Cardinality::Repeated,
        _ => return Ok((Cardinality::Required, ty)),
    };
    let PathArguments::AngleBracketed(arguments) = arguments else {
        return Err(syn::Error::new_spanned(
            ty,
            format!("{name} multipart fields require exactly one type argument"),
        ));
    };
    let mut types = arguments.args.iter().filter_map(|argument| match argument {
        GenericArgument::Type(ty) => Some(ty),
        _ => None,
    });
    let value_type = types.next().ok_or_else(|| {
        syn::Error::new_spanned(
            ty,
            format!("{name} multipart fields require exactly one type argument"),
        )
    })?;
    if types.next().is_some() || arguments.args.len() != 1 {
        return Err(syn::Error::new_spanned(
            ty,
            format!("{name} multipart fields require exactly one type argument"),
        ));
    }
    Ok((cardinality, value_type))
}

fn terminal_type_name(ty: &Type) -> Option<String> {
    terminal_segment(ty).map(|(name, _)| name)
}

fn terminal_segment(ty: &Type) -> Option<(String, &PathArguments)> {
    let Type::Path(path) = ty else {
        return None;
    };
    if path.qself.is_some() {
        return None;
    }
    let segment = path.path.segments.last()?;
    Some((segment.ident.to_string(), &segment.arguments))
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use super::expand_with_runtime;

    #[test]
    fn accepts_required_optional_and_repeated_text_and_file_shapes() {
        let input = syn::parse_str(
            "struct Upload { title: String, note: Option<String>, tag: Vec<String>, #[form_file] avatar: FormFile, #[form_file] preview: Option<FormFile>, #[form_file] files: Vec<FormFile> }",
        )
        .unwrap();
        let expansion = expand_with_runtime(input, quote!(::lily_http_api))
            .expect("supported multipart DTO expands");
        let output = expansion.to_string();
        assert!(output.contains("FromMultipartForm"));
        assert!(output.contains("multipart_file_field"));
        assert!(output.contains("multipart_text_field"));
    }

    #[test]
    fn rejects_ambiguous_or_unsupported_field_shapes() {
        for (source, expected) in [
            (
                "struct Upload { file: FormFile }",
                "FormFile fields require #[form_file]",
            ),
            (
                "struct Upload { #[form_file] file: String }",
                "#[form_file] fields must use FormFile",
            ),
            (
                "struct Upload { count: u32 }",
                "multipart text fields must use String",
            ),
            ("struct Upload(String);", "struct with named fields"),
        ] {
            let input = syn::parse_str(source).unwrap();
            let error = expand_with_runtime(input, quote!(::lily_http_api))
                .expect_err("unsupported DTO must be rejected");
            assert!(error.to_string().contains(expected), "{error}");
        }
    }
}
