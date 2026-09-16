use syn::{GenericArgument, PathArguments, Type, TypePath};

/// Error type for type mapping operations
#[derive(Debug)]
pub(crate) enum TypeMapperError {
    /// Unsupported Rust type
    UnsupportedType(String),
    /// Complex type that cannot be automatically mapped
    ComplexType,
    /// Generic type without arguments
    MissingGenericArguments,
}

impl std::fmt::Display for TypeMapperError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TypeMapperError::UnsupportedType(ty) => {
                write!(
                    f,
                    "Unsupported type: {}. Use #[clickhouse(type = \"...\")] to specify ClickHouse type manually.",
                    ty
                )
            }
            TypeMapperError::ComplexType => {
                write!(
                    f,
                    "Complex type cannot be automatically mapped. Use #[clickhouse(type = \"...\")] attribute."
                )
            }
            TypeMapperError::MissingGenericArguments => {
                write!(f, "Generic type is missing type arguments")
            }
        }
    }
}

/// Maps Rust types to ClickHouse types
pub(crate) struct TypeMapper;

impl TypeMapper {
    /// Convert a Rust type to its ClickHouse equivalent
    ///
    /// # Supported Mappings
    ///
    /// ## Primitive Types
    /// - `String`, `str` → `String`
    /// - `u8` → `UInt8`
    /// - `u16` → `UInt16`
    /// - `u32` → `UInt32`
    /// - `u64` → `UInt64`
    /// - `u128` → `UInt128`
    /// - `i8` → `Int8`
    /// - `i16` → `Int16`
    /// - `i32` → `Int32`
    /// - `i64` → `Int64`
    /// - `i128` → `Int128`
    /// - `f32` → `Float32`
    /// - `f64` → `Float64`
    /// - `bool` → `Boolean`
    ///
    /// ## Complex Types
    /// - `Option<T>` → `Nullable(T)`
    /// - `Vec<T>` → `Array(T)`
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let ty: Type = parse_quote!(u64);
    /// assert_eq!(TypeMapper::rust_to_clickhouse(&ty).unwrap(), "UInt64");
    ///
    /// let ty: Type = parse_quote!(Option<String>);
    /// assert_eq!(TypeMapper::rust_to_clickhouse(&ty).unwrap(), "Nullable(String)");
    /// ```
    pub(crate) fn rust_to_clickhouse(ty: &Type) -> Result<String, TypeMapperError> {
        match ty {
            Type::Path(type_path) => Self::map_type_path(type_path),
            Type::Reference(type_ref) => {
                // Handle &str → String
                Self::rust_to_clickhouse(&type_ref.elem)
            }
            _ => Err(TypeMapperError::ComplexType),
        }
    }

    fn map_type_path(type_path: &TypePath) -> Result<String, TypeMapperError> {
        let last_segment = type_path
            .path
            .segments
            .last()
            .ok_or(TypeMapperError::ComplexType)?;

        let type_name = last_segment.ident.to_string();

        match type_name.as_str() {
            // String types
            "String" | "str" => Ok("String".to_string()),

            // Unsigned integers
            "u8" => Ok("UInt8".to_string()),
            "u16" => Ok("UInt16".to_string()),
            "u32" => Ok("UInt32".to_string()),
            "u64" => Ok("UInt64".to_string()),
            "u128" => Ok("UInt128".to_string()),

            // Signed integers
            "i8" => Ok("Int8".to_string()),
            "i16" => Ok("Int16".to_string()),
            "i32" => Ok("Int32".to_string()),
            "i64" => Ok("Int64".to_string()),
            "i128" => Ok("Int128".to_string()),

            // Floating point
            "f32" => Ok("Float32".to_string()),
            "f64" => Ok("Float64".to_string()),

            // Boolean
            "bool" => Ok("Boolean".to_string()),

            // Option<T> → Nullable(T)
            "Option" => {
                let inner_type = Self::extract_generic_type(&last_segment.arguments)?;
                let inner_ch = Self::rust_to_clickhouse(&inner_type)?;
                Ok(format!("Nullable({})", inner_ch))
            }

            // Vec<T> → Array(T)
            "Vec" => {
                let inner_type = Self::extract_generic_type(&last_segment.arguments)?;
                let inner_ch = Self::rust_to_clickhouse(&inner_type)?;
                Ok(format!("Array({})", inner_ch))
            }

            "HashMap" => match &last_segment.arguments {
                PathArguments::AngleBracketed(angle_args) => {
                    let mut args_iter = angle_args.args.iter();

                    let key_ty = match args_iter.next() {
                        Some(GenericArgument::Type(ty)) => ty,
                        _ => return Err(TypeMapperError::ComplexType),
                    };

                    let value_ty = match args_iter.next() {
                        Some(GenericArgument::Type(ty)) => ty,
                        _ => return Err(TypeMapperError::ComplexType),
                    };

                    let key_ch = Self::rust_to_clickhouse(key_ty)?;
                    let value_ch = Self::rust_to_clickhouse(value_ty)?;

                    Ok(format!("Map({}, {})", key_ch, value_ch))
                }
                _ => Err(TypeMapperError::MissingGenericArguments),
            },

            // Unsupported type
            _ => Err(TypeMapperError::UnsupportedType(type_name)),
        }
    }

    fn extract_generic_type(args: &PathArguments) -> Result<Type, TypeMapperError> {
        match args {
            PathArguments::AngleBracketed(angle_args) => {
                let first_arg = angle_args
                    .args
                    .first()
                    .ok_or(TypeMapperError::MissingGenericArguments)?;

                match first_arg {
                    GenericArgument::Type(ty) => Ok(ty.clone()),
                    _ => Err(TypeMapperError::ComplexType),
                }
            }
            _ => Err(TypeMapperError::MissingGenericArguments),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::parse_quote;

    #[test]
    fn test_primitive_types() {
        // String types
        let ty: Type = parse_quote!(String);
        assert_eq!(TypeMapper::rust_to_clickhouse(&ty).unwrap(), "String");

        // Unsigned integers
        let ty: Type = parse_quote!(u8);
        assert_eq!(TypeMapper::rust_to_clickhouse(&ty).unwrap(), "UInt8");

        let ty: Type = parse_quote!(u16);
        assert_eq!(TypeMapper::rust_to_clickhouse(&ty).unwrap(), "UInt16");

        let ty: Type = parse_quote!(u32);
        assert_eq!(TypeMapper::rust_to_clickhouse(&ty).unwrap(), "UInt32");

        let ty: Type = parse_quote!(u64);
        assert_eq!(TypeMapper::rust_to_clickhouse(&ty).unwrap(), "UInt64");

        let ty: Type = parse_quote!(u128);
        assert_eq!(TypeMapper::rust_to_clickhouse(&ty).unwrap(), "UInt128");

        // Signed integers
        let ty: Type = parse_quote!(i8);
        assert_eq!(TypeMapper::rust_to_clickhouse(&ty).unwrap(), "Int8");

        let ty: Type = parse_quote!(i16);
        assert_eq!(TypeMapper::rust_to_clickhouse(&ty).unwrap(), "Int16");

        let ty: Type = parse_quote!(i32);
        assert_eq!(TypeMapper::rust_to_clickhouse(&ty).unwrap(), "Int32");

        let ty: Type = parse_quote!(i64);
        assert_eq!(TypeMapper::rust_to_clickhouse(&ty).unwrap(), "Int64");

        let ty: Type = parse_quote!(i128);
        assert_eq!(TypeMapper::rust_to_clickhouse(&ty).unwrap(), "Int128");

        // Floating point
        let ty: Type = parse_quote!(f32);
        assert_eq!(TypeMapper::rust_to_clickhouse(&ty).unwrap(), "Float32");

        let ty: Type = parse_quote!(f64);
        assert_eq!(TypeMapper::rust_to_clickhouse(&ty).unwrap(), "Float64");

        // Boolean
        let ty: Type = parse_quote!(bool);
        assert_eq!(TypeMapper::rust_to_clickhouse(&ty).unwrap(), "Boolean");
    }

    #[test]
    fn test_option_types() {
        let ty: Type = parse_quote!(Option<String>);
        assert_eq!(
            TypeMapper::rust_to_clickhouse(&ty).unwrap(),
            "Nullable(String)"
        );

        let ty: Type = parse_quote!(Option<u64>);
        assert_eq!(
            TypeMapper::rust_to_clickhouse(&ty).unwrap(),
            "Nullable(UInt64)"
        );

        let ty: Type = parse_quote!(Option<bool>);
        assert_eq!(
            TypeMapper::rust_to_clickhouse(&ty).unwrap(),
            "Nullable(Boolean)"
        );
    }

    #[test]
    fn test_vec_types() {
        let ty: Type = parse_quote!(Vec<String>);
        assert_eq!(
            TypeMapper::rust_to_clickhouse(&ty).unwrap(),
            "Array(String)"
        );

        let ty: Type = parse_quote!(Vec<u32>);
        assert_eq!(
            TypeMapper::rust_to_clickhouse(&ty).unwrap(),
            "Array(UInt32)"
        );

        let ty: Type = parse_quote!(Vec<i64>);
        assert_eq!(TypeMapper::rust_to_clickhouse(&ty).unwrap(), "Array(Int64)");
    }

    #[test]
    fn test_nested_types() {
        // Option<Vec<String>> → Nullable(Array(String))
        let ty: Type = parse_quote!(Option<Vec<String>>);
        assert_eq!(
            TypeMapper::rust_to_clickhouse(&ty).unwrap(),
            "Nullable(Array(String))"
        );

        // Vec<Option<u64>> → Array(Nullable(UInt64))
        let ty: Type = parse_quote!(Vec<Option<u64>>);
        assert_eq!(
            TypeMapper::rust_to_clickhouse(&ty).unwrap(),
            "Array(Nullable(UInt64))"
        );
    }

    #[test]
    fn test_reference_types() {
        // &str → String
        let ty: Type = parse_quote!(&str);
        assert_eq!(TypeMapper::rust_to_clickhouse(&ty).unwrap(), "String");
    }

    #[test]
    fn test_unsupported_types() {
        // Custom type should error
        let ty: Type = parse_quote!(CustomType);
        assert!(matches!(
            TypeMapper::rust_to_clickhouse(&ty),
            Err(TypeMapperError::UnsupportedType(_))
        ));
    }
}
