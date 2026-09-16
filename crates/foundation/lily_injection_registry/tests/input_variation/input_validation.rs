//! Deterministic malformed-input coverage for metadata validation.

use arbitrary::Arbitrary;
use lily_injection_registry::*;
use std::any::TypeId;

#[derive(Arbitrary, Debug)]
struct MetadataInput {
    type_name: String,
    lifetime: u8, // Will be converted to ServiceLifetime
    dependency_count: u8,
}

impl MetadataInput {
    fn to_service_metadata(&self) -> ServiceMetadata {
        let lifetime = match self.lifetime % 3 {
            0 => ServiceLifetime::Singleton,
            1 => ServiceLifetime::Scoped,
            _ => ServiceLifetime::Transient,
        };

        let dependencies = (0..self.dependency_count % 10)
            .map(|_| TypeId::of::<i32>())
            .collect();

        ServiceMetadata {
            type_id: TypeId::of::<String>(),
            type_name: "VariationService",
            trait_type_id: None,
            trait_name: None,
            lifetime,
            factory_fn: |_| {
                Box::pin(async {
                    Ok(Box::new(String::new()) as Box<dyn std::any::Any + Send + Sync>)
                })
            },
            dependencies,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_input_conversion_is_bounded() {
        let metadata_input = MetadataInput {
            type_name: "TestService".to_string(),
            lifetime: 0,
            dependency_count: 2,
        };

        let metadata = metadata_input.to_service_metadata();
        assert_eq!(metadata.type_name, "VariationService");
        assert_eq!(metadata.lifetime, ServiceLifetime::Singleton);
        assert_eq!(metadata.dependencies.len(), 2);
    }
}
