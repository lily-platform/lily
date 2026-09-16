use std::{collections::BTreeMap, marker::PhantomData};

use schemars::{JsonSchema, generate::SchemaSettings};
use serde_json::Value;

use crate::{
    AsyncApiBuildError, limits,
    validation::{normalize_json, validate_component_identifier},
};

/// Selects the Serde contract described by a generated schema.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaDirection {
    /// Bytes entering the Lily application (`Deserialize`).
    Inbound,
    /// Bytes emitted by the Lily application (`Serialize`).
    Outbound,
}

/// A monomorphized, allocation-free schema factory stored in accepted metadata.
#[doc(hidden)]
#[derive(Clone, Copy)]
pub struct SchemaFactory {
    generate: fn() -> Result<GeneratedSchemaSet, AsyncApiBuildError>,
    direction: SchemaDirection,
    marker: PhantomData<fn()>,
}

impl std::fmt::Debug for SchemaFactory {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SchemaFactory")
            .field("direction", &self.direction)
            .finish_non_exhaustive()
    }
}

impl SchemaFactory {
    /// Creates a Draft 7 schema factory for an inbound DTO.
    pub fn inbound<T: JsonSchema>() -> Self {
        Self {
            generate: generate_inbound::<T>,
            direction: SchemaDirection::Inbound,
            marker: PhantomData,
        }
    }

    /// Creates a Draft 7 schema factory for an outbound DTO.
    pub fn outbound<T: JsonSchema>() -> Self {
        Self {
            generate: generate_outbound::<T>,
            direction: SchemaDirection::Outbound,
            marker: PhantomData,
        }
    }

    pub(crate) fn generate(self) -> Result<GeneratedSchemaSet, AsyncApiBuildError> {
        (self.generate)()
    }
}

#[derive(Debug)]
pub(crate) struct GeneratedSchemaSet {
    pub(crate) root_name: String,
    pub(crate) root_id: String,
    pub(crate) definitions: BTreeMap<String, Value>,
}

fn generate_inbound<T: JsonSchema>() -> Result<GeneratedSchemaSet, AsyncApiBuildError> {
    generate::<T>(SchemaDirection::Inbound)
}

fn generate_outbound<T: JsonSchema>() -> Result<GeneratedSchemaSet, AsyncApiBuildError> {
    generate::<T>(SchemaDirection::Outbound)
}

fn generate<T: JsonSchema>(
    direction: SchemaDirection,
) -> Result<GeneratedSchemaSet, AsyncApiBuildError> {
    let root_name = T::schema_name().into_owned();
    let root_id = T::schema_id().into_owned();
    validate_component_identifier("schema.name", &root_name)?;
    crate::validation::validate_required("schema.id", &root_id, limits::SCHEMA_ID_BYTES)?;

    let settings = SchemaSettings::draft07().with(|settings| {
        settings.definitions_path = "#/components/schemas/".into();
        settings.contract = match direction {
            SchemaDirection::Inbound => schemars::generate::Contract::Deserialize,
            SchemaDirection::Outbound => schemars::generate::Contract::Serialize,
        };
    });
    let mut generator = settings.into_generator();
    let root = generator.subschema_for::<T>();
    let mut definitions: BTreeMap<String, Value> = generator
        .take_definitions(true)
        .into_iter()
        .map(|(name, schema)| normalize_json(&schema).map(|schema| (name, schema)))
        .collect::<Result<_, _>>()?;

    if definitions.is_empty() || !definitions.contains_key(&root_name) {
        let root_value =
            serde_json::to_value(root).map_err(|error| AsyncApiBuildError::Schema {
                schema: root_name.clone(),
                detail: error.to_string(),
            })?;
        definitions.insert(root_name.clone(), normalize_json(&root_value)?);
    }

    if definitions.len() > limits::SCHEMA_COUNT {
        return Err(AsyncApiBuildError::validation(
            "components.schemas",
            format!(
                "generated {} components; maximum is {}",
                definitions.len(),
                limits::SCHEMA_COUNT
            ),
        ));
    }

    for name in definitions.keys() {
        validate_component_identifier("schema.component", name)?;
    }
    Ok(GeneratedSchemaSet {
        root_name,
        root_id,
        definitions,
    })
}

#[derive(Debug, Default)]
pub(crate) struct SchemaRegistry {
    schemas: BTreeMap<String, Value>,
    root_name_to_id: BTreeMap<String, String>,
    root_id_to_name: BTreeMap<String, String>,
}

impl SchemaRegistry {
    pub(crate) fn register(
        &mut self,
        factory: SchemaFactory,
    ) -> Result<String, AsyncApiBuildError> {
        let generated = factory.generate()?;

        if let Some(existing_id) = self.root_name_to_id.get(&generated.root_name)
            && existing_id != &generated.root_id
        {
            return Err(AsyncApiBuildError::Collision {
                kind: "schema",
                identifier: generated.root_name,
                detail: format!("component name was already bound to schema ID `{existing_id}`"),
            });
        }
        if let Some(existing_name) = self.root_id_to_name.get(&generated.root_id)
            && existing_name != &generated.root_name
        {
            return Err(AsyncApiBuildError::Collision {
                kind: "schema",
                identifier: generated.root_id,
                detail: format!("schema ID was already bound to component name `{existing_name}`"),
            });
        }

        for (name, schema) in &generated.definitions {
            if let Some(existing) = self.schemas.get(name)
                && existing != schema
            {
                return Err(AsyncApiBuildError::Collision {
                    kind: "schema",
                    identifier: name.clone(),
                    detail: "normalized schema content differs from the existing component"
                        .to_owned(),
                });
            }
        }

        let new_component_count = generated
            .definitions
            .keys()
            .filter(|name| !self.schemas.contains_key(*name))
            .count();
        if self.schemas.len() + new_component_count > limits::SCHEMA_COUNT {
            return Err(AsyncApiBuildError::validation(
                "components.schemas",
                format!("maximum is {}", limits::SCHEMA_COUNT),
            ));
        }

        let root_name = generated.root_name.clone();
        self.root_name_to_id
            .insert(generated.root_name.clone(), generated.root_id.clone());
        self.root_id_to_name
            .insert(generated.root_id, generated.root_name);
        for (name, schema) in generated.definitions {
            self.schemas.entry(name).or_insert(schema);
        }
        Ok(root_name)
    }

    /// Registers a schema used by an official binding field whose contract
    /// requires an object schema with an explicit `properties` map.
    pub(crate) fn register_object(
        &mut self,
        factory: SchemaFactory,
        field: &'static str,
    ) -> Result<String, AsyncApiBuildError> {
        let name = self.register(factory)?;
        let schema = self
            .schemas
            .get(&name)
            .ok_or_else(|| AsyncApiBuildError::Schema {
                schema: name.clone(),
                detail: "registered root component is missing".to_owned(),
            })?;
        let object = schema
            .as_object()
            .ok_or_else(|| AsyncApiBuildError::Schema {
                schema: name.clone(),
                detail: format!("`{field}` must be an object schema with a `properties` map"),
            })?;
        let is_object = object.get("type").and_then(Value::as_str) == Some("object");
        let has_properties = object.get("properties").is_some_and(Value::is_object);
        if !is_object || !has_properties {
            return Err(AsyncApiBuildError::Schema {
                schema: name.clone(),
                detail: format!("`{field}` must be an object schema with a `properties` map"),
            });
        }
        Ok(name)
    }

    pub(crate) fn into_schemas(self) -> BTreeMap<String, Value> {
        self.schemas
    }
}

#[cfg(test)]
mod tests {
    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    struct RecursiveNode {
        value: String,
        child: Option<Box<RecursiveNode>>,
    }

    #[test]
    fn recursive_schema_uses_asyncapi_component_references() {
        let generated = SchemaFactory::inbound::<RecursiveNode>()
            .generate()
            .expect("schema");
        let serialized = serde_json::to_string(&generated.definitions).expect("json");
        assert!(serialized.contains("#/components/schemas/RecursiveNode"));
        assert!(!serialized.contains("#/definitions/"));
    }

    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    #[allow(dead_code)]
    struct Directional {
        #[serde(skip_serializing)]
        input_only: String,
        common: String,
    }

    #[test]
    fn serialize_and_deserialize_contracts_do_not_silently_collapse() {
        let mut registry = SchemaRegistry::default();
        registry
            .register(SchemaFactory::inbound::<Directional>())
            .expect("inbound");
        let error = registry
            .register(SchemaFactory::outbound::<Directional>())
            .expect_err("different wire contracts must collide");
        assert!(matches!(error, AsyncApiBuildError::Collision { .. }));
    }

    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    #[schemars(rename = "SharedName")]
    struct CollisionA {
        first: String,
    }

    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    #[schemars(rename = "SharedName")]
    struct CollisionB {
        second: u64,
    }

    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    #[schemars(rename = "Foo")]
    struct LegitimateFoo {
        value: String,
    }

    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    #[schemars(rename = "Foo2")]
    struct LegitimateFoo2 {
        value: u64,
    }

    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    struct LegitimateNumericNames {
        first: LegitimateFoo,
        second: LegitimateFoo2,
    }

    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    #[schemars(rename = "NestedShared")]
    struct NestedSameA {
        value: String,
    }

    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    #[schemars(rename = "NestedShared")]
    struct NestedSameB {
        value: String,
    }

    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    #[schemars(rename = "NestedShared")]
    struct NestedDifferent {
        different: u64,
    }

    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    struct RootWithNestedA {
        nested: NestedSameA,
    }

    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    struct RootWithNestedB {
        nested: NestedSameB,
    }

    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    struct RootWithDifferentNested {
        nested: NestedDifferent,
    }

    #[test]
    fn schema_name_collision_is_rejected_without_suffix_or_last_write_wins() {
        let mut registry = SchemaRegistry::default();
        registry
            .register(SchemaFactory::inbound::<CollisionA>())
            .expect("first schema");
        let error = registry
            .register(SchemaFactory::inbound::<CollisionB>())
            .expect_err("different schema IDs sharing a name must fail");
        assert!(matches!(error, AsyncApiBuildError::Collision { .. }));
        assert!(!registry.schemas.contains_key("SharedName2"));
    }

    #[test]
    fn legitimate_component_names_ending_in_digits_are_not_false_collisions() {
        let mut registry = SchemaRegistry::default();
        registry
            .register(SchemaFactory::inbound::<LegitimateNumericNames>())
            .expect("legitimate numeric schema names");
        assert!(registry.schemas.contains_key("Foo"));
        assert!(registry.schemas.contains_key("Foo2"));
    }

    #[test]
    fn identical_nested_wire_components_are_reused_across_factories() {
        let mut registry = SchemaRegistry::default();
        registry
            .register(SchemaFactory::inbound::<RootWithNestedA>())
            .expect("first root");
        registry
            .register(SchemaFactory::inbound::<RootWithNestedB>())
            .expect("wire-identical nested component");
        assert!(registry.schemas.contains_key("NestedShared"));
    }

    #[test]
    fn different_nested_wire_components_with_the_same_key_fail_closed() {
        let mut registry = SchemaRegistry::default();
        registry
            .register(SchemaFactory::inbound::<RootWithNestedA>())
            .expect("first root");
        let error = registry
            .register(SchemaFactory::inbound::<RootWithDifferentNested>())
            .expect_err("different nested wire schema must collide");
        assert!(matches!(error, AsyncApiBuildError::Collision { .. }));
    }

    #[test]
    fn object_registration_rejects_scalar_schema() {
        let mut registry = SchemaRegistry::default();
        let error = registry
            .register_object(SchemaFactory::inbound::<String>(), "websocket.query")
            .expect_err("scalar binding schemas must fail closed");
        assert!(matches!(error, AsyncApiBuildError::Schema { .. }));
    }

    #[test]
    fn object_registration_accepts_struct_schema() {
        let mut registry = SchemaRegistry::default();
        registry
            .register_object(
                SchemaFactory::inbound::<LegitimateNumericNames>(),
                "websocket.headers",
            )
            .expect("object binding schema");
    }

    #[test]
    fn identical_schema_registration_is_reused() {
        let mut registry = SchemaRegistry::default();
        let first = registry
            .register(SchemaFactory::inbound::<CollisionA>())
            .expect("first");
        for index in 1..limits::SCHEMA_COUNT {
            registry.schemas.insert(
                format!("Existing{index}"),
                serde_json::json!({ "type": "null" }),
            );
        }
        let second = registry
            .register(SchemaFactory::inbound::<CollisionA>())
            .expect("reuse");
        assert_eq!(first, second);
        assert_eq!(registry.schemas.len(), limits::SCHEMA_COUNT);
    }

    #[test]
    fn schema_component_limit_rejects_plus_one() {
        let mut registry = SchemaRegistry::default();
        for index in 0..limits::SCHEMA_COUNT {
            registry.schemas.insert(
                format!("Existing{index}"),
                serde_json::json!({ "type": "null" }),
            );
        }
        assert_eq!(registry.schemas.len(), limits::SCHEMA_COUNT);
        assert!(
            registry
                .register(SchemaFactory::inbound::<CollisionA>())
                .is_err()
        );
    }
}
