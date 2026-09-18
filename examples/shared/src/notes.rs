use crate::{DemoError, validate_text};
use lily_example_models::{NoteInput, NoteView};
use lilyrs::{
    injection::{Injectable, ServiceTrait},
    mongodb::{
        BaseService, Collection, CrudService, DatabaseService, MongoCollection, Repository,
        bson::oid::ObjectId,
    },
    trace::lily_trace,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Serialize, Deserialize)]
pub struct NoteDocument {
    #[serde(skip_serializing_if = "Option::is_none")]
    _id: Option<ObjectId>,
    text: String,
}

#[derive(Default, Injectable, MongoCollection)]
#[service(lifetime = "Singleton")]
#[collection("example_notes")]
#[collection_type(NoteDocument)]
struct NoteCollection {
    #[inject]
    db: Arc<DatabaseService>,
    collection: Option<Collection<NoteDocument>>,
}

#[derive(Default, Injectable, Repository)]
#[service(lifetime = "Singleton")]
#[entity_type(NoteDocument)]
#[collection_type(NoteCollection)]
struct NoteRepository {
    #[inject]
    collection: Arc<NoteCollection>,
}
impl ServiceTrait for NoteRepository {}

// CrudService, Repository and MongoCollection all come from the MongoDB facade.
// The same type is both the persistence DTO and entity here to keep the example small.
#[derive(Default, Injectable, CrudService)]
#[service(lifetime = "Scoped")]
#[entity_type(NoteDocument)]
#[dto_type(NoteDocument)]
#[repository_type(NoteRepository)]
pub struct NoteService {
    #[inject]
    repository: Arc<NoteRepository>,
}
impl ServiceTrait for NoteService {}

fn view(document: NoteDocument) -> Result<NoteView, DemoError> {
    Ok(NoteView {
        id: document._id.ok_or(DemoError::Unavailable)?.to_hex(),
        text: document.text,
    })
}
fn parse_id(id: &str) -> Result<ObjectId, DemoError> {
    ObjectId::parse_str(id).map_err(|_| DemoError::InvalidInput)
}
impl NoteService {
    #[lily_trace(name = "example.note.create", result)]
    pub async fn add(&self, input: NoteInput) -> Result<NoteView, DemoError> {
        validate_text(&input.text)?;
        view(
            self.create(NoteDocument {
                _id: None,
                text: input.text,
            })
            .await?,
        )
    }
    #[lily_trace(name = "example.note.get", result)]
    pub async fn get(&self, id: &str) -> Result<NoteView, DemoError> {
        parse_id(id)?;
        view(self.find_by_id(id).await?)
    }
    #[lily_trace(name = "example.note.update", result)]
    pub async fn replace(&self, id: &str, input: NoteInput) -> Result<NoteView, DemoError> {
        let id = parse_id(id)?;
        validate_text(&input.text)?;
        view(
            self.update(NoteDocument {
                _id: Some(id),
                text: input.text,
            })
            .await?,
        )
    }
    #[lily_trace(name = "example.note.delete", result)]
    pub async fn remove(&self, id: &str) -> Result<(), DemoError> {
        parse_id(id)?;
        if !self.delete_by_id(id).await? {
            return Err(DemoError::NotFound);
        }
        Ok(())
    }
}
