//! Transport contracts shared by the three hosts and their clients.
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SubmitJob {
    /// A non-nil UUID supplied by the caller. Reuse it when retrying a submission.
    pub id: String,
    pub text: String,
}

/// The queue contract is versioned by the handler and publish metadata (v1).
pub type JobRequested = SubmitJob;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobView {
    pub id: String,
    pub text: String,
    pub result: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobTicket {
    pub id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GetJob {
    pub id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NoteInput {
    pub text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NoteView {
    pub id: String,
    pub text: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorBody {
    pub code: String,
    pub message: String,
}
