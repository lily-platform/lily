use lily::{
    injection::InjectionError,
    postgresql::PgError,
    trace::{TraceFailure, TraceResultError},
};
use lily_example_models::SubmitJob;

/// Public messages and trace codes are stable; infrastructure details are not exposed.
#[derive(Debug, thiserror::Error)]
pub enum DemoError {
    #[error("A non-nil UUID and 1–2000 bytes of non-blank text are required.")]
    InvalidInput,
    #[error("The requested item was not found.")]
    NotFound,
    #[error("This job identifier is already associated with different text.")]
    Conflict,
    #[error("The service is temporarily unavailable.")]
    Unavailable,
    #[error("The operation was cancelled.")]
    Cancelled,
}

impl DemoError {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidInput => "INVALID_INPUT",
            Self::NotFound => "NOT_FOUND",
            Self::Conflict => "CONFLICT",
            Self::Unavailable => "SERVICE_UNAVAILABLE",
            Self::Cancelled => "CANCELLED",
        }
    }

    pub const fn is_rejected(&self) -> bool {
        matches!(self, Self::InvalidInput | Self::NotFound | Self::Conflict)
    }
}

impl TraceResultError for DemoError {
    fn trace_failure(&self) -> TraceFailure {
        if self.is_rejected() {
            TraceFailure::Rejected { code: self.code() }
        } else {
            TraceFailure::Error { code: self.code() }
        }
    }
}

impl From<PgError> for DemoError {
    fn from(error: PgError) -> Self {
        if matches!(
            error,
            PgError::OperationCancelled | PgError::TransactionCancelled
        ) {
            Self::Cancelled
        } else {
            Self::Unavailable
        }
    }
}

impl From<InjectionError> for DemoError {
    fn from(_: InjectionError) -> Self {
        Self::Unavailable
    }
}

impl From<lily::mongodb::BaseServiceError> for DemoError {
    fn from(error: lily::mongodb::BaseServiceError) -> Self {
        match error {
            lily::mongodb::BaseServiceError::NotFound { .. } => Self::NotFound,
            _ => Self::Unavailable,
        }
    }
}

pub fn validate_text(text: &str) -> Result<(), DemoError> {
    if text.trim().is_empty() || text.len() > 2000 {
        Err(DemoError::InvalidInput)
    } else {
        Ok(())
    }
}

pub fn validate_job(job: &SubmitJob) -> Result<uuid::Uuid, DemoError> {
    validate_text(&job.text)?;
    validate_id(&job.id)
}

pub(crate) fn validate_id(id: &str) -> Result<uuid::Uuid, DemoError> {
    let parsed = uuid::Uuid::parse_str(id).map_err(|_| DemoError::InvalidInput)?;
    // Require canonical spelling so one UUID cannot create multiple database/cache keys.
    if parsed.is_nil() || parsed.to_string() != id {
        return Err(DemoError::InvalidInput);
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn validation_rejects_empty_oversized_nil_and_noncanonical_input() {
        let mut job = SubmitJob {
            id: "12345678-1234-4234-8234-123456789abc".into(),
            text: "hello".into(),
        };
        assert!(validate_job(&job).is_ok());
        for text in ["".into(), " \n".into(), "é".repeat(1001)] {
            job.text = text;
            assert!(matches!(validate_job(&job), Err(DemoError::InvalidInput)));
        }
        job.text = "x".repeat(2000);
        assert!(validate_job(&job).is_ok());
        for id in [
            "bad",
            "00000000-0000-0000-0000-000000000000",
            "12345678-1234-4234-8234-123456789ABC",
        ] {
            job.id = id.into();
            assert!(matches!(validate_job(&job), Err(DemoError::InvalidInput)));
        }
    }
    #[test]
    fn application_rejections_and_infrastructure_failures_keep_distinct_trace_codes() {
        assert!(matches!(
            DemoError::Conflict.trace_failure(),
            TraceFailure::Rejected { code: "CONFLICT" }
        ));
        assert!(matches!(
            DemoError::Unavailable.trace_failure(),
            TraceFailure::Error {
                code: "SERVICE_UNAVAILABLE"
            }
        ));
    }
}
