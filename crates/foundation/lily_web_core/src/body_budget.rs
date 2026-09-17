//! Protocol-independent body-size budgets shared by buffered and streaming
//! HTTP adapters.

/// Default request/response body budget used outside an effective server
/// configuration.
pub const DEFAULT_BODY_LIMIT_BYTES: usize = 16 * 1024 * 1024;

/// Framework safety ceiling for a configured HTTP body budget.
pub const MAX_BODY_LIMIT_BYTES: usize = 1024 * 1024 * 1024;

/// A validated, immutable byte budget for one HTTP body representation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BodyBudget {
    limit_bytes: usize,
}

impl BodyBudget {
    /// Validates a body limit before it enters request, response, or streaming
    /// state.
    pub const fn new(limit_bytes: usize) -> Result<Self, BodyBudgetError> {
        if limit_bytes == 0 || limit_bytes > MAX_BODY_LIMIT_BYTES {
            return Err(BodyBudgetError::InvalidLimit {
                limit_bytes,
                max_limit_bytes: MAX_BODY_LIMIT_BYTES,
            });
        }
        Ok(Self { limit_bytes })
    }

    #[must_use]
    /// Returns the validated byte limit.
    pub const fn limit_bytes(self) -> usize {
        self.limit_bytes
    }

    /// Checks one complete representation without allocating or retaining it.
    pub const fn ensure_length(self, length: usize) -> Result<(), BodyBudgetError> {
        if length > self.limit_bytes {
            Err(BodyBudgetError::LimitExceeded {
                limit_bytes: self.limit_bytes,
            })
        } else {
            Ok(())
        }
    }

    /// Computes the next retained length before a buffer reserve or copy.
    pub const fn checked_next_length(
        self,
        current: usize,
        additional: usize,
    ) -> Result<usize, BodyBudgetError> {
        let Some(next) = current.checked_add(additional) else {
            return Err(BodyBudgetError::LengthOverflow {
                limit_bytes: self.limit_bytes,
            });
        };
        if next > self.limit_bytes {
            return Err(BodyBudgetError::LimitExceeded {
                limit_bytes: self.limit_bytes,
            });
        }
        Ok(next)
    }
}

impl Default for BodyBudget {
    fn default() -> Self {
        Self {
            limit_bytes: DEFAULT_BODY_LIMIT_BYTES,
        }
    }
}

/// Stable failure produced before a body buffer may grow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum BodyBudgetError {
    /// The configured limit is zero or exceeds the framework ceiling.
    #[error("body limit {limit_bytes} is outside 1..={max_limit_bytes}")]
    InvalidLimit {
        /// The rejected limit.
        limit_bytes: usize,
        /// The largest limit accepted by the framework.
        max_limit_bytes: usize,
    },
    /// Adding another chunk overflowed `usize`.
    #[error("body length arithmetic overflowed the configured {limit_bytes}-byte limit")]
    LengthOverflow {
        /// The active body limit.
        limit_bytes: usize,
    },
    /// The complete representation would exceed the active limit.
    #[error("body exceeds the configured {limit_bytes}-byte limit")]
    LimitExceeded {
        /// The active body limit.
        limit_bytes: usize,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_limits_and_rejects_growth_before_allocation() {
        assert!(BodyBudget::new(0).is_err());
        assert!(BodyBudget::new(MAX_BODY_LIMIT_BYTES + 1).is_err());

        let budget = BodyBudget::new(8).unwrap();
        assert_eq!(budget.checked_next_length(3, 5), Ok(8));
        assert_eq!(
            budget.checked_next_length(8, 1),
            Err(BodyBudgetError::LimitExceeded { limit_bytes: 8 })
        );
        assert_eq!(
            budget.checked_next_length(usize::MAX, 1),
            Err(BodyBudgetError::LengthOverflow { limit_bytes: 8 })
        );
    }
}
