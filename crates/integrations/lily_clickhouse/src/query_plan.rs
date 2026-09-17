use serde::{Serialize, Serializer};

use crate::options::{quote_column_identifier, quote_identifier};
use crate::{ClickhouseError, ClickhousePageRequest};

const MAX_PREDICATES: usize = 16;
const MAX_ORDER_COLUMNS: usize = 8;
const MAX_IN_VALUES: usize = 1_000;
const MAX_MAP_KEY_BYTES: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClickhouseComparison {
    Equal,
    Greater,
    GreaterOrEqual,
    LessOrEqual,
    In,
}

impl ClickhouseComparison {
    const fn sql(self) -> &'static str {
        match self {
            Self::Equal => "=",
            Self::Greater => ">",
            Self::GreaterOrEqual => ">=",
            Self::LessOrEqual => "<=",
            Self::In => "IN",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
/// Bound value types accepted by Lily's constrained ClickHouse query planner.
pub enum ClickhouseValue {
    /// UTF-8 string value.
    String(String),
    /// Signed 64-bit integer value.
    I64(i64),
    /// Unsigned 64-bit integer value.
    U64(u64),
    /// Unsigned 8-bit integer value.
    U8(u8),
    /// Bounded string list used by `IN` predicates.
    Strings(Vec<String>),
}

impl Serialize for ClickhouseValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::String(value) => value.serialize(serializer),
            Self::I64(value) => value.serialize(serializer),
            Self::U64(value) => value.serialize(serializer),
            Self::U8(value) => value.serialize(serializer),
            Self::Strings(value) => value.serialize(serializer),
        }
    }
}

impl From<String> for ClickhouseValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for ClickhouseValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<i64> for ClickhouseValue {
    fn from(value: i64) -> Self {
        Self::I64(value)
    }
}

impl From<u64> for ClickhouseValue {
    fn from(value: u64) -> Self {
        Self::U64(value)
    }
}

impl From<u8> for ClickhouseValue {
    fn from(value: u8) -> Self {
        Self::U8(value)
    }
}

impl From<Vec<String>> for ClickhouseValue {
    fn from(value: Vec<String>) -> Self {
        Self::Strings(value)
    }
}

#[derive(Debug, Clone, PartialEq)]
/// One bound and allowlist-checked predicate in a select plan.
pub struct ClickhousePredicate {
    pub(crate) column: String,
    pub(crate) map_key: Option<String>,
    pub(crate) comparison: ClickhouseComparison,
    pub(crate) value: ClickhouseValue,
}

impl ClickhousePredicate {
    /// Creates an equality predicate.
    pub fn equal(column: impl Into<String>, value: impl Into<ClickhouseValue>) -> Self {
        Self::new(column, ClickhouseComparison::Equal, value)
    }

    /// Creates a strict greater-than predicate.
    pub fn greater(column: impl Into<String>, value: impl Into<ClickhouseValue>) -> Self {
        Self::new(column, ClickhouseComparison::Greater, value)
    }

    /// Creates a greater-than-or-equal predicate.
    pub fn greater_or_equal(column: impl Into<String>, value: impl Into<ClickhouseValue>) -> Self {
        Self::new(column, ClickhouseComparison::GreaterOrEqual, value)
    }

    /// Creates a less-than-or-equal predicate.
    pub fn less_or_equal(column: impl Into<String>, value: impl Into<ClickhouseValue>) -> Self {
        Self::new(column, ClickhouseComparison::LessOrEqual, value)
    }

    /// Creates a string-list `IN` predicate.
    pub fn in_strings(column: impl Into<String>, values: Vec<String>) -> Self {
        Self::new(column, ClickhouseComparison::In, values)
    }

    /// Compares one entry of an allowlisted ClickHouse `Map` column.
    ///
    /// The column remains structural and is checked against the generated
    /// schema allowlist. Both `key` and `value` remain query parameters; they
    /// are never interpolated into SQL.
    pub fn map_equal(
        column: impl Into<String>,
        key: impl Into<String>,
        value: impl Into<ClickhouseValue>,
    ) -> Self {
        Self {
            column: column.into(),
            map_key: Some(key.into()),
            comparison: ClickhouseComparison::Equal,
            value: value.into(),
        }
    }

    fn new(
        column: impl Into<String>,
        comparison: ClickhouseComparison,
        value: impl Into<ClickhouseValue>,
    ) -> Self {
        Self {
            column: column.into(),
            map_key: None,
            comparison,
            value: value.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClickhouseSortDirection {
    Ascending,
    Descending,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Allowlist-checked sort expression for a select plan.
pub struct ClickhouseSort {
    pub(crate) column: String,
    pub(crate) direction: ClickhouseSortDirection,
}

impl ClickhouseSort {
    /// Sorts an allowlisted column in ascending order.
    pub fn ascending(column: impl Into<String>) -> Self {
        Self {
            column: column.into(),
            direction: ClickhouseSortDirection::Ascending,
        }
    }

    /// Sorts an allowlisted column in descending order.
    pub fn descending(column: impl Into<String>) -> Self {
        Self {
            column: column.into(),
            direction: ClickhouseSortDirection::Descending,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
/// Bounded, parameterized ClickHouse select plan.
pub struct ClickhouseSelectPlan {
    pub(crate) predicates: Vec<ClickhousePredicate>,
    pub(crate) order: Vec<ClickhouseSort>,
    pub(crate) page: ClickhousePageRequest,
}

impl ClickhouseSelectPlan {
    /// Validates predicates, ordering and pagination before any query executes.
    pub fn new(
        predicates: Vec<ClickhousePredicate>,
        order: Vec<ClickhouseSort>,
        page: ClickhousePageRequest,
    ) -> Result<Self, ClickhouseError> {
        if predicates.len() > MAX_PREDICATES {
            return Err(invalid("predicate count exceeds 16"));
        }
        if order.len() > MAX_ORDER_COLUMNS {
            return Err(invalid("order column count exceeds 8"));
        }
        for predicate in &predicates {
            if let Some(key) = &predicate.map_key {
                if predicate.comparison != ClickhouseComparison::Equal {
                    return Err(invalid("map predicates support equality only"));
                }
                if key.is_empty()
                    || key.len() > MAX_MAP_KEY_BYTES
                    || key.bytes().any(|byte| byte.is_ascii_control())
                {
                    return Err(invalid(
                        "map key is empty, oversized, or contains control bytes",
                    ));
                }
            }
            if let ClickhouseValue::Strings(values) = &predicate.value {
                if predicate.comparison != ClickhouseComparison::In {
                    return Err(invalid("list values require an IN predicate"));
                }
                if values.len() > MAX_IN_VALUES {
                    return Err(invalid("IN predicate exceeds 1000 values"));
                }
            } else if predicate.comparison == ClickhouseComparison::In {
                return Err(invalid("IN predicate requires a list value"));
            }
        }
        Ok(Self {
            predicates,
            order,
            page,
        })
    }

    pub(crate) fn has_empty_set(&self) -> bool {
        self.predicates.iter().any(|predicate| {
            matches!(&predicate.value, ClickhouseValue::Strings(values) if values.is_empty())
        })
    }

    pub(crate) fn statement(
        &self,
        table: &str,
        allowed_columns: &[&str],
    ) -> Result<String, ClickhouseError> {
        let table = quote_identifier(table, "table")?;
        let mut statement = format!("SELECT ?fields FROM {table}");
        if !self.predicates.is_empty() {
            statement.push_str(" WHERE ");
            for (index, predicate) in self.predicates.iter().enumerate() {
                if index != 0 {
                    statement.push_str(" AND ");
                }
                validate_allowed(allowed_columns, &predicate.column)?;
                let column = quote_column_identifier(&predicate.column)?;
                if predicate.map_key.is_some() {
                    statement.push_str("mapGet(");
                    statement.push_str(&column);
                    statement.push_str(", ?)");
                } else {
                    statement.push_str(&column);
                }
                statement.push(' ');
                statement.push_str(predicate.comparison.sql());
                statement.push_str(" ?");
            }
        }
        if !self.order.is_empty() {
            statement.push_str(" ORDER BY ");
            for (index, sort) in self.order.iter().enumerate() {
                if index != 0 {
                    statement.push_str(", ");
                }
                validate_allowed(allowed_columns, &sort.column)?;
                statement.push_str(&quote_column_identifier(&sort.column)?);
                statement.push_str(match sort.direction {
                    ClickhouseSortDirection::Ascending => " ASC",
                    ClickhouseSortDirection::Descending => " DESC",
                });
            }
        }
        statement.push_str(" LIMIT ? OFFSET ?");
        Ok(statement)
    }
}

fn validate_allowed(allowed: &[&str], column: &str) -> Result<(), ClickhouseError> {
    if !allowed.contains(&column) {
        return Err(ClickhouseError::InvalidIdentifier(
            "query column is not part of the generated schema allowlist".into(),
        ));
    }
    Ok(())
}

fn invalid(message: &str) -> ClickhouseError {
    ClickhouseError::InvalidQueryPlan(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn structure_is_allowlisted_and_values_never_enter_sql() {
        let plan = ClickhouseSelectPlan::new(
            vec![ClickhousePredicate::equal("tenant", "x' OR 1=1")],
            vec![ClickhouseSort::descending("timestamp")],
            ClickhousePageRequest::new(50, 0).unwrap(),
        )
        .unwrap();
        let statement = plan.statement("events", &["tenant", "timestamp"]).unwrap();
        assert!(!statement.contains("OR 1=1"));
        assert!(statement.contains("`tenant` = ?"));
        assert!(plan.statement("events", &["other"]).is_err());
    }

    #[test]
    fn map_key_and_value_remain_bound_parameters() {
        let plan = ClickhouseSelectPlan::new(
            vec![ClickhousePredicate::map_equal(
                "ResourceAttributes",
                "tenant.id' OR 1=1",
                "tenant' OR 1=1",
            )],
            Vec::new(),
            ClickhousePageRequest::new(10, 0).unwrap(),
        )
        .unwrap();

        let statement = plan
            .statement("otel_logs", &["ResourceAttributes"])
            .unwrap();
        assert!(statement.contains("mapGet(`ResourceAttributes`, ?) = ?"));
        assert!(!statement.contains("tenant.id"));
        assert!(!statement.contains("OR 1=1"));
    }

    #[test]
    fn empty_and_oversized_sets_are_bounded() {
        let empty = ClickhouseSelectPlan::new(
            vec![ClickhousePredicate::in_strings("id", Vec::new())],
            Vec::new(),
            ClickhousePageRequest::new(1, 0).unwrap(),
        )
        .unwrap();
        assert!(empty.has_empty_set());
        assert!(
            ClickhouseSelectPlan::new(
                vec![ClickhousePredicate::in_strings(
                    "id",
                    vec![String::new(); MAX_IN_VALUES + 1]
                )],
                Vec::new(),
                ClickhousePageRequest::new(1, 0).unwrap(),
            )
            .is_err()
        );
    }
}
