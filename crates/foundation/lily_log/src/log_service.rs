//! Bounded, parameterized queries for OpenTelemetry data in ClickHouse.

use std::sync::Arc;

use lily_clickhouse::{
    CancellationToken, ClickhouseError, ClickhousePageRequest, ClickhousePredicate,
    ClickhouseSelectPlan, ClickhouseSort,
};
use lily_error::injection::InjectionError;
use lily_injectable_derive::Injectable;
use lily_injection::ServiceTrait;

use crate::schemas::{
    OtelLog, OtelLogRepository, OtelMetricExponentialHistogram,
    OtelMetricExponentialHistogramRepository, OtelMetricGauge, OtelMetricGaugeRepository,
    OtelMetricHistogram, OtelMetricHistogramRepository, OtelMetricSum, OtelMetricSumRepository,
    OtelMetricSummary, OtelMetricSummaryRepository, OtelTrace, OtelTraceRepository,
};

const DEFAULT_QUERY_LIMIT: u64 = 1_000;
const MAX_QUERY_LIMIT: u64 = 1_000;
const MAX_CURSOR_OFFSET: u64 = 100_000;
const MAX_FILTER_BYTES: usize = 256;
const MAX_FILTER_VALUES: usize = 100;
const MAX_LOG_TRACE_RANGE_MILLIS: i64 = 24 * 60 * 60 * 1_000;
const MAX_METRIC_RANGE_NANOS: i64 = 24 * 60 * 60 * 1_000_000_000;
const TENANT_ATTRIBUTE_KEY: &str = "tenant.id";

/// Verified analytics data boundary. `None` means the authenticated principal
/// has the explicit all-services grant; an empty service set is never valid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnalyticsScope {
    tenant_id: String,
    allowed_services: Option<Vec<String>>,
}

impl AnalyticsScope {
    pub fn new(
        tenant_id: impl Into<String>,
        allowed_services: Option<Vec<String>>,
    ) -> Result<Self, ClickhouseError> {
        let tenant_id = tenant_id.into();
        validate_filter("tenant id", &tenant_id)?;

        let allowed_services = match allowed_services {
            Some(mut services) => {
                if services.is_empty() || services.len() > MAX_FILTER_VALUES {
                    return Err(invalid_plan(
                        "analytics service scope must contain between 1 and 100 values",
                    ));
                }
                for service in &services {
                    validate_filter("service scope", service)?;
                }
                services.sort();
                services.dedup();
                Some(services)
            }
            None => None,
        };

        Ok(Self {
            tenant_id,
            allowed_services,
        })
    }

    fn predicates(&self) -> Vec<ClickhousePredicate> {
        let mut predicates = vec![ClickhousePredicate::map_equal(
            "ResourceAttributes",
            TENANT_ATTRIBUTE_KEY,
            self.tenant_id.clone(),
        )];
        if let Some(services) = &self.allowed_services {
            predicates.push(ClickhousePredicate::in_strings(
                "ServiceName",
                services.clone(),
            ));
        }
        predicates
    }

    fn ensure_service_allowed(&self, service_name: &str) -> Result<(), ClickhouseError> {
        validate_filter("service name", service_name)?;
        if self
            .allowed_services
            .as_ref()
            .is_some_and(|services| !services.iter().any(|service| service == service_name))
        {
            return Err(invalid_plan(
                "service is outside the authenticated data scope",
            ));
        }
        Ok(())
    }
}

/// Bounded cursor page. The cursor is deliberately typed and versioned; it
/// contains only a bounded offset and grants no data access by itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnalyticsPageRequest {
    limit: u64,
    offset: u64,
}

impl AnalyticsPageRequest {
    pub fn new(limit: Option<u64>, cursor: Option<&str>) -> Result<Self, ClickhouseError> {
        let limit = limit.unwrap_or(DEFAULT_QUERY_LIMIT);
        if limit == 0 || limit > MAX_QUERY_LIMIT {
            return Err(invalid_plan("analytics limit must be between 1 and 1000"));
        }
        let offset = match cursor {
            None => 0,
            Some(cursor) => cursor
                .strip_prefix("v1:")
                .and_then(|value| value.parse::<u64>().ok())
                .filter(|offset| *offset <= MAX_CURSOR_OFFSET)
                .ok_or_else(|| invalid_plan("analytics cursor is invalid or out of range"))?,
        };
        Ok(Self { limit, offset })
    }

    pub const fn limit(self) -> u64 {
        self.limit
    }

    pub const fn offset(self) -> u64 {
        self.offset
    }

    pub fn next_cursor(self, returned_rows: usize) -> Option<String> {
        if returned_rows < self.limit as usize {
            return None;
        }
        self.offset
            .checked_add(self.limit)
            .filter(|offset| *offset <= MAX_CURSOR_OFFSET)
            .map(|offset| format!("v1:{offset}"))
    }
}

/// Per-request query authority, budget and cancellation context.
#[derive(Clone)]
pub struct AnalyticsQuery {
    scope: AnalyticsScope,
    page: AnalyticsPageRequest,
    cancellation: CancellationToken,
}

impl std::fmt::Debug for AnalyticsQuery {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AnalyticsQuery")
            .field("tenant", &"<redacted>")
            .field(
                "service_scope",
                &self.scope.allowed_services.as_ref().map(Vec::len),
            )
            .field("page", &self.page)
            .field("cancelled", &self.cancellation.is_cancelled())
            .finish()
    }
}

impl AnalyticsQuery {
    pub fn new(
        scope: AnalyticsScope,
        page: AnalyticsPageRequest,
        cancellation: CancellationToken,
    ) -> Self {
        Self {
            scope,
            page,
            cancellation,
        }
    }

    pub const fn page(&self) -> AnalyticsPageRequest {
        self.page
    }

    fn plan(
        &self,
        mut predicates: Vec<ClickhousePredicate>,
        order: Vec<ClickhouseSort>,
    ) -> Result<ClickhouseSelectPlan, ClickhouseError> {
        let mut scoped = self.scope.predicates();
        scoped.append(&mut predicates);
        ClickhouseSelectPlan::new(
            scoped,
            order,
            ClickhousePageRequest::new(self.page.limit, self.page.offset)?,
        )
    }
}

#[derive(Injectable, Default)]
#[service(lifetime = "Singleton")]
pub struct LogService {
    #[inject]
    pub log_repo: Arc<OtelLogRepository>,
    #[inject]
    pub trace_repo: Arc<OtelTraceRepository>,
    #[inject]
    pub sum_repo: Arc<OtelMetricSumRepository>,
    #[inject]
    pub gauge_repo: Arc<OtelMetricGaugeRepository>,
    #[inject]
    pub histogram_repo: Arc<OtelMetricHistogramRepository>,
    #[inject]
    pub summary_repo: Arc<OtelMetricSummaryRepository>,
    #[inject]
    pub exponential_histogram_repo: Arc<OtelMetricExponentialHistogramRepository>,
}

#[async_trait::async_trait]
impl ServiceTrait for LogService {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        tracing::info!("LogService initialized");
        Ok(())
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        tracing::info!("LogService disposed");
        Ok(())
    }
}

impl LogService {
    pub async fn query_logs_by_trace(
        &self,
        trace_ids: Vec<String>,
        span_ids: Vec<String>,
        start_time: i64,
        end_time: i64,
        query: &AnalyticsQuery,
    ) -> Result<Vec<OtelLog>, ClickhouseError> {
        validate_filter_values("trace ids", &trace_ids)?;
        validate_filter_values("span ids", &span_ids)?;
        let (start, end) = milliseconds_range(start_time, end_time)?;
        let plan = query.plan(
            vec![
                ClickhousePredicate::in_strings("TraceId", trace_ids),
                ClickhousePredicate::in_strings("SpanId", span_ids),
                ClickhousePredicate::greater_or_equal("Timestamp", start),
                ClickhousePredicate::less_or_equal("Timestamp", end),
            ],
            vec![
                ClickhouseSort::descending("Timestamp"),
                ClickhouseSort::descending("ServiceName"),
            ],
        )?;
        let operation = self
            .log_repo
            .operation_context(query.cancellation.clone())?;
        self.log_repo.select(&plan, &operation).await
    }

    pub async fn query_logs_by_time_range(
        &self,
        start_time: i64,
        end_time: i64,
        query: &AnalyticsQuery,
    ) -> Result<Vec<OtelLog>, ClickhouseError> {
        let (start, end) = milliseconds_range(start_time, end_time)?;
        self.select_logs(
            range_predicates("Timestamp", start, end),
            vec![
                ClickhouseSort::descending("Timestamp"),
                ClickhouseSort::descending("ServiceName"),
            ],
            query,
        )
        .await
    }

    pub async fn query_logs_by_service(
        &self,
        service_name: &str,
        start_time: i64,
        end_time: i64,
        query: &AnalyticsQuery,
    ) -> Result<Vec<OtelLog>, ClickhouseError> {
        query.scope.ensure_service_allowed(service_name)?;
        let (start, end) = milliseconds_range(start_time, end_time)?;
        let mut predicates = vec![ClickhousePredicate::equal("ServiceName", service_name)];
        predicates.extend(range_predicates("Timestamp", start, end));
        self.select_logs(
            predicates,
            vec![
                ClickhouseSort::descending("Timestamp"),
                ClickhouseSort::descending("ServiceName"),
            ],
            query,
        )
        .await
    }

    pub async fn query_logs_by_severity(
        &self,
        min_severity: u8,
        start_time: i64,
        end_time: i64,
        query: &AnalyticsQuery,
    ) -> Result<Vec<OtelLog>, ClickhouseError> {
        let (start, end) = milliseconds_range(start_time, end_time)?;
        let mut predicates = vec![ClickhousePredicate::greater_or_equal(
            "SeverityNumber",
            min_severity,
        )];
        predicates.extend(range_predicates("Timestamp", start, end));
        self.select_logs(
            predicates,
            vec![
                ClickhouseSort::descending("Timestamp"),
                ClickhouseSort::descending("ServiceName"),
            ],
            query,
        )
        .await
    }

    pub async fn query_logs_by_trace_id(
        &self,
        trace_id: &str,
        start_time: i64,
        end_time: i64,
        query: &AnalyticsQuery,
    ) -> Result<Vec<OtelLog>, ClickhouseError> {
        validate_filter("trace id", trace_id)?;
        let (start, end) = milliseconds_range(start_time, end_time)?;
        let mut predicates = vec![ClickhousePredicate::equal("TraceId", trace_id)];
        predicates.extend(range_predicates("Timestamp", start, end));
        self.select_logs(
            predicates,
            vec![ClickhouseSort::descending("Timestamp")],
            query,
        )
        .await
    }

    pub async fn query_error_logs(
        &self,
        service_name: Option<&str>,
        start_time: i64,
        end_time: i64,
        query: &AnalyticsQuery,
    ) -> Result<Vec<OtelLog>, ClickhouseError> {
        let (start, end) = milliseconds_range(start_time, end_time)?;
        let mut predicates = vec![ClickhousePredicate::greater_or_equal(
            "SeverityNumber",
            17_u8,
        )];
        if let Some(service_name) = service_name {
            query.scope.ensure_service_allowed(service_name)?;
            predicates.push(ClickhousePredicate::equal("ServiceName", service_name));
        }
        predicates.extend(range_predicates("Timestamp", start, end));
        self.select_logs(
            predicates,
            vec![
                ClickhouseSort::descending("Timestamp"),
                ClickhouseSort::descending("ServiceName"),
            ],
            query,
        )
        .await
    }

    async fn select_logs(
        &self,
        predicates: Vec<ClickhousePredicate>,
        order: Vec<ClickhouseSort>,
        query: &AnalyticsQuery,
    ) -> Result<Vec<OtelLog>, ClickhouseError> {
        let plan = query.plan(predicates, order)?;
        let operation = self
            .log_repo
            .operation_context(query.cancellation.clone())?;
        self.log_repo.select(&plan, &operation).await
    }

    pub async fn query_trace_by_id(
        &self,
        trace_id: &str,
        start_time: i64,
        end_time: i64,
        query: &AnalyticsQuery,
    ) -> Result<Vec<OtelTrace>, ClickhouseError> {
        validate_filter("trace id", trace_id)?;
        let (start, end) = milliseconds_range(start_time, end_time)?;
        let mut predicates = vec![ClickhousePredicate::equal("TraceId", trace_id)];
        predicates.extend(range_predicates("Timestamp", start, end));
        self.select_traces(
            predicates,
            vec![
                ClickhouseSort::ascending("TraceId"),
                ClickhouseSort::descending("Timestamp"),
            ],
            query,
        )
        .await
    }

    pub async fn query_traces_by_service(
        &self,
        service_name: &str,
        start_time: i64,
        end_time: i64,
        query: &AnalyticsQuery,
    ) -> Result<Vec<OtelTrace>, ClickhouseError> {
        query.scope.ensure_service_allowed(service_name)?;
        let (start, end) = milliseconds_range(start_time, end_time)?;
        let mut predicates = vec![ClickhousePredicate::equal("ServiceName", service_name)];
        predicates.extend(range_predicates("Timestamp", start, end));
        self.select_traces(
            predicates,
            vec![
                ClickhouseSort::ascending("TraceId"),
                ClickhouseSort::descending("Timestamp"),
            ],
            query,
        )
        .await
    }

    pub async fn query_traces_by_time_range(
        &self,
        start_time: i64,
        end_time: i64,
        query: &AnalyticsQuery,
    ) -> Result<Vec<OtelTrace>, ClickhouseError> {
        let (start, end) = milliseconds_range(start_time, end_time)?;
        self.select_traces(
            range_predicates("Timestamp", start, end),
            vec![
                ClickhouseSort::ascending("TraceId"),
                ClickhouseSort::descending("Timestamp"),
            ],
            query,
        )
        .await
    }

    pub async fn query_error_traces(
        &self,
        service_name: Option<&str>,
        start_time: i64,
        end_time: i64,
        query: &AnalyticsQuery,
    ) -> Result<Vec<OtelTrace>, ClickhouseError> {
        let (start, end) = milliseconds_range(start_time, end_time)?;
        let mut predicates = vec![ClickhousePredicate::equal("StatusCode", "ERROR")];
        if let Some(service_name) = service_name {
            query.scope.ensure_service_allowed(service_name)?;
            predicates.push(ClickhousePredicate::equal("ServiceName", service_name));
        }
        predicates.extend(range_predicates("Timestamp", start, end));
        self.select_traces(
            predicates,
            vec![ClickhouseSort::descending("Timestamp")],
            query,
        )
        .await
    }

    pub async fn query_slow_traces(
        &self,
        duration_threshold_ns: u64,
        service_name: Option<&str>,
        start_time: i64,
        end_time: i64,
        query: &AnalyticsQuery,
    ) -> Result<Vec<OtelTrace>, ClickhouseError> {
        let (start, end) = milliseconds_range(start_time, end_time)?;
        let mut predicates = vec![ClickhousePredicate::greater(
            "Duration",
            duration_threshold_ns,
        )];
        if let Some(service_name) = service_name {
            query.scope.ensure_service_allowed(service_name)?;
            predicates.push(ClickhousePredicate::equal("ServiceName", service_name));
        }
        predicates.extend(range_predicates("Timestamp", start, end));
        self.select_traces(
            predicates,
            vec![ClickhouseSort::descending("Duration")],
            query,
        )
        .await
    }

    async fn select_traces(
        &self,
        predicates: Vec<ClickhousePredicate>,
        order: Vec<ClickhouseSort>,
        query: &AnalyticsQuery,
    ) -> Result<Vec<OtelTrace>, ClickhouseError> {
        let plan = query.plan(predicates, order)?;
        let operation = self
            .trace_repo
            .operation_context(query.cancellation.clone())?;
        self.trace_repo.select(&plan, &operation).await
    }

    pub async fn query_sum_metrics_by_name(
        &self,
        metric_name: &str,
        start_time: i64,
        end_time: i64,
        query: &AnalyticsQuery,
    ) -> Result<Vec<OtelMetricSum>, ClickhouseError> {
        let plan = metric_plan("MetricName", metric_name, start_time, end_time, query)?;
        let operation = self
            .sum_repo
            .operation_context(query.cancellation.clone())?;
        self.sum_repo.select(&plan, &operation).await
    }

    pub async fn query_sum_metrics_by_service(
        &self,
        service_name: &str,
        start_time: i64,
        end_time: i64,
        query: &AnalyticsQuery,
    ) -> Result<Vec<OtelMetricSum>, ClickhouseError> {
        query.scope.ensure_service_allowed(service_name)?;
        let plan = metric_plan("ServiceName", service_name, start_time, end_time, query)?;
        let operation = self
            .sum_repo
            .operation_context(query.cancellation.clone())?;
        self.sum_repo.select(&plan, &operation).await
    }

    pub async fn query_gauge_metrics_by_name(
        &self,
        metric_name: &str,
        start_time: i64,
        end_time: i64,
        query: &AnalyticsQuery,
    ) -> Result<Vec<OtelMetricGauge>, ClickhouseError> {
        let plan = metric_plan("MetricName", metric_name, start_time, end_time, query)?;
        let operation = self
            .gauge_repo
            .operation_context(query.cancellation.clone())?;
        self.gauge_repo.select(&plan, &operation).await
    }

    pub async fn query_gauge_metrics_by_service(
        &self,
        service_name: &str,
        start_time: i64,
        end_time: i64,
        query: &AnalyticsQuery,
    ) -> Result<Vec<OtelMetricGauge>, ClickhouseError> {
        query.scope.ensure_service_allowed(service_name)?;
        let plan = metric_plan("ServiceName", service_name, start_time, end_time, query)?;
        let operation = self
            .gauge_repo
            .operation_context(query.cancellation.clone())?;
        self.gauge_repo.select(&plan, &operation).await
    }

    pub async fn query_histogram_metrics_by_name(
        &self,
        metric_name: &str,
        start_time: i64,
        end_time: i64,
        query: &AnalyticsQuery,
    ) -> Result<Vec<OtelMetricHistogram>, ClickhouseError> {
        let plan = metric_plan("MetricName", metric_name, start_time, end_time, query)?;
        let operation = self
            .histogram_repo
            .operation_context(query.cancellation.clone())?;
        self.histogram_repo.select(&plan, &operation).await
    }

    pub async fn query_histogram_metrics_by_service(
        &self,
        service_name: &str,
        start_time: i64,
        end_time: i64,
        query: &AnalyticsQuery,
    ) -> Result<Vec<OtelMetricHistogram>, ClickhouseError> {
        query.scope.ensure_service_allowed(service_name)?;
        let plan = metric_plan("ServiceName", service_name, start_time, end_time, query)?;
        let operation = self
            .histogram_repo
            .operation_context(query.cancellation.clone())?;
        self.histogram_repo.select(&plan, &operation).await
    }

    pub async fn query_summary_metrics_by_name(
        &self,
        metric_name: &str,
        start_time: i64,
        end_time: i64,
        query: &AnalyticsQuery,
    ) -> Result<Vec<OtelMetricSummary>, ClickhouseError> {
        let plan = metric_plan("MetricName", metric_name, start_time, end_time, query)?;
        let operation = self
            .summary_repo
            .operation_context(query.cancellation.clone())?;
        self.summary_repo.select(&plan, &operation).await
    }

    pub async fn query_summary_metrics_by_service(
        &self,
        service_name: &str,
        start_time: i64,
        end_time: i64,
        query: &AnalyticsQuery,
    ) -> Result<Vec<OtelMetricSummary>, ClickhouseError> {
        query.scope.ensure_service_allowed(service_name)?;
        let plan = metric_plan("ServiceName", service_name, start_time, end_time, query)?;
        let operation = self
            .summary_repo
            .operation_context(query.cancellation.clone())?;
        self.summary_repo.select(&plan, &operation).await
    }

    pub async fn query_exponential_histogram_metrics_by_name(
        &self,
        metric_name: &str,
        start_time: i64,
        end_time: i64,
        query: &AnalyticsQuery,
    ) -> Result<Vec<OtelMetricExponentialHistogram>, ClickhouseError> {
        let plan = metric_plan("MetricName", metric_name, start_time, end_time, query)?;
        let operation = self
            .exponential_histogram_repo
            .operation_context(query.cancellation.clone())?;
        self.exponential_histogram_repo
            .select(&plan, &operation)
            .await
    }

    pub async fn query_exponential_histogram_metrics_by_service(
        &self,
        service_name: &str,
        start_time: i64,
        end_time: i64,
        query: &AnalyticsQuery,
    ) -> Result<Vec<OtelMetricExponentialHistogram>, ClickhouseError> {
        query.scope.ensure_service_allowed(service_name)?;
        let plan = metric_plan("ServiceName", service_name, start_time, end_time, query)?;
        let operation = self
            .exponential_histogram_repo
            .operation_context(query.cancellation.clone())?;
        self.exponential_histogram_repo
            .select(&plan, &operation)
            .await
    }
}

fn metric_plan(
    filter_column: &str,
    filter_value: &str,
    start_time: i64,
    end_time: i64,
    query: &AnalyticsQuery,
) -> Result<ClickhouseSelectPlan, ClickhouseError> {
    validate_filter("metric filter", filter_value)?;
    let (start, end) = nanoseconds_range(start_time, end_time)?;
    let mut predicates = vec![ClickhousePredicate::equal(filter_column, filter_value)];
    predicates.extend(range_predicates("TimeUnix", start, end));
    query.plan(
        predicates,
        vec![
            ClickhouseSort::ascending("ServiceName"),
            ClickhouseSort::ascending("MetricName"),
            ClickhouseSort::descending("TimeUnix"),
        ],
    )
}

fn range_predicates(column: &str, start: i64, end: i64) -> Vec<ClickhousePredicate> {
    vec![
        ClickhousePredicate::greater_or_equal(column, start),
        ClickhousePredicate::less_or_equal(column, end),
    ]
}

fn milliseconds_range(start: i64, end: i64) -> Result<(i64, i64), ClickhouseError> {
    checked_input_range(start, end, MAX_LOG_TRACE_RANGE_MILLIS)?;
    checked_range(start / 1_000, end / 1_000)
}

fn nanoseconds_range(start: i64, end: i64) -> Result<(i64, i64), ClickhouseError> {
    checked_input_range(start, end, MAX_METRIC_RANGE_NANOS)?;
    checked_range(start / 1_000_000_000, end / 1_000_000_000)
}

fn checked_input_range(start: i64, end: i64, maximum_width: i64) -> Result<(), ClickhouseError> {
    let width = end
        .checked_sub(start)
        .ok_or_else(|| invalid_plan("analytics time range overflowed"))?;
    if width < 0 {
        return Err(invalid_plan("start time must not be after end time"));
    }
    if width > maximum_width {
        return Err(invalid_plan(
            "analytics time range exceeds the 24 hour budget",
        ));
    }
    Ok(())
}

fn checked_range(start: i64, end: i64) -> Result<(i64, i64), ClickhouseError> {
    if start > end {
        return Err(ClickhouseError::InvalidQueryPlan(
            "start time must not be after end time".into(),
        ));
    }
    Ok((start, end))
}

fn validate_filter(label: &str, value: &str) -> Result<(), ClickhouseError> {
    if value.is_empty()
        || value.len() > MAX_FILTER_BYTES
        || value.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(invalid_plan(&format!(
            "{label} is empty, oversized, or contains control bytes"
        )));
    }
    Ok(())
}

fn validate_filter_values(label: &str, values: &[String]) -> Result<(), ClickhouseError> {
    if values.is_empty() || values.len() > MAX_FILTER_VALUES {
        return Err(invalid_plan(&format!(
            "{label} must contain between 1 and 100 values"
        )));
    }
    for value in values {
        validate_filter(label, value)?;
    }
    Ok(())
}

fn invalid_plan(message: &str) -> ClickhouseError {
    ClickhouseError::InvalidQueryPlan(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_range_limit_and_cursor_are_fail_closed() {
        assert!(milliseconds_range(2_000, 1_000).is_err());
        assert!(milliseconds_range(0, MAX_LOG_TRACE_RANGE_MILLIS + 1).is_err());
        assert!(nanoseconds_range(0, MAX_METRIC_RANGE_NANOS + 1).is_err());
        assert!(AnalyticsPageRequest::new(Some(0), None).is_err());
        assert!(AnalyticsPageRequest::new(Some(1_001), None).is_err());
        assert!(AnalyticsPageRequest::new(Some(10), Some("10")).is_err());
        assert!(AnalyticsPageRequest::new(Some(10), Some("v1:100001")).is_err());
    }

    #[test]
    fn cursor_is_versioned_and_bounded() {
        let page = AnalyticsPageRequest::new(Some(25), Some("v1:50")).unwrap();
        assert_eq!(page.limit(), 25);
        assert_eq!(page.offset(), 50);
        assert_eq!(page.next_cursor(25).as_deref(), Some("v1:75"));
        assert!(page.next_cursor(24).is_none());
    }

    #[test]
    fn scope_is_mandatory_and_service_access_is_fail_closed() {
        assert!(AnalyticsScope::new("", None).is_err());
        assert!(AnalyticsScope::new("tenant-a", Some(Vec::new())).is_err());
        let scope = AnalyticsScope::new("tenant-a", Some(vec!["api".into()])).unwrap();
        assert!(scope.ensure_service_allowed("api").is_ok());
        assert!(scope.ensure_service_allowed("billing").is_err());

        let query = AnalyticsQuery::new(
            scope,
            AnalyticsPageRequest::new(Some(10), None).unwrap(),
            CancellationToken::new(),
        );
        assert_eq!(query.scope.predicates().len(), 2);
        assert!(
            query
                .plan(Vec::new(), vec![ClickhouseSort::descending("Timestamp")])
                .is_ok()
        );
    }
}
