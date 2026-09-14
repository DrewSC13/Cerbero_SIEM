use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

pub const EVENTS_PARSED_TOTAL: &str = "events_parsed_total";
pub const EVENTS_NORMALIZED_TOTAL: &str = "events_normalized_total";
pub const PARSE_SUCCESS_TOTAL: &str = "parse_success_total";
pub const PARSE_PARTIAL_TOTAL: &str = "parse_partial_total";
pub const PARSE_FAILED_TOTAL: &str = "parse_failed_total";
pub const PARSE_UNSUPPORTED_TOTAL: &str = "parse_unsupported_total";
pub const NORMALIZATION_SUCCESS_TOTAL: &str = "normalization_success_total";
pub const NORMALIZATION_PARTIAL_TOTAL: &str = "normalization_partial_total";
pub const NORMALIZATION_FAILED_TOTAL: &str = "normalization_failed_total";
pub const PARSER_LATENCY: &str = "parser_latency";
pub const NORMALIZATION_LATENCY: &str = "normalization_latency";
pub const PARSER_BY_ID: &str = "parser_by_id";
pub const MAPPING_BY_ID: &str = "mapping_by_id";
pub const DLQ_NORMALIZATION_TOTAL: &str = "dlq_normalization_total";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParseMetricStatus {
    Success,
    Partial,
    Failed,
    Unsupported,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NormalizationMetricStatus {
    Success,
    Partial,
    Failed,
}

pub trait NormalizerMetrics: Send + Sync {
    fn record_parse(&self, status: ParseMetricStatus, parser_id: Option<&str>, latency: Duration);
    fn record_normalization(
        &self,
        status: NormalizationMetricStatus,
        mapping_id: Option<&str>,
        latency: Duration,
    );
    fn record_dlq(&self);
}

#[derive(Debug, Default)]
pub struct NoopNormalizerMetrics;

impl NormalizerMetrics for NoopNormalizerMetrics {
    fn record_parse(
        &self,
        _status: ParseMetricStatus,
        _parser_id: Option<&str>,
        _latency: Duration,
    ) {
    }
    fn record_normalization(
        &self,
        _status: NormalizationMetricStatus,
        _mapping_id: Option<&str>,
        _latency: Duration,
    ) {
    }
    fn record_dlq(&self) {}
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetricsSnapshot {
    pub events_parsed_total: u64,
    pub events_normalized_total: u64,
    pub parse_success_total: u64,
    pub parse_partial_total: u64,
    pub parse_failed_total: u64,
    pub parse_unsupported_total: u64,
    pub normalization_success_total: u64,
    pub normalization_partial_total: u64,
    pub normalization_failed_total: u64,
    pub parser_latency_observations: u64,
    pub parser_latency_total_nanoseconds: u64,
    pub normalization_latency_observations: u64,
    pub normalization_latency_total_nanoseconds: u64,
    pub dlq_normalization_total: u64,
    pub parser_by_id: BTreeMap<String, u64>,
    pub mapping_by_id: BTreeMap<String, u64>,
}

#[derive(Debug, Default)]
pub struct MemoryNormalizerMetrics {
    events_parsed_total: AtomicU64,
    events_normalized_total: AtomicU64,
    parse_success_total: AtomicU64,
    parse_partial_total: AtomicU64,
    parse_failed_total: AtomicU64,
    parse_unsupported_total: AtomicU64,
    normalization_success_total: AtomicU64,
    normalization_partial_total: AtomicU64,
    normalization_failed_total: AtomicU64,
    parser_latency_observations: AtomicU64,
    parser_latency_total_nanoseconds: AtomicU64,
    normalization_latency_observations: AtomicU64,
    normalization_latency_total_nanoseconds: AtomicU64,
    dlq_normalization_total: AtomicU64,
    parser_by_id: Mutex<BTreeMap<String, u64>>,
    mapping_by_id: Mutex<BTreeMap<String, u64>>,
}

impl MemoryNormalizerMetrics {
    #[must_use]
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            events_parsed_total: self.events_parsed_total.load(Ordering::Relaxed),
            events_normalized_total: self.events_normalized_total.load(Ordering::Relaxed),
            parse_success_total: self.parse_success_total.load(Ordering::Relaxed),
            parse_partial_total: self.parse_partial_total.load(Ordering::Relaxed),
            parse_failed_total: self.parse_failed_total.load(Ordering::Relaxed),
            parse_unsupported_total: self.parse_unsupported_total.load(Ordering::Relaxed),
            normalization_success_total: self.normalization_success_total.load(Ordering::Relaxed),
            normalization_partial_total: self.normalization_partial_total.load(Ordering::Relaxed),
            normalization_failed_total: self.normalization_failed_total.load(Ordering::Relaxed),
            parser_latency_observations: self.parser_latency_observations.load(Ordering::Relaxed),
            parser_latency_total_nanoseconds: self
                .parser_latency_total_nanoseconds
                .load(Ordering::Relaxed),
            normalization_latency_observations: self
                .normalization_latency_observations
                .load(Ordering::Relaxed),
            normalization_latency_total_nanoseconds: self
                .normalization_latency_total_nanoseconds
                .load(Ordering::Relaxed),
            dlq_normalization_total: self.dlq_normalization_total.load(Ordering::Relaxed),
            parser_by_id: lock_map(&self.parser_by_id).clone(),
            mapping_by_id: lock_map(&self.mapping_by_id).clone(),
        }
    }
}

impl NormalizerMetrics for MemoryNormalizerMetrics {
    fn record_parse(&self, status: ParseMetricStatus, parser_id: Option<&str>, latency: Duration) {
        saturating_increment(&self.events_parsed_total, 1);
        saturating_increment(&self.parser_latency_observations, 1);
        saturating_increment(
            &self.parser_latency_total_nanoseconds,
            duration_nanoseconds(latency),
        );
        match status {
            ParseMetricStatus::Success => saturating_increment(&self.parse_success_total, 1),
            ParseMetricStatus::Partial => saturating_increment(&self.parse_partial_total, 1),
            ParseMetricStatus::Failed => saturating_increment(&self.parse_failed_total, 1),
            ParseMetricStatus::Unsupported => {
                saturating_increment(&self.parse_unsupported_total, 1);
            }
        }
        if let Some(parser_id) = parser_id {
            increment_map(&self.parser_by_id, parser_id);
        }
    }

    fn record_normalization(
        &self,
        status: NormalizationMetricStatus,
        mapping_id: Option<&str>,
        latency: Duration,
    ) {
        saturating_increment(&self.normalization_latency_observations, 1);
        saturating_increment(
            &self.normalization_latency_total_nanoseconds,
            duration_nanoseconds(latency),
        );
        match status {
            NormalizationMetricStatus::Success => {
                saturating_increment(&self.events_normalized_total, 1);
                saturating_increment(&self.normalization_success_total, 1);
            }
            NormalizationMetricStatus::Partial => {
                saturating_increment(&self.events_normalized_total, 1);
                saturating_increment(&self.normalization_partial_total, 1);
            }
            NormalizationMetricStatus::Failed => {
                saturating_increment(&self.normalization_failed_total, 1);
            }
        }
        if let Some(mapping_id) = mapping_id {
            increment_map(&self.mapping_by_id, mapping_id);
        }
    }

    fn record_dlq(&self) {
        saturating_increment(&self.dlq_normalization_total, 1);
    }
}

fn duration_nanoseconds(value: Duration) -> u64 {
    u64::try_from(value.as_nanos()).unwrap_or(u64::MAX)
}

fn saturating_increment(counter: &AtomicU64, value: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(value))
    });
}

fn lock_map(
    map: &Mutex<BTreeMap<String, u64>>,
) -> std::sync::MutexGuard<'_, BTreeMap<String, u64>> {
    map.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn increment_map(map: &Mutex<BTreeMap<String, u64>>, key: &str) {
    let mut values = lock_map(map);
    let entry = values.entry(key.to_string()).or_insert(0);
    *entry = entry.saturating_add(1);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_metrics_are_backend_neutral_and_bounded_by_governed_ids() {
        let metrics = MemoryNormalizerMetrics::default();
        metrics.record_parse(
            ParseMetricStatus::Partial,
            Some("cerbero.parser.syslog.rfc3164"),
            Duration::from_millis(2),
        );
        metrics.record_normalization(
            NormalizationMetricStatus::Success,
            Some("linux.ssh.authentication"),
            Duration::from_millis(3),
        );
        metrics.record_dlq();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.events_parsed_total, 1);
        assert_eq!(snapshot.parse_partial_total, 1);
        assert_eq!(snapshot.events_normalized_total, 1);
        assert_eq!(snapshot.normalization_success_total, 1);
        assert_eq!(snapshot.dlq_normalization_total, 1);
        assert_eq!(snapshot.parser_by_id["cerbero.parser.syslog.rfc3164"], 1);
        assert_eq!(snapshot.mapping_by_id["linux.ssh.authentication"], 1);
        assert_eq!(snapshot.parser_latency_observations, 1);
        assert_eq!(snapshot.normalization_latency_observations, 1);
    }
}
