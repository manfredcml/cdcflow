use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Lock-free pipeline metrics updated from the hot path.
///
/// All counters use relaxed ordering — snapshots are approximate
/// point-in-time views, which is acceptable for monitoring.
pub struct PipelineMetrics {
    events_total: AtomicU64,
    batches_total: AtomicU64,
    last_batch_size: AtomicU64,
    errors_total: AtomicU64,
    last_flush_at: AtomicI64,
    started_at: Instant,
}

impl PipelineMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            events_total: AtomicU64::new(0),
            batches_total: AtomicU64::new(0),
            last_batch_size: AtomicU64::new(0),
            errors_total: AtomicU64::new(0),
            last_flush_at: AtomicI64::new(0),
            started_at: Instant::now(),
        })
    }

    pub fn record_batch(&self, event_count: u64) {
        self.events_total.fetch_add(event_count, Ordering::Relaxed);
        self.batches_total.fetch_add(1, Ordering::Relaxed);
        self.last_batch_size.store(event_count, Ordering::Relaxed);
        self.last_flush_at
            .store(chrono::Utc::now().timestamp(), Ordering::Relaxed);
    }

    pub fn record_error(&self) {
        self.errors_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let uptime = self.started_at.elapsed();
        let events_total = self.events_total.load(Ordering::Relaxed);
        let uptime_secs = uptime.as_secs();
        let events_per_sec = if uptime_secs > 0 {
            events_total as f64 / uptime_secs as f64
        } else {
            0.0
        };

        MetricsSnapshot {
            events_total,
            events_per_sec,
            batches_total: self.batches_total.load(Ordering::Relaxed),
            last_batch_size: self.last_batch_size.load(Ordering::Relaxed),
            errors_total: self.errors_total.load(Ordering::Relaxed),
            last_flush_at: self.last_flush_at.load(Ordering::Relaxed),
            uptime_secs,
        }
    }
}

/// Serializable point-in-time metrics snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub events_total: u64,
    pub events_per_sec: f64,
    pub batches_total: u64,
    pub last_batch_size: u64,
    pub errors_total: u64,
    pub last_flush_at: i64,
    pub uptime_secs: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_batch_increments() {
        let m = PipelineMetrics::new();
        m.record_batch(5);
        m.record_batch(3);

        let snap = m.snapshot();
        assert_eq!(snap.events_total, 8);
        assert_eq!(snap.batches_total, 2);
        assert_eq!(snap.last_batch_size, 3);
    }

    #[test]
    fn test_record_error_increments() {
        let m = PipelineMetrics::new();
        m.record_error();
        m.record_error();
        m.record_error();

        let snap = m.snapshot();
        assert_eq!(snap.errors_total, 3);
    }

    #[test]
    fn test_events_per_sec_zero_when_no_uptime() {
        let m = PipelineMetrics::new();
        let snap = m.snapshot();
        // With 0 events and ~0 uptime, events_per_sec should be 0
        assert_eq!(snap.events_per_sec, 0.0);
    }

    #[test]
    fn test_last_flush_at_set_on_batch() {
        let m = PipelineMetrics::new();
        assert_eq!(m.snapshot().last_flush_at, 0);

        m.record_batch(1);
        let snap = m.snapshot();
        assert!(snap.last_flush_at > 0);
    }
}
