//! Stats types for Oxana job queue monitoring.

use serde::{Deserialize, Serialize};

use crate::job_envelope::JobEnvelope;

/// Overall statistics for the Oxana job queue system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stats {
    /// Global aggregate statistics.
    pub global: StatsGlobal,
    /// List of active processes.
    pub processes: Vec<Process>,
    /// Jobs currently being processed.
    pub processing: Vec<StatsProcessing>,
    /// Per-queue statistics.
    pub queues: Vec<QueueStats>,
}

/// Global aggregate statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsGlobal {
    /// Total number of jobs (enqueued + scheduled).
    pub jobs: usize,
    /// Number of jobs currently enqueued.
    pub enqueued: usize,
    /// Total number of jobs processed.
    pub processed: i64,
    /// Total number of jobs failed.
    pub failed: i64,
    /// Number of dead jobs.
    pub dead: usize,
    /// Number of scheduled jobs.
    pub scheduled: usize,
    /// Number of jobs in retry queue.
    pub retries: usize,
    /// Maximum latency across all queues in seconds.
    pub latency_s_max: f64,
}

/// Information about a job currently being processed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsProcessing {
    /// The process ID handling the job.
    pub process_id: String,
    /// The job envelope being processed.
    pub job_envelope: JobEnvelope,
}

/// Historical per-queue rate estimates.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct QueueRateStats {
    /// Number of minutes used to calculate these rates.
    pub window_minutes: usize,
    /// Jobs processed per minute over the window.
    pub processed_per_minute: f64,
    /// Jobs succeeded per minute over the window.
    pub succeeded_per_minute: f64,
    /// Jobs failed per minute over the window.
    pub failed_per_minute: f64,
    /// Net queue growth per minute over the window.
    pub growth_per_minute: f64,
    /// Observed queue drain after incoming growth is accounted for.
    pub effective_drain_per_minute: f64,
    /// Estimated seconds until the waiting queue drains.
    pub eta_s: Option<f64>,
}

impl QueueRateStats {
    pub(crate) fn calculate(
        window_minutes: usize,
        enqueued: usize,
        window_start_enqueued: usize,
        processed: u64,
        succeeded: u64,
        failed: u64,
    ) -> Self {
        let window_minutes = window_minutes.max(1);
        let window = window_minutes as f64;
        let processed_per_minute = processed as f64 / window;
        let succeeded_per_minute = succeeded as f64 / window;
        let failed_per_minute = failed as f64 / window;
        let growth_per_minute = (enqueued as f64 - window_start_enqueued as f64) / window;
        let effective_drain_per_minute =
            Self::effective_drain(processed_per_minute, growth_per_minute);

        Self {
            window_minutes,
            processed_per_minute,
            succeeded_per_minute,
            failed_per_minute,
            growth_per_minute,
            effective_drain_per_minute,
            eta_s: Self::eta(enqueued, effective_drain_per_minute),
        }
    }

    pub(crate) fn aggregate(
        window_minutes: usize,
        enqueued: usize,
        rates: impl IntoIterator<Item = Self>,
    ) -> Self {
        let window_minutes = window_minutes.max(1);
        let mut processed_per_minute = 0.0;
        let mut succeeded_per_minute = 0.0;
        let mut failed_per_minute = 0.0;
        let mut growth_per_minute = 0.0;

        for rate in rates {
            processed_per_minute += rate.processed_per_minute;
            succeeded_per_minute += rate.succeeded_per_minute;
            failed_per_minute += rate.failed_per_minute;
            growth_per_minute += rate.growth_per_minute;
        }

        let effective_drain_per_minute =
            Self::effective_drain(processed_per_minute, growth_per_minute);

        Self {
            window_minutes,
            processed_per_minute,
            succeeded_per_minute,
            failed_per_minute,
            growth_per_minute,
            effective_drain_per_minute,
            eta_s: Self::eta(enqueued, effective_drain_per_minute),
        }
    }

    fn eta(enqueued: usize, effective_drain_per_minute: f64) -> Option<f64> {
        if enqueued == 0 {
            Some(0.0)
        } else if effective_drain_per_minute > 0.0 {
            Some(enqueued as f64 / (effective_drain_per_minute / 60.0))
        } else {
            None
        }
    }

    fn effective_drain(processed_per_minute: f64, growth_per_minute: f64) -> f64 {
        if processed_per_minute > 0.0 {
            (-growth_per_minute).max(0.0)
        } else {
            0.0
        }
    }
}

/// Statistics for a specific queue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueStats {
    /// The queue key/name.
    pub key: String,

    /// Number of jobs currently enqueued.
    pub enqueued: usize,
    /// Total number of jobs processed.
    pub processed: i64,
    /// Total number of jobs succeeded.
    pub succeeded: i64,
    /// Total number of jobs panicked.
    pub panicked: i64,
    /// Total number of jobs failed.
    pub failed: i64,
    /// Current latency in seconds.
    pub latency_s: f64,
    /// Historical rate estimates for this queue.
    #[serde(default)]
    pub rate: QueueRateStats,

    /// Dynamic sub-queue statistics (if any).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub queues: Vec<DynamicQueueStats>,
}

/// Statistics for a dynamic sub-queue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DynamicQueueStats {
    /// The dynamic queue suffix.
    pub suffix: String,

    /// Number of jobs currently enqueued.
    pub enqueued: usize,
    /// Total number of jobs processed.
    pub processed: i64,
    /// Total number of jobs succeeded.
    pub succeeded: i64,
    /// Total number of jobs panicked.
    pub panicked: i64,
    /// Total number of jobs failed.
    pub failed: i64,
    /// Current latency in seconds.
    pub latency_s: f64,
    /// Historical rate estimates for this dynamic sub-queue.
    #[serde(default)]
    pub rate: QueueRateStats,
}

/// Information about an Oxana worker process.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Process {
    /// The hostname where the process is running.
    pub hostname: String,
    /// The process ID.
    pub pid: u32,
    /// Random identifier generated when the storage is created. A container
    /// restarted in place keeps its hostname and its pid, so this is what tells
    /// the restarted process apart from the one that died. Empty for records
    /// written by releases that did not set it.
    ///
    /// This identifies one worker runtime rather than one OS process, and must
    /// stay that way: each runtime owns its own `processing:<id>` list, so a
    /// process-global identifier would make two runtimes in the same process
    /// share one list, and the first to shut down would clear the entry while
    /// the second still has jobs in flight.
    #[serde(default)]
    pub instance_id: String,
    /// Last heartbeat timestamp (Unix timestamp).
    pub heartbeat_at: i64,
    /// Process start timestamp (Unix timestamp).
    pub started_at: i64,
}

impl Process {
    /// Returns a unique identifier for the process.
    #[must_use]
    pub fn id(&self) -> String {
        if self.instance_id.is_empty() {
            // A record written before `instance_id` existed. Unreachable once
            // every such process has been swept, which a rolling upgrade does
            // within one dead process threshold; remove this arm in 3.0.
            format!("{}-{}", self.hostname, self.pid)
        } else {
            format!("{}-{}-{}", self.hostname, self.pid, self.instance_id)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Process, QueueRateStats};

    fn run(instance_id: &str) -> Process {
        Process {
            hostname: "host".to_string(),
            pid: 1,
            instance_id: instance_id.to_string(),
            heartbeat_at: 0,
            started_at: 0,
        }
    }

    #[test]
    fn process_id_tells_apart_two_runs_with_the_same_hostname_and_pid() {
        assert_eq!(run("a").id(), "host-1-a");
        assert_ne!(run("a").id(), run("b").id());
    }

    #[test]
    fn process_id_of_a_record_without_instance_id_keeps_the_legacy_form() {
        let recorded_by_an_older_release: Process =
            serde_json::from_str(r#"{"hostname":"host","pid":1,"heartbeat_at":0,"started_at":0}"#)
                .expect("a process record without instance_id deserializes");
        assert_eq!(recorded_by_an_older_release.id(), "host-1");
    }

    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 1e-9,
            "expected {expected}, got {actual}"
        );
    }

    #[test]
    fn queue_rate_calculates_eta_when_effective_drain_is_positive() {
        let rate = QueueRateStats::calculate(10, 10, 30, 50, 40, 10);

        assert_close(rate.processed_per_minute, 5.0);
        assert_close(rate.succeeded_per_minute, 4.0);
        assert_close(rate.failed_per_minute, 1.0);
        assert_close(rate.growth_per_minute, -2.0);
        assert_close(rate.effective_drain_per_minute, 2.0);
        assert_close(rate.eta_s.expect("eta should be finite"), 300.0);
    }

    #[test]
    fn queue_rate_eta_is_unknown_when_queue_grows_despite_processing() {
        let rate = QueueRateStats::calculate(10, 20, 0, 100, 100, 0);

        assert_close(rate.processed_per_minute, 10.0);
        assert_close(rate.growth_per_minute, 2.0);
        assert_close(rate.effective_drain_per_minute, 0.0);
        assert!(rate.eta_s.is_none());
    }

    #[test]
    fn queue_rate_eta_is_zero_for_empty_queue() {
        let rate = QueueRateStats::calculate(10, 0, 10, 0, 0, 0);

        assert_close(rate.growth_per_minute, -1.0);
        assert_eq!(rate.eta_s, Some(0.0));
    }

    #[test]
    fn queue_rate_eta_is_unknown_without_processing_rate() {
        let rate = QueueRateStats::calculate(10, 10, 20, 0, 0, 0);

        assert_close(rate.growth_per_minute, -1.0);
        assert_close(rate.effective_drain_per_minute, 0.0);
        assert!(rate.eta_s.is_none());
    }

    #[test]
    fn queue_rate_aggregates_dynamic_children() {
        let first = QueueRateStats::calculate(10, 10, 20, 30, 25, 5);
        let second = QueueRateStats::calculate(10, 20, 30, 40, 35, 5);

        let rate = QueueRateStats::aggregate(10, 30, [first, second]);

        assert_close(rate.processed_per_minute, 7.0);
        assert_close(rate.succeeded_per_minute, 6.0);
        assert_close(rate.failed_per_minute, 1.0);
        assert_close(rate.growth_per_minute, -2.0);
        assert_close(rate.effective_drain_per_minute, 2.0);
        assert_close(rate.eta_s.expect("eta should be finite"), 900.0);
    }
}
