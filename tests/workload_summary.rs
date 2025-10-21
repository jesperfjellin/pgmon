use chrono::{Duration, TimeZone, Utc};
use pgmon::{WorkloadSample, WorkloadSummary};

#[test]
fn workload_summary_computes_rates() {
    let previous = WorkloadSample {
        collected_at: Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
        total_xacts: 1_000.0,
        total_calls: Some(2_000.0),
        total_time_ms: Some(40_000.0),
        wal_bytes: Some(1_000),
        temp_bytes: Some(10_000),
        checkpoints_timed: Some(10),
        checkpoints_requested: Some(2),
    };

    let current = WorkloadSample {
        collected_at: previous.collected_at + Duration::seconds(60),
        total_xacts: 1_360.0,
        total_calls: Some(2_300.0),
        total_time_ms: Some(43_000.0),
        wal_bytes: Some(1_600),
        temp_bytes: Some(16_000),
        checkpoints_timed: Some(11),
        checkpoints_requested: Some(3),
    };

    let summary = WorkloadSummary::from_samples(&previous, &current).expect("summary");
    assert!((summary.tps.expect("tps") - 6.0).abs() < 1e-6);
    assert!((summary.qps.expect("qps") - 5.0).abs() < 1e-6);
    assert!((summary.mean_latency_ms.expect("lat") - 10.0).abs() < 1e-6);
    assert!(summary.latency_p95_ms.is_none());
    assert!(summary.latency_p99_ms.is_none());
    assert_eq!(summary.wal_bytes_total, Some(1_600));
    assert!((summary.wal_bytes_per_second.expect("wal rate") - 10.0).abs() < 1e-6);
    assert_eq!(summary.temp_bytes_total, Some(16_000));
    assert!((summary.temp_bytes_per_second.expect("temp rate") - 100.0).abs() < 1e-6);
    assert_eq!(summary.checkpoints_timed_total, Some(11));
    assert_eq!(summary.checkpoints_requested_total, Some(3));
    assert_eq!(summary.checkpoint_requested_ratio, Some(0.5));
    assert!(
        summary
            .checkpoint_mean_interval_seconds
            .map(|v| (v - 30.0).abs() < 1e-6)
            .unwrap_or(false)
    );
}

#[test]
fn workload_summary_handles_zero_elapsed() {
    let timestamp = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
    let sample = WorkloadSample {
        collected_at: timestamp,
        total_xacts: 100.0,
        total_calls: Some(200.0),
        total_time_ms: Some(1_000.0),
        wal_bytes: Some(1_000),
        temp_bytes: Some(2_000),
        checkpoints_timed: Some(5),
        checkpoints_requested: Some(1),
    };
    assert!(WorkloadSummary::from_samples(&sample, &sample).is_none());
}
