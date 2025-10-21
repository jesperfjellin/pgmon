use chrono::Utc;
use pgmon::state::{MetricHistory};

#[test]
fn metric_history_ring_buffer_truncates() {
    let mut history = MetricHistory::new(10);
    for i in 0..25 {
        history.record_tps(Utc::now(), i as f64);
    }
    assert_eq!(history.tps.len(), 10, "ring buffer should retain only max_points");
    // Oldest points should be discarded; last value should be 24
    assert!((history.tps.last().unwrap().value - 24.0).abs() < 1e-9);
}
