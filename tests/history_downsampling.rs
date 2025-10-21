use chrono::Utc;
use pgmon::http::maybe_downsample;
use pgmon::state::TimePoint;

// Validate that the downsampling helper reduces large vectors when over limit.
#[test]
fn history_downsampling_reduces_point_count() {
    // Build synthetic points
    let mut points = Vec::new();
    for i in 0..1500 {
        points.push(TimePoint {
            ts: Utc::now(),
            value: i as f64,
        });
    }
    let (sampled, downsampled) = maybe_downsample(points, 500);
    assert!(downsampled, "expected downsample flag");
    assert!(sampled.len() <= 500, "sampled size <= target");
}
