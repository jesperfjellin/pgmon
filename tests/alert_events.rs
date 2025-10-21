use chrono::Utc;
use pgmon::{OverviewSnapshot, SharedState};

#[tokio::test]
async fn alert_events_start_and_clear() {
    let state = SharedState::new();

    // Initial overview with no alerts
    let mut snap = OverviewSnapshot::default();
    snap.generated_at = Some(Utc::now());
    state.update_overview(snap.clone()).await;
    assert!(state.list_alert_events().await.is_empty());

    // Introduce warn alert
    snap.open_alerts = vec!["Connections at 85% (85/100)".to_string()];
    state.update_overview(snap.clone()).await;
    let events = state.list_alert_events().await;
    assert_eq!(events.len(), 1, "expected start event");
    assert_eq!(events[0].severity, "warn");
    assert!(events[0].cleared_at.is_none());

    // Escalate to crit (warn removed, crit added)
    snap.open_alerts.clear();
    snap.open_crit_alerts = vec!["Connections critical 95% (95/100)".to_string()];
    state.update_overview(snap.clone()).await;
    let events = state.list_alert_events().await;
    // Should have a clear for warn + start for crit → +2 events
    assert_eq!(events.len(), 3, "expected clear + start events");
    // Second event should be start crit or clear warn depending on diff ordering; current implementation pushes starts first.
    assert_eq!(events[1].severity, "crit", "expected crit start second");
    assert!(events[1].cleared_at.is_none());
    // Third event: clear warn
    assert_eq!(events[2].severity, "warn", "expected warn clear last");
    assert!(events[2].cleared_at.is_some());

    // All clear
    snap.open_crit_alerts.clear();
    state.update_overview(snap.clone()).await;
    let events = state.list_alert_events().await;
    assert_eq!(events.len(), 4, "expected clear event for crit");
    let crit_clear = events.last().unwrap();
    assert!(crit_clear.cleared_at.is_some());
}
