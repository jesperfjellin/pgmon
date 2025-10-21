use chrono::{TimeZone, Utc};
use pgmon::state::{MetricHistory, TimePoint};

#[test]
fn metric_history_ring_buffer_truncates() {
    let mut history = MetricHistory::new(10);
    for i in 0..25 {
        history.record_tps(Utc::now(), i as f64);
    }
    assert_eq!(
        history.tps.len(),
        10,
        "ring buffer should retain only max_points"
    );
    // Oldest points should be discarded; last value should be 24
    assert!((history.tps.last().unwrap().value - 24.0).abs() < 1e-9);
}

#[test]
fn aggregate_to_daily_creates_summary_for_yesterday() {
    let mut history = MetricHistory::new(1000);

    // Simulate "now" as 2025-01-15 00:30:00 UTC (just after midnight)
    let now = Utc.with_ymd_and_hms(2025, 1, 15, 0, 30, 0).unwrap();

    // Add data points for yesterday (2025-01-14)
    let yesterday = Utc.with_ymd_and_hms(2025, 1, 14, 0, 0, 0).unwrap();

    // Add points throughout the day with known values
    history.tps.push(TimePoint {
        ts: yesterday + chrono::Duration::hours(2),
        value: 100.0,
    });
    history.tps.push(TimePoint {
        ts: yesterday + chrono::Duration::hours(6),
        value: 150.0,
    });
    history.tps.push(TimePoint {
        ts: yesterday + chrono::Duration::hours(12),
        value: 200.0,
    });
    history.tps.push(TimePoint {
        ts: yesterday + chrono::Duration::hours(18),
        value: 50.0,
    });

    // Also add a point for today (should not be included in yesterday's summary)
    history.tps.push(TimePoint {
        ts: now,
        value: 999.0,
    });

    // Run aggregation
    let count = history.aggregate_to_daily(now);

    // Should create 1 daily summary
    assert_eq!(count, 1, "should create exactly 1 daily summary");
    assert_eq!(history.daily.len(), 1, "should have 1 daily summary");

    let summary = &history.daily[0];
    assert_eq!(summary.date, "2025-01-14");

    // mean = (100 + 150 + 200 + 50) / 4 = 125.0
    assert!(
        (summary.mean - 125.0).abs() < 1e-9,
        "mean should be 125.0, got {}",
        summary.mean
    );

    // max = 200.0
    assert!(
        (summary.max - 200.0).abs() < 1e-9,
        "max should be 200.0, got {}",
        summary.max
    );

    // min = 50.0
    assert!(
        (summary.min - 50.0).abs() < 1e-9,
        "min should be 50.0, got {}",
        summary.min
    );
}

#[test]
fn aggregate_to_daily_prunes_old_high_res_points() {
    let mut history = MetricHistory::new(1000);

    let now = Utc.with_ymd_and_hms(2025, 1, 15, 0, 30, 0).unwrap();

    // Add points from 3 days ago (should be pruned - outside 48h window)
    let three_days_ago = now - chrono::Duration::days(3);
    history.tps.push(TimePoint {
        ts: three_days_ago,
        value: 100.0,
    });

    // Add points from yesterday (should be kept - within 48h window)
    let yesterday = now - chrono::Duration::days(1);
    history.tps.push(TimePoint {
        ts: yesterday + chrono::Duration::hours(12),
        value: 150.0,
    });

    // Add points from today (should be kept)
    history.tps.push(TimePoint {
        ts: now,
        value: 200.0,
    });

    let initial_count = history.tps.len();
    assert_eq!(initial_count, 3, "should start with 3 points");

    // Run aggregation
    history.aggregate_to_daily(now);

    // Old points (>48h) should be pruned, keeping only last 48h
    assert_eq!(
        history.tps.len(),
        2,
        "should prune points older than 48 hours, keeping 2"
    );

    // Verify the oldest remaining point is from yesterday
    let oldest_ts = history.tps.iter().map(|p| p.ts).min().unwrap();
    assert!(
        oldest_ts >= now - chrono::Duration::hours(48),
        "oldest point should be within 48h window"
    );
}

#[test]
fn aggregate_to_daily_skips_if_already_exists() {
    let mut history = MetricHistory::new(1000);

    let now = Utc.with_ymd_and_hms(2025, 1, 15, 0, 30, 0).unwrap();
    let yesterday = now - chrono::Duration::days(1);

    // Add yesterday's data
    history.tps.push(TimePoint {
        ts: yesterday + chrono::Duration::hours(12),
        value: 100.0,
    });

    // First aggregation should succeed
    let count1 = history.aggregate_to_daily(now);
    assert_eq!(count1, 1, "first aggregation should create 1 summary");
    assert_eq!(history.daily.len(), 1);

    // Second aggregation with same "now" should skip (already exists)
    let count2 = history.aggregate_to_daily(now);
    assert_eq!(count2, 0, "second aggregation should skip (already exists)");
    assert_eq!(history.daily.len(), 1, "should still have only 1 summary");
}

#[test]
fn aggregate_to_daily_prunes_old_daily_summaries() {
    let mut history = MetricHistory::new(1000);

    let now = Utc.with_ymd_and_hms(2025, 1, 15, 0, 30, 0).unwrap();

    // Manually add daily summaries from various dates
    // One from 40 days ago (should be pruned - outside 30d retention)
    history.daily.push(pgmon::state::DailyMetricSummary {
        date: (now - chrono::Duration::days(40))
            .format("%Y-%m-%d")
            .to_string(),
        mean: 100.0,
        max: 150.0,
        min: 50.0,
    });

    // One from 20 days ago (should be kept - within 30d retention)
    history.daily.push(pgmon::state::DailyMetricSummary {
        date: (now - chrono::Duration::days(20))
            .format("%Y-%m-%d")
            .to_string(),
        mean: 200.0,
        max: 250.0,
        min: 150.0,
    });

    // Add yesterday's high-res data
    let yesterday = now - chrono::Duration::days(1);
    history.tps.push(TimePoint {
        ts: yesterday + chrono::Duration::hours(12),
        value: 300.0,
    });

    assert_eq!(
        history.daily.len(),
        2,
        "should start with 2 daily summaries"
    );

    // Run aggregation (should add yesterday's summary and prune old ones)
    history.aggregate_to_daily(now);

    // Should have 2 summaries: the 20-day-old one and yesterday's new one
    // The 40-day-old one should be pruned
    assert_eq!(
        history.daily.len(),
        2,
        "should have 2 daily summaries after pruning"
    );

    // Verify the 40-day-old summary was pruned
    let has_old = history.daily.iter().any(|d| {
        d.date
            == (now - chrono::Duration::days(40))
                .format("%Y-%m-%d")
                .to_string()
    });
    assert!(!has_old, "should have pruned the 40-day-old summary");
}

#[test]
fn aggregate_to_daily_handles_empty_data() {
    let mut history = MetricHistory::new(1000);

    let now = Utc.with_ymd_and_hms(2025, 1, 15, 0, 30, 0).unwrap();

    // No data for yesterday - should not create a daily summary
    let count = history.aggregate_to_daily(now);

    assert_eq!(count, 0, "should not create summary when no data exists");
    assert_eq!(history.daily.len(), 0, "should have no daily summaries");
}

#[test]
fn aggregate_to_weekly_creates_summary_from_daily_data() {
    let mut history = MetricHistory::new(1000);

    // Simulate "now" as Monday 2025-01-20 00:30:00 UTC (start of week 4)
    let now = Utc.with_ymd_and_hms(2025, 1, 20, 0, 30, 0).unwrap();

    // Add daily summaries for last week (week 3: 2025-01-13 to 2025-01-19)
    // Week 3 in 2025 spans Mon Jan 13 - Sun Jan 19
    for day_offset in 0..7 {
        let date = Utc.with_ymd_and_hms(2025, 1, 13, 0, 0, 0).unwrap()
            + chrono::Duration::days(day_offset);
        history.daily.push(pgmon::state::DailyMetricSummary {
            date: date.format("%Y-%m-%d").to_string(),
            mean: 100.0 + (day_offset as f64) * 10.0, // 100, 110, 120, 130, 140, 150, 160
            max: 200.0,
            min: 50.0,
        });
    }

    // Run weekly aggregation
    let count = history.aggregate_to_weekly(now);

    assert_eq!(count, 1, "should create exactly 1 weekly summary");
    assert_eq!(history.weekly.len(), 1, "should have 1 weekly summary");

    let summary = &history.weekly[0];
    assert_eq!(summary.week, "2025-W03", "should be week 3 of 2025");

    // Weekly mean should be average of daily means: (100+110+120+130+140+150+160)/7 = 130.0
    assert!(
        (summary.mean - 130.0).abs() < 1e-9,
        "weekly mean should be 130.0, got {}",
        summary.mean
    );

    // Weekly max should be max of daily maxes: 200.0
    assert!(
        (summary.max - 200.0).abs() < 1e-9,
        "weekly max should be 200.0, got {}",
        summary.max
    );

    // Weekly min should be min of daily mins: 50.0
    assert!(
        (summary.min - 50.0).abs() < 1e-9,
        "weekly min should be 50.0, got {}",
        summary.min
    );
}

#[test]
fn aggregate_to_weekly_skips_if_already_exists() {
    let mut history = MetricHistory::new(1000);

    let now = Utc.with_ymd_and_hms(2025, 1, 20, 0, 30, 0).unwrap();

    // Add daily summary for last week
    let last_week_date = Utc.with_ymd_and_hms(2025, 1, 13, 0, 0, 0).unwrap();
    history.daily.push(pgmon::state::DailyMetricSummary {
        date: last_week_date.format("%Y-%m-%d").to_string(),
        mean: 100.0,
        max: 150.0,
        min: 50.0,
    });

    // First aggregation should succeed
    let count1 = history.aggregate_to_weekly(now);
    assert_eq!(
        count1, 1,
        "first aggregation should create 1 weekly summary"
    );
    assert_eq!(history.weekly.len(), 1);

    // Second aggregation should skip (already exists)
    let count2 = history.aggregate_to_weekly(now);
    assert_eq!(count2, 0, "second aggregation should skip (already exists)");
    assert_eq!(
        history.weekly.len(),
        1,
        "should still have only 1 weekly summary"
    );
}

#[test]
fn aggregate_to_weekly_prunes_old_weekly_summaries() {
    let mut history = MetricHistory::new(1000);

    let now = Utc.with_ymd_and_hms(2025, 1, 20, 0, 30, 0).unwrap();

    // Add weekly summary from 60 weeks ago (should be pruned - outside 52 week retention)
    history.weekly.push(pgmon::state::WeeklyMetricSummary {
        week: "2023-W40".to_string(),
        mean: 100.0,
        max: 150.0,
        min: 50.0,
    });

    // Add weekly summary from 30 weeks ago (should be kept - within 52 week retention)
    history.weekly.push(pgmon::state::WeeklyMetricSummary {
        week: "2024-W25".to_string(),
        mean: 200.0,
        max: 250.0,
        min: 150.0,
    });

    // Add daily data for last week to trigger aggregation
    let last_week_date = Utc.with_ymd_and_hms(2025, 1, 13, 0, 0, 0).unwrap();
    history.daily.push(pgmon::state::DailyMetricSummary {
        date: last_week_date.format("%Y-%m-%d").to_string(),
        mean: 300.0,
        max: 350.0,
        min: 250.0,
    });

    assert_eq!(
        history.weekly.len(),
        2,
        "should start with 2 weekly summaries"
    );

    // Run aggregation
    history.aggregate_to_weekly(now);

    // Should have 2 summaries: the 30-week-old one and last week's new one
    // The 60-week-old one should be pruned
    assert_eq!(
        history.weekly.len(),
        2,
        "should have 2 weekly summaries after pruning"
    );

    // Verify the old summary was pruned
    let has_very_old = history.weekly.iter().any(|w| w.week == "2023-W40");
    assert!(!has_very_old, "should have pruned the 60-week-old summary");

    // Verify we kept the 30-week-old and the new one
    let has_medium_old = history.weekly.iter().any(|w| w.week == "2024-W25");
    assert!(has_medium_old, "should have kept the 30-week-old summary");
}

#[test]
fn aggregate_to_weekly_handles_missing_daily_data() {
    let mut history = MetricHistory::new(1000);

    let now = Utc.with_ymd_and_hms(2025, 1, 20, 0, 30, 0).unwrap();

    // No daily summaries for last week - should not create a weekly summary
    let count = history.aggregate_to_weekly(now);

    assert_eq!(
        count, 0,
        "should not create weekly summary when no daily data exists"
    );
    assert_eq!(history.weekly.len(), 0, "should have no weekly summaries");
}

#[test]
fn full_aggregation_pipeline_yesterday_to_daily_to_weekly() {
    let mut history = MetricHistory::new(1000);

    // Scenario: We're on Monday 2025-01-20 00:30:00 UTC
    // We want to:
    // 1. Aggregate yesterday (Sunday 2025-01-19) into a daily summary
    // 2. Since we now have a complete week (Mon 2025-01-13 through Sun 2025-01-19), aggregate it to weekly

    let now = Utc.with_ymd_and_hms(2025, 1, 20, 0, 30, 0).unwrap();

    // Add high-res data for yesterday (Sunday 2025-01-19)
    let yesterday = Utc.with_ymd_and_hms(2025, 1, 19, 0, 0, 0).unwrap();
    history.tps.push(TimePoint {
        ts: yesterday + chrono::Duration::hours(6),
        value: 100.0,
    });
    history.tps.push(TimePoint {
        ts: yesterday + chrono::Duration::hours(12),
        value: 200.0,
    });
    history.tps.push(TimePoint {
        ts: yesterday + chrono::Duration::hours(18),
        value: 150.0,
    });

    // Add daily summaries for the rest of last week (Mon-Sat)
    for day_offset in 0..6 {
        let date = Utc.with_ymd_and_hms(2025, 1, 13, 0, 0, 0).unwrap()
            + chrono::Duration::days(day_offset);
        history.daily.push(pgmon::state::DailyMetricSummary {
            date: date.format("%Y-%m-%d").to_string(),
            mean: 120.0,
            max: 180.0,
            min: 80.0,
        });
    }

    // Step 1: Aggregate yesterday into daily summary
    let daily_count = history.aggregate_to_daily(now);
    assert_eq!(daily_count, 1, "should create 1 new daily summary");
    assert_eq!(
        history.daily.len(),
        7,
        "should have 7 daily summaries (complete week)"
    );

    // Verify yesterday's daily summary
    let yesterday_summary = history
        .daily
        .iter()
        .find(|d| d.date == "2025-01-19")
        .unwrap();
    assert!(
        (yesterday_summary.mean - 150.0).abs() < 1e-9,
        "yesterday mean should be 150.0"
    );
    assert!(
        (yesterday_summary.max - 200.0).abs() < 1e-9,
        "yesterday max should be 200.0"
    );
    assert!(
        (yesterday_summary.min - 100.0).abs() < 1e-9,
        "yesterday min should be 100.0"
    );

    // Step 2: Aggregate the complete week into weekly summary
    let weekly_count = history.aggregate_to_weekly(now);
    assert_eq!(weekly_count, 1, "should create 1 new weekly summary");
    assert_eq!(history.weekly.len(), 1, "should have 1 weekly summary");

    // Verify weekly summary
    let weekly_summary = &history.weekly[0];
    assert_eq!(weekly_summary.week, "2025-W03");

    // Weekly mean = (6 days * 120.0 + 1 day * 150.0) / 7 = (720 + 150) / 7 = 124.29
    let expected_mean = (6.0 * 120.0 + 150.0) / 7.0;
    assert!(
        (weekly_summary.mean - expected_mean).abs() < 1e-9,
        "weekly mean should be {}, got {}",
        expected_mean,
        weekly_summary.mean
    );
}
