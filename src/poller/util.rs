use sqlx::error::DatabaseError;

pub fn is_missing_pg_stat_statements(error: &sqlx::Error) -> bool {
    matches!(error, sqlx::Error::Database(db_err) if is_missing_relation(db_err.as_ref()))
}

pub fn is_missing_relation(error: &dyn DatabaseError) -> bool {
    if let Some(code) = error.code() {
        code == "42P01" || code == "42704"
    } else {
        false
    }
}

pub fn to_optional_positive(value: Option<f64>) -> Option<f64> {
    value.and_then(|v| if v > 0.0 { Some(v) } else { None })
}

pub fn is_missing_column(error: &dyn DatabaseError) -> bool {
    if let Some(code) = error.code() {
        code == "42703"
    } else {
        false
    }
}

pub fn is_missing_function(error: &dyn DatabaseError) -> bool {
    if let Some(code) = error.code() {
        code == "42883"
    } else {
        false
    }
}
