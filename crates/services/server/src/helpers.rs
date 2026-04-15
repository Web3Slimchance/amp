use datafusion::{
    logical_expr::sqlparser::{ast, ast::Expr},
    sql,
};

/// `SETTINGS` key that enables streaming mode.
const STREAM_SETTING: &str = "stream";

/// `SETTINGS` key that overrides the microbatch max interval.
const MICROBATCH_MAX_INTERVAL_SETTING: &str = "microbatch_max_interval";

/// Check whether a parsed statement contains `SETTINGS stream = true`.
///
/// ```sql
/// SELECT * FROM eth_firehose.blocks SETTINGS stream = true
/// ```
pub fn is_streaming(stmt: &sql::parser::Statement) -> bool {
    let sql::parser::Statement::Statement(box_stmt) = stmt else {
        return false;
    };
    let ast::Statement::Query(query) = box_stmt.as_ref() else {
        return false;
    };
    let Some(settings) = &query.settings else {
        return false;
    };
    let Some(setting) = settings
        .iter()
        .find(|s| s.key.value.eq_ignore_ascii_case(STREAM_SETTING))
    else {
        return false;
    };
    let Expr::Value(v) = &setting.value else {
        return false;
    };
    let ast::Value::Boolean(is_streaming) = v.value else {
        return false;
    };

    is_streaming
}

/// Extract `microbatch_max_interval` from `SETTINGS microbatch_max_interval = <u64>`.
///
/// In single-network mode this value is a block count. In multi-network mode it
/// is interpreted as a second-based interval.
///
/// ```sql
/// SELECT * FROM eth.logs SETTINGS stream = true, microbatch_max_interval = 5000
/// ```
pub fn microbatch_max_interval(stmt: &sql::parser::Statement) -> Option<u64> {
    let sql::parser::Statement::Statement(box_stmt) = stmt else {
        return None;
    };
    let ast::Statement::Query(query) = box_stmt.as_ref() else {
        return None;
    };
    let settings = query.settings.as_ref()?;
    let setting = settings.iter().find(|s| {
        s.key
            .value
            .eq_ignore_ascii_case(MICROBATCH_MAX_INTERVAL_SETTING)
    })?;
    let Expr::Value(v) = &setting.value else {
        return None;
    };
    match &v.value {
        ast::Value::Number(n, _) => n.parse::<u64>().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use common::sql_str::SqlStr;

    use super::*;

    #[test]
    fn is_streaming_with_stream_true_returns_true() {
        //* Given
        let stmt = parse_stmt("SELECT * FROM test SETTINGS stream = true");

        //* When
        let result = is_streaming(&stmt);

        //* Then
        assert!(result, "stream = true should be detected as streaming");
    }

    #[test]
    fn is_streaming_with_uppercase_stream_true_returns_true() {
        //* Given
        let stmt = parse_stmt("SELECT * FROM test SETTINGS STREAM = True");

        //* When
        let result = is_streaming(&stmt);

        //* Then
        assert!(
            result,
            "case-insensitive STREAM = True should be detected as streaming"
        );
    }

    #[test]
    fn is_streaming_with_numeric_value_returns_false() {
        //* Given
        let stmt = parse_stmt("SELECT * FROM test SETTINGS STREAM = 1");

        //* When
        let result = is_streaming(&stmt);

        //* Then
        assert!(
            !result,
            "numeric value 1 should not be detected as streaming"
        );
    }

    #[test]
    fn is_streaming_with_subquery_and_stream_true_returns_true() {
        //* Given
        let stmt = parse_stmt("SELECT * FROM (select * from test) as t SETTINGS stream = true");

        //* When
        let result = is_streaming(&stmt);

        //* Then
        assert!(
            result,
            "subquery with stream = true should be detected as streaming"
        );
    }

    #[test]
    fn is_streaming_without_settings_returns_false() {
        //* Given
        let stmt = parse_stmt("SELECT * FROM test");

        //* When
        let result = is_streaming(&stmt);

        //* Then
        assert!(!result, "query without SETTINGS should not be streaming");
    }

    #[test]
    fn microbatch_max_interval_with_valid_value_returns_some() {
        //* Given
        let stmt =
            parse_stmt("SELECT * FROM test SETTINGS stream = true, microbatch_max_interval = 5000");

        //* When
        let result = microbatch_max_interval(&stmt);

        //* Then
        assert_eq!(
            result,
            Some(5000),
            "should extract microbatch_max_interval from SETTINGS"
        );
    }

    #[test]
    fn microbatch_max_interval_with_uppercase_key_returns_value() {
        //* Given
        let stmt = parse_stmt("SELECT * FROM test SETTINGS MICROBATCH_MAX_INTERVAL = 200");

        //* When
        let result = microbatch_max_interval(&stmt);

        //* Then
        assert_eq!(
            result,
            Some(200),
            "case-insensitive key should be recognized"
        );
    }

    #[test]
    fn microbatch_max_interval_without_setting_returns_none() {
        //* Given
        let stmt = parse_stmt("SELECT * FROM test SETTINGS stream = true");

        //* When
        let result = microbatch_max_interval(&stmt);

        //* Then
        assert_eq!(
            result, None,
            "missing microbatch_max_interval setting should return None"
        );
    }

    #[test]
    fn microbatch_max_interval_without_settings_clause_returns_none() {
        //* Given
        let stmt = parse_stmt("SELECT * FROM test");

        //* When
        let result = microbatch_max_interval(&stmt);

        //* Then
        assert_eq!(
            result, None,
            "query without SETTINGS clause should return None"
        );
    }

    #[test]
    fn microbatch_max_interval_with_boolean_value_returns_none() {
        //* Given
        let stmt = parse_stmt("SELECT * FROM test SETTINGS microbatch_max_interval = true");

        //* When
        let result = microbatch_max_interval(&stmt);

        //* Then
        assert_eq!(
            result, None,
            "boolean value should not be parsed as microbatch_max_interval"
        );
    }

    fn parse_stmt(sql: &str) -> sql::parser::Statement {
        let sql_str: SqlStr = sql.parse().expect("sql should be valid SqlStr");
        common::sql::parse(&sql_str).expect("sql should parse successfully")
    }
}
