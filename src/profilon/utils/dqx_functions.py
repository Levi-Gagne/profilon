# src/profilon/utils/dqx_functions.py

# Dictionary of supported functions and their argument schemas.
# This is the single source of truth used by the app for dropdowns and help.

DQX_FUNCTIONS = {
    "dataset-level_checks": [
        {
            "Check": "is_unique",
            "Description": (
                "Values in the input column(s) are unique. Supports composite keys. "
                "By default NULLs are distinct per ANSI."
            ),
            "Arguments": {
                "columns": "Column(s) to check (list of names or expressions).",
                "nulls_distinct": "Boolean; if True (default), NULLs are not duplicates.",
            },
        },
        {
            "Check": "is_aggr_not_greater_than",
            "Description": "Aggregate value(s) over group/all are not greater than limit.",
            "Arguments": {
                "column": "Column to aggregate (optional for 'count').",
                "limit": "Limit as number, column name or SQL expression.",
                "aggr_type": "Aggregation: count (default), sum, avg, min, max.",
                "group_by": "(Optional) list of cols or expressions to group by.",
            },
        },
        {
            "Check": "is_aggr_not_less_than",
            "Description": "Aggregate value(s) over group/all are not less than limit.",
            "Arguments": {
                "column": "Column to aggregate (optional for 'count').",
                "limit": "Limit as number, column name or SQL expression.",
                "aggr_type": "Aggregation: count (default), sum, avg, min, max.",
                "group_by": "(Optional) list of cols or expressions to group by.",
            },
        },
        {
            "Check": "is_aggr_equal",
            "Description": "Aggregate value(s) equal to the provided limit.",
            "Arguments": {
                "column": "Column to aggregate (optional for 'count').",
                "limit": "Limit as number, column name or SQL expression.",
                "aggr_type": "Aggregation: count (default), sum, avg, min, max.",
                "group_by": "(Optional) list of cols or expressions to group by.",
            },
        },
        {
            "Check": "is_aggr_not_equal",
            "Description": "Aggregate value(s) not equal to the provided limit.",
            "Arguments": {
                "column": "Column to aggregate (optional for 'count').",
                "limit": "Limit as number, column name or SQL expression.",
                "aggr_type": "Aggregation: count (default), sum, avg, min, max.",
                "group_by": "(Optional) list of cols or expressions to group by.",
            },
        },
        {
            "Check": "foreign_key",
            "Description": (
                "Values in input column(s) exist in a reference DataFrame/Table (FK check). "
                "Scalable alternative to is_in_list."
            ),
            "Arguments": {
                "columns": "Column(s) to check (list of names or expressions).",
                "ref_columns": "Reference column(s) to check in the reference DF/Table.",
                "ref_df_name": "(Optional) name of reference DataFrame.",
                "ref_table": "(Optional) fully-qualified reference table name.",
                "negate": "If True, fail when values DO exist in reference.",
            },
        },
        {
            "Check": "sql_query",
            "Description": (
                "Run a SQL query that returns merge columns and a boolean condition column "
                "(True = fail). Merges back to input DF."
            ),
            "Arguments": {
                "query": "SQL string (use {{ input_placeholder }} for the input DF).",
                "input_placeholder": "Name used in SQL for the input DF (default provided upstream).",
                "merge_columns": "List of columns used to merge results back to input DF.",
                "condition_column": "Name of the boolean violation column from the query.",
                "msg": "(Optional) message to output.",
                "name": "(Optional) check name override.",
                "negate": "If True, negate condition.",
            },
        },
        {
            "Check": "compare_datasets",
            "Description": (
                "Compare two datasets for row- and column-level diffs. "
                "Useful for migrations, drift, regression tests."
            ),
            "Arguments": {
                "columns": "Source columns used for row matching (list or expressions).",
                "ref_columns": "Reference columns for matching (order matters; aligns with 'columns').",
                "exclude_columns": "(Optional) columns to skip in value comparison.",
                "ref_df_name": "(Optional) reference DataFrame name.",
                "ref_table": "(Optional) reference table FQN.",
                "check_missing_records": "Boolean; include records missing on either side.",
                "null_safe_row_matching": "Boolean; treat NULLs as equal in row matching (default True).",
                "null_safe_column_value_matching": "Boolean; treat NULLs as equal in value compare (default True).",
            },
        },
        {
            "Check": "is_data_fresh_per_time_window",
            "Description": (
                "Freshness guard: at least X records arrive in every Y-minute window "
                "(optionally within lookback windows)."
            ),
            "Arguments": {
                "column": "Timestamp column (name or expression).",
                "window_minutes": "Time window in minutes.",
                "min_records_per_window": "Minimum records expected per time window.",
                "lookback_windows": "(Optional) number of windows to look back from now.",
                "curr_timestamp": "(Optional) current timestamp column; defaults to current_timestamp().",
            },
        },
    ],
    "row-level_checks": [
        {
            "Check": "is_not_null",
            "Description": "Column values are not NULL.",
            "Arguments": {"column": "Column to check."},
        },
        {
            "Check": "is_not_empty",
            "Description": "Column values are not empty (may be NULL).",
            "Arguments": {"column": "Column to check."},
        },
        {
            "Check": "is_not_null_and_not_empty",
            "Description": "Values are neither NULL nor empty.",
            "Arguments": {"column": "Column to check.", "trim_strings": "Trim spaces before checking (bool)."},
        },
        {
            "Check": "is_in_list",
            "Description": "Values are present in the allowed list (NULL allowed).",
            "Arguments": {"column": "Column to check.", "allowed": "List of allowed values."},
        },
        {
            "Check": "is_not_null_and_is_in_list",
            "Description": "Non-NULL values must be in the allowed list.",
            "Arguments": {"column": "Column to check.", "allowed": "List of allowed values."},
        },
        {
            "Check": "is_not_null_and_not_empty_array",
            "Description": "Array values are not NULL and not empty.",
            "Arguments": {"column": "Array column to check."},
        },
        {
            "Check": "is_in_range",
            "Description": "Values are inside [min_limit, max_limit] (inclusive).",
            "Arguments": {
                "column": "Column to check.",
                "min_limit": "Min (number/date/timestamp/col/expression).",
                "max_limit": "Max (number/date/timestamp/col/expression).",
            },
        },
        {
            "Check": "is_not_in_range",
            "Description": "Values are outside [min_limit, max_limit] (inclusive).",
            "Arguments": {
                "column": "Column to check.",
                "min_limit": "Min (number/date/timestamp/col/expression).",
                "max_limit": "Max (number/date/timestamp/col/expression).",
            },
        },
        {
            "Check": "is_not_less_than",
            "Description": "Values are not less than the provided limit.",
            "Arguments": {"column": "Column to check.", "limit": "Limit (number/date/timestamp/col/expression)."},
        },
        {
            "Check": "is_not_greater_than",
            "Description": "Values are not greater than the provided limit.",
            "Arguments": {"column": "Column to check.", "limit": "Limit (number/date/timestamp/col/expression)."},
        },
        {
            "Check": "is_valid_date",
            "Description": "Values have valid date format.",
            "Arguments": {"column": "Column to check.", "date_format": "(Optional) e.g. 'yyyy-MM-dd'"},
        },
        {
            "Check": "is_valid_timestamp",
            "Description": "Values have valid timestamp format.",
            "Arguments": {"column": "Column to check.", "timestamp_format": "(Optional) e.g. 'yyyy-MM-dd HH:mm:ss'"},
        },
        {
            "Check": "is_not_in_future",
            "Description": "Timestamp is not in the future (using optional offset).",
            "Arguments": {"column": "Timestamp column.", "offset": "Seconds offset.", "curr_timestamp": "(Optional) column."},
        },
        {
            "Check": "is_not_in_near_future",
            "Description": "Timestamp is not in near-future ( > now AND < now+offset ).",
            "Arguments": {"column": "Timestamp column.", "offset": "Seconds offset.", "curr_timestamp": "(Optional) column."},
        },
        {
            "Check": "is_older_than_n_days",
            "Description": "Column values are at least N days older than another date (now or column).",
            "Arguments": {"column": "Column to check.", "days": "Number of days.", "curr_date": "(Optional) date column.", "negate": "Boolean."},
        },
        {
            "Check": "is_older_than_col2_for_n_days",
            "Description": "column1 is at least N days older than column2.",
            "Arguments": {"column1": "First column.", "column2": "Second column.", "days": "Number of days.", "negate": "Boolean."},
        },
        {
            "Check": "regex_match",
            "Description": "Values match a regex (negate to require non-match).",
            "Arguments": {"column": "Column to check.", "regex": "Regex pattern.", "negate": "Boolean."},
        },
        {
            "Check": "is_valid_ipv4_address",
            "Description": "Values have valid IPv4 address format.",
            "Arguments": {"column": "Column to check."},
        },
        {
            "Check": "is_ipv4_address_in_cidr",
            "Description": "IPv4 values fall within a given CIDR block.",
            "Arguments": {"column": "Column to check.", "cidr_block": "CIDR block string."},
        },
        {
            "Check": "sql_expression",
            "Description": (
                "Boolean SQL expression; fail if expression evaluates to True. "
                "Add guards (CASE/IS NOT NULL) to avoid runtime errors."
            ),
            "Arguments": {
                "expression": "SQL expression evaluated on the DataFrame.",
                "msg": "(Optional) message.",
                "name": "(Optional) name override.",
                "negate": "Boolean.",
                "columns": "(Optional) list of columns for reporting/name prefix.",
            },
        },
        {
            "Check": "PII_detection",
            "Description": "Detect PII using a custom implementation (external deps).",
            "Arguments": {"column": "Column to check (example stub)."},
        },
        {
            "Check": "is_data_fresh",
            "Description": "Timestamp column is not older than max_age_minutes relative to base_timestamp (or now).",
            "Arguments": {
                "column": "Timestamp/date column.",
                "max_age_minutes": "Max allowed age in minutes.",
                "base_timestamp": "(Optional) base timestamp (column/expression/datetime).",
            },
        },
    ],
}