# src/profilon/utils/dqx_functions.py
# Exact wording provided by you (from Databricks/DQX docs). No paraphrasing.
# Each check includes:
#   - Check
#   - Description
#   - Arguments: list of {"name": <arg>, "description": <exact text>}
#   - ArgNames: simple list of argument names (for UI compatibility)

def _argnames(args):
    return [a["name"] for a in args]

DQX_FUNCTIONS = {
    "dataset_level_checks": [
        {
            "Check": "is_unique",
            "Description": (
                "Checks whether the values in the input column are unique and reports an issue for each row that "
                "contains a duplicate value. It supports uniqueness check for multiple columns (composite key). "
                "Null values are not considered duplicates by default, following the ANSI SQL standard."
            ),
            "Arguments": [
                {
                    "name": "columns",
                    "description": "columns to check (can be a list of column names or column expressions)",
                },
                {
                    "name": "nulls_distinct",
                    "description": "controls how null values are treated, default is True, thus nulls are not duplicates, "
                                   "eg. (NULL, NULL) not equals (NULL, NULL) and (1, NULL) not equals (1, NULL)",
                },
            ],
        },
        {
            "Check": "is_aggr_not_greater_than",
            "Description": "Checks whether the aggregated values over group of rows or all rows are not greater than the provided limit.",
            "Arguments": [
                {
                    "name": "column",
                    "description": "column to check (can be a string column name or a column expression), optional for 'count' aggregation",
                },
                {
                    "name": "limit",
                    "description": "limit as number, column name or sql expression",
                },
                {
                    "name": "aggr_type",
                    "description": 'aggregation function to use, such as "count" (default), "sum", "avg", "min", and "max"',
                },
                {
                    "name": "group_by",
                    "description": "(optional) list of columns or column expressions to group the rows for aggregation (no grouping by default)",
                },
            ],
        },
        {
            "Check": "is_aggr_not_less_than",
            "Description": "Checks whether the aggregated values over group of rows or all rows are not less than the provided limit.",
            "Arguments": [
                {
                    "name": "column",
                    "description": "column to check (can be a string column name or a column expression), optional for 'count' aggregation",
                },
                {
                    "name": "limit",
                    "description": "limit as number, column name or sql expression",
                },
                {
                    "name": "aggr_type",
                    "description": 'aggregation function to use, such as "count" (default), "sum", "avg", "min", and "max"',
                },
                {
                    "name": "group_by",
                    "description": "(optional) list of columns or column expressions to group the rows for aggregation (no grouping by default)",
                },
            ],
        },
        {
            "Check": "is_aggr_equal",
            "Description": "Checks whether the aggregated values over group of rows or all rows are equal to the provided limit.",
            "Arguments": [
                {
                    "name": "column",
                    "description": "column to check (can be a string column name or a column expression), optional for 'count' aggregation",
                },
                {
                    "name": "limit",
                    "description": "limit as number, column name or sql expression",
                },
                {
                    "name": "aggr_type",
                    "description": 'aggregation function to use, such as "count" (default), "sum", "avg", "min", and "max"',
                },
                {
                    "name": "group_by",
                    "description": "(optional) list of columns or column expressions to group the rows for aggregation (no grouping by default)",
                },
            ],
        },
        {
            "Check": "is_aggr_not_equal",
            "Description": "Checks whether the aggregated values over group of rows or all rows are not equal to the provided limit.",
            "Arguments": [
                {
                    "name": "column",
                    "description": "column to check (can be a string column name or a column expression), optional for 'count' aggregation",
                },
                {
                    "name": "limit",
                    "description": "limit as number, column name or sql expression",
                },
                {
                    "name": "aggr_type",
                    "description": 'aggregation function to use, such as "count" (default), "sum", "avg", "min", and "max"',
                },
                {
                    "name": "group_by",
                    "description": "(optional) list of columns or column expressions to group the rows for aggregation (no grouping by default)",
                },
            ],
        },
        {
            "Check": "foreign_key (aka is_in_list)",
            "Description": (
                "Checks whether input column or columns can be found in the reference DataFrame or Table (foreign key check). "
                "It supports foreign key check on single and composite keys. This check can be used to validate whether values in the "
                "input column(s) exist in a predefined list of allowed values (stored in the reference DataFrame or Table). "
                "It serves as a scalable alternative to is_in_list row-level checks, when working with large lists."
            ),
            "Arguments": [
                {
                    "name": "columns",
                    "description": "columns to check (can be a list of string column names or column expressions)",
                },
                {
                    "name": "ref_columns",
                    "description": "columns to check for existence in the reference DataFrame or Table (can be a list string column name or a column expression)",
                },
                {
                    "name": "ref_df_name",
                    "description": "(optional) name of the reference DataFrame (dictionary of DataFrames can be passed when applying checks)",
                },
                {
                    "name": "ref_table",
                    "description": "(optional) fully qualified reference table name; either ref_df_name or ref_table must be provided but never both",
                },
                {
                    "name": "negate",
                    "description": "if True the condition is negated (i.e. the check fails when the foreign key values exist in the reference DataFrame/Table), "
                                   "if False the check fails when the foreign key values do not exist in the reference",
                },
            ],
        },
        {
            "Check": "sql_query",
            "Description": (
                "Checks whether the condition column produced by a SQL query is satisfied. The check expects the query to return "
                "a boolean condition column indicating whether a record meets the requirement (True = fail, False = pass), and one or more merge columns "
                "so that results can be joined back to the input DataFrame to preserve all original records. Important considerations: if merge columns "
                "aren't unique, multiple query rows can attach to a single input row, potentially causing false positives. Performance tip: since the check "
                "must join back to the input DataFrame to retain original records, writing a custom dataset-level rule is usually more performant than sql_query check."
            ),
            "Arguments": [
                {"name": "query", "description": "query string, must return all merge columns and condition column"},
                {"name": "input_placeholder", "description": "name to be used in the sql query as {{ input_placeholder }} to refer to the input DataFrame, optional reference DataFrames are referred by the name provided in the dictionary of reference DataFrames (e.g. {{ ref_view }}, dictionary of DataFrames can be passed when applying checks)"},
                {"name": "merge_columns", "description": "list of columns used for merging with the input DataFrame which must exist in the input DataFrame and be present in output of the sql query"},
                {"name": "condition_column", "description": "name of the column indicating a violation (False = pass, True = fail)"},
                {"name": "msg", "description": "(optional) message to output"},
                {"name": "name", "description": "(optional) name of the resulting check (it can be overwritten by name specified at the check level)"},
                {"name": "negate", "description": "if the condition should be negated"},
            ],
        },
        {
            "Check": "compare_datasets",
            "Description": (
                "Compares two DataFrames at both row and column levels, providing detailed information about differences, including new or missing rows and column-level changes. "
                "Only columns present in both the source and reference DataFrames are compared. Use with caution if check_missing_records is enabled, as this may increase the number "
                "of rows in the output beyond the original input DataFrame. The comparison does not support Map types (any column comparison on map type is skipped automatically). "
                "Comparing datasets is valuable for validating data during migrations, detecting drift, performing regression testing, or verifying synchronization between source and target systems."
            ),
            "Arguments": [
                {"name": "columns", "description": 'columns to use for row matching with the reference DataFrame (can be a list of string column names or column expressions, but only simple column expressions are allowed such as \'F.col("col1")\'), if not having primary keys or wanting to match against all columns you can pass \'df.columns\''},
                {"name": "ref_columns", "description": 'list of columns in the reference DataFrame or Table to row match against the source DataFrame (can be a list of string column names or column expressions, but only simple column expressions are allowed such as \'F.col("col1")\'), if not having primary keys or wanting to match against all columns you can pass \'ref_df.columns\''},
                {"name": "exclude_columns", "description": "(optional) list of columns to exclude from the value comparison but not from row matching (can be a list of string column names or column expressions, but only simple column expressions are allowed such as 'F.col(\"col1\")'); the exclude_columns field does not alter the list of columns used to determine row matches (columns), it only controls which columns are skipped during the value comparison"},
                {"name": "ref_df_name", "description": "(optional) name of the reference DataFrame (dictionary of DataFrames can be passed when applying checks)"},
                {"name": "ref_table", "description": "(optional) fully qualified reference table name; either ref_df_name or ref_table must be provided but never both"},
                {"name": "check_missing_records", "description": "perform a FULL OUTER JOIN to identify records that are missing from source or reference DataFrames, default is False; use with caution as this may increase the number of rows in the output, as unmatched rows from both sides are included"},
                {"name": "null_safe_row_matching", "description": "(optional) treat NULLs as equal when matching rows using columns and ref_columns (default: True)"},
                {"name": "null_safe_column_value_matching", "description": "(optional) treat NULLs as equal when comparing column values (default: True)"},
            ],
        },
        {
            "Check": "is_data_fresh_per_time_window",
            "Description": "Freshness check that validates whether at least X records arrive within every Y-minute time window.",
            "Arguments": [
                {"name": "column", "description": "timestamp column (can be a string column name or a column expression)"},
                {"name": "window_minutes", "description": "time window in minutes to check for data arrival"},
                {"name": "min_records_per_window", "description": "minimum number of records expected per time window"},
                {"name": "lookback_windows", "description": "(optional) number of time windows to look back from curr_timestamp, it filters records to include only those within the specified number of time windows from curr_timestamp (if no lookback is provided, the check is applied to the entire dataset)"},
                {"name": "curr_timestamp", "description": "(optional) current timestamp column (if not provided, current_timestamp() function is used)"},
            ],
        },
    ],
    "row_level_checks": [
        {
            "Check": "is_not_null",
            "Description": "Checks whether the values in the input column are not null.",
            "Arguments": [{"name": "column", "description": "column to check (can be a string column name or a column expression)"}],
        },
        {
            "Check": "is_not_empty",
            "Description": "Checks whether the values in the input column are not empty (but may be null).",
            "Arguments": [{"name": "column", "description": "column to check (can be a string column name or a column expression)"}],
        },
        {
            "Check": "is_not_null_and_not_empty",
            "Description": "Checks whether the values in the input column are not null and not empty.",
            "Arguments": [
                {"name": "column", "description": "column to check (can be a string column name or a column expression)"},
                {"name": "trim_strings", "description": "optional boolean flag to trim spaces from strings"},
            ],
        },
        {
            "Check": "is_in_list",
            "Description": "Checks whether the values in the input column are present in the list of allowed values (null values are allowed). This check is not suited for large lists of allowed values. In such cases, it’s recommended to use the foreign_key dataset-level check instead.",
            "Arguments": [
                {"name": "column", "description": "column to check (can be a string column name or a column expression)"},
                {"name": "allowed", "description": "list of allowed values"},
            ],
        },
        {
            "Check": "is_not_null_and_is_in_list",
            "Description": "Checks whether the values in the input column are not null and present in the list of allowed values. This check is not suited for large lists of allowed values. In such cases, it’s recommended to use the foreign_key dataset-level check instead.",
            "Arguments": [
                {"name": "column", "description": "column to check (can be a string column name or a column expression)"},
                {"name": "allowed", "description": "list of allowed values"},
            ],
        },
        {
            "Check": "is_not_null_and_not_empty_array",
            "Description": "Checks whether the values in the array input column are not null and not empty.",
            "Arguments": [{"name": "column", "description": "column to check (can be a string column name or a column expression)"}],
        },
        {
            "Check": "is_in_range",
            "Description": "Checks whether the values in the input column are in the provided range (inclusive of both boundaries).",
            "Arguments": [
                {"name": "column", "description": "column to check (can be a string column name or a column expression)"},
                {"name": "min_limit", "description": "min limit as number, date, timestamp, column name or sql expression"},
                {"name": "max_limit", "description": "max limit as number, date, timestamp, column name or sql expression"},
            ],
        },
        {
            "Check": "is_not_in_range",
            "Description": "Checks whether the values in the input column are outside the provided range (inclusive of both boundaries).",
            "Arguments": [
                {"name": "column", "description": "column to check (can be a string column name or a column expression)"},
                {"name": "min_limit", "description": "min limit as number, date, timestamp, column name or sql expression"},
                {"name": "max_limit", "description": "max limit as number, date, timestamp, column name or sql expression"},
            ],
        },
        {
            "Check": "is_not_less_than",
            "Description": "Checks whether the values in the input column are not less than the provided limit.",
            "Arguments": [
                {"name": "column", "description": "column to check (can be a string column name or a column expression)"},
                {"name": "limit", "description": "limit as number, date, timestamp, column name or sql expression"},
            ],
        },
        {
            "Check": "is_not_greater_than",
            "Description": "Checks whether the values in the input column are not greater than the provided limit.",
            "Arguments": [
                {"name": "column", "description": "column to check (can be a string column name or a column expression)"},
                {"name": "limit", "description": "limit as number, date, timestamp, column name or sql expression"},
            ],
        },
        {
            "Check": "is_valid_date",
            "Description": "Checks whether the values in the input column have valid date formats.",
            "Arguments": [
                {"name": "column", "description": "column to check (can be a string column name or a column expression)"},
                {"name": "date_format", "description": "optional date format (e.g. 'yyyy-mm-dd')"},
            ],
        },
        {
            "Check": "is_valid_timestamp",
            "Description": "Checks whether the values in the input column have valid timestamp formats.",
            "Arguments": [
                {"name": "column", "description": "column to check (can be a string column name or a column expression)"},
                {"name": "timestamp_format", "description": "optional timestamp format (e.g. 'yyyy-mm-dd HH:mm:ss')"},
            ],
        },
        {
            "Check": "is_not_in_future",
            "Description": "Checks whether the values in the input column contain a timestamp that is not in the future, where 'future' is defined as current_timestamp + offset (in seconds).",
            "Arguments": [
                {"name": "column", "description": "column to check (can be a string column name or a column expression)"},
                {"name": "offset", "description": "offset to use"},
                {"name": "curr_timestamp", "description": "current timestamp, if not provided current_timestamp() function is used"},
            ],
        },
        {
            "Check": "is_not_in_near_future",
            "Description": "Checks whether the values in the input column contain a timestamp that is not in the near future, where 'near future' is defined as greater than the current timestamp but less than the current_timestamp + offset (in seconds).",
            "Arguments": [
                {"name": "column", "description": "column to check (can be a string column name or a column expression)"},
                {"name": "offset", "description": "offset to use"},
                {"name": "curr_timestamp", "description": "current timestamp, if not provided current_timestamp() function is used"},
            ],
        },
        {
            "Check": "is_older_than_n_days",
            "Description": "Checks whether the values in one input column are at least N days older than the values in another column.",
            "Arguments": [
                {"name": "column", "description": "column to check (can be a string column name or a column expression)"},
                {"name": "days", "description": "number of days"},
                {"name": "curr_date", "description": "current date, if not provided current_date() function is used"},
                {"name": "negate", "description": "if the condition should be negated"},
            ],
        },
        {
            "Check": "is_older_than_col2_for_n_days",
            "Description": "Checks whether the values in one input column are at least N days older than the values in another column.",
            "Arguments": [
                {"name": "column1", "description": "first column to check (can be a string column name or a column expression)"},
                {"name": "column2", "description": "second column to check (can be a string column name or a column expression)"},
                {"name": "days", "description": "number of days"},
                {"name": "negate", "description": "if the condition should be negated"},
            ],
        },
        {
            "Check": "regex_match",
            "Description": "Checks whether the values in the input column match a given regex.",
            "Arguments": [
                {"name": "column", "description": "column to check (can be a string column name or a column expression)"},
                {"name": "regex", "description": "regex to check"},
                {"name": "negate", "description": "if the condition should be negated (true) or not"},
            ],
        },
        {
            "Check": "is_valid_ipv4_address",
            "Description": "Checks whether the values in the input column have valid IPv4 address format.",
            "Arguments": [
                {"name": "column", "description": "column to check (can be a string column name or a column expression)"},
            ],
        },
        {
            "Check": "is_ipv4_address_in_cidr",
            "Description": "Checks whether the values in the input column have valid IPv4 address format and fall within the given CIDR block.",
            "Arguments": [
                {"name": "column", "description": "column to check (can be a string column name or a column expression)"},
                {"name": "cidr_block", "description": "CIDR block string"},
            ],
        },
        {
            "Check": "sql_expression",
            "Description": (
                "Checks whether the values meet the condition provided as an SQL expression, e.g. a = 'str1' and a > b. "
                "SQL expressions are evaluated at runtime, so ensure that the expression is safe and that functions used within it "
                "(e.g. h3_ischildof, division) do not throw exceptions. You can achieve this by validating input arguments or columns "
                "beforehand using guards such as CASE WHEN, IS NOT NULL, RLIKE, or type try casts."
            ),
            "Arguments": [
                {"name": "expression", "description": "sql expression to check on a DataFrame (fail the check if expression evaluates to True, pass if it evaluates to False)"},
                {"name": "msg", "description": "optional message to output"},
                {"name": "name", "description": "optional name of the resulting column (it can be overwritten by name specified at the check level)"},
                {"name": "negate", "description": "if the condition should be negated"},
                {"name": "columns", "description": "optional list of columns to be used for reporting and as name prefix if name not provided, unused in the actual logic"},
            ],
        },
        {
            "Check": "PII detection",
            "Description": "Checks whether the values in the input column contain Personally Identifiable Information (PII). This check is not included in DQX built-in rules to avoid introducing 3rd-party dependencies. An example implementation can be found here.",
            "Arguments": [
                {"name": "column", "description": "column to check (can be a string column name or a column expression)"},
            ],
        },
        {
            "Check": "is_data_fresh",
            "Description": "Checks whether the values in the input timestamp column are not older than the specified number of minutes from the base timestamp column. This is useful for identifying stale data due to delayed pipelines and helps catch upstream issues early.",
            "Arguments": [
                {"name": "column", "description": "column of type timestamp/date to check (can be a string column name or a column expression)"},
                {"name": "max_age_minutes", "description": "maximum age in minutes before data is considered stale"},
                {"name": "base_timestamp", "description": "optional base timestamp column from which the stale check is calculated. This can be a string, column expression, datetime value or literal value ex:F.lit(datetime(2024,1,1)). If not provided current_timestamp() function is used"},
            ],
        },
    ],
}

# Add ArgNames for convenience (keeps older UI happy)
for section in ("dataset_level_checks", "row_level_checks"):
    for item in DQX_FUNCTIONS[section]:
        item["ArgNames"] = _argnames(item["Arguments"])