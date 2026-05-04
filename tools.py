from __future__ import annotations
import pandas as pd

TOOL_DISPLAY_NAMES = {
    "query_warehouse_efficiency": "Warehouse Credits",
    "query_performance": "Query Performance",
    "query_task_health": "Pipeline Health",
    "query_ingestion_health": "Ingestion Health",
    "query_transformation_health": "Transform Health",
    "query_cost_breakdown": "Cost Breakdown",
    "query_ecosystem_anomalies": "Ecosystem Anomalies",
    "query_storage": "Storage",
    "query_containers": "Containers",
    "query_serverless": "Serverless Tasks",
    "query_data_transfer": "Data Transfer",
    "query_users": "User Spending",
}

_WAREHOUSE_QUERIES = {
    "most_expensive": """
SELECT warehouse_name, warehouse_id, ROUND(SUM(credits_used), 2) AS credits
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD(DAY, -{days}, CURRENT_DATE()) AND start_time < CURRENT_DATE()
GROUP BY warehouse_name, warehouse_id
ORDER BY credits DESC
LIMIT 100""",
    "idle": """
WITH recent AS (
    SELECT warehouse_name, SUM(credits_used) AS credits
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE start_time >= DATEADD(DAY, -{days}, CURRENT_DATE())
    GROUP BY warehouse_name
),
prior AS (
    SELECT warehouse_name, SUM(credits_used) AS credits
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE start_time >= DATEADD(DAY, -{prior_days}, CURRENT_DATE())
        AND start_time < DATEADD(DAY, -{days}, CURRENT_DATE())
    GROUP BY warehouse_name
)
SELECT
    COALESCE(r.warehouse_name, p.warehouse_name) AS warehouse_name,
    ROUND(COALESCE(p.credits, 0), 2) AS prior_period,
    ROUND(COALESCE(r.credits, 0), 2) AS recent_period,
    ROUND(COALESCE(r.credits, 0) - COALESCE(p.credits, 0), 2) AS change,
    ROUND(((COALESCE(r.credits, 0) - COALESCE(p.credits, 0)) / NULLIF(p.credits, 0)) * 100, 1) AS pct_change
FROM recent r
FULL OUTER JOIN prior p ON r.warehouse_name = p.warehouse_name
ORDER BY recent_period ASC
LIMIT 15""",
    "trend": """
SELECT DATE_TRUNC('DAY', start_time) AS day, ROUND(SUM(credits_used), 2) AS credits
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD(DAY, -{days}, CURRENT_DATE()) AND start_time < CURRENT_DATE()
GROUP BY day
ORDER BY day""",
}

_PERFORMANCE_QUERIES = {
    "slowest": """
SELECT query_id, warehouse_name, user_name,
    ROUND(credits_attributed_compute, 2) AS credits_compute,
    ROUND(credits_used_query_acceleration, 2) AS credits_qas, start_time
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY
WHERE start_time >= DATEADD(HOUR, -{hours}, CURRENT_TIMESTAMP())
    AND credits_attributed_compute > 0
ORDER BY credits_attributed_compute DESC
LIMIT 15""",
    "most_data_scanned": """
SELECT query_id, query_type, warehouse_name, user_name,
    ROUND(bytes_scanned / POW(1024, 3), 2) AS gb_scanned,
    total_elapsed_time / 1000 AS elapsed_seconds
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD(HOUR, -{hours}, CURRENT_TIMESTAMP()) AND bytes_scanned > 0
ORDER BY bytes_scanned DESC
LIMIT 20""",
    "failed": """
SELECT query_id, query_type, warehouse_name, user_name, error_code, error_message, start_time
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD(HOUR, -{hours}, CURRENT_TIMESTAMP()) AND execution_status = 'FAIL'
ORDER BY start_time DESC
LIMIT 20""",
    "cache_misses": """
SELECT warehouse_name, COUNT(*) AS total_queries,
    SUM(CASE WHEN bytes_scanned > 0 THEN 1 ELSE 0 END) AS cache_misses,
    ROUND(SUM(CASE WHEN bytes_scanned > 0 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS miss_pct
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD(HOUR, -{hours}, CURRENT_TIMESTAMP()) AND warehouse_name IS NOT NULL
GROUP BY warehouse_name
ORDER BY miss_pct DESC
LIMIT 15""",
    "all": """
SELECT query_parameterized_hash,
    ROUND(SUM(credits_attributed_compute), 2) AS total_credits,
    COUNT(query_id) AS execution_count,
    ROUND(SUM(credits_attributed_compute) / NULLIF(COUNT(query_id), 0), 4) AS avg_credits_per_execution,
    ANY_VALUE(query_id) AS example_query_id
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY
WHERE start_time >= DATEADD(HOUR, -{hours}, CURRENT_TIMESTAMP()) AND query_parameterized_hash IS NOT NULL
GROUP BY query_parameterized_hash
ORDER BY total_credits DESC
LIMIT 10""",
}

_TASK_HEALTH_QUERIES = {
    "failures": """
SELECT name, database_name, schema_name, state, error_code, error_message, scheduled_time, completed_time
FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
WHERE scheduled_time >= DATEADD(HOUR, -{hours}, CURRENT_TIMESTAMP()) AND state = 'FAILED'
ORDER BY scheduled_time DESC
LIMIT 20""",
    "duration": """
SELECT name, database_name, schema_name, COUNT(*) AS run_count,
    ROUND(AVG(DATEDIFF('second', query_start_time, completed_time)), 1) AS avg_duration_sec,
    ROUND(MAX(DATEDIFF('second', query_start_time, completed_time)), 1) AS max_duration_sec
FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
WHERE scheduled_time >= DATEADD(HOUR, -{hours}, CURRENT_TIMESTAMP()) AND state = 'SUCCEEDED'
GROUP BY name, database_name, schema_name
ORDER BY avg_duration_sec DESC
LIMIT 15""",
    "flaky": """
SELECT name, database_name, schema_name, COUNT(*) AS total_runs,
    SUM(CASE WHEN state = 'FAILED' THEN 1 ELSE 0 END) AS failures,
    ROUND(SUM(CASE WHEN state = 'FAILED' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS failure_rate_pct
FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
WHERE scheduled_time >= DATEADD(HOUR, -{hours}, CURRENT_TIMESTAMP())
GROUP BY name, database_name, schema_name
HAVING total_runs >= 3 AND failures >= 1 AND failure_rate_pct BETWEEN 10 AND 90
ORDER BY failure_rate_pct DESC
LIMIT 15""",
}

_INGESTION_QUERIES = {
    "all": """
SELECT pipe_name, pipe_catalog_name AS database_name,
    ROUND(SUM(credits_used), 2) AS credits, SUM(files_inserted) AS files, SUM(bytes_inserted) AS bytes_inserted
FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
WHERE start_time >= DATEADD(HOUR, -{hours}, CURRENT_TIMESTAMP())
GROUP BY pipe_name, pipe_catalog_name
ORDER BY credits DESC
LIMIT 20""",
    "failed_loads": """
SELECT table_catalog_name AS database_name, table_schema_name AS schema_name,
    table_name, file_name, status, error_count, first_error_message, last_load_time
FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
WHERE last_load_time >= DATEADD(HOUR, -{hours}, CURRENT_TIMESTAMP()) AND status != 'Loaded'
ORDER BY last_load_time DESC
LIMIT 20""",
    "copy_loads": """
SELECT table_catalog_name AS database_name, table_schema_name AS schema_name,
    table_name, COUNT(*) AS load_count, SUM(row_count) AS total_rows, SUM(file_size) AS total_bytes
FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
WHERE last_load_time >= DATEADD(HOUR, -{hours}, CURRENT_TIMESTAMP()) AND status = 'Loaded'
GROUP BY table_catalog_name, table_schema_name, table_name
ORDER BY total_rows DESC
LIMIT 20""",
    "pipe_usage": """
SELECT pipe_name, pipe_catalog_name AS database_name,
    ROUND(SUM(credits_used), 2) AS credits, SUM(files_inserted) AS files
FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
WHERE start_time >= DATEADD(HOUR, -{hours}, CURRENT_TIMESTAMP())
GROUP BY pipe_name, pipe_catalog_name
ORDER BY credits DESC
LIMIT 20""",
}

_TRANSFORMATION_QUERIES = {
    "all": """
SELECT name, database_name, schema_name, state, state_message, refresh_trigger,
    ROUND(statistics:numInsertedRows, 0) AS rows_inserted,
    DATEDIFF('second', refresh_start_time, refresh_end_time) AS duration_sec
FROM SNOWFLAKE.ACCOUNT_USAGE.DYNAMIC_TABLE_REFRESH_HISTORY
WHERE refresh_start_time >= DATEADD(HOUR, -{hours}, CURRENT_TIMESTAMP())
ORDER BY refresh_start_time DESC
LIMIT 20""",
    "failures": """
SELECT name, database_name, schema_name, state, state_message, refresh_start_time, refresh_end_time
FROM SNOWFLAKE.ACCOUNT_USAGE.DYNAMIC_TABLE_REFRESH_HISTORY
WHERE refresh_start_time >= DATEADD(HOUR, -{hours}, CURRENT_TIMESTAMP()) AND state = 'FAILED'
ORDER BY refresh_start_time DESC
LIMIT 20""",
    "slowest": """
SELECT name, database_name, schema_name, COUNT(*) AS refresh_count,
    ROUND(AVG(DATEDIFF('second', refresh_start_time, refresh_end_time)), 1) AS avg_duration_sec,
    ROUND(MAX(DATEDIFF('second', refresh_start_time, refresh_end_time)), 1) AS max_duration_sec
FROM SNOWFLAKE.ACCOUNT_USAGE.DYNAMIC_TABLE_REFRESH_HISTORY
WHERE refresh_start_time >= DATEADD(HOUR, -{hours}, CURRENT_TIMESTAMP()) AND state = 'SUCCEEDED'
GROUP BY name, database_name, schema_name
ORDER BY avg_duration_sec DESC
LIMIT 15""",
    "upstream_failures": """
SELECT name, database_name, schema_name, state, state_message, refresh_start_time
FROM SNOWFLAKE.ACCOUNT_USAGE.DYNAMIC_TABLE_REFRESH_HISTORY
WHERE refresh_start_time >= DATEADD(HOUR, -{hours}, CURRENT_TIMESTAMP())
    AND state_message ILIKE '%upstream%'
ORDER BY refresh_start_time DESC
LIMIT 15""",
}

_STORAGE_QUERIES = {
    "top_databases": """
SELECT database_name,
    ROUND(AVG(database_bytes) / POW(1024, 3), 2) AS avg_storage_gb,
    ROUND(AVG(failsafe_bytes) / POW(1024, 3), 2) AS avg_failsafe_gb
FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY
WHERE usage_date >= DATEADD(DAY, -30, CURRENT_DATE())
GROUP BY database_name
ORDER BY avg_storage_gb DESC
LIMIT 20""",
    "top_tables": """
SELECT table_catalog AS database_name, table_schema AS schema_name, table_name,
    ROUND(active_bytes / POW(1024, 3), 2) AS active_gb,
    ROUND(time_travel_bytes / POW(1024, 3), 2) AS time_travel_gb,
    ROUND(failsafe_bytes / POW(1024, 3), 2) AS failsafe_gb
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
WHERE active_bytes > 0
ORDER BY active_bytes DESC
LIMIT 20""",
}

_CONTAINER_QUERIES = {
    "top_pools": """
SELECT compute_pool_name,
    ROUND(SUM(credits_used), 2) AS total_credits,
    COUNT(DISTINCT instance_id) AS instance_count
FROM SNOWFLAKE.ACCOUNT_USAGE.SNOWPARK_CONTAINER_SERVICES_HISTORY
WHERE start_time >= DATEADD(HOUR, -{hours}, CURRENT_TIMESTAMP())
GROUP BY compute_pool_name
ORDER BY total_credits DESC
LIMIT 20""",
}

_SERVERLESS_QUERIES = {
    "top_tasks": """
SELECT name, database_name, schema_name,
    ROUND(SUM(credits_used), 2) AS total_credits,
    COUNT(*) AS execution_count,
    ROUND(AVG(credits_used), 4) AS avg_credits_per_run
FROM SNOWFLAKE.ACCOUNT_USAGE.SERVERLESS_TASK_HISTORY
WHERE start_time >= DATEADD(HOUR, -{hours}, CURRENT_TIMESTAMP())
GROUP BY name, database_name, schema_name
ORDER BY total_credits DESC
LIMIT 20""",
}

_DATA_TRANSFER_QUERY = """
SELECT source_cloud, source_region, target_cloud, target_region,
    ROUND(SUM(bytes_transferred) / POW(1024, 3), 2) AS gb_transferred,
    ROUND(SUM(credits_used), 2) AS credits_used
FROM SNOWFLAKE.ACCOUNT_USAGE.DATA_TRANSFER_HISTORY
WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE())
GROUP BY source_cloud, source_region, target_cloud, target_region
ORDER BY credits_used DESC
LIMIT 20"""

_USER_QUERIES = {
    "top_spenders": """
SELECT user_name,
    ROUND(SUM(credits_attributed_compute), 2) AS compute_credits,
    COUNT(DISTINCT query_id) AS query_count,
    ROUND(SUM(credits_attributed_compute) / NULLIF(COUNT(DISTINCT query_id), 0), 4) AS avg_credits_per_query
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY
WHERE start_time >= DATEADD(HOUR, -{hours}, CURRENT_TIMESTAMP())
    AND user_name IS NOT NULL
GROUP BY user_name
ORDER BY compute_credits DESC
LIMIT 20""",
    "by_warehouse": """
SELECT user_name, warehouse_name,
    ROUND(SUM(credits_attributed_compute), 2) AS compute_credits,
    COUNT(DISTINCT query_id) AS query_count
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY
WHERE start_time >= DATEADD(HOUR, -{hours}, CURRENT_TIMESTAMP())
    AND user_name IS NOT NULL AND warehouse_name IS NOT NULL
GROUP BY user_name, warehouse_name
ORDER BY compute_credits DESC
LIMIT 20""",
}

_COST_BREAKDOWN_QUERY = """
SELECT service_type, ROUND(SUM(credits_used), 2) AS total_credits,
    ROUND(SUM(credits_used) / SUM(SUM(credits_used)) OVER () * 100, 1) AS percentage_of_total
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
WHERE start_time >= DATEADD(HOUR, -{hours}, CURRENT_TIMESTAMP())
GROUP BY service_type
ORDER BY total_credits DESC"""

_ECOSYSTEM_ANOMALIES_QUERY = """
WITH wow AS (
    SELECT 'current_week' AS period, ROUND(IFNULL(SUM(credits_used), 0), 2) AS credits_used
    FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
    WHERE start_time >= DATEADD(DAY, -7, CURRENT_DATE()) AND start_time < CURRENT_DATE()
    UNION ALL
    SELECT 'previous_week' AS period, ROUND(IFNULL(SUM(credits_used), 0), 2) AS credits_used
    FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
    WHERE start_time >= DATEADD(DAY, -14, CURRENT_DATE()) AND start_time < DATEADD(DAY, -7, CURRENT_DATE())
),
service_breakdown AS (
    SELECT service_type, ROUND(SUM(credits_used), 2) AS total_credits
    FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
    WHERE start_time >= DATEADD(HOUR, -{hours}, CURRENT_TIMESTAMP())
    GROUP BY service_type
),
top_warehouses AS (
    SELECT warehouse_name, ROUND(SUM(credits_used), 2) AS credits
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE start_time >= DATEADD(HOUR, -{hours}, CURRENT_TIMESTAMP())
    GROUP BY warehouse_name ORDER BY credits DESC LIMIT 5
),
failed_tasks AS (
    SELECT COUNT(*) AS failed_count
    FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
    WHERE scheduled_time >= DATEADD(HOUR, -{hours}, CURRENT_TIMESTAMP()) AND state = 'FAILED'
)
SELECT 'week_over_week' AS category, period AS item, credits_used AS value FROM wow
UNION ALL SELECT 'service_breakdown', service_type, total_credits FROM service_breakdown
UNION ALL SELECT 'top_warehouses', warehouse_name, credits FROM top_warehouses
UNION ALL SELECT 'failed_tasks', 'total_failures', failed_count FROM failed_tasks"""


def _hours_to_days(hours: int) -> int:
    return max(1, hours // 24)


def execute_tool(name: str, inputs: dict, session) -> tuple[pd.DataFrame, str]:
    hours = inputs.get("time_window_hours", 24)
    focus = inputs.get("focus", "all")
    days = _hours_to_days(hours)
    prior_days = days * 2

    if name == "query_warehouse_efficiency":
        sql = _WAREHOUSE_QUERIES.get(focus, _WAREHOUSE_QUERIES["most_expensive"]).format(days=days, prior_days=prior_days)
        desc = f"Warehouse credits ({focus}) - last {days}d"
    elif name == "query_performance":
        sql = _PERFORMANCE_QUERIES.get(focus, _PERFORMANCE_QUERIES["all"]).format(hours=hours)
        desc = f"Query performance ({focus}) - last {hours}h"
    elif name == "query_task_health":
        sql = _TASK_HEALTH_QUERIES.get(focus, _TASK_HEALTH_QUERIES["failures"]).format(hours=hours)
        desc = f"Task health ({focus}) - last {hours}h"
    elif name == "query_ingestion_health":
        sql = _INGESTION_QUERIES.get(focus, _INGESTION_QUERIES["all"]).format(hours=hours)
        desc = f"Ingestion ({focus}) - last {hours}h"
    elif name == "query_transformation_health":
        sql = _TRANSFORMATION_QUERIES.get(focus, _TRANSFORMATION_QUERIES["all"]).format(hours=hours)
        desc = f"Dynamic tables ({focus}) - last {hours}h"
    elif name == "query_cost_breakdown":
        sql = _COST_BREAKDOWN_QUERY.format(hours=hours)
        desc = f"Cost breakdown by service - last {hours}h"
    elif name == "query_ecosystem_anomalies":
        sql = _ECOSYSTEM_ANOMALIES_QUERY.format(hours=hours)
        desc = f"Ecosystem overview - last {hours}h"
    elif name == "query_storage":
        sql = _STORAGE_QUERIES.get(focus, _STORAGE_QUERIES["top_databases"])
        desc = f"Storage ({focus})"
    elif name == "query_containers":
        sql = _CONTAINER_QUERIES.get(focus, _CONTAINER_QUERIES["top_pools"]).format(hours=hours)
        desc = f"Containers ({focus})"
    elif name == "query_serverless":
        sql = _SERVERLESS_QUERIES.get(focus, _SERVERLESS_QUERIES["top_tasks"]).format(hours=hours)
        desc = f"Serverless tasks ({focus})"
    elif name == "query_data_transfer":
        sql = _DATA_TRANSFER_QUERY
        desc = "Data transfer by region"
    elif name == "query_users":
        sql = _USER_QUERIES.get(focus, _USER_QUERIES["top_spenders"]).format(hours=hours)
        desc = f"User spending ({focus})"
    else:
        raise ValueError(f"Unknown tool: {name}")

    df = session.sql(sql).to_pandas()
    return df, desc