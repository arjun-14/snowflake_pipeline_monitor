from __future__ import annotations
import pandas as pd

# ── Tool schemas (Anthropic Messages API format for Cortex COMPLETE) ──────────

TOOL_SCHEMAS = [
    {
        "name": "query_task_health",
        "description": (
            "Analyze Snowflake task and pipeline execution health. Returns failure rates, "
            "durations, auto-suspensions, and error patterns from TASK_HISTORY. "
            "Use when asked about pipelines, tasks, job failures, or data pipeline reliability."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "time_window_hours": {
                    "type": "number",
                    "description": "Hours back to look. Default 24. Use 168 for last week.",
                },
                "limit": {"type": "integer", "description": "Max rows. Default 20."},
                "focus": {
                    "type": "string",
                    "enum": ["failures", "duration", "flaky", "all"],
                    "description": (
                        "'failures' = worst failure rates, 'duration' = slowest tasks, "
                        "'flaky' = intermittently failing, 'all' = general overview."
                    ),
                },
            },
            "required": [],
        },
    },
    {
        "name": "query_warehouse_efficiency",
        "description": (
            "Analyze warehouse credit consumption, utilization, and efficiency from "
            "WAREHOUSE_METERING_HISTORY. Use when asked about costs, credits, warehouse "
            "usage, compute spend, or idle warehouses."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "time_window_hours": {"type": "number", "description": "Hours back to look. Default 24."},
                "limit": {"type": "integer", "description": "Max rows. Default 20."},
                "focus": {
                    "type": "string",
                    "enum": ["most_expensive", "idle", "trend", "all"],
                    "description": (
                        "'most_expensive' = top credit consumers, 'idle' = underutilized, "
                        "'trend' = daily credit trend, 'all' = overview."
                    ),
                },
            },
            "required": [],
        },
    },
    {
        "name": "query_performance",
        "description": (
            "Analyze query execution performance including slow queries, cache hit rate, and "
            "data scanned from QUERY_HISTORY. Use when asked about query speed, performance "
            "bottlenecks, cache efficiency, or heavy data-scanning queries."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "time_window_hours": {"type": "number", "description": "Hours back to look. Default 24."},
                "limit": {"type": "integer", "description": "Max rows. Default 20."},
                "focus": {
                    "type": "string",
                    "enum": ["slowest", "most_data_scanned", "failed", "cache_misses", "all"],
                    "description": "Aspect to focus on.",
                },
            },
            "required": [],
        },
    },
    {
        "name": "query_ingestion_health",
        "description": (
            "Analyze data ingestion health from PIPE_USAGE_HISTORY (Snowpipe) and COPY_HISTORY "
            "(bulk COPY INTO). Returns pipe credit usage, files inserted, load success rates. "
            "Use when asked about data ingestion, Snowpipe, data loading, or raw data freshness."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "time_window_hours": {"type": "number", "description": "Hours back to look. Default 24."},
                "limit": {"type": "integer", "description": "Max rows. Default 20."},
                "focus": {
                    "type": "string",
                    "enum": ["pipe_usage", "copy_loads", "failed_loads", "all"],
                    "description": "Which ingestion mechanism to focus on.",
                },
            },
            "required": [],
        },
    },
    {
        "name": "query_transformation_health",
        "description": (
            "Analyze dynamic table refresh health from DYNAMIC_TABLE_REFRESH_HISTORY. Returns "
            "refresh failures, lag, upstream failures, and refresh duration. Use when asked about "
            "dynamic tables, transformation health, data freshness, or downstream pipeline delays."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "time_window_hours": {"type": "number", "description": "Hours back to look. Default 24."},
                "limit": {"type": "integer", "description": "Max rows. Default 20."},
                "focus": {
                    "type": "string",
                    "enum": ["failures", "slowest", "upstream_failures", "all"],
                    "description": "What to focus on.",
                },
            },
            "required": [],
        },
    },
    {
        "name": "query_cost_breakdown",
        "description": (
            "Cross-domain cost breakdown combining warehouse compute credits "
            "(WAREHOUSE_METERING_HISTORY) and data transfer costs (DATA_TRANSFER_HISTORY). "
            "Use when asked about total spend, cost attribution, budget, or where money is going."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "time_window_hours": {
                    "type": "number",
                    "description": "Hours back to look. Default 168 (last week).",
                },
            },
            "required": [],
        },
    },
    {
        "name": "query_ecosystem_anomalies",
        "description": (
            "Detect signals and anomalies across all domains simultaneously: task failures, "
            "warehouse credit spikes, query failures, load failures, and dynamic table failures. "
            "Use when asked about anomalies, incidents, root causes, 'what went wrong', or a "
            "full ecosystem health check."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "time_window_hours": {
                    "type": "number",
                    "description": "Hours back to scan for anomalies. Default 24.",
                },
            },
            "required": [],
        },
    },
]

TOOL_DISPLAY_NAMES = {
    "query_task_health": "Pipeline & Task Health",
    "query_warehouse_efficiency": "Warehouse Efficiency",
    "query_performance": "Query Performance",
    "query_ingestion_health": "Data Ingestion Health",
    "query_transformation_health": "Transformation Health",
    "query_cost_breakdown": "Cost Breakdown",
    "query_ecosystem_anomalies": "Ecosystem Anomalies",
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def _hours(val, default: int = 24) -> int:
    return int(val) if val else default


def _limit(val, default: int = 20) -> int:
    return int(val) if val else default


def execute_tool(name: str, inputs: dict, session) -> tuple[pd.DataFrame, str]:
    executors = {
        "query_task_health": _task_health,
        "query_warehouse_efficiency": _warehouse_efficiency,
        "query_performance": _query_performance,
        "query_ingestion_health": _ingestion_health,
        "query_transformation_health": _transformation_health,
        "query_cost_breakdown": _cost_breakdown,
        "query_ecosystem_anomalies": _ecosystem_anomalies,
    }
    return executors[name](inputs, session)


# ── Tool executors ─────────────────────────────────────────────────────────────

def _task_health(inputs: dict, session) -> tuple[pd.DataFrame, str]:
    h = _hours(inputs.get("time_window_hours"), 24)
    lim = _limit(inputs.get("limit"), 20)
    focus = inputs.get("focus", "all")

    if focus == "failures":
        sql = f"""
            SELECT NAME AS task_name,
                   COUNT(*) AS total_runs,
                   COUNT_IF(STATE IN ('FAILED','FAILED_AND_AUTO_SUSPENDED')) AS failed_runs,
                   ROUND(COUNT_IF(STATE IN ('FAILED','FAILED_AND_AUTO_SUSPENDED'))
                         / NULLIF(COUNT(*), 0) * 100, 2) AS failure_rate_pct,
                   MAX(ERROR_MESSAGE) AS last_error
            FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
            WHERE SCHEDULED_TIME >= DATEADD('hour', -{h}, CURRENT_TIMESTAMP())
            GROUP BY NAME
            HAVING failed_runs > 0
            ORDER BY failure_rate_pct DESC
            LIMIT {lim}
        """
        desc = f"Top failing tasks in the last {h}h"
    elif focus == "duration":
        sql = f"""
            SELECT NAME AS task_name,
                   COUNT(*) AS total_runs,
                   ROUND(AVG(DATEDIFF('second', SCHEDULED_TIME, COMPLETED_TIME)), 1) AS avg_duration_seconds,
                   MAX(DATEDIFF('second', SCHEDULED_TIME, COMPLETED_TIME)) AS max_duration_seconds
            FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
            WHERE SCHEDULED_TIME >= DATEADD('hour', -{h}, CURRENT_TIMESTAMP())
              AND STATE = 'SUCCEEDED'
              AND COMPLETED_TIME IS NOT NULL
            GROUP BY NAME
            ORDER BY avg_duration_seconds DESC
            LIMIT {lim}
        """
        desc = f"Slowest tasks by avg duration in the last {h}h"
    elif focus == "flaky":
        sql = f"""
            SELECT NAME AS task_name,
                   COUNT_IF(STATE = 'SUCCEEDED') AS successful_runs,
                   COUNT_IF(STATE IN ('FAILED','FAILED_AND_AUTO_SUSPENDED')) AS failed_runs,
                   ROUND(COUNT_IF(STATE IN ('FAILED','FAILED_AND_AUTO_SUSPENDED'))
                         / NULLIF(COUNT(*), 0) * 100, 2) AS failure_rate_pct
            FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
            WHERE SCHEDULED_TIME >= DATEADD('hour', -{h}, CURRENT_TIMESTAMP())
            GROUP BY NAME
            HAVING successful_runs > 0 AND failed_runs > 0
            ORDER BY failure_rate_pct DESC
            LIMIT {lim}
        """
        desc = f"Intermittently failing (flaky) tasks in the last {h}h"
    else:
        sql = f"""
            SELECT NAME AS task_name,
                   COUNT(*) AS total_runs,
                   COUNT_IF(STATE = 'SUCCEEDED') AS succeeded,
                   COUNT_IF(STATE IN ('FAILED','FAILED_AND_AUTO_SUSPENDED')) AS failed,
                   COUNT_IF(STATE = 'FAILED_AND_AUTO_SUSPENDED') AS auto_suspended,
                   ROUND(COUNT_IF(STATE IN ('FAILED','FAILED_AND_AUTO_SUSPENDED'))
                         / NULLIF(COUNT(*), 0) * 100, 2) AS failure_rate_pct,
                   ROUND(AVG(DATEDIFF('second', SCHEDULED_TIME, COMPLETED_TIME)), 1) AS avg_duration_seconds
            FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
            WHERE SCHEDULED_TIME >= DATEADD('hour', -{h}, CURRENT_TIMESTAMP())
            GROUP BY NAME
            ORDER BY failed DESC, failure_rate_pct DESC
            LIMIT {lim}
        """
        desc = f"Task execution overview for the last {h}h"

    return session.sql(sql).to_pandas(), desc


def _warehouse_efficiency(inputs: dict, session) -> tuple[pd.DataFrame, str]:
    h = _hours(inputs.get("time_window_hours"), 24)
    lim = _limit(inputs.get("limit"), 20)
    focus = inputs.get("focus", "all")

    if focus == "most_expensive":
        sql = f"""
            SELECT WAREHOUSE_NAME,
                   ROUND(SUM(CREDITS_USED), 4) AS total_credits,
                   ROUND(AVG(CREDITS_USED), 4) AS avg_credits_per_hour,
                   COUNT(*) AS active_hours
            FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
            WHERE START_TIME >= DATEADD('hour', -{h}, CURRENT_TIMESTAMP())
            GROUP BY WAREHOUSE_NAME
            ORDER BY total_credits DESC
            LIMIT {lim}
        """
        desc = f"Most expensive warehouses by credit consumption in the last {h}h"
    elif focus == "idle":
        sql = f"""
            SELECT WAREHOUSE_NAME,
                   ROUND(SUM(CREDITS_USED), 4) AS total_credits,
                   COUNT(*) AS active_hours,
                   ROUND(AVG(CREDITS_USED), 4) AS avg_credits_per_hour
            FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
            WHERE START_TIME >= DATEADD('hour', -{h}, CURRENT_TIMESTAMP())
            GROUP BY WAREHOUSE_NAME
            HAVING total_credits < 0.5
            ORDER BY total_credits ASC
            LIMIT {lim}
        """
        desc = f"Underutilized (near-idle) warehouses in the last {h}h"
    elif focus == "trend":
        sql = f"""
            SELECT START_TIME::DATE AS metering_date,
                   ROUND(SUM(CREDITS_USED), 4) AS total_credits,
                   COUNT(DISTINCT WAREHOUSE_NAME) AS active_warehouses
            FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
            WHERE START_TIME >= DATEADD('hour', -{h}, CURRENT_TIMESTAMP())
            GROUP BY metering_date
            ORDER BY metering_date ASC
        """
        desc = f"Daily credit consumption trend over the last {h}h"
    else:
        sql = f"""
            SELECT WAREHOUSE_NAME,
                   ROUND(SUM(CREDITS_USED), 4) AS total_credits,
                   ROUND(SUM(CREDITS_USED_COMPUTE), 4) AS compute_credits,
                   ROUND(SUM(CREDITS_USED_CLOUD_SERVICES), 4) AS cloud_service_credits,
                   COUNT(*) AS active_hours,
                   ROUND(AVG(CREDITS_USED), 4) AS avg_credits_per_hour
            FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
            WHERE START_TIME >= DATEADD('hour', -{h}, CURRENT_TIMESTAMP())
            GROUP BY WAREHOUSE_NAME
            ORDER BY total_credits DESC
            LIMIT {lim}
        """
        desc = f"Warehouse credit consumption overview for the last {h}h"

    return session.sql(sql).to_pandas(), desc


def _query_performance(inputs: dict, session) -> tuple[pd.DataFrame, str]:
    h = _hours(inputs.get("time_window_hours"), 24)
    lim = _limit(inputs.get("limit"), 20)
    focus = inputs.get("focus", "all")

    if focus == "slowest":
        sql = f"""
            SELECT QUERY_ID, WAREHOUSE_NAME, USER_NAME, QUERY_TYPE,
                   ROUND(TOTAL_ELAPSED_TIME / 1000, 2) AS duration_seconds,
                   ROUND(BYTES_SCANNED / 1073741824.0, 2) AS gb_scanned,
                   EXECUTION_STATUS
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE START_TIME >= DATEADD('hour', -{h}, CURRENT_TIMESTAMP())
              AND QUERY_TYPE NOT IN ('SHOW','DESCRIBE','USE')
            ORDER BY TOTAL_ELAPSED_TIME DESC
            LIMIT {lim}
        """
        desc = f"Slowest queries in the last {h}h"
    elif focus == "most_data_scanned":
        sql = f"""
            SELECT QUERY_ID, WAREHOUSE_NAME, USER_NAME,
                   ROUND(BYTES_SCANNED / 1073741824.0, 2) AS gb_scanned,
                   ROUND(TOTAL_ELAPSED_TIME / 1000, 2) AS duration_seconds,
                   PARTITIONS_SCANNED, PARTITIONS_TOTAL
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE START_TIME >= DATEADD('hour', -{h}, CURRENT_TIMESTAMP())
              AND BYTES_SCANNED > 0
            ORDER BY BYTES_SCANNED DESC
            LIMIT {lim}
        """
        desc = f"Queries scanning the most data in the last {h}h"
    elif focus == "failed":
        sql = f"""
            SELECT QUERY_ID, USER_NAME, WAREHOUSE_NAME, QUERY_TYPE,
                   ERROR_CODE, ERROR_MESSAGE,
                   ROUND(TOTAL_ELAPSED_TIME / 1000, 2) AS duration_seconds
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE START_TIME >= DATEADD('hour', -{h}, CURRENT_TIMESTAMP())
              AND EXECUTION_STATUS = 'FAIL'
            ORDER BY START_TIME DESC
            LIMIT {lim}
        """
        desc = f"Failed queries in the last {h}h"
    elif focus == "cache_misses":
        sql = f"""
            SELECT WAREHOUSE_NAME,
                   COUNT(*) AS total_queries,
                   COUNT_IF(PERCENTAGE_SCANNED_FROM_CACHE < 0.5) AS cache_miss_queries,
                   ROUND(AVG(PERCENTAGE_SCANNED_FROM_CACHE) * 100, 1) AS avg_cache_hit_pct,
                   ROUND(SUM(BYTES_SCANNED) / 1073741824.0, 2) AS total_gb_scanned
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE START_TIME >= DATEADD('hour', -{h}, CURRENT_TIMESTAMP())
              AND BYTES_SCANNED > 0
            GROUP BY WAREHOUSE_NAME
            ORDER BY cache_miss_queries DESC
            LIMIT {lim}
        """
        desc = f"Cache miss analysis by warehouse in the last {h}h"
    else:
        sql = f"""
            SELECT QUERY_ID,
                   WAREHOUSE_NAME,
                   USER_NAME,
                   QUERY_TYPE,
                   ROUND(TOTAL_ELAPSED_TIME / 1000, 2) AS duration_seconds,
                   ROUND(BYTES_SCANNED / 1073741824.0, 2) AS gb_scanned,
                   ROUND(PERCENTAGE_SCANNED_FROM_CACHE * 100, 1) AS cache_hit_pct,
                   EXECUTION_STATUS
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE START_TIME >= DATEADD('hour', -{h}, CURRENT_TIMESTAMP())
              AND QUERY_TYPE NOT IN ('SHOW','DESCRIBE','USE')
            ORDER BY TOTAL_ELAPSED_TIME DESC
            LIMIT {lim}
        """
        desc = f"Slowest individual queries in the last {h}h"

    return session.sql(sql).to_pandas(), desc


def _ingestion_health(inputs: dict, session) -> tuple[pd.DataFrame, str]:
    h = _hours(inputs.get("time_window_hours"), 24)
    lim = _limit(inputs.get("limit"), 20)
    focus = inputs.get("focus", "all")

    if focus == "pipe_usage":
        sql = f"""
            SELECT PIPE_NAME,
                   ROUND(SUM(CREDITS_USED), 4) AS total_credits,
                   SUM(BYTES_INSERTED) AS total_bytes_inserted,
                   SUM(FILES_INSERTED) AS total_files_inserted,
                   ROUND(AVG(CREDITS_USED / NULLIF(FILES_INSERTED, 0)), 6) AS avg_credits_per_file
            FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
            WHERE START_TIME >= DATEADD('hour', -{h}, CURRENT_TIMESTAMP())
            GROUP BY PIPE_NAME
            ORDER BY total_credits DESC
            LIMIT {lim}
        """
        desc = f"Snowpipe credit and volume usage for the last {h}h"
    elif focus == "copy_loads":
        sql = f"""
            SELECT TABLE_CATALOG_NAME AS database_name,
                   TABLE_SCHEMA_NAME AS schema_name,
                   TABLE_NAME,
                   PIPE_NAME,
                   COUNT(*) AS total_loads,
                   COUNT_IF(STATUS = 'Loaded') AS successful_loads,
                   COUNT_IF(STATUS NOT IN ('Loaded','Copy already done')) AS failed_loads,
                   SUM(ROW_COUNT) AS total_rows_loaded
            FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
            WHERE LAST_LOAD_TIME >= DATEADD('hour', -{h}, CURRENT_TIMESTAMP())
            GROUP BY 1,2,3,4
            ORDER BY failed_loads DESC, total_loads DESC
            LIMIT {lim}
        """
        desc = f"COPY INTO load history for the last {h}h"
    elif focus == "failed_loads":
        sql = f"""
            SELECT TABLE_NAME, PIPE_NAME, STAGE_LOCATION,
                   STATUS, ERROR_COUNT, ERROR_LIMIT,
                   LAST_LOAD_TIME
            FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
            WHERE LAST_LOAD_TIME >= DATEADD('hour', -{h}, CURRENT_TIMESTAMP())
              AND STATUS NOT IN ('Loaded','Copy already done','Loading')
            ORDER BY LAST_LOAD_TIME DESC
            LIMIT {lim}
        """
        desc = f"Failed data loads in the last {h}h"
    else:
        sql = f"""
            SELECT 'Snowpipe' AS ingestion_type,
                   PIPE_NAME AS source,
                   ROUND(SUM(CREDITS_USED), 4) AS total_credits,
                   SUM(BYTES_INSERTED) AS bytes_processed,
                   SUM(FILES_INSERTED) AS unit_count,
                   0 AS failed_units
            FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
            WHERE START_TIME >= DATEADD('hour', -{h}, CURRENT_TIMESTAMP())
            GROUP BY PIPE_NAME
            UNION ALL
            SELECT 'COPY INTO' AS ingestion_type,
                   TABLE_NAME AS source,
                   NULL AS total_credits,
                   SUM(ROW_COUNT) AS bytes_processed,
                   COUNT(*) AS unit_count,
                   COUNT_IF(STATUS NOT IN ('Loaded','Copy already done')) AS failed_units
            FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
            WHERE LAST_LOAD_TIME >= DATEADD('hour', -{h}, CURRENT_TIMESTAMP())
            GROUP BY TABLE_NAME
            ORDER BY ingestion_type, total_credits DESC NULLS LAST
            LIMIT {lim}
        """
        desc = f"Data ingestion overview (Snowpipe + COPY INTO) for the last {h}h"

    return session.sql(sql).to_pandas(), desc


def _transformation_health(inputs: dict, session) -> tuple[pd.DataFrame, str]:
    h = _hours(inputs.get("time_window_hours"), 24)
    lim = _limit(inputs.get("limit"), 20)
    focus = inputs.get("focus", "all")

    if focus == "failures":
        sql = f"""
            SELECT NAME AS table_name,
                   DATABASE_NAME, SCHEMA_NAME,
                   STATE, REFRESH_ACTION,
                   REFRESH_START_TIME, REFRESH_END_TIME,
                   DATEDIFF('second', REFRESH_START_TIME, REFRESH_END_TIME) AS duration_seconds
            FROM SNOWFLAKE.ACCOUNT_USAGE.DYNAMIC_TABLE_REFRESH_HISTORY
            WHERE REFRESH_START_TIME >= DATEADD('hour', -{h}, CURRENT_TIMESTAMP())
              AND STATE IN ('FAILED','CANCELLED')
            ORDER BY REFRESH_START_TIME DESC
            LIMIT {lim}
        """
        desc = f"Failed dynamic table refreshes in the last {h}h"
    elif focus == "upstream_failures":
        sql = f"""
            SELECT NAME AS table_name,
                   DATABASE_NAME, SCHEMA_NAME,
                   COUNT(*) AS upstream_failed_refreshes,
                   MAX(REFRESH_START_TIME) AS last_upstream_failure
            FROM SNOWFLAKE.ACCOUNT_USAGE.DYNAMIC_TABLE_REFRESH_HISTORY
            WHERE REFRESH_START_TIME >= DATEADD('hour', -{h}, CURRENT_TIMESTAMP())
              AND STATE = 'UPSTREAM_FAILED'
            GROUP BY 1,2,3
            ORDER BY upstream_failed_refreshes DESC
            LIMIT {lim}
        """
        desc = f"Dynamic tables blocked by upstream failures in the last {h}h"
    elif focus == "slowest":
        sql = f"""
            SELECT NAME AS table_name,
                   DATABASE_NAME, SCHEMA_NAME,
                   COUNT(*) AS total_refreshes,
                   ROUND(AVG(DATEDIFF('second', REFRESH_START_TIME, REFRESH_END_TIME)), 1) AS avg_refresh_seconds,
                   MAX(DATEDIFF('second', REFRESH_START_TIME, REFRESH_END_TIME)) AS max_refresh_seconds
            FROM SNOWFLAKE.ACCOUNT_USAGE.DYNAMIC_TABLE_REFRESH_HISTORY
            WHERE REFRESH_START_TIME >= DATEADD('hour', -{h}, CURRENT_TIMESTAMP())
              AND STATE = 'SUCCEEDED'
              AND REFRESH_END_TIME IS NOT NULL
            GROUP BY 1,2,3
            ORDER BY avg_refresh_seconds DESC
            LIMIT {lim}
        """
        desc = f"Slowest dynamic table refreshes in the last {h}h"
    else:
        sql = f"""
            SELECT NAME AS table_name,
                   DATABASE_NAME, SCHEMA_NAME,
                   COUNT(*) AS total_refreshes,
                   COUNT_IF(STATE = 'SUCCEEDED') AS succeeded,
                   COUNT_IF(STATE IN ('FAILED','CANCELLED')) AS failed,
                   COUNT_IF(STATE = 'UPSTREAM_FAILED') AS upstream_failed,
                   ROUND(COUNT_IF(STATE IN ('FAILED','CANCELLED'))
                         / NULLIF(COUNT(*), 0) * 100, 2) AS failure_rate_pct,
                   ROUND(AVG(DATEDIFF('second', REFRESH_START_TIME, REFRESH_END_TIME)), 1) AS avg_refresh_seconds
            FROM SNOWFLAKE.ACCOUNT_USAGE.DYNAMIC_TABLE_REFRESH_HISTORY
            WHERE REFRESH_START_TIME >= DATEADD('hour', -{h}, CURRENT_TIMESTAMP())
            GROUP BY 1,2,3
            ORDER BY failed DESC, failure_rate_pct DESC
            LIMIT {lim}
        """
        desc = f"Dynamic table refresh overview for the last {h}h"

    return session.sql(sql).to_pandas(), desc


def _cost_breakdown(inputs: dict, session) -> tuple[pd.DataFrame, str]:
    h = _hours(inputs.get("time_window_hours"), 168)

    sql = f"""
        WITH warehouse_costs AS (
            SELECT 'Compute (Warehouses)' AS cost_category,
                   WAREHOUSE_NAME AS resource_name,
                   ROUND(SUM(CREDITS_USED), 4) AS total_credits,
                   NULL AS bytes_transferred
            FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
            WHERE START_TIME >= DATEADD('hour', -{h}, CURRENT_TIMESTAMP())
            GROUP BY WAREHOUSE_NAME
        ),
        transfer_costs AS (
            SELECT 'Data Transfer' AS cost_category,
                   COALESCE(TARGET_CLOUD || ' (' || TARGET_REGION || ')', 'Unknown') AS resource_name,
                   NULL AS total_credits,
                   SUM(BYTES_TRANSFERRED) AS bytes_transferred
            FROM SNOWFLAKE.ACCOUNT_USAGE.DATA_TRANSFER_HISTORY
            WHERE START_TIME >= DATEADD('hour', -{h}, CURRENT_TIMESTAMP())
            GROUP BY 2
        )
        SELECT * FROM warehouse_costs
        UNION ALL
        SELECT * FROM transfer_costs
        ORDER BY cost_category, total_credits DESC NULLS LAST
    """
    return session.sql(sql).to_pandas(), f"Cross-domain cost breakdown for the last {h}h"


def _ecosystem_anomalies(inputs: dict, session) -> tuple[pd.DataFrame, str]:
    h = _hours(inputs.get("time_window_hours"), 24)

    sql = f"""
        WITH task_signals AS (
            SELECT 'Task Failures' AS signal,
                   COUNT_IF(STATE IN ('FAILED','FAILED_AND_AUTO_SUSPENDED')) AS event_count,
                   COUNT(*) AS total,
                   ROUND(COUNT_IF(STATE IN ('FAILED','FAILED_AND_AUTO_SUSPENDED'))
                         / NULLIF(COUNT(*), 0) * 100, 1) AS rate_pct
            FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
            WHERE SCHEDULED_TIME >= DATEADD('hour', -{h}, CURRENT_TIMESTAMP())
        ),
        warehouse_signals AS (
            SELECT 'Credit Consumption' AS signal,
                   ROUND(SUM(CREDITS_USED), 2) AS event_count,
                   COUNT(DISTINCT WAREHOUSE_NAME) AS total,
                   ROUND(AVG(CREDITS_USED), 4) AS rate_pct
            FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
            WHERE START_TIME >= DATEADD('hour', -{h}, CURRENT_TIMESTAMP())
        ),
        query_signals AS (
            SELECT 'Query Failures' AS signal,
                   COUNT_IF(EXECUTION_STATUS = 'FAIL') AS event_count,
                   COUNT(*) AS total,
                   ROUND(COUNT_IF(EXECUTION_STATUS = 'FAIL')
                         / NULLIF(COUNT(*), 0) * 100, 1) AS rate_pct
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE START_TIME >= DATEADD('hour', -{h}, CURRENT_TIMESTAMP())
              AND QUERY_TYPE NOT IN ('SHOW','DESCRIBE','USE')
        ),
        copy_signals AS (
            SELECT 'Load Failures' AS signal,
                   COUNT_IF(STATUS NOT IN ('Loaded','Copy already done')) AS event_count,
                   COUNT(*) AS total,
                   ROUND(COUNT_IF(STATUS NOT IN ('Loaded','Copy already done'))
                         / NULLIF(COUNT(*), 0) * 100, 1) AS rate_pct
            FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
            WHERE LAST_LOAD_TIME >= DATEADD('hour', -{h}, CURRENT_TIMESTAMP())
        ),
        dt_signals AS (
            SELECT 'Dynamic Table Failures' AS signal,
                   COUNT_IF(STATE IN ('FAILED','CANCELLED','UPSTREAM_FAILED')) AS event_count,
                   COUNT(*) AS total,
                   ROUND(COUNT_IF(STATE IN ('FAILED','CANCELLED','UPSTREAM_FAILED'))
                         / NULLIF(COUNT(*), 0) * 100, 1) AS rate_pct
            FROM SNOWFLAKE.ACCOUNT_USAGE.DYNAMIC_TABLE_REFRESH_HISTORY
            WHERE REFRESH_START_TIME >= DATEADD('hour', -{h}, CURRENT_TIMESTAMP())
        )
        SELECT signal, event_count, total, rate_pct
        FROM task_signals
        UNION ALL SELECT * FROM warehouse_signals
        UNION ALL SELECT * FROM query_signals
        UNION ALL SELECT * FROM copy_signals
        UNION ALL SELECT * FROM dt_signals
        ORDER BY event_count DESC
    """
    return session.sql(sql).to_pandas(), f"Cross-ecosystem health signals for the last {h}h"
