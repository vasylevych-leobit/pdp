"""
Task 4: Data quality checks executed directly against the Delta table
(or CSV files in the Volume) using the Databricks SQL connector.
No separate notebook required.

Checks performed:
  1. Row count  > 0
  2. No null event_time values
  3. event_type only contains allowed values
  4. price > 0 for all rows
  5. Duplicate user_session detection (warn, not fail)
"""

import logging
from datetime import datetime

from databricks import sql as databricks_sql

log = logging.getLogger(__name__)

CATALOG = "main"
SCHEMA = "default"
TABLE = "ecommerce_events"

ALLOWED_EVENT_TYPES = {"view", "cart", "purchase"}


def get_connection():
    """
    Reads connection details from environment variables:
      DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH
    """
    import os
    return databricks_sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"].replace("https://", ""),
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )


def scalar(cursor, query: str, params=None):
    cursor.execute(query, params or [])
    row = cursor.fetchone()
    return row[0] if row else None


def run(execution_date_str: str, **kwargs) -> dict:
    """
    Returns a dict of check results.
    Raises AssertionError on any hard failure so Airflow marks the task as failed.
    """
    execution_date = datetime.fromisoformat(execution_date_str)
    partition_filter = (
        f"year = {execution_date.year} "
        f"AND month = {execution_date.month} "
        f"AND day = {execution_date.day} "
        f"AND hour = {execution_date.hour}"
    )

    results = {}

    with get_connection() as conn:
        with conn.cursor() as cur:

            # ── Check 1: row count ──────────────────────────────────────────
            row_count = scalar(
                cur,
                f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.{TABLE} WHERE {partition_filter}",
            )
            results["row_count"] = row_count
            log.info("Check 1 — row count: %d", row_count)
            assert row_count > 0, (
                f"DQ FAIL: row count is 0 for partition "
                f"{execution_date.strftime('%Y-%m-%d %H:00')}"
            )

            # ── Check 2: no null event_time ─────────────────────────────────
            null_event_times = scalar(
                cur,
                f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.{TABLE} "
                f"WHERE {partition_filter} AND event_time IS NULL",
            )
            results["null_event_times"] = null_event_times
            log.info("Check 2 — null event_time: %d", null_event_times)
            assert null_event_times == 0, (
                f"DQ FAIL: {null_event_times} rows have null event_time"
            )

            # ── Check 3: valid event_type values ────────────────────────────
            cur.execute(
                f"SELECT DISTINCT event_type FROM {CATALOG}.{SCHEMA}.{TABLE} "
                f"WHERE {partition_filter}"
            )
            found_types = {row[0] for row in cur.fetchall()}
            invalid_types = found_types - ALLOWED_EVENT_TYPES
            results["invalid_event_types"] = list(invalid_types)
            log.info("Check 3 — event_type values found: %s", found_types)
            assert not invalid_types, (
                f"DQ FAIL: unexpected event_type values: {invalid_types}"
            )

            # ── Check 4: price > 0 ──────────────────────────────────────────
            bad_prices = scalar(
                cur,
                f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.{TABLE} "
                f"WHERE {partition_filter} AND (price IS NULL OR price <= 0)",
            )
            results["bad_prices"] = bad_prices
            log.info("Check 4 — bad prices: %d", bad_prices)
            assert bad_prices == 0, (
                f"DQ FAIL: {bad_prices} rows have price <= 0 or NULL"
            )

            # ── Check 5: duplicate user_session (warning only) ──────────────
            dup_sessions = scalar(
                cur,
                f"""
                SELECT COUNT(*) FROM (
                    SELECT user_session
                    FROM {CATALOG}.{SCHEMA}.{TABLE}
                    WHERE {partition_filter}
                    GROUP BY user_session
                    HAVING COUNT(*) > 1
                )
                """,
            )
            results["duplicate_sessions"] = dup_sessions
            log.info("Check 5 — duplicate user_sessions: %d", dup_sessions)
            if dup_sessions > 0:
                log.warning(
                    "DQ WARNING: %d duplicate user_session values found "
                    "(acceptable for view→cart→purchase funnels)",
                    dup_sessions,
                )

    log.info("All DQ checks passed: %s", results)
    return results
