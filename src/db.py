"""
SQLite database layer for Anthropic Usage Stats Collector.

Provides functions for:
- Database initialization with schema
- UPSERT operations for usage and cost records
- Sync logging
- Plan limits management
- Daily and weekly aggregation queries

All functions use dependency injection: they accept a sqlite3.Connection
as the first argument rather than maintaining global state.
"""

import sqlite3
from datetime import datetime, timedelta
from typing import Optional


def init_db(db_path: str) -> sqlite3.Connection:
    """
    Initialize SQLite database and create all required tables.

    Args:
        db_path: Path to SQLite database file (or ":memory:" for in-memory DB)

    Returns:
        sqlite3.Connection: Database connection with WAL mode enabled

    Creates 4 tables if they don't exist:
    - usage_records: Token usage snapshots by bucket
    - cost_records: Cost breakdowns by bucket
    - plan_limits: Daily/weekly token and cost limits
    - sync_log: Sync operation audit trail
    """
    conn = sqlite3.connect(db_path)

    # Enable WAL mode for better concurrency with multiple readers/writers
    conn.execute("PRAGMA journal_mode=WAL;")

    cursor = conn.cursor()

    # Create usage_records table: stores token usage snapshots
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS usage_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fetched_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
        bucket_start TEXT NOT NULL,
        bucket_end TEXT NOT NULL,
        model TEXT,
        workspace_id TEXT,
        api_key_id TEXT,
        service_tier TEXT,
        context_window TEXT,
        inference_geo TEXT,
        uncached_input_tokens INTEGER DEFAULT 0,
        cache_read_input_tokens INTEGER DEFAULT 0,
        cache_creation_1h_tokens INTEGER DEFAULT 0,
        cache_creation_5m_tokens INTEGER DEFAULT 0,
        output_tokens INTEGER DEFAULT 0,
        web_search_requests INTEGER DEFAULT 0,
        UNIQUE(bucket_start, bucket_end, model, workspace_id, api_key_id, service_tier)
    )
    """
    )

    # Create cost_records table: stores cost breakdowns
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS cost_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fetched_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
        bucket_start TEXT NOT NULL,
        bucket_end TEXT NOT NULL,
        model TEXT,
        workspace_id TEXT,
        service_tier TEXT,
        cost_type TEXT,
        token_type TEXT,
        amount_cents REAL NOT NULL,
        currency TEXT DEFAULT 'USD',
        description TEXT,
        UNIQUE(bucket_start, bucket_end, model, workspace_id, service_tier, cost_type, token_type)
    )
    """
    )

    # Create plan_limits table: stores token/cost limits by date
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS plan_limits (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        effective_date TEXT NOT NULL DEFAULT (date('now')),
        daily_token_limit INTEGER,
        daily_cost_limit_cents INTEGER,
        weekly_token_limit INTEGER,
        weekly_cost_limit_cents INTEGER,
        notes TEXT,
        UNIQUE(effective_date)
    )
    """
    )

    # Create sync_log table: audit trail of sync operations
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS sync_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
        status TEXT NOT NULL,
        records_fetched INTEGER DEFAULT 0,
        error_message TEXT
    )
    """
    )

    conn.commit()
    return conn


def upsert_usage_records(conn: sqlite3.Connection, records: list[dict]) -> int:
    """
    Insert or replace usage records (UPSERT).

    Maps API response fields to database columns:
    - uncached_input_tokens → uncached_input_tokens
    - cache_read_input_tokens → cache_read_input_tokens
    - snapshot["cache_creation"]["ephemeral_1h_input_tokens"] → cache_creation_1h_tokens
    - snapshot["cache_creation"]["ephemeral_5m_input_tokens"] → cache_creation_5m_tokens
    - output_tokens → output_tokens
    - snapshot["server_tool_use"]["web_search_requests"] → web_search_requests
    - Other fields map directly by name

    Args:
        conn: sqlite3.Connection
        records: List of dicts with usage data

    Returns:
        int: Number of records upserted
    """
    cursor = conn.cursor()
    count = 0

    for record in records:
        # Extract nested fields from the API response structure
        cache_creation = record.get("cache_creation", {})
        cache_1h_tokens = cache_creation.get("ephemeral_1h_input_tokens", 0)
        cache_5m_tokens = cache_creation.get("ephemeral_5m_input_tokens", 0)

        server_tool_use = record.get("server_tool_use", {})
        web_search_requests = server_tool_use.get("web_search_requests", 0)

        # Prepare values with safe defaults
        cursor.execute(
            """
        INSERT OR REPLACE INTO usage_records (
            bucket_start,
            bucket_end,
            model,
            workspace_id,
            api_key_id,
            service_tier,
            context_window,
            inference_geo,
            uncached_input_tokens,
            cache_read_input_tokens,
            cache_creation_1h_tokens,
            cache_creation_5m_tokens,
            output_tokens,
            web_search_requests
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                record.get("bucket_start"),
                record.get("bucket_end"),
                record.get("model"),
                record.get("workspace_id"),
                record.get("api_key_id"),
                record.get("service_tier"),
                record.get("context_window"),
                record.get("inference_geo"),
                record.get("uncached_input_tokens", 0),
                record.get("cache_read_input_tokens", 0),
                cache_1h_tokens,
                cache_5m_tokens,
                record.get("output_tokens", 0),
                web_search_requests,
            ),
        )
        count += 1

    conn.commit()
    return count


def upsert_cost_records(conn: sqlite3.Connection, records: list[dict]) -> int:
    """
    Insert or replace cost records (UPSERT).

    Maps the "amount" field to "amount_cents" column.

    Args:
        conn: sqlite3.Connection
        records: List of dicts with cost data

    Returns:
        int: Number of records upserted
    """
    cursor = conn.cursor()
    count = 0

    for record in records:
        cursor.execute(
            """
        INSERT OR REPLACE INTO cost_records (
            bucket_start,
            bucket_end,
            model,
            workspace_id,
            service_tier,
            cost_type,
            token_type,
            amount_cents,
            currency,
            description
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                record.get("bucket_start"),
                record.get("bucket_end"),
                record.get("model"),
                record.get("workspace_id"),
                record.get("service_tier"),
                record.get("cost_type"),
                record.get("token_type"),
                record.get("amount"),  # Map "amount" field to "amount_cents"
                record.get("currency", "USD"),
                record.get("description"),
            ),
        )
        count += 1

    conn.commit()
    return count


def log_sync(
    conn: sqlite3.Connection,
    status: str,
    records_fetched: int,
    error_message: Optional[str] = None,
) -> None:
    """
    Log a sync operation to the audit trail.

    Args:
        conn: sqlite3.Connection
        status: Sync status (e.g., "success", "failure", "partial")
        records_fetched: Number of records fetched in this sync
        error_message: Error message if sync failed (optional)
    """
    cursor = conn.cursor()

    cursor.execute(
        """
    INSERT INTO sync_log (status, records_fetched, error_message)
    VALUES (?, ?, ?)
    """,
        (status, records_fetched, error_message),
    )

    conn.commit()


def set_plan_limits(
    conn: sqlite3.Connection,
    effective_date: str,
    daily_token_limit: Optional[int] = None,
    daily_cost_limit_cents: Optional[int] = None,
    weekly_token_limit: Optional[int] = None,
    weekly_cost_limit_cents: Optional[int] = None,
    notes: Optional[str] = None,
) -> None:
    """
    Set or update plan limits for a given effective date.

    Uses UPSERT (INSERT OR REPLACE) to handle updates.

    Args:
        conn: sqlite3.Connection
        effective_date: ISO date string (YYYY-MM-DD)
        daily_token_limit: Daily token limit (optional)
        daily_cost_limit_cents: Daily cost limit in cents (optional)
        weekly_token_limit: Weekly token limit (optional)
        weekly_cost_limit_cents: Weekly cost limit in cents (optional)
        notes: Notes or description of the plan (optional)
    """
    cursor = conn.cursor()

    cursor.execute(
        """
    INSERT OR REPLACE INTO plan_limits (
        effective_date,
        daily_token_limit,
        daily_cost_limit_cents,
        weekly_token_limit,
        weekly_cost_limit_cents,
        notes
    ) VALUES (?, ?, ?, ?, ?, ?)
    """,
        (
            effective_date,
            daily_token_limit,
            daily_cost_limit_cents,
            weekly_token_limit,
            weekly_cost_limit_cents,
            notes,
        ),
    )

    conn.commit()


def get_plan_limits(conn: sqlite3.Connection, date: str) -> Optional[dict]:
    """
    Get plan limits effective for a given date.

    Returns the most recent plan_limits row where effective_date <= date.

    Args:
        conn: sqlite3.Connection
        date: ISO date string (YYYY-MM-DD)

    Returns:
        dict with keys: daily_token_limit, daily_cost_limit_cents, weekly_token_limit,
        weekly_cost_limit_cents, notes, effective_date. Returns None if no limits found.
    """
    cursor = conn.cursor()

    cursor.execute(
        """
    SELECT
        daily_token_limit,
        daily_cost_limit_cents,
        weekly_token_limit,
        weekly_cost_limit_cents,
        notes,
        effective_date
    FROM plan_limits
    WHERE effective_date <= ?
    ORDER BY effective_date DESC
    LIMIT 1
    """,
        (date,),
    )

    row = cursor.fetchone()

    if row is None:
        return None

    return {
        "daily_token_limit": row[0],
        "daily_cost_limit_cents": row[1],
        "weekly_token_limit": row[2],
        "weekly_cost_limit_cents": row[3],
        "notes": row[4],
        "effective_date": row[5],
    }


def get_daily_totals(conn: sqlite3.Connection, date: str) -> dict:
    """
    Get aggregated usage and cost totals for a single day.

    Sums all token types from usage_records and joins with cost_records
    for the specified date.

    Args:
        conn: sqlite3.Connection
        date: ISO date string (YYYY-MM-DD)

    Returns:
        dict with keys:
        - total_tokens: Sum of all token types
        - total_cost_cents: Sum of all costs
    """
    cursor = conn.cursor()

    # Query usage_records for the given date and sum all token types
    cursor.execute(
        """
    SELECT
        COALESCE(SUM(
            uncached_input_tokens +
            cache_read_input_tokens +
            cache_creation_1h_tokens +
            cache_creation_5m_tokens +
            output_tokens
        ), 0)
    FROM usage_records
    WHERE DATE(bucket_start) = ?
    """,
        (date,),
    )

    total_tokens = cursor.fetchone()[0] or 0

    # Query cost_records for the given date and sum all costs
    cursor.execute(
        """
    SELECT COALESCE(SUM(amount_cents), 0)
    FROM cost_records
    WHERE DATE(bucket_start) = ?
    """,
        (date,),
    )

    total_cost_cents = cursor.fetchone()[0] or 0

    return {
        "total_tokens": int(total_tokens),
        "total_cost_cents": total_cost_cents,
    }


def get_weekly_totals(conn: sqlite3.Connection, date: str) -> dict:
    """
    Get aggregated usage and cost totals for the ISO week containing the given date.

    Sums all token types from usage_records and joins with cost_records
    for the entire ISO week.

    Args:
        conn: sqlite3.Connection
        date: ISO date string (YYYY-MM-DD)

    Returns:
        dict with keys:
        - total_tokens: Sum of all token types
        - total_cost_cents: Sum of all costs
    """
    cursor = conn.cursor()

    # Calculate ISO week start and end from the given date
    # strftime('%W') gives week number (0-53), but we use '%j' to compute ISO week properly
    # SQLite's CAST to week number uses %W which starts on Monday, day 1 is 1
    # To get ISO week properly, we use: strftime('%Y-W%W-%w', date)
    # But simpler: use DATE(date, 'weekday 1', '-7 days') to get Monday of week
    # Then add 6 days to get Sunday

    cursor.execute(
        """
    SELECT
        COALESCE(SUM(
            uncached_input_tokens +
            cache_read_input_tokens +
            cache_creation_1h_tokens +
            cache_creation_5m_tokens +
            output_tokens
        ), 0)
    FROM usage_records
    WHERE DATE(bucket_start) BETWEEN
        DATE(?, 'weekday 1', '-7 days') AND
        DATE(?, 'weekday 1', '-1 days')
    """,
        (date, date),
    )

    total_tokens = cursor.fetchone()[0] or 0

    # Query cost_records for the same ISO week
    cursor.execute(
        """
    SELECT COALESCE(SUM(amount_cents), 0)
    FROM cost_records
    WHERE DATE(bucket_start) BETWEEN
        DATE(?, 'weekday 1', '-7 days') AND
        DATE(?, 'weekday 1', '-1 days')
    """,
        (date, date),
    )

    total_cost_cents = cursor.fetchone()[0] or 0

    return {
        "total_tokens": int(total_tokens),
        "total_cost_cents": total_cost_cents,
    }
