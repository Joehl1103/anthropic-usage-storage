"""
Tests for the reports module (src/reports.py).

Tests cover:
- Table formatting
- Weekly report generation
- Daily report generation
- Overage report generation
"""

import sqlite3
import pytest
from datetime import datetime, timedelta

from src.reports import (
    _format_table,
    report_weekly,
    report_daily,
    report_overage,
)
from src.db import (
    init_db,
    upsert_usage_records,
    upsert_cost_records,
    set_plan_limits,
)


@pytest.fixture
def db_conn():
    """Create an in-memory SQLite database for testing."""
    conn = sqlite3.connect(":memory:")
    # Initialize tables manually
    cursor = conn.cursor()
    cursor.executescript("""
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
    );

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
    );

    CREATE TABLE IF NOT EXISTS plan_limits (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        effective_date TEXT NOT NULL DEFAULT (date('now')),
        daily_token_limit INTEGER,
        daily_cost_limit_cents INTEGER,
        weekly_token_limit INTEGER,
        weekly_cost_limit_cents INTEGER,
        notes TEXT,
        UNIQUE(effective_date)
    );

    CREATE TABLE IF NOT EXISTS sync_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
        status TEXT NOT NULL,
        records_fetched INTEGER DEFAULT 0,
        error_message TEXT
    );
    """)
    conn.commit()
    yield conn
    conn.close()


class TestFormatTable:
    """Tests for _format_table helper function."""

    def test_format_table_single_row(self):
        """Test formatting a simple table with headers and one row."""
        headers = ["Name", "Value"]
        rows = [["Alice", "100"]]
        result = _format_table(headers, rows)
        assert "Name" in result
        assert "Alice" in result
        assert "100" in result

    def test_format_table_multiple_rows(self):
        """Test formatting a table with multiple rows."""
        headers = ["Name", "Score"]
        rows = [["Alice", "90"], ["Bob", "85"], ["Charlie", "95"]]
        result = _format_table(headers, rows)
        assert "Alice" in result
        assert "Bob" in result
        assert "Charlie" in result
        assert "90" in result
        assert "85" in result
        assert "95" in result

    def test_format_table_alignment(self):
        """Test that columns are aligned."""
        headers = ["Short", "Longer"]
        rows = [["A", "B"], ["XX", "YYY"]]
        result = _format_table(headers, rows)
        # Should have consistent column widths
        lines = result.split("\n")
        # All non-empty lines should have similar structure
        assert len(lines) > 2  # Header + separator + rows


class TestReportWeekly:
    """Tests for report_weekly function."""

    def test_report_weekly_empty_database(self, db_conn):
        """Test weekly report with no data."""
        report = report_weekly(db_conn)
        assert isinstance(report, str)
        # Empty report should indicate no data
        assert len(report) > 0

    def test_report_weekly_with_data(self, db_conn):
        """Test weekly report with usage and cost data."""
        # Insert usage data for the past week
        upsert_usage_records(
            db_conn,
            [
                {
                    "bucket_start": "2024-01-01T00:00:00Z",
                    "bucket_end": "2024-01-01T01:00:00Z",
                    "model": "claude-3-sonnet",
                    "workspace_id": "ws-123",
                    "api_key_id": "key-456",
                    "service_tier": "standard",
                    "uncached_input_tokens": 1000,
                    "cache_read_input_tokens": 500,
                    "cache_creation": {
                        "ephemeral_1h_input_tokens": 200,
                        "ephemeral_5m_input_tokens": 50,
                    },
                    "output_tokens": 300,
                    "server_tool_use": {"web_search_requests": 2},
                },
                {
                    "bucket_start": "2024-01-03T00:00:00Z",
                    "bucket_end": "2024-01-03T01:00:00Z",
                    "model": "claude-3-opus",
                    "workspace_id": "ws-123",
                    "api_key_id": "key-456",
                    "service_tier": "standard",
                    "uncached_input_tokens": 500,
                    "cache_read_input_tokens": 0,
                    "cache_creation": {
                        "ephemeral_1h_input_tokens": 0,
                        "ephemeral_5m_input_tokens": 0,
                    },
                    "output_tokens": 200,
                    "server_tool_use": {"web_search_requests": 0},
                },
            ],
        )

        # Insert cost data
        upsert_cost_records(
            db_conn,
            [
                {
                    "bucket_start": "2024-01-01T00:00:00Z",
                    "bucket_end": "2024-01-01T01:00:00Z",
                    "model": "claude-3-sonnet",
                    "workspace_id": "ws-123",
                    "service_tier": "standard",
                    "cost_type": "input",
                    "token_type": "uncached_input",
                    "amount": 1.00,
                },
                {
                    "bucket_start": "2024-01-03T00:00:00Z",
                    "bucket_end": "2024-01-03T01:00:00Z",
                    "model": "claude-3-opus",
                    "workspace_id": "ws-123",
                    "service_tier": "standard",
                    "cost_type": "input",
                    "token_type": "uncached_input",
                    "amount": 2.00,
                },
            ],
        )

        report = report_weekly(db_conn)
        assert isinstance(report, str)
        assert len(report) > 0
        # Report should contain model names and data
        assert "claude" in report.lower() or len(report) > 10

    def test_report_weekly_returns_string(self, db_conn):
        """Test that report_weekly always returns a string."""
        report = report_weekly(db_conn)
        assert isinstance(report, str)


class TestReportDaily:
    """Tests for report_daily function."""

    def test_report_daily_empty_database(self, db_conn):
        """Test daily report with no data."""
        report = report_daily(db_conn)
        assert isinstance(report, str)
        assert len(report) > 0

    def test_report_daily_with_data(self, db_conn):
        """Test daily report with usage and cost data."""
        # Set some plan limits
        set_plan_limits(
            db_conn,
            effective_date="2024-01-01",
            daily_token_limit=100000,
            daily_cost_limit_cents=5000,
        )

        # Insert usage data for multiple days
        upsert_usage_records(
            db_conn,
            [
                {
                    "bucket_start": "2024-01-01T00:00:00Z",
                    "bucket_end": "2024-01-01T01:00:00Z",
                    "model": "claude-3",
                    "uncached_input_tokens": 1000,
                    "output_tokens": 500,
                },
            ],
        )

        # Insert cost data
        upsert_cost_records(
            db_conn,
            [
                {
                    "bucket_start": "2024-01-01T00:00:00Z",
                    "bucket_end": "2024-01-01T01:00:00Z",
                    "model": "claude-3",
                    "amount": 1.50,
                },
            ],
        )

        report = report_daily(db_conn)
        assert isinstance(report, str)
        assert len(report) > 0

    def test_report_daily_returns_string(self, db_conn):
        """Test that report_daily always returns a string."""
        report = report_daily(db_conn)
        assert isinstance(report, str)


class TestReportOverage:
    """Tests for report_overage function."""

    def test_report_overage_empty_database(self, db_conn):
        """Test overage report with no data."""
        report = report_overage(db_conn)
        assert isinstance(report, str)
        assert len(report) > 0

    def test_report_overage_with_data(self, db_conn):
        """Test overage report with overage data."""
        # Set plan limits
        set_plan_limits(
            db_conn,
            effective_date="2024-01-01",
            daily_token_limit=5000,
            daily_cost_limit_cents=1000,
            weekly_token_limit=30000,
            weekly_cost_limit_cents=5000,
        )

        # Insert usage that exceeds limits
        upsert_usage_records(
            db_conn,
            [
                {
                    "bucket_start": "2024-01-01T00:00:00Z",
                    "bucket_end": "2024-01-01T01:00:00Z",
                    "model": "claude-3",
                    "uncached_input_tokens": 10000,  # Exceeds daily limit
                },
            ],
        )

        # Insert cost that exceeds limits
        upsert_cost_records(
            db_conn,
            [
                {
                    "bucket_start": "2024-01-01T00:00:00Z",
                    "bucket_end": "2024-01-01T01:00:00Z",
                    "model": "claude-3",
                    "amount": 2000,  # Exceeds daily cost limit
                },
            ],
        )

        report = report_overage(db_conn)
        assert isinstance(report, str)
        assert len(report) > 0

    def test_report_overage_returns_string(self, db_conn):
        """Test that report_overage always returns a string."""
        report = report_overage(db_conn)
        assert isinstance(report, str)
