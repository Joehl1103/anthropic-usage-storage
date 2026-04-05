"""
Tests for the limits module (src/limits.py).

Tests cover:
- Threshold checking logic (under limit, warning, critical)
- Daily and weekly limit validation
- Warning message formatting and output
"""

import sqlite3
import pytest
import sys
from io import StringIO
from datetime import datetime, timedelta

from src.limits import (
    _check_threshold,
    check_limits,
    print_warnings,
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


class TestCheckThreshold:
    """Tests for _check_threshold function."""

    def test_check_threshold_under_80_percent(self):
        """Test that actual < 80% of limit returns no warnings."""
        warnings = _check_threshold(actual=500, limit=1000, label="Test")
        assert warnings == []

    def test_check_threshold_at_80_percent(self):
        """Test that actual at 80% of limit returns WARNING."""
        warnings = _check_threshold(actual=800, limit=1000, label="Tokens")
        assert len(warnings) == 1
        assert warnings[0]["level"] == "WARNING"
        assert "80" in warnings[0]["message"]
        assert "Tokens" in warnings[0]["message"]

    def test_check_threshold_at_85_percent(self):
        """Test that actual at 85% of limit returns WARNING."""
        warnings = _check_threshold(actual=850, limit=1000, label="Test Label")
        assert len(warnings) == 1
        assert warnings[0]["level"] == "WARNING"
        assert "85" in warnings[0]["message"]

    def test_check_threshold_at_100_percent(self):
        """Test that actual at 100% of limit returns CRITICAL."""
        warnings = _check_threshold(actual=1000, limit=1000, label="Cost")
        assert len(warnings) == 1
        assert warnings[0]["level"] == "CRITICAL"
        assert "100" in warnings[0]["message"]
        assert "Cost" in warnings[0]["message"]

    def test_check_threshold_over_100_percent(self):
        """Test that actual over 100% of limit returns CRITICAL."""
        warnings = _check_threshold(actual=1200, limit=1000, label="Weekly")
        assert len(warnings) == 1
        assert warnings[0]["level"] == "CRITICAL"
        assert "120" in warnings[0]["message"]

    def test_check_threshold_with_float_values(self):
        """Test that threshold checking works with float values."""
        warnings = _check_threshold(actual=50.5, limit=100.0, label="Costs")
        assert warnings == []

        warnings = _check_threshold(actual=85.0, limit=100.0, label="Costs")
        assert len(warnings) == 1
        assert warnings[0]["level"] == "WARNING"


class TestCheckLimits:
    """Tests for check_limits function."""

    def test_check_limits_no_limits_configured(self, db_conn):
        """Test that no limits returns empty list."""
        warnings = check_limits(db_conn, "2024-01-01")
        assert warnings == []

    def test_check_limits_with_daily_token_limit(self, db_conn):
        """Test checking daily token limit."""
        # Set limit to 1000 tokens per day
        set_plan_limits(
            db_conn,
            effective_date="2024-01-01",
            daily_token_limit=1000,
        )

        # Insert usage data: 900 tokens (80% warning)
        upsert_usage_records(
            db_conn,
            [
                {
                    "bucket_start": "2024-01-01T00:00:00Z",
                    "bucket_end": "2024-01-01T01:00:00Z",
                    "model": "claude-3",
                    "uncached_input_tokens": 900,
                }
            ],
        )

        warnings = check_limits(db_conn, "2024-01-01")
        assert len(warnings) == 1
        assert warnings[0]["level"] == "WARNING"
        assert "daily" in warnings[0]["message"].lower()

    def test_check_limits_with_daily_cost_limit_critical(self, db_conn):
        """Test checking daily cost limit with overage."""
        # Set daily cost limit to $10
        set_plan_limits(
            db_conn,
            effective_date="2024-01-01",
            daily_cost_limit_cents=1000,
        )

        # Insert cost data: $15 (150% overage)
        upsert_cost_records(
            db_conn,
            [
                {
                    "bucket_start": "2024-01-01T00:00:00Z",
                    "bucket_end": "2024-01-01T01:00:00Z",
                    "model": "claude-3",
                    "amount": 1500,
                }
            ],
        )

        warnings = check_limits(db_conn, "2024-01-01")
        assert len(warnings) == 1
        assert warnings[0]["level"] == "CRITICAL"

    def test_check_limits_with_weekly_token_limit(self, db_conn):
        """Test checking weekly token limit."""
        # Set weekly token limit to 5000
        set_plan_limits(
            db_conn,
            effective_date="2024-01-01",
            weekly_token_limit=5000,
        )

        # Insert usage data for the week: 4000 tokens (80% warning)
        # Use 2024-01-03 (Wednesday) which is in the week starting 2024-01-01
        upsert_usage_records(
            db_conn,
            [
                {
                    "bucket_start": "2024-01-03T00:00:00Z",
                    "bucket_end": "2024-01-03T01:00:00Z",
                    "model": "claude-3",
                    "uncached_input_tokens": 4000,
                }
            ],
        )

        # Check limits with a date in the same week (2024-01-03)
        warnings = check_limits(db_conn, "2024-01-03")
        assert len(warnings) == 1
        assert warnings[0]["level"] == "WARNING"
        assert "weekly" in warnings[0]["message"].lower()

    def test_check_limits_multiple_warnings(self, db_conn):
        """Test that multiple limit violations return multiple warnings."""
        # Set multiple limits
        set_plan_limits(
            db_conn,
            effective_date="2024-01-01",
            daily_token_limit=1000,
            daily_cost_limit_cents=1000,
            weekly_token_limit=5000,
            weekly_cost_limit_cents=5000,
        )

        # Insert data that exceeds daily limits
        upsert_usage_records(
            db_conn,
            [
                {
                    "bucket_start": "2024-01-01T00:00:00Z",
                    "bucket_end": "2024-01-01T01:00:00Z",
                    "model": "claude-3",
                    "uncached_input_tokens": 1000,
                }
            ],
        )

        upsert_cost_records(
            db_conn,
            [
                {
                    "bucket_start": "2024-01-01T00:00:00Z",
                    "bucket_end": "2024-01-01T01:00:00Z",
                    "model": "claude-3",
                    "amount": 1000,
                }
            ],
        )

        warnings = check_limits(db_conn, "2024-01-01")
        # Should have 2 warnings: daily tokens and daily cost (weekly limits not exceeded)
        assert len(warnings) == 2
        assert any(w["level"] == "CRITICAL" for w in warnings)
        assert any("daily" in w["message"].lower() for w in warnings)

    def test_check_limits_uses_provided_date(self, db_conn):
        """Test that check_limits uses the provided date."""
        # Set limits effective 2024-01-01
        set_plan_limits(
            db_conn,
            effective_date="2024-01-01",
            daily_token_limit=1000,
        )

        # Query for 2024-01-10 (same limits should apply)
        warnings = check_limits(db_conn, "2024-01-10")
        assert warnings == []  # No data for that date


class TestPrintWarnings:
    """Tests for print_warnings function."""

    def test_print_warnings_empty(self, capsys):
        """Test printing empty warning list."""
        print_warnings([])
        captured = capsys.readouterr()
        # Should print nothing to stderr
        assert captured.err == ""

    def test_print_warnings_single_warning(self, capsys):
        """Test printing a single warning."""
        warnings = [{"level": "WARNING", "message": "Usage at 85%"}]
        print_warnings(warnings)
        captured = capsys.readouterr()
        assert "[WARNING]" in captured.err
        assert "Usage at 85%" in captured.err

    def test_print_warnings_single_critical(self, capsys):
        """Test printing a critical warning."""
        warnings = [{"level": "CRITICAL", "message": "Limit exceeded: 1200 / 1000"}]
        print_warnings(warnings)
        captured = capsys.readouterr()
        assert "[CRITICAL]" in captured.err
        assert "Limit exceeded: 1200 / 1000" in captured.err

    def test_print_warnings_multiple(self, capsys):
        """Test printing multiple warnings."""
        warnings = [
            {"level": "WARNING", "message": "Daily tokens: 850 / 1000 (85%)"},
            {"level": "CRITICAL", "message": "Daily cost: 1500 / 1000 (150%)"},
        ]
        print_warnings(warnings)
        captured = capsys.readouterr()
        assert "[WARNING]" in captured.err
        assert "[CRITICAL]" in captured.err
        assert "Daily tokens" in captured.err
        assert "Daily cost" in captured.err

    def test_print_warnings_goes_to_stderr(self, capsys):
        """Test that warnings go to stderr, not stdout."""
        warnings = [{"level": "WARNING", "message": "Test"}]
        print_warnings(warnings)
        captured = capsys.readouterr()
        assert captured.out == ""
        assert "[WARNING]" in captured.err
