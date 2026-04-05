"""
Tests for the collect module (src/collect.py).

Tests cover:
- Argument parsing
- CLI mode handling (collect, report, set-limits)
- Environment variable defaults
- API key validation
- Dry-run mode
- Error handling
"""

import sqlite3
import pytest
import os
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta

from src.collect import (
    _parse_args,
    _run_set_limits,
    _run_report,
    _run_collect,
    main,
)
from src.db import init_db, upsert_usage_records, upsert_cost_records


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


class TestParseArgs:
    """Tests for _parse_args function."""

    def test_parse_args_defaults(self):
        """Test default argument values."""
        args = _parse_args([])
        assert args.lookback == 25
        assert args.bucket_width == "1h"
        assert args.dry_run is False
        assert args.set_limits is False
        assert args.report is None

    def test_parse_args_custom_lookback(self):
        """Test custom lookback value."""
        args = _parse_args(["--lookback", "48"])
        assert args.lookback == 48

    def test_parse_args_custom_bucket_width(self):
        """Test custom bucket width."""
        args = _parse_args(["--bucket-width", "1d"])
        assert args.bucket_width == "1d"

    def test_parse_args_dry_run(self):
        """Test --dry-run flag."""
        args = _parse_args(["--dry-run"])
        assert args.dry_run is True

    def test_parse_args_set_limits_with_values(self):
        """Test --set-limits with limit values."""
        args = _parse_args(
            [
                "--set-limits",
                "--daily-token-limit",
                "100000",
                "--daily-cost-limit",
                "5000",
            ]
        )
        assert args.set_limits is True
        assert args.daily_token_limit == 100000
        assert args.daily_cost_limit == 5000

    def test_parse_args_report_type(self):
        """Test --report flag with type."""
        args = _parse_args(["--report", "weekly"])
        assert args.report == "weekly"

    def test_parse_args_env_var_db_path(self):
        """Test that USAGE_DB_PATH env var is used as default."""
        with patch.dict(os.environ, {"USAGE_DB_PATH": "/custom/path.db"}):
            args = _parse_args([])
            assert args.db == "/custom/path.db"

    def test_parse_args_env_var_lookback(self):
        """Test that LOOKBACK_HOURS env var is used as default."""
        with patch.dict(os.environ, {"LOOKBACK_HOURS": "48"}):
            args = _parse_args([])
            assert args.lookback == 48

    def test_parse_args_flag_overrides_env_var(self):
        """Test that command-line flags override env vars."""
        with patch.dict(os.environ, {"LOOKBACK_HOURS": "48"}):
            args = _parse_args(["--lookback", "12"])
            assert args.lookback == 12


class TestRunSetLimits:
    """Tests for _run_set_limits function."""

    def test_run_set_limits_sets_daily_token_limit(self, db_conn):
        """Test that _run_set_limits sets daily token limit."""
        args = MagicMock()
        args.daily_token_limit = 100000
        args.daily_cost_limit = None
        args.weekly_token_limit = None
        args.weekly_cost_limit = None

        result = _run_set_limits(db_conn, args)

        assert result == 0

        # Verify the limit was set
        cursor = db_conn.cursor()
        cursor.execute("SELECT daily_token_limit FROM plan_limits LIMIT 1")
        limit = cursor.fetchone()
        assert limit is not None
        assert limit[0] == 100000

    def test_run_set_limits_returns_zero(self, db_conn):
        """Test that _run_set_limits returns 0 on success."""
        args = MagicMock()
        args.daily_token_limit = 50000
        args.daily_cost_limit = None
        args.weekly_token_limit = None
        args.weekly_cost_limit = None

        result = _run_set_limits(db_conn, args)
        assert result == 0


class TestRunReport:
    """Tests for _run_report function."""

    def test_run_report_weekly(self, db_conn, capsys):
        """Test _run_report with weekly report type."""
        # Insert some test data
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

        result = _run_report(db_conn, "weekly")

        assert result == 0
        captured = capsys.readouterr()
        assert len(captured.out) > 0

    def test_run_report_daily(self, db_conn, capsys):
        """Test _run_report with daily report type."""
        result = _run_report(db_conn, "daily")
        assert result == 0
        captured = capsys.readouterr()
        assert len(captured.out) > 0

    def test_run_report_overage(self, db_conn, capsys):
        """Test _run_report with overage report type."""
        result = _run_report(db_conn, "overage")
        assert result == 0
        captured = capsys.readouterr()
        assert len(captured.out) > 0

    def test_run_report_returns_zero(self, db_conn):
        """Test that _run_report returns 0 on success."""
        result = _run_report(db_conn, "weekly")
        assert result == 0


class TestRunCollect:
    """Tests for _run_collect function."""

    @patch("src.collect.fetch_usage")
    @patch("src.collect.fetch_costs")
    def test_run_collect_missing_api_key(self, mock_costs, mock_usage, db_conn):
        """Test that _run_collect exits with code 1 when API key is missing."""
        args = MagicMock()
        args.dry_run = False

        with patch.dict(os.environ, {}, clear=True):
            # Ensure ANTHROPIC_ADMIN_API_KEY is not set
            result = _run_collect(db_conn, args)
            assert result == 1

    @patch("src.collect.fetch_usage")
    @patch("src.collect.fetch_costs")
    def test_run_collect_dry_run(self, mock_costs, mock_usage, db_conn, capsys):
        """Test that _run_collect with --dry-run doesn't write to DB."""
        args = MagicMock()
        args.dry_run = True
        args.lookback = 25
        args.bucket_width = "1h"

        # Mock API responses
        mock_usage.return_value = [
            {
                "bucket_start": "2024-01-01T00:00:00Z",
                "bucket_end": "2024-01-01T01:00:00Z",
                "model": "claude-3",
                "uncached_input_tokens": 1000,
            }
        ]
        mock_costs.return_value = [
            {
                "bucket_start": "2024-01-01T00:00:00Z",
                "bucket_end": "2024-01-01T01:00:00Z",
                "model": "claude-3",
                "amount": 1.00,
            }
        ]

        with patch.dict(os.environ, {"ANTHROPIC_ADMIN_API_KEY": "test-key"}):
            result = _run_collect(db_conn, args)

        # Should return 0 (success) in dry-run mode
        assert result == 0

        # Verify no data was written to DB
        cursor = db_conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM usage_records")
        count = cursor.fetchone()[0]
        assert count == 0

    @patch("src.collect.fetch_usage")
    @patch("src.collect.fetch_costs")
    def test_run_collect_success(self, mock_costs, mock_usage, db_conn):
        """Test successful collect operation."""
        args = MagicMock()
        args.dry_run = False
        args.lookback = 25
        args.bucket_width = "1h"

        # Mock API responses
        mock_usage.return_value = [
            {
                "bucket_start": "2024-01-01T00:00:00Z",
                "bucket_end": "2024-01-01T01:00:00Z",
                "model": "claude-3",
                "uncached_input_tokens": 1000,
            }
        ]
        mock_costs.return_value = [
            {
                "bucket_start": "2024-01-01T00:00:00Z",
                "bucket_end": "2024-01-01T01:00:00Z",
                "model": "claude-3",
                "amount": 1.00,
            }
        ]

        with patch.dict(os.environ, {"ANTHROPIC_ADMIN_API_KEY": "test-key"}):
            result = _run_collect(db_conn, args)

        # Should return 0 on success
        assert result == 0

        # Verify data was written to DB
        cursor = db_conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM usage_records")
        usage_count = cursor.fetchone()[0]
        assert usage_count == 1

        cursor.execute("SELECT COUNT(*) FROM cost_records")
        cost_count = cursor.fetchone()[0]
        assert cost_count == 1


class TestMain:
    """Tests for main function."""

    def test_main_with_report_flag(self, capsys):
        """Test main with --report flag."""
        result = main(["--report", "weekly"])
        assert result == 0
        captured = capsys.readouterr()
        # Should output the report
        assert len(captured.out) > 0

    @patch("src.collect.fetch_usage")
    @patch("src.collect.fetch_costs")
    def test_main_with_missing_api_key(self, mock_costs, mock_usage):
        """Test main exits with code 1 when API key is missing."""
        with patch.dict(os.environ, {}, clear=True):
            result = main([])
            assert result == 1

    def test_main_help_flag(self, capsys):
        """Test main with --help flag."""
        # --help causes sys.exit(0) in argparse, which we should catch
        with pytest.raises(SystemExit) as exc_info:
            main(["--help"])
        assert exc_info.value.code == 0

    def test_main_set_limits_mode(self):
        """Test main in set-limits mode."""
        result = main(
            [
                "--set-limits",
                "--daily-token-limit",
                "100000",
            ]
        )
        assert result == 0
