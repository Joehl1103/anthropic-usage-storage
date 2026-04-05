#!/usr/bin/env python3
"""
Anthropic Usage Stats Collector — CLI entry point.

Fetches usage and cost data from the Anthropic Admin API
and stores it in a local SQLite database.

Supports multiple modes:
- Collection: Fetch and store data (default)
- Reporting: Generate usage, cost, and overage reports
- Limits: Set plan limits for tracking
"""

import argparse
import os
import sys
from datetime import date

from src.db import (
    init_db,
    upsert_usage_records,
    upsert_cost_records,
    log_sync,
    set_plan_limits,
)
from src.api import fetch_usage, fetch_costs, compute_time_window
from src.limits import check_limits, print_warnings
from src.reports import report_weekly, report_daily, report_overage


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """
    Parse and return command-line arguments.

    Reads defaults from environment variables if available:
    - USAGE_DB_PATH: Default database path
    - LOOKBACK_HOURS: Default lookback window

    Args:
        argv: Optional list of arguments. If None, uses sys.argv[1:]

    Returns:
        argparse.Namespace with all parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="Fetch and manage Anthropic usage statistics"
    )

    # General options
    parser.add_argument(
        "--db",
        default=os.getenv("USAGE_DB_PATH", "./usage.db"),
        help="SQLite database path (default: ./usage.db or $USAGE_DB_PATH)",
    )

    parser.add_argument(
        "--lookback",
        type=int,
        default=int(os.getenv("LOOKBACK_HOURS", "25")),
        help="Hours of history to fetch (default: 25)",
    )

    parser.add_argument(
        "--bucket-width",
        choices=["1m", "1h", "1d"],
        default="1h",
        help="Time granularity: 1m, 1h, 1d (default: 1h)",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and print data without writing to DB",
    )

    # Set limits mode
    parser.add_argument(
        "--set-limits",
        action="store_true",
        help="Set plan limits",
    )

    parser.add_argument(
        "--daily-token-limit",
        type=int,
        help="Daily token limit",
    )

    parser.add_argument(
        "--daily-cost-limit",
        type=int,
        help="Daily cost limit in cents",
    )

    parser.add_argument(
        "--weekly-token-limit",
        type=int,
        help="Weekly token limit",
    )

    parser.add_argument(
        "--weekly-cost-limit",
        type=int,
        help="Weekly cost limit in cents",
    )

    # Report mode
    parser.add_argument(
        "--report",
        choices=["weekly", "daily", "overage"],
        help="Generate a report: weekly, daily, or overage",
    )

    return parser.parse_args(argv)


def _run_set_limits(conn, args) -> int:
    """
    Handle --set-limits mode.

    Sets plan limits based on command-line arguments.

    Args:
        conn: sqlite3.Connection
        args: argparse.Namespace with limit values

    Returns:
        0 on success, non-zero on failure
    """
    today = date.today().isoformat()

    set_plan_limits(
        conn,
        effective_date=today,
        daily_token_limit=args.daily_token_limit,
        daily_cost_limit_cents=args.daily_cost_limit,
        weekly_token_limit=args.weekly_token_limit,
        weekly_cost_limit_cents=args.weekly_cost_limit,
    )

    return 0


def _run_report(conn, report_type: str) -> int:
    """
    Handle --report mode.

    Generates and prints a report of the specified type.

    Args:
        conn: sqlite3.Connection
        report_type: Type of report ("weekly", "daily", "overage")

    Returns:
        0 on success
    """
    if report_type == "weekly":
        report = report_weekly(conn)
    elif report_type == "daily":
        report = report_daily(conn)
    elif report_type == "overage":
        report = report_overage(conn)
    else:
        return 1

    print(report)
    return 0


def _run_collect(conn, args) -> int:
    """
    Main collection flow.

    Fetches usage and cost data from the API, optionally stores to DB,
    and checks limits. Returns different exit codes based on limit violations.

    Args:
        conn: sqlite3.Connection
        args: argparse.Namespace with collection settings

    Returns:
        0 on success (no limit violations)
        1 on error (missing API key or other error)
        2 if limits were exceeded
    """
    # Read API key from environment
    api_key = os.getenv("ANTHROPIC_ADMIN_API_KEY")
    if not api_key:
        sys.stderr.write("Error: ANTHROPIC_ADMIN_API_KEY not set\n")
        return 1

    try:
        # Compute time window
        start, end = compute_time_window(args.lookback)

        # Fetch data from API
        usage_records = fetch_usage(api_key, start, end, bucket_width=args.bucket_width)
        cost_records = fetch_costs(api_key, start, end, bucket_width=args.bucket_width)

        # In dry-run mode, just print records and exit
        if args.dry_run:
            print("Usage Records:")
            for record in usage_records:
                print(f"  {record}")
            print("\nCost Records:")
            for record in cost_records:
                print(f"  {record}")
            return 0

        # Upsert records to DB
        usage_count = upsert_usage_records(conn, usage_records)
        cost_count = upsert_cost_records(conn, cost_records)

        # Check limits and print warnings
        today = date.today().isoformat()
        warnings = check_limits(conn, today)
        print_warnings(warnings)

        # Log the sync
        total_records = usage_count + cost_count
        log_sync(conn, status="success", records_fetched=total_records)

        # Return 2 if any CRITICAL warnings
        if any(w["level"] == "CRITICAL" for w in warnings):
            return 2

        return 0

    except Exception as e:
        sys.stderr.write(f"Error: {str(e)}\n")
        log_sync(conn, status="failure", records_fetched=0, error_message=str(e))
        return 1


def main(argv: list[str] | None = None) -> int:
    """
    Main entry point.

    Parses arguments, initializes DB, and dispatches to the appropriate handler
    (collect, report, or set-limits mode).

    Args:
        argv: Optional list of arguments. If None, uses sys.argv[1:]

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    args = _parse_args(argv)

    # Initialize database
    conn = init_db(args.db)

    try:
        # Dispatch to appropriate handler
        if args.set_limits:
            return _run_set_limits(conn, args)
        elif args.report:
            return _run_report(conn, args.report)
        else:
            return _run_collect(conn, args)

    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
