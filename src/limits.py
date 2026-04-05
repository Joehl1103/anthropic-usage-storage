"""
Limit checking and warning generation for usage and cost metrics.

This module provides functions to:
- Check actual usage/cost against configured limits
- Generate warning messages (WARNING and CRITICAL levels)
- Print warnings to stderr in a standardized format
"""

import sys
import sqlite3
from typing import Optional

from src.db import get_plan_limits, get_daily_totals, get_weekly_totals


def _check_threshold(actual: int | float, limit: int | float, label: str) -> list[dict]:
    """
    Compare actual usage against a limit and generate warning(s) if needed.

    Returns warnings based on percentage of limit consumed:
    - 0-79%: No warning
    - 80-99%: WARNING level
    - 100%+: CRITICAL level

    Args:
        actual: The actual usage/cost value
        limit: The limit threshold
        label: Human-readable label for the metric (e.g., "Daily Tokens")

    Returns:
        A list of warning dicts. Each dict has:
        - "level": "WARNING" or "CRITICAL"
        - "message": Formatted message with actual, limit, and percentage
    """
    if limit <= 0:
        return []

    # Calculate percentage of limit
    percentage = (actual / limit) * 100

    warnings = []

    # Check thresholds in order of priority (critical first)
    if percentage >= 100:
        message = f"{label}: {actual} / {limit} (100%+)"
        warnings.append({"level": "CRITICAL", "message": message})
    elif percentage >= 80:
        message = f"{label}: {actual} / {limit} ({int(percentage)}%)"
        warnings.append({"level": "WARNING", "message": message})

    return warnings


def check_limits(conn: sqlite3.Connection, date: Optional[str] = None) -> list[dict]:
    """
    Check all configured limits for a given date and return warnings.

    Retrieves plan limits effective for the date, then fetches daily and weekly
    totals from the database. Compares each limit against actuals using
    _check_threshold and returns all generated warnings.

    Args:
        conn: sqlite3.Connection
        date: ISO date string (YYYY-MM-DD). Defaults to today if not provided.

    Returns:
        A list of warning dicts (empty if no limits configured or no overages).
    """
    from datetime import date as dateobj

    # Default to today if no date provided
    if date is None:
        date = dateobj.today().isoformat()

    # Get plan limits for this date
    limits = get_plan_limits(conn, date)

    # If no limits are configured, return empty list
    if limits is None:
        return []

    warnings = []

    # Get daily and weekly totals
    daily = get_daily_totals(conn, date)
    weekly = get_weekly_totals(conn, date)

    # Check daily token limit
    if limits.get("daily_token_limit"):
        daily_token_warnings = _check_threshold(
            actual=daily["total_tokens"],
            limit=limits["daily_token_limit"],
            label="Daily tokens",
        )
        warnings.extend(daily_token_warnings)

    # Check daily cost limit
    if limits.get("daily_cost_limit_cents"):
        daily_cost_warnings = _check_threshold(
            actual=daily["total_cost_cents"],
            limit=limits["daily_cost_limit_cents"],
            label="Daily cost",
        )
        warnings.extend(daily_cost_warnings)

    # Check weekly token limit
    if limits.get("weekly_token_limit"):
        weekly_token_warnings = _check_threshold(
            actual=weekly["total_tokens"],
            limit=limits["weekly_token_limit"],
            label="Weekly tokens",
        )
        warnings.extend(weekly_token_warnings)

    # Check weekly cost limit
    if limits.get("weekly_cost_limit_cents"):
        weekly_cost_warnings = _check_threshold(
            actual=weekly["total_cost_cents"],
            limit=limits["weekly_cost_limit_cents"],
            label="Weekly cost",
        )
        warnings.extend(weekly_cost_warnings)

    return warnings


def print_warnings(warnings: list[dict]) -> None:
    """
    Print warnings to stderr in a standardized format.

    Each warning is printed on its own line with the format:
    [LEVEL] message

    Args:
        warnings: List of warning dicts with "level" and "message" keys
    """
    for warning in warnings:
        level = warning["level"]
        message = warning["message"]
        sys.stderr.write(f"[{level}] {message}\n")
